package io.github.sullis.valkey.playground;

import glide.api.GlideClient;
import glide.api.models.configuration.BackoffStrategy;
import glide.api.models.configuration.GlideClientConfiguration;
import glide.api.models.configuration.NodeAddress;
import glide.api.models.configuration.ReadFrom;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;

import static io.github.sullis.valkey.playground.Futures.get;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * A running set of Valkey servers -- one primary plus {@code numReplicas} replicas, each in its own
 * container on a private Docker network -- together with the clients, keyspace and exec plumbing
 * needed to drive them.
 *
 * <p>This is replication, not cluster mode: replicas are attached with {@code --replicaof} and
 * every client here is a standalone {@link GlideClient}, so there are no hash slots and no
 * {@code CLUSTER} membership involved.
 *
 * <p>A test class owns one of these as a static field annotated {@code @RegisterExtension}, which
 * leaves the lifecycle to JUnit: started once before the class and stopped after it, including
 * when the start itself fails partway. Starting the servers costs seconds, so they are shared
 * across the methods of a class rather than rebuilt per method.
 */
final class ValkeyReplication implements BeforeAllCallback, AfterAllCallback {
  private static final Logger LOGGER = LoggerFactory.getLogger(ValkeyReplication.class);

  /**
   * Network alias for the primary. A replica must reach the primary over the Docker network,
   * so it cannot use the host/mapped-port pair returned by getHost()/getFirstMappedPort().
   */
  private static final String PRIMARY_ALIAS = "valkey-primary";

  /**
   * Every container gets its own network namespace, so the primary and its replicas can all
   * listen on the same port without colliding.
   */
  private static final int VALKEY_PORT = 6379;

  /** Bounds how long a node has to satisfy its wait strategy before its start() fails. */
  private static final Duration STARTUP_TIMEOUT = Duration.ofSeconds(15);

  /** Bounds {@link #awaitReplication}, which fails as a short acknowledgement count. */
  private static final Duration REPLICATION_TIMEOUT = Duration.ofSeconds(15);

  private final int numReplicas;

  private final Network network = Network.newNetwork();

  /**
   * Populated as containers start rather than once they all have: a container registered here
   * before its {@code start()} is one {@link #close()} can still stop if a later container in the
   * set never comes up.
   */
  private final List<GenericContainer<?>> containers = new ArrayList<>();

  /**
   * Clients are built on first use and kept for the life of the servers: each one costs a Glide
   * native runtime and its connections, which is not worth paying per test method. Tracked here so
   * that {@link #close()} closes whichever ones a test class actually asked for.
   */
  private final List<GlideClient> clients = new ArrayList<>();

  private GlideClient primaryClient;
  private GlideClient replicaReadingClient;

  private ValkeyReplication(final int numReplicas) {
    this.numReplicas = numReplicas;
  }

  /** Declares the servers; nothing starts until JUnit calls {@link #beforeAll}. */
  static ValkeyReplication withReplicas(final int numReplicas) {
    return new ValkeyReplication(numReplicas);
  }

  @Override
  public void beforeAll(final ExtensionContext context) {
    try {
      startContainers();
    } catch (RuntimeException e) {
      // JUnit does not call afterAll for a failed beforeAll, so whatever did come up has to be
      // torn down here rather than left running.
      close();
      throw e;
    }
  }

  @Override
  public void afterAll(final ExtensionContext context) {
    close();
  }

  /**
   * Starts the nodes in primary-first order: a replica's wait strategy blocks on its initial sync,
   * which cannot complete until the primary is accepting connections.
   */
  private void startContainers() {
    final int numContainers = 1 + numReplicas;
    for (int i = 0; i < numContainers; i++) {
      final boolean isPrimary = i == 0;
      // slf4j interleaves every container's output into one stream, so without a prefix per
      // container the only hint at which node logged a line is valkey's own M/S role character.
      final String role = isPrimary ? "primary" : "replica-" + (i - 1);
      List<String> command = new ArrayList<>(List.of("valkey-server", "--port", String.valueOf(VALKEY_PORT),
          // Without this the primary waits repl-diskless-sync-delay (5 seconds by default) before
          // forking for the replica's initial sync, which is dead time in every run.
          "--repl-diskless-sync-delay", "0",
          // Nothing here reads persisted data, so skip RDB snapshotting entirely.
          "--save", ""));
      GenericContainer<?> container = new GenericContainer<>(ValkeyImage.VALKEY)
          .withNetwork(network)
          .withExposedPorts(VALKEY_PORT)
          .withLogConsumer(new Slf4jLogConsumer(LOGGER).withPrefix(role))
          .withStartupTimeout(STARTUP_TIMEOUT);
      if (isPrimary) {
        // The default port-listening probe can succeed before the server is actually serving
        // commands, so wait for the line valkey logs once it is ready.
        container = container.withNetworkAliases(PRIMARY_ALIAS)
            .waitingFor(Wait.forLogMessage(".*Ready to accept connections.*\\n", 1));
      } else {
        // Replicate from the primary's in-network address, not its host-mapped port.
        command.addAll(List.of("--replicaof", PRIMARY_ALIAS, String.valueOf(VALKEY_PORT)));
        // Do not hand out the replica until it has finished its initial sync.
        container = container.waitingFor(Wait.forLogMessage(".*REPLICA sync: Finished with success.*\\n", 1));
      }
      container = container.withCommand(command.toArray(new String[0]));
      containers.add(container);
      container.start();
      LOGGER.info("started container id={} role={}", container.getContainerId(), role);
    }
  }

  GenericContainer<?> primary() {
    return containers.get(0);
  }

  GenericContainer<?> replica(final int index) {
    return containers.get(1 + index);
  }

  /** A client whose reads and writes both land on the primary. */
  GlideClient primaryClient() throws Exception {
    if (primaryClient == null) {
      primaryClient = newClient(ReadFrom.PRIMARY, primary());
    }
    return primaryClient;
  }

  /**
   * A client whose reads land on a replica while its writes still go to the primary, for asserting
   * replica-side state.
   *
   * <p>A standalone client always resolves a primary from its address list and rejects a list that
   * only contains replicas ("No primary node found"), so reading replica-side state means handing
   * it every address and asking for {@link ReadFrom#PREFER_REPLICA}: writes still go to the
   * primary, while reads -- INFO included -- are served by a replica.
   */
  GlideClient replicaReadingClient() throws Exception {
    if (numReplicas == 0) {
      throw new IllegalStateException("servers were started without replicas");
    }
    if (replicaReadingClient == null) {
      replicaReadingClient =
          newClient(ReadFrom.PREFER_REPLICA, containers.toArray(new GenericContainer<?>[0]));
    }
    return replicaReadingClient;
  }

  /** Builds a standalone client over the host-mapped addresses of the given containers. */
  private GlideClient newClient(final ReadFrom readFrom, final GenericContainer<?>... targets)
      throws Exception {
    List<NodeAddress> addresses = Arrays.stream(targets)
        .map(c -> NodeAddress.builder().host(c.getHost()).port(c.getFirstMappedPort()).build())
        .toList();
    LOGGER.info("connecting to {} readFrom={}", addresses, readFrom);

    BackoffStrategy backoff = BackoffStrategy.builder().numOfRetries(3).factor(2).exponentBase(10).build();
    GlideClientConfiguration config = GlideClientConfiguration.builder()
        .addresses(addresses)
        .readFrom(readFrom)
        .reconnectStrategy(backoff)
        .build();

    GlideClient client = get(GlideClient.createClient(config));
    clients.add(client);
    return client;
  }

  /**
   * Clears the keyspace, for a test that has to observe only the keys it wrote itself. Called by
   * that test rather than before every one, so that the assertion depending on an empty keyspace
   * sits next to the thing that empties it. FLUSHALL replicates, so this clears the replicas too
   * -- but only once they have applied it, hence the barrier.
   */
  void flushKeyspace() throws Exception {
    GlideClient client = primaryClient();
    get(client.flushall());
    awaitReplication(client);
  }

  /**
   * Blocks until every replica has acknowledged the writes already issued on {@code client}'s
   * connection, so that a replica-side read afterwards is a deterministic assertion instead of a
   * poll. WAIT returns the number of replicas that acknowledged, which is asserted here: a
   * timeout shows up as a short count rather than as a later, more confusing read failure.
   *
   * <p>{@code client} has to be the one whose writes are being waited on -- WAIT reports on the
   * offset of the connection it arrives on -- and it has to route to the primary, since a replica
   * has no replicas of its own to wait for.
   */
  void awaitReplication(final GlideClient client) throws Exception {
    if (numReplicas == 0) {
      return;
    }
    assertThat(get(client.wait(numReplicas, REPLICATION_TIMEOUT.toMillis())))
        .as("replicas that acknowledged the write")
        .isEqualTo((long) numReplicas);
  }

  /**
   * Runs valkey-cli inside a container. This is the only way to send a write to a replica: a
   * standalone client routes writes to whichever node it resolved as primary, and standalone mode
   * has no per-command routing (that is cluster-client only).
   *
   * <p>valkey-cli exits 0 even when the server replies with an error, so the exit code below only
   * confirms the process itself ran -- the reply has to be asserted on by the caller.
   */
  String valkeyCli(final GenericContainer<?> container, final String... args) throws Exception {
    List<String> command = new ArrayList<>(List.of("valkey-cli", "-p", String.valueOf(VALKEY_PORT)));
    command.addAll(List.of(args));
    Container.ExecResult result = container.execInContainer(command.toArray(new String[0]));
    assertThat(result.getExitCode()).as("valkey-cli process exit code").isZero();
    return result.getStdout();
  }

  /**
   * Closes the clients, stops every node and then the network they are attached to. Each step is
   * independent, and the network is closed either way: a leaked container or network outlives the
   * JVM, so one that refuses to go away must not take the others down with it.
   */
  private void close() {
    for (GlideClient client : clients) {
      try {
        client.close();
      } catch (Exception e) {
        // GlideClient.close() is declared to throw: a client that will not close cleanly must not
        // abort the container teardown below.
        LOGGER.warn("failed to close a client", e);
      }
    }
    try {
      // Replicas first, so that none is left reconnecting to a primary that is already gone.
      for (int i = containers.size() - 1; i >= 0; i--) {
        GenericContainer<?> container = containers.get(i);
        try {
          container.stop();
        } catch (RuntimeException e) {
          LOGGER.warn("failed to stop container id={}", container.getContainerId(), e);
        }
      }
    } finally {
      network.close();
    }
  }
}
