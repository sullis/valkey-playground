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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import static io.github.sullis.valkey.playground.Futures.get;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * A running Valkey topology -- one primary plus {@code numReplicas} replicas, each in its own
 * container on a private Docker network -- together with the client, keyspace and exec plumbing
 * needed to drive it.
 *
 * <p>Owned by the test class that {@link #start started} it, which is expected to hold it in a
 * static field and {@link #close} it in {@code @AfterAll}: starting a topology costs seconds, so
 * it is shared across the methods of a class rather than rebuilt per method.
 */
final class ValkeyCluster implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(ValkeyCluster.class);
  private static final DockerImageName IMAGE = DockerImageName.parse("valkey/valkey:9.1.2");

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

  private final Network network = Network.newNetwork();

  /**
   * Populated as containers start rather than once they all have: a container registered here
   * before its {@code start()} is one {@link #close()} can still stop if a later container in the
   * cluster never comes up.
   */
  private final List<GenericContainer<?>> containers = new ArrayList<>();

  /**
   * Used for keyspace cleanup between tests. Tests that exercise client behaviour build their own
   * clients so that each can pick its own {@link ReadFrom}.
   */
  private GlideClient adminClient;

  private ValkeyCluster() {
  }

  /**
   * Starts a primary plus {@code numReplicas} replicas and connects the admin client. Nothing is
   * left running if any part of that fails.
   */
  static ValkeyCluster start(final int numReplicas) throws Exception {
    ValkeyCluster cluster = new ValkeyCluster();
    try {
      cluster.startContainers(numReplicas);
      cluster.adminClient = cluster.newClient(ReadFrom.PRIMARY, cluster.primary());
    } catch (Exception e) {
      cluster.close();
      throw e;
    }
    return cluster;
  }

  /**
   * Starts the nodes in primary-first order: a replica's wait strategy blocks on its initial sync,
   * which cannot complete until the primary is accepting connections.
   */
  private void startContainers(final int numReplicas) {
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
      GenericContainer<?> container = new GenericContainer<>(IMAGE)
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

  private int numReplicas() {
    return containers.size() - 1;
  }

  /** A client whose reads and writes both land on the primary. */
  GlideClient newPrimaryClient() throws Exception {
    return newClient(ReadFrom.PRIMARY, primary());
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
  GlideClient newReplicaReadingClient() throws Exception {
    if (numReplicas() == 0) {
      throw new IllegalStateException("cluster was started without replicas");
    }
    return newClient(ReadFrom.PREFER_REPLICA, containers.toArray(new GenericContainer<?>[0]));
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

    return get(GlideClient.createClient(config));
  }

  /**
   * Clears the keyspace, so that a test observes only the keys it wrote itself rather than those
   * of its predecessors in the same class. FLUSHALL replicates, so this clears the replicas too --
   * but only once they have applied it, hence the barrier.
   */
  void flushKeyspace() throws Exception {
    get(adminClient.flushall());
    awaitReplication(adminClient);
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
    final long expectedReplicas = numReplicas();
    if (expectedReplicas == 0) {
      return;
    }
    assertThat(get(client.wait(expectedReplicas, REPLICATION_TIMEOUT.toMillis())))
        .as("replicas that acknowledged the write")
        .isEqualTo(expectedReplicas);
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
   * Stops every node and then the network they are attached to. Each node is stopped
   * independently, and the network is closed either way: a leaked container or network outlives
   * the JVM, so one that refuses to go away must not take the others down with it.
   */
  @Override
  public void close() {
    try {
      if (adminClient != null) {
        adminClient.close();
      }
    } catch (Exception e) {
      // GlideClient.close() is declared to throw, but AutoCloseable.close() here is not: a client
      // that will not close cleanly must not abort the container teardown below.
      LOGGER.warn("failed to close the admin client", e);
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
