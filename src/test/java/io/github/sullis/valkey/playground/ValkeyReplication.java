package io.github.sullis.valkey.playground;

import glide.api.GlideClient;
import glide.api.models.configuration.BackoffStrategy;
import glide.api.models.configuration.GlideClientConfiguration;
import glide.api.models.configuration.NodeAddress;
import glide.api.models.configuration.ReadFrom;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import org.testcontainers.utility.DockerImageName;

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
 * <p>A test class that wants one version owns one of these as a static field annotated
 * {@code @RegisterExtension}, which leaves the lifecycle to JUnit: started once before the class
 * and stopped after it, including when the start itself fails partway. A class parameterized over
 * versions cannot use that -- a static extension field is set up once, before any parameter
 * exists -- so it calls {@link #start()} and {@link #close()} from its own
 * {@code @BeforeParameterizedClassInvocation} hooks instead. Either way the servers start once per
 * set of test methods rather than per method, because starting them costs seconds.
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

  private final DockerImageName image;

  private final int numReplicas;

  /**
   * One availability zone per node, primary first, or empty for nodes started without the
   * {@code availability-zone} setting at all. A node that reports no zone is in no zone as far as
   * a client is concerned, which is why the zones are fixed here at startup rather than set later.
   */
  private final List<String> availabilityZones;

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

  /**
   * AZ-aware clients, keyed by the strategy and client zone they were built for, so that a test
   * method asking for the same pair twice does not pay for a second native runtime. Also held in
   * {@link #clients}, which is what closes them.
   */
  private final Map<String, GlideClient> azClients = new HashMap<>();

  /**
   * Set by the first {@link #close()}, so that a second one is a no-op. A start that fails partway
   * closes what it built and then still meets its caller's own teardown, and closing the network
   * twice would throw.
   */
  private boolean closed;

  private ValkeyReplication(final DockerImageName image, final int numReplicas,
      final List<String> availabilityZones) {
    this.image = image;
    this.numReplicas = numReplicas;
    this.availabilityZones = availabilityZones;
  }

  /**
   * Declares the servers on the image every fixture shares; nothing starts until JUnit calls
   * {@link #beforeAll}.
   */
  static ValkeyReplication withReplicas(final int numReplicas) {
    return new ValkeyReplication(ValkeyImage.DEFAULT_VALKEY_IMAGE, numReplicas, List.of());
  }

  /**
   * As {@link #withReplicas}, but on a caller-supplied image -- for a test about one Valkey
   * version in particular, or a run pointed at a mirror of the upstream image.
   *
   * <p>It has to be a Valkey image or a rebuild of one, not a Redis image: the nodes are started
   * by running {@code valkey-server} by name and {@link #valkeyCli} shells out to
   * {@code valkey-cli}, neither of which a Redis image ships. Both wait strategies in
   * {@link #startContainers} also match on Valkey's own log text rather than on a port being
   * open, so an image that logs something else surfaces as a startup timeout rather than as a
   * clear error.
   */
  static ValkeyReplication withImage(final DockerImageName image, final int numReplicas) {
    return new ValkeyReplication(image, numReplicas, List.of());
  }

  /**
   * Declares one node per entry of {@code availabilityZones} -- the first is the primary's zone,
   * each one after it a replica's -- with every node started as if it ran in that zone. For a test
   * about an AZ-aware read strategy: GLIDE learns a node's zone by asking the node for its own
   * {@code availability-zone} setting, so a node started without one can never be matched by
   * affinity, and two nodes can be put in one zone by repeating it.
   *
   * <p>The zones are labels, not a claim about where anything runs -- every container here is on
   * one host. That is not a weakness of the fixture: a zone is only ever what the node reports and
   * what the client was told to prefer, so routing by it is exactly as testable on one host as it
   * would be across three real zones.
   *
   * <p>Requires a server that has the setting at all, which means Valkey 8.0 or newer.
   */
  static ValkeyReplication withAvailabilityZones(final List<String> availabilityZones) {
    if (availabilityZones.isEmpty()) {
      // The first zone is the primary's, and there is always a primary.
      throw new IllegalArgumentException("at least one zone is needed, for the primary");
    }
    return new ValkeyReplication(ValkeyImage.DEFAULT_VALKEY_IMAGE,
        availabilityZones.size() - 1, List.copyOf(availabilityZones));
  }

  @Override
  public void beforeAll(final ExtensionContext context) {
    start();
  }

  /**
   * Starts the servers, for a caller that drives the lifecycle itself rather than through
   * {@code @RegisterExtension} -- see the class comment. A start that fails partway tears down
   * whatever did come up before it throws, so the caller owes it no cleanup it is not already
   * doing.
   */
  void start() {
    try {
      startContainers();
    } catch (RuntimeException e) {
      // A failed start has no matching teardown call -- JUnit does not run afterAll for a failed
      // beforeAll -- so whatever did come up has to be stopped here rather than left running.
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
      if (!availabilityZones.isEmpty()) {
        // The zone the node reports to anyone who asks -- INFO SERVER, CONFIG GET, and the
        // CONFIG GET that GLIDE itself issues per connection to decide where an AZ-affinity read
        // may go.
        command.addAll(List.of("--availability-zone", availabilityZones.get(i)));
      }
      GenericContainer<?> container = new GenericContainer<>(image)
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
      // The image is logged because a class parameterized over versions runs this twice, and
      // surefire labels the two invocations [1] and [2] rather than by image.
      LOGGER.info("started container id={} role={} image={}", container.getContainerId(), role, image);
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
      primaryClient = newClient(ReadFrom.PRIMARY, null, primary());
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
          newClient(ReadFrom.PREFER_REPLICA, null, containers.toArray(new GenericContainer<?>[0]));
    }
    return replicaReadingClient;
  }

  /**
   * A client that routes its reads by availability zone: {@code clientAz} is the zone the client
   * claims to be in, and {@code readFrom} decides what "near" means -- see {@link ReadFrom} for
   * each strategy's fallback order. Writes still go to the primary wherever it is.
   *
   * <p>Handed every node's address, for the reason {@link #replicaReadingClient} is: a standalone
   * client resolves its primary from the address list and rejects one that holds only replicas.
   */
  GlideClient azAwareClient(final ReadFrom readFrom, final String clientAz) throws Exception {
    if (availabilityZones.isEmpty()) {
      throw new IllegalStateException("servers were started without availability zones");
    }
    String cacheKey = readFrom + "@" + clientAz;
    GlideClient cached = azClients.get(cacheKey);
    if (cached != null) {
      return cached;
    }
    GlideClient client =
        newClient(readFrom, clientAz, containers.toArray(new GenericContainer<?>[0]));
    azClients.put(cacheKey, client);
    return client;
  }

  /**
   * Builds a standalone client over the host-mapped addresses of the given containers. A
   * {@code clientAz} of null leaves the client in no zone, which is the only sensible thing for a
   * strategy that does not read one.
   */
  private GlideClient newClient(final ReadFrom readFrom, final String clientAz,
      final GenericContainer<?>... targets) throws Exception {
    List<NodeAddress> addresses = Arrays.stream(targets)
        .map(c -> NodeAddress.builder().host(c.getHost()).port(c.getFirstMappedPort()).build())
        .toList();
    LOGGER.info("connecting to {} readFrom={} clientAz={}", addresses, readFrom, clientAz);

    BackoffStrategy backoff = BackoffStrategy.builder().numOfRetries(3).factor(2).exponentBase(10).build();
    GlideClientConfiguration config = GlideClientConfiguration.builder()
        .addresses(addresses)
        .readFrom(readFrom)
        // Passed as-is, null included: a null zone is left out of the connection request, which
        // is what a strategy that does not read one wants.
        .clientAZ(clientAz)
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
   *
   * <p>Safe to call on a set that never started, and safe to call twice, so a caller driving the
   * lifecycle itself can tear down unconditionally.
   */
  void close() {
    if (closed) {
      return;
    }
    closed = true;
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
