package io.github.sullis.valkey.playground;

import glide.api.models.GlideString;
import glide.api.models.commands.InfoOptions.Section;
import glide.api.models.configuration.BackoffStrategy;
import glide.api.models.configuration.ReadFrom;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import glide.api.GlideClient;
import glide.api.models.configuration.GlideClientConfiguration;
import glide.api.models.configuration.NodeAddress;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;


public class ContainerTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(ContainerTest.class);
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
  private static final Duration TIMEOUT = Duration.ofSeconds(15);

  private static Network network;
  private static List<GenericContainer<?>> containers;

  /**
   * Used for keyspace cleanup between tests. Tests that exercise client behaviour build their own
   * clients so that each can pick its own {@link ReadFrom}.
   */
  private static GlideClient adminClient;

  /**
   * Starts a primary plus {@code numReplicas} replicas, in that order: a replica's wait strategy
   * blocks on its initial sync, which cannot complete until the primary is accepting connections.
   */
  private static List<GenericContainer<?>> startValkeyContainers(final int numReplicas) {
    network = Network.newNetwork();
    List<GenericContainer<?>> cluster = new ArrayList<>();
    final int numContainers = 1 + numReplicas;
    for (int i = 0; i < numContainers; i++) {
      List<String> command = new ArrayList<>(List.of("valkey-server", "--port", String.valueOf(VALKEY_PORT),
          // Without this the primary waits repl-diskless-sync-delay (5 seconds by default) before
          // forking for the replica's initial sync, which is dead time in every run of this class.
          "--repl-diskless-sync-delay", "0",
          // Nothing here reads persisted data, so skip RDB snapshotting entirely.
          "--save", ""));
      GenericContainer<?> container = new GenericContainer<>(IMAGE)
          .withNetwork(network)
          .withExposedPorts(VALKEY_PORT)
          .withLogConsumer(new Slf4jLogConsumer(LOGGER))
          .withStartupTimeout(TIMEOUT);
      if (i == 0) {
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
      cluster.add(container);
      container.start();
      LOGGER.info("started container id={} role={}", container.getContainerId(), i == 0 ? "primary" : "replica");
    }
    return cluster;
  }

  private static GenericContainer<?> primary() {
    return containers.get(0);
  }

  private static GenericContainer<?> replica() {
    return containers.get(1);
  }

  /**
   * Awaits a Glide command, so that call sites read as the behaviour under test rather than as
   * future plumbing.
   */
  private static <T> T get(final CompletableFuture<T> future) throws Exception {
    return future.get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
  }

  /**
   * Builds a standalone client over the host-mapped addresses of the given containers.
   *
   * <p>A standalone client always resolves a primary from its address list and rejects a list that
   * only contains replicas ("No primary node found"), so reading replica-side state means handing
   * it both addresses and asking for {@link ReadFrom#PREFER_REPLICA}: writes still go to the
   * primary, while reads -- INFO included -- are served by the replica.
   */
  private static GlideClient newClient(final ReadFrom readFrom, final GenericContainer<?>... targets)
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

    LOGGER.info("client config: {}", config);
    return get(GlideClient.createClient(config));
  }

  private static String replicationInfo(final GlideClient client) throws Exception {
    return get(client.info(new Section[]{Section.REPLICATION}));
  }

  private static String role(final GlideClient client) throws Exception {
    Object[] response = (Object[]) get(client.customCommand(new String[]{"role"}));
    return response[0].toString();
  }

  /**
   * Runs valkey-cli inside a container. This is the only way to send a write to the replica: a
   * standalone client routes writes to whichever node it resolved as primary, and standalone mode
   * has no per-command routing (that is cluster-client only).
   *
   * <p>valkey-cli exits 0 even when the server replies with an error, so the exit code below only
   * confirms the process itself ran -- the reply has to be asserted on by the caller.
   */
  private static String valkeyCli(final GenericContainer<?> container, final String... args) throws Exception {
    List<String> command = new ArrayList<>(List.of("valkey-cli", "-p", String.valueOf(VALKEY_PORT)));
    command.addAll(List.of(args));
    Container.ExecResult result = container.execInContainer(command.toArray(new String[0]));
    assertThat(result.getExitCode()).as("valkey-cli process exit code").isZero();
    return result.getStdout();
  }

  @BeforeAll
  static void beforeAll() throws Exception {
    containers = startValkeyContainers(1);
    adminClient = newClient(ReadFrom.PRIMARY, primary());
  }

  @AfterAll
  static void afterAll() throws Exception {
    if (adminClient != null) {
      adminClient.close();
    }
    if (containers != null) {
      containers.forEach(GenericContainer::stop);
    }
    if (network != null) {
      network.close();
    }
  }

  /**
   * Tests in this class share one primary, so clear the keyspace between them rather than let each
   * test observe keys written by its predecessors. FLUSHALL replicates, so this clears the replica
   * too.
   */
  @BeforeEach
  void flushKeyspace() throws Exception {
    get(adminClient.flushall());
  }

  @Test
  void testValkeyClient() throws Exception {
    try (GlideClient client = newClient(ReadFrom.PRIMARY, primary())) {
      assertThat(get(client.ping("Hello world"))).isEqualTo("Hello world");

      String clientInfo = get(client.info());
      assertThat(clientInfo)
          .containsPattern("connected_clients:[1-9][0-9]*")
          .contains("server_name:valkey")
          .contains("role:master");

      assertThat(role(client)).isEqualTo("master");

      final String valuePrefix = "value-";

      Set<String> keys = new HashSet<>();
      for (int i = 0; i < 5; i++) {
        String key = UUID.randomUUID().toString();
        keys.add(key);
        get(client.set(key, valuePrefix + key));
      }

      for (String key : keys) {
        assertThat(get(client.get(key))).isEqualTo(valuePrefix + key);
      }

      // The keyspace was flushed before this test and only the keys above were written, so
      // RANDOMKEY has to draw from exactly that set.
      GlideString randomKeyBinary = get(client.randomKeyBinary());
      assertThat(randomKeyBinary).isNotNull();
      assertThat(randomKeyBinary.getString()).isIn(keys);
    }
  }

  @Test
  void testReplication() throws Exception {
    try (GlideClient primaryClient = newClient(ReadFrom.PRIMARY, primary());
         GlideClient replicaClient = newClient(ReadFrom.PREFER_REPLICA, primary(), replica())) {
      assertThat(replicationInfo(primaryClient))
          .contains("role:master")
          .contains("connected_slaves:1")
          .containsPattern("slave0:ip=.*,state=online");

      // Reads on replicaClient land on the replica, so this is the replica's own view.
      assertThat(replicationInfo(replicaClient))
          .contains("role:slave")
          .contains("master_link_status:up")
          .contains("slave_read_only:1");

      assertThat(role(replicaClient)).isEqualTo("slave");

      String key = UUID.randomUUID().toString();
      String value = "replicated-" + key;
      get(primaryClient.set(key, value));

      // Replication is asynchronous, so poll the replica until the write lands.
      await().atMost(TIMEOUT).untilAsserted(() ->
          assertThat(get(replicaClient.get(key))).isEqualTo(value));
    }
  }

  @Test
  void testReplicaRejectsWrites() throws Exception {
    assertThat(valkeyCli(replica(), "set", UUID.randomUUID().toString(), "nope"))
        .contains("READONLY");
  }
}
