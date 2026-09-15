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
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
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
  private static final int PRIMARY_PORT = 6379;
  private static final Duration TIMEOUT = Duration.ofSeconds(15);

  private static final List<GenericContainer<?>> containers = createValkeyContainers(1, PRIMARY_PORT);

  private static List<GenericContainer<?>> createValkeyContainers(final int numReplicas, final int basePort) {
    Network network = Network.newNetwork();
    List<GenericContainer<?>> cluster = new ArrayList<>();
    int port = basePort;
    final int numContainers = 1 + numReplicas;
    for (int i = 0; i < numContainers; i++) {
      List<String> command = new ArrayList<>(List.of("valkey-server", "--port", String.valueOf(port),
          // Without this the primary waits repl-diskless-sync-delay (5 seconds by default) before
          // forking for the replica's initial sync, which is dead time in every run of this class.
          "--repl-diskless-sync-delay", "0",
          // Nothing here reads persisted data, so skip RDB snapshotting entirely.
          "--save", ""));
      GenericContainer<?> container = new GenericContainer<>(IMAGE)
          .withNetwork(network)
          .withExposedPorts(port)
          .withLogConsumer(new Slf4jLogConsumer(LOGGER));
      if (i == 0) {
        container = container.withNetworkAliases(PRIMARY_ALIAS);
      } else {
        // Replicate from the primary's in-network address, not its host-mapped port.
        command.addAll(List.of("--replicaof", PRIMARY_ALIAS, String.valueOf(basePort)));
        // Do not hand out the replica until it has finished its initial sync.
        container = container.withStartupTimeout(TIMEOUT)
            .waitingFor(Wait.forLogMessage(".*REPLICA sync: Finished with success.*\\n", 1));
      }
      container = container.withCommand(command.toArray(new String[0]));
      cluster.add(container);
      container.start();
      port++;
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
    return GlideClient.createClient(config).get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
  }

  private static String replicationInfo(final GlideClient client) throws Exception {
    return client.info(new Section[]{Section.REPLICATION}).get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
  }

  private static String role(final GlideClient client) throws Exception {
    Object[] response = (Object[]) client.customCommand(new String[]{"role"})
        .get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
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
    List<String> command = new ArrayList<>(List.of("valkey-cli", "-p", String.valueOf(container.getExposedPorts().get(0))));
    command.addAll(List.of(args));
    Container.ExecResult result = container.execInContainer(command.toArray(new String[0]));
    assertThat(result.getExitCode()).as("valkey-cli process exit code").isZero();
    return result.getStdout();
  }

  private static void logStatus(final GenericContainer<?> container) {
    LOGGER.info("container isRunning={} id={}", container.isRunning(), container.getContainerId());
  }

  @BeforeAll
  static void beforeAll() {
    assertThat(containers).hasSize(2);
    containers.forEach(c -> {
      logStatus(c);
      assertThat(c.isRunning()).isTrue();
    });
  }

  @AfterAll
  static void afterAll() {
    containers.forEach(GenericContainer::stop);
  }

  @Test
  void testValkeyClient() throws Exception {
    try (GlideClient client = newClient(ReadFrom.PRIMARY, primary())) {
      assertThat(client.ping("Hello world").get(TIMEOUT.toSeconds(), TimeUnit.SECONDS))
          .isEqualTo("Hello world");

      String clientInfo = client.info().get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
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
        client.set(key, valuePrefix + key).get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
      }

      for (String key : keys) {
        assertThat(client.get(key).get(TIMEOUT.toSeconds(), TimeUnit.SECONDS))
            .isEqualTo(valuePrefix + key);
      }

      // RANDOMKEY draws from the whole keyspace, which other tests in this class also write to,
      // so assert the invariant that holds regardless of ordering: the key it returns exists.
      GlideString randomKeyBinary = client.randomKeyBinary().get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
      assertThat(randomKeyBinary).isNotNull();
      assertThat(client.exists(new GlideString[]{randomKeyBinary}).get(TIMEOUT.toSeconds(), TimeUnit.SECONDS))
          .isEqualTo(1L);
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
      primaryClient.set(key, value).get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);

      // Replication is asynchronous, so poll the replica until the write lands.
      await().atMost(TIMEOUT).untilAsserted(() ->
          assertThat(replicaClient.get(key).get(TIMEOUT.toSeconds(), TimeUnit.SECONDS)).isEqualTo(value));
    }
  }

  @Test
  void testReplicaRejectsWrites() throws Exception {
    assertThat(valkeyCli(replica(), "set", UUID.randomUUID().toString(), "nope"))
        .contains("READONLY");
  }
}
