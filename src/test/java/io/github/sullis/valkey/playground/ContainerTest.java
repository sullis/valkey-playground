package io.github.sullis.valkey.playground;

import glide.api.models.GlideString;
import glide.api.models.configuration.BackoffStrategy;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerImageName;
import glide.api.GlideClient;
import glide.api.models.configuration.GlideClientConfiguration;
import glide.api.models.configuration.NodeAddress;

import static org.assertj.core.api.Assertions.assertThat;


public class ContainerTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(ContainerTest.class);
  private static final List<GenericContainer> containers = createValkeyContainers(1, 6379);

  private static List<GenericContainer> createValkeyContainers(final int numReplicas, final int basePort) {
    Network network = Network.newNetwork();
    GenericContainer primary = null;
    List<GenericContainer> cluster = new ArrayList<>();
    int port = basePort;
    final int numContainers = 1 + numReplicas;
    for (int i = 0; i < numContainers; i++) {
      StringBuilder command = new StringBuilder();
      command.append("valkey-server --port " + port);
      GenericContainer<?> container = new GenericContainer(DockerImageName.parse("valkey/valkey:8.0.0-rc2"))
          .withNetwork(network)
          .withExposedPorts(port)
          .withLogConsumer(new Slf4jLogConsumer(LOGGER));
      if (i == 0) {
        primary = container;
      } else {
        command.append(" --replicaof " + primary.getHost() + " " + primary.getFirstMappedPort());
      }
      container = container.withCommand(command.toString());
      cluster.add(container);
      container.start();
      port++;
    }
    return cluster;
  }

  private static void logStatus(final GenericContainer<?> container) {
    LOGGER.info("container isRunning="
        + container.isRunning()
        + " "
        + container.getContainerId());
  }

  @BeforeAll
  static void beforeAll() {
    assertThat(containers).isNotEmpty();
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
    GlideClientConfiguration.GlideClientConfigurationBuilder<?, ?> configBuilder = GlideClientConfiguration.builder();
    containers.forEach(c -> {
      NodeAddress address =  NodeAddress.builder().host(c.getHost()).port(c.getFirstMappedPort()).build();
      LOGGER.info("NodeAddress: " + address.getHost() + ":" + address.getPort());
      configBuilder.address(address);
    });

    BackoffStrategy backoff = BackoffStrategy.builder().numOfRetries(3).factor(2).exponentBase(10).build();
    configBuilder.reconnectStrategy(backoff);

    GlideClientConfiguration config = configBuilder.build();

    LOGGER.info("client config: " + config);

    try (GlideClient client = GlideClient.createClient(config).get()) {
      assertThat(client.ping("Hello world").get()).isEqualTo("Hello world");

      String clientInfo = client.info().get();
      assertThat(clientInfo).contains("connected_clients:1").contains("server_name:valkey").contains("role:master");

      // LOGGER.info("client.info: " + clientInfo);

      Object[] roleResponse = (Object[]) client.customCommand(new String[]{"role"}).get();
      String roleName = roleResponse[0].toString();
      assertThat(roleName).isEqualTo("master");

      final String valuePrefix = "value-";

      Set<String> keys = new HashSet<>();
      for (int i = 0; i < 5; i++) {
        String key = UUID.randomUUID().toString();
        keys.add(key);
        client.set(key, valuePrefix + key).get();
      }

      for (String key : keys) {
        assertThat(client.get(key).get()).startsWith(valuePrefix);
      }

      GlideString randomKeyBinary = client.randomKeyBinary().get();
      assertThat(randomKeyBinary).isNotNull();

    }
  }
}
