package io.github.sullis.valkey.playground;

import java.util.ArrayList;
import java.util.List;
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
  private static final List<GenericContainer> containers = createValkeyCluster(1, 6379);

  private static List<GenericContainer> createValkeyCluster(final int numReplicas, final int basePort) {
    Network network = Network.newNetwork();
    GenericContainer primary = null;
    List<GenericContainer> cluster = new ArrayList<>();
    int port = basePort;
    final int numContainers = 1 + numReplicas;
    for (int i = 0; i < numContainers; i++) {
      StringBuilder command = new StringBuilder();
      command.append("valkey-server --port " + port);
      GenericContainer container = new GenericContainer(DockerImageName.parse("valkey/valkey"))
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

  private static void logStatus(final GenericContainer container) {
    LOGGER.info("container " + container.getContainerId() + " isRunning=" + container.isRunning());
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
    GlideClientConfiguration.GlideClientConfigurationBuilder configBuilder = GlideClientConfiguration.builder();
    containers.forEach(c -> {
      NodeAddress address =  NodeAddress.builder().host(c.getHost()).port(c.getFirstMappedPort()).build();
      LOGGER.info("NodeAddress: " + address.getHost() + ":" + address.getPort());
      configBuilder.address(address);
    });

    try (GlideClient client = GlideClient.createClient(configBuilder.build()).get()) {
      assertThat(client.ping("Hello world").get()).isEqualTo("Hello world");

      String clientInfo = client.info().get();
      assertThat(clientInfo).contains("connected_clients:1").contains("server_name:valkey").contains("role:master");

      // LOGGER.info("client.info: " + clientInfo);

      Object[] roleResponse = (Object[]) client.customCommand(new String[]{"role"}).get();
      String roleName = roleResponse[0].toString();
      assertThat(roleName).isEqualTo("master");
    }
  }
}
