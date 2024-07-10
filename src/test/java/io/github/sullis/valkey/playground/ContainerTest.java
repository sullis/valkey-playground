package io.github.sullis.valkey.playground;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;
import glide.api.GlideClient;
import glide.api.models.configuration.GlideClientConfiguration;
import glide.api.models.configuration.NodeAddress;

import static org.assertj.core.api.Assertions.assertThat;


public class ContainerTest {
  private static final GenericContainer container = new GenericContainer(DockerImageName.parse("valkey/valkey")).withExposedPorts(6379);

  @BeforeAll
  static void beforeAll() {
    container.start();
  }

  @AfterAll
  static void afterAll() {
    container.stop();
  }

  @Test
  void testValkeyClient() throws Exception {
    NodeAddress address =  NodeAddress.builder().host(container.getHost()).port(container.getFirstMappedPort()).build();
    GlideClientConfiguration config = GlideClientConfiguration.builder().address(address).build();
    GlideClient client = GlideClient.createClient(config).get();
    assertThat(client.ping("Hello world").get()).isEqualTo("Hello world");
  }
}
