package io.github.sullis.valkey.playground;

import glide.api.GlideClient;
import glide.api.models.GlideString;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static io.github.sullis.valkey.playground.Futures.get;
import static org.assertj.core.api.Assertions.assertThat;

/** Single-node behaviour: server identity and plain key round-trips. */
public class StandaloneCommandsTest {
  private static ValkeyCluster cluster;

  @BeforeAll
  static void beforeAll() throws Exception {
    // Nothing here observes replication, so a lone primary is the whole topology.
    cluster = ValkeyCluster.start(0);
  }

  @AfterAll
  static void afterAll() {
    if (cluster != null) {
      cluster.close();
    }
  }

  @BeforeEach
  void flushKeyspace() throws Exception {
    cluster.flushKeyspace();
  }

  @Test
  void serverIdentifiesItselfAsAValkeyPrimary() throws Exception {
    try (GlideClient client = cluster.newPrimaryClient()) {
      assertThat(get(client.ping("Hello world"))).isEqualTo("Hello world");

      assertThat(get(client.info()))
          .contains("server_name:valkey")
          .contains("role:master");

      Object[] role = (Object[]) get(client.customCommand(new String[]{"role"}));
      assertThat(role[0].toString()).isEqualTo("master");
    }
  }

  @Test
  void writesAreReadableBackAndVisibleToRandomkey() throws Exception {
    try (GlideClient client = cluster.newPrimaryClient()) {
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
}
