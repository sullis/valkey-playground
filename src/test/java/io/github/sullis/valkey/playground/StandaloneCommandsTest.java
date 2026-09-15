package io.github.sullis.valkey.playground;

import glide.api.GlideClient;
import glide.api.models.GlideString;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import static io.github.sullis.valkey.playground.Futures.get;
import static org.assertj.core.api.Assertions.assertThat;

/** Single-node behaviour: server identity and plain key round-trips. */
public class StandaloneCommandsTest {
  // Nothing here observes replication, so a lone primary is the whole topology.
  @RegisterExtension
  static final ValkeyTopology topology = ValkeyTopology.withReplicas(0);

  @Test
  void serverIdentifiesItselfAsAValkeyPrimary() throws Exception {
    GlideClient client = topology.primaryClient();

    assertThat(get(client.ping("Hello world"))).isEqualTo("Hello world");

    assertThat(get(client.info()))
        .contains("server_name:valkey")
        .contains("role:master");

    Object[] role = (Object[]) get(client.customCommand(new String[]{"role"}));
    assertThat(role[0].toString()).isEqualTo("master");
  }

  @Test
  void writesAreReadableBackAndVisibleToRandomkey() throws Exception {
    GlideClient client = topology.primaryClient();
    final String valuePrefix = "value-";

    // RANDOMKEY draws from the whole keyspace, so the assertion below is only meaningful if the
    // keys written here are the only ones there are.
    topology.flushKeyspace();

    Set<String> keys = new HashSet<>();
    for (int i = 0; i < 5; i++) {
      String key = UUID.randomUUID().toString();
      keys.add(key);
      get(client.set(key, valuePrefix + key));
    }

    for (String key : keys) {
      assertThat(get(client.get(key))).isEqualTo(valuePrefix + key);
    }

    GlideString randomKeyBinary = get(client.randomKeyBinary());
    assertThat(randomKeyBinary).isNotNull();
    assertThat(randomKeyBinary.getString()).isIn(keys);
  }
}
