package io.github.sullis.valkey.playground;

import glide.api.GlideClusterClient;
import glide.api.models.ClusterValue;
import glide.api.models.configuration.RequestRoutingConfiguration.ByAddressRoute;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import static glide.api.models.configuration.RequestRoutingConfiguration.SimpleMultiNodeRoute.ALL_NODES;
import static glide.api.models.configuration.RequestRoutingConfiguration.SimpleMultiNodeRoute.ALL_PRIMARIES;
import static io.github.sullis.valkey.playground.Futures.get;
import static org.assertj.core.api.Assertions.assertThat;

/** Cluster-mode behaviour: how a client finds the nodes, and where a key ends up. */
public class ClusterTest {
  private static final int NUM_SHARDS = 3;

  @RegisterExtension
  static final ValkeyCluster cluster = ValkeyCluster.withShards(NUM_SHARDS);

  /**
   * A key's slot is a property of the key alone -- CRC16(key) % 16384 -- and {@code --cluster
   * create} hands the shards an even, ascending split of the range, so these three keys land one
   * per shard on every run. That is what makes the per-node assertions below exact rather than
   * statistical.
   */
  private static final String SHARD_0_KEY = "alpha";
  private static final int SHARD_0_SLOT = 865; // of 0-5460

  private static final String SHARD_1_KEY = "bravo";
  private static final int SHARD_1_SLOT = 8623; // of 5461-10922

  private static final String SHARD_2_KEY = "echo";
  private static final int SHARD_2_SLOT = 14438; // of 10923-16383

  @Test
  void clientDiscoversEveryNodeFromASingleSeedAddress() throws Exception {
    GlideClusterClient client = cluster.client();

    assertThat(get(client.ping("Hello world"))).isEqualTo("Hello world");

    // The client was given one seed address, so a reply from all three nodes is the discovery
    // working: it only knows about the other two because it asked the seed for the topology.
    ClusterValue<Long> clientIds = get(client.clientId(ALL_NODES));
    assertThat(clientIds.hasMultiData()).isTrue();
    assertThat(clientIds.getMultiValue().keySet())
        .containsExactlyInAnyOrderElementsOf(cluster.nodeAddresses());

    assertThat(get(client.clusterShards())).hasSize(NUM_SHARDS);
  }

  @Test
  void everyNodeAgreesTheSlotsAreFullyCovered() throws Exception {
    ClusterValue<String> clusterInfo = get(cluster.client().clusterInfo(ALL_PRIMARIES));

    assertThat(clusterInfo.getMultiValue()).hasSize(NUM_SHARDS);
    assertThat(clusterInfo.getMultiValue().values())
        .allSatisfy(info -> assertThat(info)
            .contains("cluster_state:ok")
            .contains("cluster_slots_assigned:16384")
            .contains("cluster_known_nodes:" + NUM_SHARDS));
  }

  @Test
  void aKeyIsStoredOnlyByTheShardThatOwnsItsSlot() throws Exception {
    GlideClusterClient client = cluster.client();

    // The per-node key counts below are only meaningful if the keys written here are the only
    // ones there are.
    cluster.flushKeyspace();

    // Pin the slots, so that a change in how keys hash shows up here rather than as a confusing
    // failure of the per-node counts further down.
    assertThat(get(client.clusterKeySlot(SHARD_0_KEY))).isEqualTo(SHARD_0_SLOT);
    assertThat(get(client.clusterKeySlot(SHARD_1_KEY))).isEqualTo(SHARD_1_SLOT);
    assertThat(get(client.clusterKeySlot(SHARD_2_KEY))).isEqualTo(SHARD_2_SLOT);

    // One write per shard. The client picks the node from the key's slot; nothing here says where
    // the write should go.
    for (String key : new String[] {SHARD_0_KEY, SHARD_1_KEY, SHARD_2_KEY}) {
      get(client.set(key, "value-" + key));
    }

    // Asking each node for its own key count is the assertion that the keyspace is really split:
    // a cluster that routed everything to one node would still read back correctly below.
    for (int index = 0; index < NUM_SHARDS; index++) {
      ByAddressRoute route = new ByAddressRoute(cluster.nodeHost(), cluster.nodePort(index));
      assertThat(get(client.dbsize(route)))
          .as("keys held by node %d", index)
          .isEqualTo(1L);
    }

    for (String key : new String[] {SHARD_0_KEY, SHARD_1_KEY, SHARD_2_KEY}) {
      assertThat(get(client.get(key))).isEqualTo("value-" + key);
    }
  }
}
