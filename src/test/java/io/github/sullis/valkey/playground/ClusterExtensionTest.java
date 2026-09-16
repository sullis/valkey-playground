package io.github.sullis.valkey.playground;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import static io.github.sullis.valkey.playground.Futures.get;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * That {@link ValkeyCluster} works as a JUnit extension: declared as a static field, formed before
 * the class and stopped after it, with no lifecycle call anywhere in the test code.
 *
 * <p>Its own class because {@link ClusterTest} cannot cover this. That one is parameterized over
 * Valkey versions, and a static extension field is set up once, before any parameter exists, so it
 * has to drive {@code start()} and {@code close()} from its own invocation hooks -- which leaves
 * {@code beforeAll}/{@code afterAll} unexercised even though {@link ValkeyCluster}'s class comment
 * presents {@code @RegisterExtension} as the normal way to own one. One cluster on the default
 * image is enough for that: what is under test here is the wiring, not cluster-mode behaviour.
 */
public class ClusterExtensionTest {
  private static final int NUM_SHARDS = 3;

  @RegisterExtension
  static final ValkeyCluster cluster = ValkeyCluster.withShards(NUM_SHARDS);

  @Test
  void theExtensionFormsAReadyClusterBeforeTheClassRuns() throws Exception {
    // numShards() is what the fixture was asked for and clusterShards() is what the running
    // cluster reports. Both, because either one alone would pass on a cluster that never formed:
    // the first reads back a constructor argument, and the second would be satisfied by a cluster
    // of any size this class did not specify.
    assertThat(cluster.numShards()).isEqualTo(NUM_SHARDS);
    assertThat(get(cluster.client().clusterShards())).hasSize(NUM_SHARDS);
  }
}
