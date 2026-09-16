package io.github.sullis.valkey.playground;

import glide.api.models.configuration.ReadFrom;
import java.util.List;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

/**
 * What the fixtures do before anything is running: the guards that reject a set of servers which
 * could not work, and the teardown of one that was declared but never started.
 *
 * <p>No containers here, deliberately. Every case below is settled by the declaration alone, and
 * the classes that do pay for containers cannot reach any of them -- a test cannot ask its own
 * {@code @RegisterExtension} fixture for a client the fixture is supposed to refuse, because the
 * refusal would come out as a failed test rather than as an assertion.
 */
public class FixtureContractTest {
  /**
   * The shard floor sits in {@link ValkeyCluster}'s constructor rather than in one factory, so that
   * every route to a cluster is held to it -- this is the assertion that says so. Valkey will not
   * form a cluster with fewer than three primaries, and a fixture that asked for two would surface
   * as a startup timeout rather than as anything readable.
   */
  @Test
  void aClusterCannotBeDeclaredWithFewerThanThreeShards() {
    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(() -> ValkeyCluster.withShards(2))
        .withMessageContaining("at least 3 shards");
  }

  /** The first zone is the primary's, so an empty list describes a set with no nodes in it. */
  @Test
  void serversCannotBeDeclaredWithNoAvailabilityZones() {
    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(() -> ValkeyReplication.withAvailabilityZones(List.of()))
        .withMessageContaining("at least one zone");
  }

  /**
   * Asked of a lone primary, {@code replicaReadingClient()} has nothing to read from. It says so
   * rather than handing back a client whose reads quietly land on the primary, which would make a
   * test that meant to observe replica-side state pass without observing anything.
   */
  @Test
  void aReplicaReadingClientIsRefusedWhenThereAreNoReplicas() {
    ValkeyReplication servers = ValkeyReplication.withReplicas(0);

    assertThatExceptionOfType(IllegalStateException.class)
        .isThrownBy(servers::replicaReadingClient)
        .withMessageContaining("without replicas");
  }

  /**
   * Likewise for zones: GLIDE learns a node's zone by asking the node, so nodes started without the
   * {@code availability-zone} setting can never be matched by affinity. A client built against them
   * would fall back to some other node every time and look like it was routing by zone.
   */
  @Test
  void anAzAwareClientIsRefusedWhenTheNodesHaveNoZones() {
    ValkeyReplication servers = ValkeyReplication.withReplicas(1);

    assertThatExceptionOfType(IllegalStateException.class)
        .isThrownBy(() -> servers.azAwareClient(ReadFrom.AZ_AFFINITY, "us-east-1a"))
        .withMessageContaining("without availability zones");
  }

  /**
   * A cluster that was declared and never started still has to tear down, and twice: a caller that
   * drives the lifecycle itself -- {@link ClusterTest} -- closes unconditionally in a hook that
   * runs even when the start threw, and a start that fails partway has already closed itself.
   */
  @Test
  void aClusterThatNeverStartedClosesQuietly() {
    ValkeyCluster cluster = ValkeyCluster.withShards(3);

    assertThatCode(() -> {
      cluster.close();
      cluster.close();
    }).doesNotThrowAnyException();
  }

  /**
   * The same contract for the replication fixture, where the second close is the more delicate of
   * the two: {@link ValkeyReplication#close()} closes the Docker network in a {@code finally}, and
   * closing a network twice throws. The {@code closed} flag is what keeps that out of the teardown.
   */
  @Test
  void serversThatNeverStartedCloseQuietly() {
    ValkeyReplication servers = ValkeyReplication.withReplicas(1);

    assertThatCode(() -> {
      servers.close();
      servers.close();
    }).doesNotThrowAnyException();
  }
}
