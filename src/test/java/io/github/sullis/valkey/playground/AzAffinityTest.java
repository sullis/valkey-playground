package io.github.sullis.valkey.playground;

import glide.api.GlideClient;
import glide.api.models.configuration.ReadFrom;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.testcontainers.containers.GenericContainer;

import static io.github.sullis.valkey.playground.Futures.get;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Where an AZ-aware read actually lands. GLIDE asks each node for its {@code availability-zone}
 * and compares it to the zone the client was told it is in, so a three-node set with three
 * distinct zones is enough to pin down every branch of that decision -- no real availability zone
 * required, because the zone is only ever what a node reports and what the client prefers.
 *
 * <p>Asserted by counting, per node, how many {@code GET} calls it served: a read that returns the
 * right value says nothing about which node served it, and the whole point of these strategies is
 * which node served it. The counts come from {@code INFO commandstats} asked of each node over
 * valkey-cli rather than through a client, because a client would route the question the same way
 * it routes the reads under test.
 */
public class AzAffinityTest {
  /** The primary's zone. No replica is in it, which is what makes the two strategies differ. */
  private static final String PRIMARY_ZONE = "us-east-1c";

  private static final String REPLICA_A_ZONE = "us-east-1a";

  private static final String REPLICA_B_ZONE = "us-east-1b";

  /** A zone no node is in, for the fallback path. */
  private static final String UNOCCUPIED_ZONE = "eu-west-1a";

  /** Enough reads to tell "every read" from "half the reads", and even so a round robin splits. */
  private static final int READS = 6;

  @RegisterExtension
  static final ValkeyReplication servers =
      ValkeyReplication.withAvailabilityZones(List.of(PRIMARY_ZONE, REPLICA_A_ZONE, REPLICA_B_ZONE));

  /**
   * That the nodes report the zones they were started in. Without this every assertion below
   * would pass just as well against a server that ignored {@code --availability-zone} outright:
   * a client whose zone matches nothing falls back to the replicas, which is hard to tell from
   * affinity working.
   */
  @Test
  void everyNodeReportsTheZoneItWasStartedIn() throws Exception {
    assertThat(servers.valkeyCli(servers.primary(), "info", "server"))
        .contains("availability_zone:" + PRIMARY_ZONE);
    assertThat(servers.valkeyCli(servers.replica(0), "info", "server"))
        .contains("availability_zone:" + REPLICA_A_ZONE);
    assertThat(servers.valkeyCli(servers.replica(1), "info", "server"))
        .contains("availability_zone:" + REPLICA_B_ZONE);
  }

  /** The case the strategy exists for: a replica shares the client's zone, so it serves it all. */
  @Test
  void azAffinityReadsOnlyFromTheReplicaInTheClientsZone() throws Exception {
    GlideClient client = servers.azAwareClient(ReadFrom.AZ_AFFINITY, REPLICA_B_ZONE);
    String key = writeKey(client);

    resetCommandStats();
    for (int i = 0; i < READS; i++) {
      assertThat(get(client.get(key))).isEqualTo("value-" + key);
    }

    assertThat(getCalls(servers.replica(1))).as("replica in %s", REPLICA_B_ZONE).isEqualTo(READS);
    assertThat(getCalls(servers.replica(0))).as("replica in %s", REPLICA_A_ZONE).isZero();
    assertThat(getCalls(servers.primary())).as("primary in %s", PRIMARY_ZONE).isZero();
  }

  /**
   * Affinity is a read strategy only: a write has one correct destination whatever zone it was
   * issued from, and a client that sent it to a near replica would just be told READONLY.
   */
  @Test
  void azAffinityStillWritesToThePrimary() throws Exception {
    GlideClient client = servers.azAwareClient(ReadFrom.AZ_AFFINITY, REPLICA_B_ZONE);

    resetCommandStats();
    String key = writeKey(client);

    assertThat(callsFor(servers.primary(), "set")).as("SET calls on the primary").isEqualTo(1);
    // Read back through a client pinned to the primary: the point here is that the write landed,
    // not where a read of it goes.
    assertThat(get(servers.primaryClient().get(key))).isEqualTo("value-" + key);
  }

  /**
   * No replica in the client's zone, so affinity has nothing to prefer and the reads spread over
   * the replicas that do exist -- in another zone, but still off the primary.
   */
  @Test
  void azAffinityFallsBackToTheRemainingReplicas() throws Exception {
    GlideClient client = servers.azAwareClient(ReadFrom.AZ_AFFINITY, UNOCCUPIED_ZONE);
    String key = writeKey(client);

    resetCommandStats();
    for (int i = 0; i < READS; i++) {
      get(client.get(key));
    }

    assertThat(getCalls(servers.replica(0)) + getCalls(servers.replica(1)))
        .as("reads served by the two replicas")
        .isEqualTo(READS);
    assertThat(getCalls(servers.replica(0))).as("replica in %s", REPLICA_A_ZONE).isPositive();
    assertThat(getCalls(servers.replica(1))).as("replica in %s", REPLICA_B_ZONE).isPositive();
  }

  /**
   * The difference between the two AZ strategies, on the one topology that shows it: the client
   * sits in the primary's zone with no replica there, so AZ_AFFINITY leaves the zone to reach a
   * replica while AZ_AFFINITY_REPLICAS_AND_PRIMARY stays and reads the local primary.
   */
  @Test
  void onlyTheReplicasAndPrimaryStrategyReadsFromANearPrimary() throws Exception {
    GlideClient replicasOnly = servers.azAwareClient(ReadFrom.AZ_AFFINITY, PRIMARY_ZONE);
    String key = writeKey(replicasOnly);

    resetCommandStats();
    for (int i = 0; i < READS; i++) {
      get(replicasOnly.get(key));
    }

    assertThat(getCalls(servers.primary()))
        .as("AZ_AFFINITY reads served by the primary sharing the client's zone")
        .isZero();

    GlideClient replicasAndPrimary =
        servers.azAwareClient(ReadFrom.AZ_AFFINITY_REPLICAS_AND_PRIMARY, PRIMARY_ZONE);

    resetCommandStats();
    for (int i = 0; i < READS; i++) {
      get(replicasAndPrimary.get(key));
    }

    assertThat(getCalls(servers.primary()))
        .as("AZ_AFFINITY_REPLICAS_AND_PRIMARY reads served by the primary sharing the client's zone")
        .isEqualTo(READS);
  }

  /**
   * Writes a key on the primary and waits for the replicas to have it, so that a read routed to
   * either of them is a statement about routing rather than about replication lag.
   */
  private static String writeKey(final GlideClient client) throws Exception {
    String key = UUID.randomUUID().toString();
    get(client.set(key, "value-" + key));
    servers.awaitReplication(servers.primaryClient());
    return key;
  }

  /**
   * Zeroes every node's counters, so that each test counts only its own reads. Called after the
   * fixture writing is done, since that traffic is not what is being counted.
   */
  private static void resetCommandStats() throws Exception {
    for (GenericContainer<?> node : List.of(servers.primary(), servers.replica(0), servers.replica(1))) {
      servers.valkeyCli(node, "config", "resetstat");
    }
  }

  private static long getCalls(final GenericContainer<?> node) throws Exception {
    return callsFor(node, "get");
  }

  /**
   * How many times {@code command} has been called on {@code node} since its counters were last
   * reset. A command never called has no {@code cmdstat_} line at all, which is zero calls.
   */
  private static long callsFor(final GenericContainer<?> node, final String command) throws Exception {
    String stats = servers.valkeyCli(node, "info", "commandstats");
    String prefix = "cmdstat_" + command + ":calls=";
    return stats.lines()
        .filter(line -> line.startsWith(prefix))
        .mapToLong(line -> Long.parseLong(line.substring(prefix.length()).split(",")[0]))
        .sum();
  }
}
