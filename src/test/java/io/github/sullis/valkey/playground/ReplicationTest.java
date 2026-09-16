package io.github.sullis.valkey.playground;

import glide.api.GlideClient;
import glide.api.models.commands.InfoOptions.Section;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import static io.github.sullis.valkey.playground.Futures.get;
import static org.assertj.core.api.Assertions.assertThat;

/** Primary/replica behaviour: how each node reports the link, and what a replica will accept. */
public class ReplicationTest {
  @RegisterExtension
  static final ValkeyReplication servers = ValkeyReplication.withReplicas(1);

  private static String replicationInfo(final GlideClient client) throws Exception {
    return get(client.info(new Section[]{Section.REPLICATION}));
  }

  @Test
  void bothNodesReportTheReplicationLinkAsUp() throws Exception {
    assertThat(replicationInfo(servers.primaryClient()))
        .contains("role:master")
        .contains("connected_slaves:1")
        .containsPattern("slave0:ip=.*,state=online");

    GlideClient replicaClient = servers.replicaReadingClient();

    // Reads on replicaClient land on the replica, so this is the replica's own view.
    assertThat(replicationInfo(replicaClient))
        .contains("role:slave")
        .contains("master_link_status:up")
        .contains("slave_read_only:1");

    Object[] role = (Object[]) get(replicaClient.customCommand(new String[]{"role"}));
    assertThat(role[0].toString()).isEqualTo("slave");
  }

  @Test
  void aWriteOnThePrimaryIsReadableFromTheReplica() throws Exception {
    GlideClient primaryClient = servers.primaryClient();
    String key = UUID.randomUUID().toString();
    String value = "replicated-" + key;
    get(primaryClient.set(key, value));

    // Replication is asynchronous, so wait for the replica to acknowledge the write before
    // reading it back. Reads on replicaReadingClient land on the replica, so this asserts that
    // the value replicated rather than that the primary still has it.
    servers.awaitReplication(primaryClient);
    assertThat(get(servers.replicaReadingClient().get(key))).isEqualTo(value);
  }

  @Test
  void theReplicaRejectsWrites() throws Exception {
    String key = UUID.randomUUID().toString();

    assertThat(servers.valkeyCli(servers.replica(0), "set", key, "nope"))
        .contains("READONLY");

    // The rejection has to mean the write did not happen: a replica that accepted it locally
    // would diverge from the primary rather than report an error.
    assertThat(servers.valkeyCli(servers.replica(0), "exists", key).trim()).isEqualTo("0");
  }
}
