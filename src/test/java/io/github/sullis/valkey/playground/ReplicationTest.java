package io.github.sullis.valkey.playground;

import glide.api.GlideClient;
import glide.api.models.commands.InfoOptions.Section;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.AfterParameterizedClassInvocation;
import org.junit.jupiter.params.BeforeParameterizedClassInvocation;
import org.junit.jupiter.params.Parameter;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.FieldSource;
import org.testcontainers.utility.DockerImageName;

import static io.github.sullis.valkey.playground.Futures.get;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Primary/replica behaviour: how each node reports the link, and what a replica will accept. Run
 * once per supported Valkey major, because the replication {@code INFO} fields asserted on below
 * are the pre-Valkey names ({@code role:master}, {@code connected_slaves}, {@code slave0:}) that a
 * major release is the place to rename.
 *
 * <p>Parameterized over the class rather than per test method: a {@code @ParameterizedTest} would
 * start a fresh set of servers for every method, and the servers cost seconds. One
 * {@code @ParameterizedClass} invocation covers every method below on one set.
 *
 * <p>The servers cannot be a static {@code @RegisterExtension} field here, the way the
 * single-version {@link StandaloneCommandsTest} has one: a static extension field is set up once,
 * before any invocation and so before any image, which is why the lifecycle is driven by the two
 * hooks below instead.
 */
@ParameterizedClass(name = "{0}")
@FieldSource("IMAGES")
public class ReplicationTest {
  static final List<DockerImageName> IMAGES = ValkeyImage.SUPPORTED_MAJORS;

  /**
   * The image this invocation runs. Declaring it is what makes the class parameterized at all --
   * without a constructor parameter or a {@code @Parameter} field, JUnit does not consider the
   * class to take an argument and refuses to inject one into the hooks below.
   */
  @Parameter
  DockerImageName image;

  private static ValkeyReplication servers;

  @BeforeParameterizedClassInvocation
  static void startServers(final DockerImageName image) {
    servers = ValkeyReplication.withImage(image, 1);
    servers.start();
  }

  @AfterParameterizedClassInvocation(injectArguments = false)
  static void stopServers() {
    // Runs even when startServers threw, which is why this tolerates a set that was never
    // assigned; a set that was assigned and then failed partway has closed itself already, and
    // ValkeyReplication.close() is a no-op the second time.
    if (servers != null) {
      servers.close();
      servers = null;
    }
  }

  private static String replicationInfo(final GlideClient client) throws Exception {
    return get(client.info(new Section[]{Section.REPLICATION}));
  }

  /**
   * That the servers really are the version this invocation asked for. Without this the matrix
   * looks like it covers two majors whether or not it does: a tag that has moved, or one image
   * silently pulled for the other, would leave every assertion below passing twice on one version.
   */
  @Test
  void theServersRunTheVersionUnderTest() throws Exception {
    String version = image.getVersionPart();

    assertThat(get(servers.primaryClient().info(new Section[]{Section.SERVER})))
        .contains("valkey_version:" + version);
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
