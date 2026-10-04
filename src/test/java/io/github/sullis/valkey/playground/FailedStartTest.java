package io.github.sullis.valkey.playground;

import com.github.dockerjava.api.model.Container;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * What the fixtures leave behind when their start fails partway: nothing. JUnit runs no
 * {@code afterAll} for a {@code beforeAll} that threw, so a fixture whose start fails after some
 * of its containers are up has to stop them itself, and a container it forgets outlives the run.
 *
 * <p>Each case starts its fixture on a throwaway image derived from the default one and broken in
 * exactly one place, so that the start gets past its first container and then fails. A start that
 * failed before anything ran -- a missing image, say -- would pass these tests without exercising
 * any teardown at all. Whether anything was left behind is asked of Docker rather than of the
 * fixture, by listing the running containers built from that image; the image is unique to the
 * test, so nothing else running at the same time can be mistaken for a leak.
 */
public class FailedStartTest {
  /**
   * The cluster's one container comes up with every node serving, and the start then fails in
   * {@code formCluster}, because the image has no {@code valkey-cli} to form the cluster with.
   * The failure is the fixture's own assertion on the exit code, which is why {@code start()}
   * catches {@link AssertionError} as well as {@link Exception}.
   */
  @Test
  void aClusterThatFailsToFormStopsItsContainer() {
    String image = derivedImage("RUN rm /usr/local/bin/valkey-cli");
    ValkeyCluster cluster = ValkeyCluster.withShards(3).onImage(DockerImageName.parse(image));

    try {
      assertThatThrownBy(cluster::start)
          .isInstanceOf(AssertionError.class)
          .hasMessageContaining("valkey-cli --cluster create exit code");
      assertThat(runningContainersOf(image)).isEmpty();
    } finally {
      cluster.close();
    }
  }

  /**
   * The primary comes up, and then the first replica cannot: the image's entrypoint refuses any
   * command with {@code --replicaof} in it, which only a replica's has. So the failure is
   * necessarily the second container's, with the primary already running and on the network --
   * the case {@link ValkeyReplication}'s start-time cleanup is for.
   */
  @Test
  void serversWhoseReplicaFailsToStartStopThePrimary() {
    String image = derivedImage(
        "RUN printf '%s\\n' '#!/bin/sh' "
            + "'case \"$*\" in *--replicaof*) echo refusing to replicate >&2; exit 1;; esac' "
            + "'exec docker-entrypoint.sh \"$@\"' > /usr/local/bin/refuse-replicaof.sh "
            + "&& chmod +x /usr/local/bin/refuse-replicaof.sh",
        "ENTRYPOINT [\"tini\", \"--\", \"refuse-replicaof.sh\"]");
    ValkeyReplication servers = ValkeyReplication.withImage(DockerImageName.parse(image), 1);

    try {
      assertThatThrownBy(servers::start).isInstanceOf(RuntimeException.class);
      assertThat(runningContainersOf(image)).isEmpty();
    } finally {
      servers.close();
    }
  }

  /**
   * Builds an image from the default Valkey image plus {@code instructions}, and returns its name.
   * Testcontainers names it uniquely and deletes it when the JVM exits.
   */
  private static String derivedImage(final String... instructions) {
    StringBuilder dockerfile =
        new StringBuilder("FROM ").append(ValkeyImage.DEFAULT_VALKEY_IMAGE.asCanonicalNameString());
    for (String instruction : instructions) {
      dockerfile.append('\n').append(instruction);
    }
    return new ImageFromDockerfile()
        .withFileFromString("Dockerfile", dockerfile.append('\n').toString())
        .get();
  }

  private static List<Container> runningContainersOf(final String image) {
    return DockerClientFactory.instance().client()
        .listContainersCmd()
        .withAncestorFilter(List.of(image))
        .exec();
  }
}
