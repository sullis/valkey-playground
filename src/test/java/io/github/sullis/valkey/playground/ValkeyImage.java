package io.github.sullis.valkey.playground;

import org.testcontainers.utility.DockerImageName;

/**
 * The Valkey image every fixture runs. Held in one place so that a version bump is one edit, and
 * so that {@link ValkeyReplication} and {@link ValkeyCluster} cannot drift onto different
 * versions -- two fixtures on two versions would make a difference between them look like a
 * difference between replication and cluster mode.
 */
final class ValkeyImage {
  /**
   * Pinned rather than floating on a tag like {@code latest}: a run either reproduces or it is not
   * evidence of anything.
   */
  static final DockerImageName VALKEY = DockerImageName.parse("valkey/valkey:9.1.2");

  private ValkeyImage() {
  }
}
