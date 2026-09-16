package io.github.sullis.valkey.playground;

import org.testcontainers.utility.DockerImageName;

/**
 * The Valkey image the fixtures run unless a test asks for another one. Held in one place so that
 * a version bump is one edit, and so that {@link ValkeyReplication} and {@link ValkeyCluster}
 * cannot drift onto different versions by default -- two fixtures on two versions would make a
 * difference between them look like a difference between replication and cluster mode. A test
 * that wants a particular version says so explicitly, through
 * {@link ValkeyReplication#withImage} or {@link ValkeyCluster#withImage}.
 */
final class ValkeyImage {
  /**
   * Pinned rather than floating on a tag like {@code latest}: a run either reproduces or it is not
   * evidence of anything.
   */
  static final DockerImageName DEFAULT_VALKEY_IMAGE = DockerImageName.parse("valkey/valkey:9.1.2");

  private ValkeyImage() {
  }
}
