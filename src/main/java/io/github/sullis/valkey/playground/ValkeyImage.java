package io.github.sullis.valkey.playground;

import java.util.List;
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
   * Pinned to a patch rather than floating on {@code 8} or {@code latest}: a run either reproduces
   * or it is not evidence of anything.
   */
  static final DockerImageName VALKEY_8 = DockerImageName.parse("valkey/valkey:8.1.10");

  /** Pinned for the same reason as {@link #VALKEY_8}. */
  static final DockerImageName VALKEY_9 = DockerImageName.parse("valkey/valkey:9.1.2");

  /** The version a fixture runs when a test does not ask for one: the newest supported major. */
  static final DockerImageName DEFAULT_VALKEY_IMAGE = VALKEY_9;

  /**
   * The images a test parameterized over versions runs against -- one per supported major, newest
   * last, so that a matrix run ends on the same version the unparameterized tests use.
   *
   * <p>One entry per major rather than per patch release: the point is to catch a behaviour or a
   * reply format that changed between majors, and a patch release that changed either would be a
   * bug in Valkey rather than something a test here should be pinned against.
   */
  static final List<DockerImageName> SUPPORTED_MAJORS = List.of(VALKEY_8, VALKEY_9);

  private ValkeyImage() {
  }
}
