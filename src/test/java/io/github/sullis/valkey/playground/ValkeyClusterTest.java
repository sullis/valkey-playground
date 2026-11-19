package io.github.sullis.valkey.playground;

import glide.api.GlideClusterClient;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.ServerSocket;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.assertj.core.api.Assumptions.assumeThat;

/**
 * The parts of {@link ValkeyCluster} that only a failure reaches: a port that is taken, and a
 * teardown whose steps throw. A passing cluster run takes none of these paths, and a real cluster
 * cannot be made to take them on demand, so none of the cases here starts a container.
 */
public class ValkeyClusterTest {
  /** The bottom of the fixture's port band; see {@code MIN_BASE_PORT} in {@link ValkeyCluster}. */
  private static final int BAND_START = 20_000;

  @Test
  void aBoundPortIsNotFree() throws IOException {
    try (ServerSocket holder = bindOrSkip(BAND_START)) {
      assertThat(ValkeyCluster.isFree(holder.getLocalPort())).isFalse();
    }
    assertThat(ValkeyCluster.isFree(BAND_START)).isTrue();
  }

  /**
   * A run of 11,999 ports leaves the band room for exactly one base, {@link #BAND_START}, so
   * holding that one port makes every attempt fail and the search give up rather than find a
   * different run.
   */
  @Test
  void reservingPortsGivesUpWhenNoRunIsFree() throws IOException {
    try (ServerSocket holder = bindOrSkip(BAND_START)) {
      assertThat(holder.isBound()).isTrue();
      assertThatIllegalStateException()
          .isThrownBy(() -> ValkeyCluster.reservePorts(11_999))
          .withMessageContaining("could not find 11999 consecutive free ports in 20 attempts");
    }
  }

  /**
   * GlideClusterClient.close() is declared to throw, and a client that will not close must not
   * keep the container from being stopped -- a leaked container outlives the JVM.
   */
  @Test
  void aClientThatFailsToCloseStillLetsTheContainerStop() throws Exception {
    ValkeyCluster cluster = ValkeyCluster.withShards(3);
    ThrowingClient client = unconstructed(ThrowingClient.class);
    RecordingContainer container = new RecordingContainer(false);
    inject(cluster, "client", client);
    inject(cluster, "container", container);

    assertThatCode(cluster::close).doesNotThrowAnyException();

    assertThat(client.closeAttempted).isTrue();
    assertThat(container.stopAttempted).isTrue();
  }

  /** A container that will not stop is logged, not thrown: teardown must not mask the test. */
  @Test
  void aContainerThatFailsToStopDoesNotFailTheTeardown() throws Exception {
    ValkeyCluster cluster = ValkeyCluster.withShards(3);
    RecordingContainer container = new RecordingContainer(true);
    inject(cluster, "container", container);

    assertThatCode(cluster::close).doesNotThrowAnyException();

    assertThat(container.stopAttempted).isTrue();
  }

  /** Skips, rather than fails, when something else on the machine already holds the port. */
  private static ServerSocket bindOrSkip(final int port) {
    try {
      return new ServerSocket(port);
    } catch (IOException e) {
      assumeThat(e).as("port %d is already in use", port).isNull();
      throw new AssertionError(e);
    }
  }

  private static void inject(final ValkeyCluster cluster, final String field, final Object value)
      throws ReflectiveOperationException {
    Field f = ValkeyCluster.class.getDeclaredField(field);
    f.setAccessible(true);
    f.set(cluster, value);
  }

  /**
   * An instance whose constructor never ran: a Glide client's constructor wants a native runtime
   * and a connection, which is the very thing these cases are avoiding.
   */
  private static <T> T unconstructed(final Class<T> type) throws ReflectiveOperationException {
    // By name, because a compile-time reference to sun.misc.Unsafe is a -Werror warning.
    Class<?> unsafeClass = Class.forName("sun.misc.Unsafe");
    Field theUnsafe = unsafeClass.getDeclaredField("theUnsafe");
    theUnsafe.setAccessible(true);
    Object unsafe = theUnsafe.get(null);
    return type.cast(
        unsafeClass.getMethod("allocateInstance", Class.class).invoke(unsafe, type));
  }

  private static final class ThrowingClient extends GlideClusterClient {
    private volatile boolean closeAttempted;

    private ThrowingClient() {
      super(null);
    }

    @Override
    public void close() throws ExecutionException {
      closeAttempted = true;
      throw new ExecutionException("will not close", new IOException());
    }
  }

  private static final class RecordingContainer extends GenericContainer<RecordingContainer> {
    private final boolean failing;
    private volatile boolean stopAttempted;

    private RecordingContainer(final boolean failing) {
      super(ValkeyImage.DEFAULT_VALKEY_IMAGE);
      this.failing = failing;
    }

    @Override
    public void stop() {
      stopAttempted = true;
      if (failing) {
        throw new IllegalStateException("will not stop");
      }
    }
  }
}
