package io.github.sullis.valkey.playground;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Awaits Glide commands, so that call sites read as the behaviour under test rather than as future
 * plumbing.
 */
final class Futures {
  /**
   * Bounds a single command. Distinct from the container startup and replication barrier timeouts
   * in {@link ValkeyServers}: a command that never returns is a different failure from a node that
   * never comes up.
   */
  private static final Duration COMMAND_TIMEOUT = Duration.ofSeconds(15);

  private Futures() {
  }

  static <T> T get(final CompletableFuture<T> future) throws Exception {
    return future.get(COMMAND_TIMEOUT.toSeconds(), TimeUnit.SECONDS);
  }
}
