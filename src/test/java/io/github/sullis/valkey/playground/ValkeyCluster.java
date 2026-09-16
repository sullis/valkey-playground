package io.github.sullis.valkey.playground;

import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.Ports;
import glide.api.GlideClusterClient;
import glide.api.models.configuration.GlideClusterClientConfiguration;
import glide.api.models.configuration.NodeAddress;
import java.io.IOException;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.IntStream;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;

import static glide.api.models.configuration.RequestRoutingConfiguration.SimpleMultiNodeRoute.ALL_PRIMARIES;
import static io.github.sullis.valkey.playground.Futures.get;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * A running Valkey cluster -- {@code numShards} primaries owning an even split of the 16384 hash
 * slots -- together with a {@link GlideClusterClient} that reaches it.
 *
 * <p>This is cluster mode, not replication: there is no {@code --replicaof} link and no single
 * primary, so the sibling {@link ValkeyReplication} is the fixture for anything about the
 * replication stream. What this one adds is hash slots, {@code CLUSTER} membership, and
 * per-command routing.
 *
 * <p>A test class owns one of these as a static field annotated {@code @RegisterExtension}, which
 * leaves the lifecycle to JUnit, exactly as {@link ValkeyReplication} does.
 *
 * <h2>Why every node lives in one container</h2>
 *
 * <p>A cluster client is given seed addresses but then connects to the addresses the cluster
 * <em>advertises</em>, so those have to be reachable from wherever the client runs. That rules out
 * the container-per-node shape {@link ValkeyReplication} uses: nodes would advertise their private
 * Docker addresses, which a test JVM on the host cannot reach at all under Docker Desktop.
 *
 * <p>{@code --cluster-announce-ip} does not rescue that shape either, because an announced address
 * is used by the cluster bus as well as by clients: announce something host-reachable and the
 * nodes can no longer gossip with each other. The announced address therefore has to be correct
 * from inside the cluster <em>and</em> from the host, which leaves one arrangement -- every node in
 * a single container announcing loopback, on ports published one-to-one to the host. Inside the
 * container {@code 127.0.0.1:port} really is a peer node; from the host it is the published port.
 */
final class ValkeyCluster implements BeforeAllCallback, AfterAllCallback {
  private static final Logger LOGGER = LoggerFactory.getLogger(ValkeyCluster.class);

  /**
   * The address every node announces, and the one the client connects to. Loopback is the only
   * address that means the same thing inside the container and on the host -- see the class
   * comment.
   */
  private static final String LOOPBACK = "127.0.0.1";

  /**
   * Client ports are drawn consecutively from a random base rather than left to docker's random
   * mapping, which cannot be used here: the port has to be identical inside and outside the
   * container. The band is bounded well below 65535 because each node also listens on its cluster
   * bus port, which is its client port plus 10000; taking the ports consecutively keeps one node's
   * bus port clear of another node's client port.
   */
  private static final int MIN_BASE_PORT = 20_000;
  private static final int MAX_BASE_PORT = 40_000;
  private static final int PORT_ATTEMPTS = 20;
  private static final Random RANDOM = new Random();

  /** Bounds how long the nodes have to come up before the container's start() fails. */
  private static final Duration STARTUP_TIMEOUT = Duration.ofSeconds(30);

  /** Bounds {@link #awaitClusterState}, which fails as a node still reporting a non-ok state. */
  private static final Duration CLUSTER_READY_TIMEOUT = Duration.ofSeconds(30);

  private final int numShards;

  private final List<Integer> ports;

  /**
   * Assigned before the container is started rather than after, so that a start which fails
   * partway still leaves {@link #close()} something to stop.
   */
  private GenericContainer<?> container;

  /**
   * Built on first use and kept for the life of the cluster: the client costs a Glide native
   * runtime and a connection per node, which is not worth paying per test method.
   */
  private GlideClusterClient client;

  private ValkeyCluster(final int numShards) {
    this.numShards = numShards;
    this.ports = reservePorts(numShards);
  }

  /**
   * Declares a cluster of {@code numShards} primaries and no replicas; nothing starts until JUnit
   * calls {@link #beforeAll}. Three is the smallest shard count that says anything about slot
   * ownership.
   */
  static ValkeyCluster withShards(final int numShards) {
    if (numShards < 3) {
      // Valkey itself refuses to form a cluster with fewer than three primaries.
      throw new IllegalArgumentException("a cluster needs at least 3 shards, got " + numShards);
    }
    return new ValkeyCluster(numShards);
  }

  @Override
  public void beforeAll(final ExtensionContext context) throws Exception {
    try {
      startContainer();
      formCluster();
      awaitClusterState();
    } catch (Exception | AssertionError e) {
      // JUnit does not call afterAll for a failed beforeAll, so a container that did come up has
      // to be torn down here rather than left running.
      close();
      throw e;
    }
  }

  @Override
  public void afterAll(final ExtensionContext context) {
    close();
  }

  private void startContainer() {
    container = new GenericContainer<>(ValkeyImage.VALKEY)
        .withExposedPorts(ports.toArray(new Integer[0]))
        // Publish each port to the identical host port, which withExposedPorts alone will not do.
        .withCreateContainerCmdModifier(cmd -> {
          Ports bindings = new Ports();
          ports.forEach(port -> bindings.bind(ExposedPort.tcp(port), Ports.Binding.bindPort(port)));
          cmd.getHostConfig().withPortBindings(bindings);
        })
        .withCommand("sh", "-c", serverCommand())
        .withLogConsumer(new Slf4jLogConsumer(LOGGER).withPrefix("cluster"))
        // The default port-listening probe can succeed before a server is serving commands, so
        // wait for the line each node logs once it is ready -- one per node.
        .waitingFor(Wait.forLogMessage(".*Ready to accept connections.*\\n", numShards))
        .withStartupTimeout(STARTUP_TIMEOUT);
    container.start();
    LOGGER.info("started container id={} ports={}", container.getContainerId(), ports);
  }

  /** Starts one {@code valkey-server} per shard in the background and keeps PID 1 alive. */
  private String serverCommand() {
    StringBuilder script = new StringBuilder();
    for (int port : ports) {
      script.append("valkey-server")
          .append(" --port ").append(port)
          .append(" --cluster-enabled yes")
          // The nodes share a filesystem, so they would otherwise all write one nodes.conf.
          .append(" --cluster-config-file /tmp/nodes-").append(port).append(".conf")
          // See the class comment: loopback is correct for the bus and for the host alike.
          .append(" --cluster-announce-ip ").append(LOOPBACK)
          // Nothing here reads persisted data, so skip RDB snapshotting entirely.
          .append(" --save ''")
          // All nodes write to the one container stdout, and valkey's own prefix is a pid, which
          // says nothing about which node logged the line. sed -u because a buffered pipe would
          // hold back the readiness line the wait strategy above is watching for.
          .append(" 2>&1 | sed -u 's/^/[node-").append(port).append("] /' &\n");
    }
    return script.append("wait\n").toString();
  }

  /**
   * Assigns the slots and joins the nodes. Driven by {@code valkey-cli} rather than by the client's
   * own {@code clusterMeet}/{@code clusterAddSlotsRange}: forming a cluster is this fixture's
   * setup, and a test asserting on a half-formed one would be asserting on the fixture.
   */
  private void formCluster() throws Exception {
    List<String> command = new ArrayList<>(List.of("valkey-cli", "--cluster", "create"));
    ports.forEach(port -> command.add(LOOPBACK + ":" + port));
    // Take the proposed slot split instead of waiting on a prompt no one is there to answer.
    command.add("--cluster-yes");

    Container.ExecResult result = container.execInContainer(command.toArray(new String[0]));
    assertThat(result.getExitCode()).as("valkey-cli --cluster create exit code").isZero();
    assertThat(result.getStdout())
        .as("valkey-cli --cluster create output")
        .contains("[OK] All 16384 slots covered.");
  }

  /**
   * Blocks until every node agrees the cluster is healthy. {@code --cluster create} returns as
   * soon as the slots are assigned, which is well before the nodes gossip their way to a shared
   * view -- around two seconds before, in the runs logged so far -- and a client built inside that
   * window fails to resolve a topology at all. Asked over valkey-cli rather than over a client,
   * because building the client is itself one of the things that needs the cluster to be ready.
   */
  void awaitClusterState() {
    Awaitility.await("every node reports cluster_state:ok")
        .atMost(CLUSTER_READY_TIMEOUT)
        .pollInterval(Duration.ofMillis(200))
        .untilAsserted(() -> {
          for (int index = 0; index < numShards; index++) {
            assertThat(valkeyCli(index, "cluster", "info"))
                .as("CLUSTER INFO from node %d", index)
                .contains("cluster_state:ok");
          }
        });
  }

  int numShards() {
    return numShards;
  }

  /** The host the cluster is reachable on, for building a route to one node. */
  String nodeHost() {
    return LOOPBACK;
  }

  int nodePort(final int index) {
    return ports.get(index);
  }

  /** Every node's address as the cluster advertises it, which is how a routed reply keys itself. */
  List<String> nodeAddresses() {
    return ports.stream().map(port -> LOOPBACK + ":" + port).toList();
  }

  /**
   * A client for the cluster, seeded with one node's address only: the rest of the topology is
   * discovered, and handing it every address would hide a discovery that does not work.
   */
  GlideClusterClient client() throws Exception {
    if (client == null) {
      NodeAddress seed = NodeAddress.builder().host(LOOPBACK).port(nodePort(0)).build();
      LOGGER.info("connecting to seed {}:{}", LOOPBACK, nodePort(0));
      GlideClusterClientConfiguration config =
          GlideClusterClientConfiguration.builder().addresses(List.of(seed)).build();
      client = get(GlideClusterClient.createClient(config));
    }
    return client;
  }

  /**
   * Clears the keyspace, for a test that has to observe only the keys it wrote itself. Routed to
   * every primary explicitly: a FLUSHALL that reached one shard would leave the others populated,
   * which is a confusing way to fail.
   */
  void flushKeyspace() throws Exception {
    get(client().flushall(ALL_PRIMARIES));
  }

  /**
   * Runs valkey-cli against one node from inside the container, for a question about that node
   * alone that the client cannot be trusted to answer -- the client routes, and routing is often
   * the thing under test.
   *
   * <p>valkey-cli exits 0 even when the server replies with an error, so the exit code below only
   * confirms the process itself ran -- the reply has to be asserted on by the caller.
   */
  String valkeyCli(final int nodeIndex, final String... args) throws Exception {
    List<String> command =
        new ArrayList<>(List.of("valkey-cli", "-p", String.valueOf(nodePort(nodeIndex))));
    command.addAll(List.of(args));
    Container.ExecResult result = container.execInContainer(command.toArray(new String[0]));
    assertThat(result.getExitCode()).as("valkey-cli process exit code").isZero();
    return result.getStdout();
  }

  /**
   * Picks {@code count} consecutive free host ports.
   *
   * <p>Free at the moment of the check, that is: the port is released again as the probe socket
   * closes, so something else can still take it before docker binds it. That race is the price of
   * needing fixed ports at all, and it surfaces as a container that fails to start rather than as
   * a wrong answer.
   */
  private static List<Integer> reservePorts(final int count) {
    for (int attempt = 0; attempt < PORT_ATTEMPTS; attempt++) {
      int base = MIN_BASE_PORT + RANDOM.nextInt(MAX_BASE_PORT - MIN_BASE_PORT - count);
      List<Integer> candidate = IntStream.range(0, count).map(i -> base + i).boxed().toList();
      if (candidate.stream().allMatch(ValkeyCluster::isFree)) {
        return candidate;
      }
    }
    throw new IllegalStateException(
        "could not find " + count + " consecutive free ports in " + PORT_ATTEMPTS + " attempts");
  }

  private static boolean isFree(final int port) {
    try (ServerSocket probe = new ServerSocket(port)) {
      return probe.getLocalPort() == port;
    } catch (IOException e) {
      return false;
    }
  }

  /**
   * Closes the client and stops the container. Each step is independent: a leaked container
   * outlives the JVM, so a client that refuses to close must not take the teardown down with it.
   */
  private void close() {
    if (client != null) {
      try {
        client.close();
      } catch (Exception e) {
        // GlideClusterClient.close() is declared to throw: a client that will not close cleanly
        // must not abort the container teardown below.
        LOGGER.warn("failed to close the cluster client", e);
      }
    }
    if (container != null) {
      try {
        container.stop();
      } catch (RuntimeException e) {
        LOGGER.warn("failed to stop container id={}", container.getContainerId(), e);
      }
    }
  }
}
