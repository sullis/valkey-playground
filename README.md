# valkey-playground

[![CI](https://github.com/sullis/valkey-playground/actions/workflows/ci.yml/badge.svg)](https://github.com/sullis/valkey-playground/actions/workflows/ci.yml)

Executable notes on [Valkey](https://valkey.io/) behaviour, written as JUnit tests that drive real
servers. Each test starts Valkey in Docker via [Testcontainers](https://testcontainers.com/) and
talks to it with [valkey-glide](https://github.com/valkey-io/valkey-glide), so an assertion here is
a statement about what a server actually does rather than about what the docs say it does.

There is no library to depend on and nothing to publish — the whole project lives under
`src/test/java`.

## Requirements

- Java 21 (`.sdkmanrc` pins `21.0.6-tem`; run `sdk env` to use it)
- A running Docker daemon, reachable by Testcontainers
- Maven 3.9+ (no wrapper; CI invokes `mvn` directly)

## Running the tests

```sh
mvn -ntp -B clean test           # everything
mvn -ntp test -Dtest=ClusterTest # one class
mvn -ntp test -Dtest='ClusterTest#aKeyIsStoredOnlyByTheShardThatOwnsItsSlot'
```

The first run pulls the Valkey images, so allow it some time. Surefire runs the test classes in
parallel across up to 8 forked JVMs (`forkCount`), and cluster formation costs a few seconds per
Valkey version.

## What it covers

| Test | Topology | Asserts |
| --- | --- | --- |
| `StandaloneCommandsTest` | one node | the server identifies itself as a Valkey primary; writes read back and show up in `RANDOMKEY` |
| `ReplicationTest` | primary + replica | the primary runs the version under test; both nodes report the replication link up; a primary write is readable from the replica; the replica rejects writes |
| `ClusterTest` | three shards | every node runs the version under test; a client discovers every node from a single seed address; every node agrees the slots are covered; a key lands only on the shard owning its slot |

`ReplicationTest` and `ClusterTest` run once per supported Valkey major — 8 and 9 today, pinned to
exact patches in `ValkeyImage` — because the reply formats they assert on (`role:master`,
`connected_slaves`, `CLUSTER INFO` fields) are a protocol surface, and a major release is the place
to change one. A pin is a patch rather than a floating `8` or `latest` so that a run either
reproduces or is not evidence of anything. The unparameterized tests run
`ValkeyImage.DEFAULT_VALKEY_IMAGE`, the newest supported major.

## Fixtures

Two Testcontainers fixtures stand up every topology in the table above:

- `ValkeyReplication.withReplicas(n)` — a primary and `n` replicas, one container each, with a
  barrier that waits for the replication link before a test runs. `withReplicas(0)` is how the
  standalone tests get a lone primary.
- `ValkeyCluster.withShards(n)` — an `n`-shard cluster splitting the 16384 hash slots, with a
  barrier that waits for slot coverage. `n` is at least 3, because Valkey itself will not form a
  cluster with fewer primaries.

Both default to `ValkeyImage.DEFAULT_VALKEY_IMAGE`, and `withImage(image, n)` is the same fixture
on a version a test names. It has to be a Valkey image or a rebuild of one: the fixtures run
`valkey-server` by name, shell out to `valkey-cli`, and wait on Valkey's own readiness log line, so
a Redis image surfaces as a startup timeout rather than as a clear error.

Unlike the replication fixture, a cluster runs all of its nodes in **one** container, announcing
`127.0.0.1` on ports published one-to-one to the host and drawn consecutively from a random base
between 20000 and 40000. A cluster client is given seed addresses but then connects to the ones the
cluster *advertises*, and loopback is the only address that means the same thing to a peer node and
to a test JVM on the host. `ValkeyCluster`'s class comment has the long version, including why
`--cluster-announce-ip` does not rescue a container-per-node shape.

## Adding a test

Pick the fixture the behaviour needs, then pick a lifecycle:

- **One version is enough.** Hold the fixture in a `static` field annotated
  `@RegisterExtension` and leave the lifecycle to JUnit. `StandaloneCommandsTest` is the short
  example.
- **The behaviour is a reply format, or anything a major release could change.** Make the class
  `@ParameterizedClass` over `ValkeyImage.SUPPORTED_MAJORS` and drive the fixture from
  `@BeforeParameterizedClassInvocation` / `@AfterParameterizedClassInvocation`, as `ReplicationTest`
  and `ClusterTest` do. A static `@RegisterExtension` field cannot work here: it is set up once,
  before any invocation and so before any image exists.

Parameterize over the class rather than over each `@Test` — a `@ParameterizedTest` would rebuild
the topology for every method, and forming a cluster costs seconds.

Two smaller conventions: await Glide's futures through `Futures.get`, which bounds one command at
15 seconds and keeps call sites reading as the behaviour under test rather than as future plumbing;
and call `flushKeyspace()` first in a test that asserts over the whole keyspace, such as one using
`RANDOMKEY`, because the methods of a class share one fixture.

## Valkey resources

- [valkey-glide](https://github.com/valkey-io/valkey-glide)
- [valkey-container](https://github.com/valkey-io/valkey-container)
- [official Docker image](https://hub.docker.com/r/valkey/valkey/)

## Articles

- [memory efficiency in Valkey 8](https://valkey.io/blog/valkey-memory-efficiency-8-0/)
- [valkey glide 1.2](https://aws.amazon.com/about-aws/whats-new/2024/11/valkey-glide-1-2-features-valkey-8-0-az-awareness/)

## License

[Apache License 2.0](LICENSE)
