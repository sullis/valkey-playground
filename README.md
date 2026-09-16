# valkey-playground

[![CI](https://github.com/sullis/valkey-playground/actions/workflows/ci.yml/badge.svg)](https://github.com/sullis/valkey-playground/actions/workflows/ci.yml)

Executable notes on [Valkey](https://valkey.io/) behaviour, written as JUnit tests that drive real
servers. Each test starts Valkey in Docker via [Testcontainers](https://testcontainers.com/) and
talks to it with [valkey-glide](https://github.com/valkey-io/valkey-glide), so an assertion here is
a statement about what a server actually does rather than about what the docs say it does.

There is no library to depend on and nothing to publish. The Testcontainers fixtures live under
`src/main/java` and the tests that drive them under `src/test/java`.

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

## Code coverage

A `mvn test` run writes a JaCoCo report to `target/site/jacoco/index.html`. What it measures is the
fixtures under `src/main/java`, which is the only tree JaCoCo's report goal reads. Take it as
"which fixture paths does the suite actually exercise" rather than as a quality bar — a fixture
method no test calls shows up here as an uncovered one.

Because surefire forks, the agent writes one execution file per fork to `target/jacoco/`
(`${surefire.forkNumber}` in the agent's `argLine`, expanded per fork), which a `merge` execution
folds into `target/jacoco.exec` before the report runs. Eight JVMs appending to a single shared
file would race.

A `check` execution then fails the build under **90% instruction coverage** over the bundle
(`jacoco.minimum.coverage` in the pom). Instructions rather than branches: fixture branch coverage
is mostly error paths a passing run never takes. One bundle-wide rule rather than a per-class one,
so that a small fixture with an uncovered branch or two need not clear the same bar as the tree.

The floor applies to the fixtures as a whole, so a run narrowed to one class will trip it —
fixture coverage is not a meaningful number when surefire only ran `ClusterTest`. Add
`-Djacoco.check.skip=true` to those runs:

```sh
mvn -ntp test -Dtest=ClusterTest -Djacoco.check.skip=true
```

## What it covers

| Test | Topology | Asserts |
| --- | --- | --- |
| `StandaloneCommandsTest` | one node | the server identifies itself as a Valkey primary; writes read back and show up in `RANDOMKEY` |
| `ReplicationTest` | primary + replica | the primary runs the version under test; both nodes report the replication link up; a primary write is readable from the replica; the replica rejects writes |
| `ClusterTest` | three shards | every node runs the version under test; a client discovers every node from a single seed address; every node agrees the slots are covered; a key lands only on the shard owning its slot |
| `AzAffinityTest` | primary + two replicas, three zones | each node reports the zone it was started in; `AZ_AFFINITY` reads only from the replica in the client's zone and falls back to the other replicas when there is none; writes still go to the primary; only `AZ_AFFINITY_REPLICAS_AND_PRIMARY` will read from a primary sharing the client's zone |

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
- `ValkeyReplication.withAvailabilityZones(zones)` — the same primary-and-replicas shape, one node
  per zone named (primary first), each started with that zone as its `availability-zone`. The zones
  are labels rather than a claim about where anything runs, which is all a client's AZ-affinity
  routing goes on: GLIDE asks each node for its own setting and compares it to the zone the client
  was given. `azAwareClient(readFrom, clientAz)` is the matching client.
- `ValkeyCluster.withShards(n)` — an `n`-shard cluster splitting the 16384 hash slots, with a
  barrier that waits for slot coverage. `n` is at least 3, because Valkey itself will not form a
  cluster with fewer primaries.

Both default to `ValkeyImage.DEFAULT_VALKEY_IMAGE`, and both can be put on a version a test names,
though they say so differently: `ValkeyReplication.withImage(image, n)` is a second factory, while
a cluster takes the modifier `ValkeyCluster.withShards(n).onImage(image)`, so that every cluster is
declared the one way. The image has to be a Valkey image or a rebuild of one: the fixtures run
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
