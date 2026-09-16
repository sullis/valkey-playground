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

The first run pulls the Valkey images, so allow it some time. Tests fork across 8 JVMs
(`maven-surefire-plugin`), and cluster formation costs a few seconds per Valkey version.

## What it covers

| Test | Topology | Asserts |
| --- | --- | --- |
| `StandaloneCommandsTest` | one node | the server identifies itself as a Valkey primary; writes read back and show up in `RANDOMKEY` |
| `ReplicationTest` | primary + replica | both nodes report the replication link up; a primary write is readable from the replica; the replica rejects writes |
| `ClusterTest` | three shards | a client discovers every node from a single seed address; every node agrees the slots are covered; a key lands only on the shard owning its slot |

`ReplicationTest` and `ClusterTest` are parameterized over every supported Valkey major, because
the reply formats they assert on — `role:master`, `connected_slaves`, `CLUSTER INFO` fields — are a
protocol surface that a major release is the place to change.

## Fixtures

Two Testcontainers fixtures stand up the topologies, each usable as a JUnit extension
(`@RegisterExtension`) or driven by hand from `@BeforeParameterizedClassInvocation` when a class is
parameterized over versions:

- `ValkeyReplication.withReplicas(n)` — a primary and `n` replicas, with a barrier that waits for
  the replication link before a test runs
- `ValkeyCluster.withShards(n)` — an `n`-shard cluster on host-reachable loopback ports, with a
  barrier that waits for slot coverage

Both default to the image in `ValkeyImage`, and both accept `withImage(...)` when a test wants a
specific version. Versions are pinned to exact patches there so a run either reproduces or is not
evidence of anything.

## Valkey resources

- [valkey-glide](https://github.com/valkey-io/valkey-glide)
- [valkey-container](https://github.com/valkey-io/valkey-container)
- [official Docker image](https://hub.docker.com/r/valkey/valkey/)

## Articles

- [memory efficiency in Valkey 8](https://valkey.io/blog/valkey-memory-efficiency-8-0/)
- [valkey glide 1.2](https://aws.amazon.com/about-aws/whats-new/2024/11/valkey-glide-1-2-features-valkey-8-0-az-awareness/)

## License

[Apache License 2.0](LICENSE)
