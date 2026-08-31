# Confluent.Kafka (.NET) vs queen-kafka

The M6 matrix row for the official Confluent client for .NET.

```
KAFKA_BOOTSTRAP=127.0.0.1:19092 RUN_ID=$(date +%s) ./run.sh all
```

`run.sh` starts nothing. The stack is `rig.sh`'s job, or yours.

## What this row is actually for

Confluent.Kafka is a P/Invoke binding over librdkafka, and librdkafka's wire is
already proven in `compat/librdkafka` by kcat and confluent-kafka-python. So the
wire is not the point here. What is new is everything the **binding** owns and
every **default the .NET package picks for you**:

* `ProducerConfig` / `ConsumerConfig` defaults, which are librdkafka's and not
  Java's — most importantly `EnableIdempotence`, which the Java client defaults
  to `true` (fatal here) and this one defaults to `false` (works);
* the `DeliveryReport<K,V>` / `ConsumeResult<K,V>` object model, and whether
  key, value, headers, partition, offset and timestamp survive the marshalling
  byte-exact;
* `IProducer` / `IConsumer` lifecycle — `Flush`, `Close`, `Commit`, `Committed`,
  `Seek`, `Assign`, `Position`, `QueryWatermarkOffsets` — each a distinct
  librdkafka entry point the other suites never call;
* `AdminClient`, which .NET users reach for first and which lands squarely on
  API keys this facade deliberately does not advertise. One of those calls
  **kills the process**; see below.

## Files

| file | what it is |
| --- | --- |
| `QueenKafkaCompat.csproj` | one console exe, one package, no test framework |
| `Program.cs` | the harness: report lines, the watchdog, the librdkafka log capture that reports negotiated API versions |
| `Core.cs` | the test bar: auto-create, 512 messages over 8 partitions with keys and headers, four compression codecs, awkward payload shapes, a consumer group, commit/resume, watermarks and seek |
| `Edges.cs` | the things the facade deliberately does not support, and what the client does about them |
| `Sasl.cs` | the M5 lane: SASL/PLAIN over TLS, right password and wrong |
| `run.sh` | runs the above in the .NET SDK container (or on a host SDK if one exists) |

## Environment

| variable | meaning |
| --- | --- |
| `KAFKA_BOOTSTRAP` | plaintext listener (default `127.0.0.1:19092`) |
| `RUN_ID` | topic/group suffix; every name is `dnet-$RUN_ID-...` so reruns never collide |
| `QUEEN_KAFKA_PARTITIONS` | what the facade was booted with (default 8) |
| `KAFKA_TLS_BOOTSTRAP` | SASL_SSL listener, for the `sasl` scenario |
| `QUEEN_KAFKA_SASL_TOKEN` | the Queen bearer token, which **is** the SASL password |
| `QUEEN_KAFKA_TLS_CA` | that listener's certificate in PEM |
| `CONFLUENT_KAFKA_VERSION` | NuGet version to test (default `2.15.0`) |
| `QK_DOTNET_MODE` | `docker` \| `host` (default: host if an SDK is installed) |
| `QK_BUDGET_S` | suite watchdog in seconds (default 900) |
| `QK_VERBOSE=1` | echo every librdkafka debug line |
| `QK_EDGE_PROBES` | comma list, to narrow `edges` when bisecting a crash |
| `QK_PROBE_LISTGROUPS=1` | run the probe that aborts the process (see below) |

Scenarios: `core` | `edges` | `sasl` | `all`. Default is `core` + `edges`.

## The container reachability rule

There is no .NET SDK on a stock dev Mac and there is no reason for the compat
matrix to require one, so `run.sh` defaults to the official SDK image (multi-arch,
arm64-native). That has one consequence and it is the only hard thing about
running this row:

> **A container cannot dial your loopback, and more to the point it cannot dial
> an ADVERTISED loopback either.**

queen-kafka hands every client an advertised address after Metadata and after
FindCoordinator, and the client re-dials *that*. A facade booted with
`QUEEN_KAFKA_ADVERTISED_ADDR=127.0.0.1:PORT` is therefore unusable from a
container no matter what bootstrap you pass: the bootstrap connection succeeds
and the next one resolves `127.0.0.1` inside the container's own namespace.

Boot the facade with `QUEEN_KAFKA_ADVERTISED_ADDR=host.docker.internal:PORT`
for this suite. A second facade on a second port can keep advertising
`127.0.0.1` for host-side clients; one broker carries both, which is what
`rig.sh --m5` already does. `run.sh` rewrites a `127.0.0.1` **bootstrap** to
`host.docker.internal` and says so, but that fixes the bootstrap, not the
advertised address.

## What is the client's fault, not the facade's

**`EnableIdempotence`.** librdkafka defaults it off, so unlike the Java client
Confluent.Kafka works out of the box. Turn it on and you get, on the first send:

```
Fatal error: Local: Required feature not supported by broker:
Idempotent producer not supported by any of the 1 connected broker(s):
requires Apache Kafka broker version >= 0.11.0
```

`_UNSUPPORTED_FEATURE`, fatal, and the producer never recovers. Same for
`TransactionalId` via `InitTransactions`. InitProducerId is M7. Set
`EnableIdempotence = false` explicitly anyway — it documents the requirement
where the next reader will look.

**zstd.** librdkafka gates zstd on Fetch v10; this facade caps Fetch at v6 on
purpose (fetch sessions, KIP-227, are out of scope). So a zstd producer logs

```
Broker does not support compression type zstd: not compressing batch
```

and sends the batch uncompressed. The records land and round-trip byte-exact.
gzip, snappy and lz4 all compress normally. See PLAN_QUEEN_KAFKA.md STATUS.

**`AdminClient.ListConsumerGroupsAsync` ABORTS THE PROCESS.** Against a broker
whose ApiVersions does not advertise ListGroups (key 16) — which queen-kafka
deliberately does not — the client corrupts its own heap:

```
free(): double free detected in tcache 2     -> exit 134 (SIGABRT)
```

or, when the corruption lands somewhere else first, exit 139 (SIGSEGV). The
request is **never sent**: a `debug=protocol` trace of the whole run is
ApiVersions + Metadata on two connections and nothing else. Reproduced on
Confluent.Kafka 2.6.1, 2.11.1 and 2.15.0, so it is not a recent regression. No
`try`/`catch` can save you — a glibc abort is not a .NET exception. The probe is
therefore **off by default** here; `QK_PROBE_LISTGROUPS=1` runs it and the suite
dies where it stands with no `RESULT` line.

`CreateTopics` and `DescribeConfigs`, on the same unadvertised surface, fail
cleanly and immediately with `Local_UnsupportedFeature` and a sentence naming
the KIP. `DescribeCluster` **succeeds**, because librdkafka answers it from an
ordinary Metadata request rather than API key 60.

**The default partitioner.** librdkafka's is `consistent_random` (CRC32); Java's
is murmur2. The same key lands on a different partition in the two clients. It
does not affect this facade, but do not expect a key produced from .NET to land
where `compat/java` put it.

## Wiring it into rig.sh

`rig.sh` runs the Go suite itself and prints copy-paste commands for the others.
This row wants the same treatment plus one extra: it needs a facade that
advertises a container-reachable name. The suggested shape, for whoever edits
`rig.sh` next:

* add a `--dotnet` flag that starts a **third** facade on `KAFKA_DOCKER_PORT`
  against the same broker, with
  `QUEEN_KAFKA_ADVERTISED_ADDR=host.docker.internal:$KAFKA_DOCKER_PORT` —
  identical to the existing facade start, two environment variables changed;
* print, in the same block as the other suites:

```
cd compat/confluent-dotnet && KAFKA_BOOTSTRAP=host.docker.internal:$KAFKA_DOCKER_PORT \
  RUN_ID=$RUN_ID QUEEN_KAFKA_PARTITIONS=$PARTITIONS ./run.sh all
```

* for the M5 lane add `KAFKA_TLS_BOOTSTRAP`, `QUEEN_KAFKA_SASL_TOKEN=$SASL_TOKEN`
  and `QUEEN_KAFKA_TLS_CA=$LOGDIR/tls.crt`. The TLS facade already advertises a
  name of its own; it needs the same `host.docker.internal` treatment, or run a
  second TLS facade for the container lane.

One certificate note for that lane: the rig's self-signed cert has SANs
`kafka.example.com`, `shared.queenmq.cloud`, `localhost` and `127.0.0.1`, but
**not** `host.docker.internal`, so every containerised TLS client has to turn
hostname verification off. `Sasl.cs` does the tighter of the two escapes — it
verifies the chain against `QUEEN_KAFKA_TLS_CA` and sets
`SslEndpointIdentificationAlgorithm.None` — and `QK_SSL_INSECURE=1` falls back
to no verification at all. Adding `host.docker.internal` as a SAN to that
certificate would delete this whole paragraph.
