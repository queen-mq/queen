# queen-kafka

A Kafka wire-protocol front for Queen. Point an unmodified Kafka client at it
(change `bootstrap.servers`, nothing else) and it translates to a Queen broker
over plain HTTP. It advertises itself as a single logical broker, keeps no
durable state of its own, and a restart behaves like a Kafka broker restart:
clients rejoin and resume from their committed offsets, which live in Queen.

## What it can do

- **Produce**: acks 0/1/all, gzip/snappy/lz4/zstd, keys, headers, timestamps.
  Topics auto-create on first use.
- **Consume**: Fetch with long-poll (capped at v6, so fetch sessions never
  exist), ListOffsets earliest/latest.
- **Consumer groups**: the classic protocol (JoinGroup/SyncGroup/Heartbeat),
  rebalancing, offsets durable in Queen KV.
- **Cloud fit**: TLS with SNI, SASL/PLAIN (the password is your Queen token),
  429s mapped to `throttle_time_ms`.

Verified live against kafkajs, librdkafka (kcat and confluent-kafka), Java
kafka-clients 3.9, franz-go, and differentially against Apache Kafka 3.9.1.

## What it refuses, loudly

Transactions/EOS (so no Kafka Streams apps), the idempotent producer (set
`enable.idempotence=false`; franz-go: `kgo.DisableIdempotentWrite()`), log
compaction, KIP-848, static membership. Unsupported requests fail fast with
clear error codes, never hangs.

## Run

```
cargo build --release
QUEEN_URL=http://localhost:6632 \
QUEEN_KAFKA_ADVERTISED_ADDR=localhost:9092 \
./target/release/queen-kafka
```

`QUEEN_KAFKA_ADVERTISED_ADDR` is required: it is the address clients are told
to connect back to, and getting it wrong is the classic Kafka footgun.

Tests: `cargo test`. Live end-to-end (Docker + Go): `compat/rig.sh --m5`,
passing `-count=1`. Full support matrix and config reference: the webdoc pages
`/reference/kafka` and `/deploy/kafka`. Plan and status:
[../PLAN_QUEEN_KAFKA.md](../PLAN_QUEEN_KAFKA.md).

Status: preview. Not in release CI, no container image yet, and behind the
Cloud proxy the consume path is not routed yet (see the plan's known-open list).
