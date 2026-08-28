# PLAN queen-kafka: Kafka wire-protocol front for Queen

Goal: unmodified Kafka clients (kcat, kafkajs, franz-go, librdkafka) produce and
consume against Queen by changing `bootstrap.servers` only. `queen-kafka` is a
separate binary deployed beside the broker (OSS) or beside the proxy (Cloud).

Non-goals, stated loudly in docs: transactions/EOS (excludes Kafka Streams apps),
log compaction, KIP-848 group protocol. Unsupported APIs fail with clear Kafka
error codes, never mysteriously.

## Architecture (decided)

- New workspace crate `queen-kafka/` (lib + bin), excluded from `default-members`
  so a plain server build never compiles the Kafka deps. Own Docker image.
- Speaks plain HTTP to broker/proxy as a normal client. No embedded broker:
  Cloud quota, metering (produce), freeze and tenancy apply untouched.
- One logical Kafka broker: advertises itself as leader of every partition;
  FindCoordinator returns self.
- Per-connection serial dispatch, connection muted until the response is written
  (mirrors Apache Kafka; a long-poll Fetch holds its connection, by design).
- No durable state in the facade. Offsets live in Queen (KV first, native group
  cursor later). Group membership is in-memory; a facade restart behaves like a
  Kafka broker restart (clients rejoin, resume from committed offsets).
- Mapping: Kafka partition n = Queen partition n (the client picks the partition;
  the record key is stored as message metadata for later dedup/re-key use).
  Values are arbitrary bytes, Queen payloads are JSON: base64 envelope in v0.
- Deps: tokio, tokio-util (LengthDelimitedCodec: frames are 4-byte BE length),
  `kafka-protocol` crate (all message types + RecordBatch v2 codec + compression),
  tokio-rustls at M5.
- `versions.rs` is the single static advertised-versions table, the compat
  contract in one place. Cap Fetch at v6: v7 introduces fetch sessions and
  version-gating deletes that entire problem.

## Core changes (server/, additive, only these)

- C1: push responses return the assigned absolute offsets (base offset per
  partition batch). Today push.rs returns none.
- C2: `POST /api/v1/fetch`: batched multi-partition fetch-from-offset with
  long-poll (`maxWaitMs`, `minBytes`) on the has_pending gate. Per entry returns
  records + highWatermark + logStartOffset, so an empty fetch doubles as the
  ListOffsets bounds probe. No destructive semantics, touches no cursor.
- C3 (deferrable): cursor get/set at an absolute offset per (group, queue,
  partition). Until it exists, OffsetCommit/OffsetFetch use KV; graduating to the
  native cursor lights up console lag for Kafka groups.

## Milestones

M0. Skeleton. Listener, framing, request-header decode, ApiVersions from the
    static table. Gate: a real client negotiates versions without errors.

M1. Metadata. Single broker (self), topics from the admin API, auto-create with
    QUEEN_KAFKA_DEFAULT_PARTITIONS (default high, e.g. 1024: cheap on Queen,
    finer lanes than any Kafka default). Gate: `kcat -L` lists topics.
    QUEEN_KAFKA_ADVERTISED_ADDR is the classic footgun: make misconfiguration
    loud at startup.

M2. Produce. RecordBatch decode (uncompressed first, then gzip/snappy/lz4/zstd),
    map to push with C1 offsets in the response, acks handling (all = durable
    push; 0/1 same path for now, relaxed class later), error-code mapping.
    Gate: `kcat -P` and a kafkajs producer land messages visible in the Queen
    console. This ships the produce-only wedge and the interop demo: Kafka
    producers in, native consumers out.

M3. Group-less consume. ListOffsets (-1 latest, -2 earliest) + Fetch capped at
    v6. Internal 100ms poll loop first, swap to C2 long-poll when it lands.
    Gate: `kcat -C` round-trips M2's messages (kcat assigns partitions itself,
    no coordinator needed); fetch below the retention watermark returns
    OFFSET_OUT_OF_RANGE correctly, or consumers loop forever.

M4. Groups (the hard month). Coordinator actor per group (tokio task + mpsc,
    single-threaded FSM: Empty, PreparingRebalance, CompletingRebalance,
    Stable), join window, per-member session timeouts, rebalance timeout,
    generation and member-id checks. OffsetCommit/OffsetFetch on KV. Gate:
    kafkajs consumer group with two members; kill one and partitions reassign;
    restart queen-kafka and clients rejoin, resuming from committed offsets.

M5. Cloud fit. TLS (rustls) + SNI for shared-host routing, SASL/PLAIN mapping
    username/password to the tenant token forwarded as normal proxy auth,
    proxy 429 mapped to Kafka throttle_time_ms (clients back off natively).
    Gate: a tenant onboards by changing bootstrap.servers + credentials, and
    metering counts the traffic as produce.

M6. Hardening. Client matrix in order: franz-go, librdkafka (covers Confluent
    JS/Python/C#), Java last. Error-code discipline audit. Differential tests:
    same scripts against a single-node KRaft Kafka container, diff observable
    behavior (offsets, error codes, rebalance sequences). Webdoc support-matrix
    page, generated from versions.rs like the other reference pages. Compat
    lane joins the harness but stays OUT of release-day CI until boring
    (proxyimpr lesson).

## Testing

- Coordinator FSM: deterministic unit tests driving synthetic Join/Sync/
  Heartbeat/Leave sequences under tokio::time::pause, including member death
  mid-sync and generation bump during commit.
- `queen-kafka/compat/`: kcat, kafkajs, franz-go scripts against a
  broker+queen-kafka stack in the existing test harness.
- Differential oracle vs real Kafka (M6).

## Config surface

QUEEN_URL, QUEEN_KAFKA_ADDR (listen), QUEEN_KAFKA_ADVERTISED_ADDR,
QUEEN_KAFKA_DEFAULT_PARTITIONS, TLS cert/key paths (M5).

## Later, deliberately out of this plan

`queen.dedup=key` topic config, DLQ pseudo-topics, CreateTopics/DescribeConfigs,
idempotent-producer sequence window, native cursor (C3), raw-bytes payload mode,
cycle re-key recipe (docs only, no facade work). Rough shape: M0-M3 in 2-3
weeks, M4 is the beast, M5-M6 turn the demo into a product over a quarter.
