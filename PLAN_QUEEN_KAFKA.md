# PLAN queen-kafka: Kafka wire-protocol front for Queen

Goal: unmodified Kafka clients (kcat, kafkajs, franz-go, librdkafka) produce and
consume against Queen by changing `bootstrap.servers` only. `queen-kafka` is a
separate binary deployed beside the broker (OSS) or beside the proxy (Cloud).

Non-goals, stated loudly in docs: transactions/EOS (excludes Kafka Streams apps),
log compaction, KIP-848 group protocol. Unsupported APIs fail with clear Kafka
error codes, never mysteriously.

## STATUS 2026-08-28

M0-M6 and C1/C2 are implemented and green in the working tree. Nothing is
committed beyond the WIP spike 33fbc8d6; the branch has never run in CI.

Suites, all `--locked`, all rebuilt from scratch for this sweep:

| Crate | Result |
| --- | --- |
| `crates/queen-protocol` | 173 passed |
| `server` | 1122 passed, 30 ignored (need Postgres); `--no-default-features` checks |
| `clients/client-rust` | 543 passed (consumes the changed `PushResult`) |
| `queen-kafka` | 454 passed (417 lib + 13 bin + 24 `tests/queen_http.rs`), 1 doc-test ignored; debug + release build, `clippy --all-targets -D warnings` and `fmt --check` clean |

The two DB-backed UTC pins this work added (`server/tests/log_fetch_timezone.rs`,
`server/tests/procedures_timezone.rs`) also pass against a throwaway Postgres.

Live rig, from a clean database: `compat/rig.sh --m5 -count=1 -v` is PASS in
131s, 26 tests and 4 subtests, zero skips (the four M5 tests ran), and the rig's
own SNI and panic sweeps pass. Everything it starts is torn down.

Client matrix, each against `rig.sh --keep` on 19092, each PASS:

| Client | Outcome |
| --- | --- |
| franz-go (in the rig) | PASS, 26 tests |
| kafkajs 2.2.4 | PASS, 7 scenarios, 0 retriable errors on the auto-create dance |
| librdkafka 2.15 via kcat 1.7 | PASS; kcat's build declines zstd itself, reported not asserted |
| librdkafka 2.15 via confluent-kafka 2.15.0 | PASS, incl. cooperative-sticky over two members |
| Java kafka-clients 3.9.1 | PASS; `QueenKafkaEdges` shows the default idempotent producer dying on INIT_PRODUCER_ID and `group.protocol=consumer` on CONSUMER_GROUP_HEARTBEAT, both fast and legible |
| differential vs `apache/kafka:3.9.1` KRaft | 47 divergences: 28 deliberate, 19 accepted, 0 left to classify |

Webdoc: `gen:check` (8 generators, including the new Kafka support matrix),
`build`, `check:markdown`, `check:brief`, `check:prose` and `lint:docs` (249
files) are all green. `check:sourceoftruth` is RED at 126 stale pairs over 69
pages, and 120 of them were created by the spike commit 33fbc8d6 itself, which
touched `server/src/{main,schema,auth,db,fusion}.rs`,
`server/src/handlers/{data,mod,fetch}.rs` and
`crates/queen-protocol/src/lib.rs`; 6 pairs over 5 pages predate it. Those pages
need re-reading (or a recorded exemption) before this branch merges.

Deliberate deviations, each documented at the code that does it and on
`/reference/kafka`: acks=0 offsets are client-invented; acks=1 and acks=-1 share
the durable path; produce `timeout_ms` is not acted on; `__`-prefixed topics do
not exist anywhere, offsets included; auto-create cannot be refused on Metadata
v0-v3 (no wire field); a concrete ListOffsets timestamp answers -1 with no
error; a heartbeat during CompletingRebalance answers NONE; fetch fills
partitions in request order without rotation; a topic's width is
`max(live, QUEEN_KAFKA_DEFAULT_PARTITIONS)`, clamped at 100k; non-numeric Queen
partitions are invisible; static membership is excluded by the version caps; a
facade restart is a broker restart.

Known open, in the order they matter:

- **The proxy blocks C2, so the Cloud shape cannot consume.** `classify` in
  `proxy/src/routes.rs` names no `/api/v1/fetch` arm, so it falls through to the
  fail-closed `/api/` default and the route is `Blocked` (a 404). Confirmed
  twice: by reading the fallthrough, and in the generated
  `proxy-route-classes` table, which lists it under **blocked**. Produce, queue
  admin and the queue listing are all reachable; only the consume path is not.
  M5's rig proves TLS, SASL, SNI and credential forwarding against a bare broker
  behind `compat/authgate`, never against the proxy, so nothing caught it. The
  facade also reaches KV for offsets, which is `Gated(Feature::Kv)`: a Cloud
  tenant without the KV feature cannot commit. Both need a proxy change before
  M5 is true end to end.
- **C3, the native cursor.** Offsets are KV, so Kafka groups do not appear in
  the console's consumer-group lag views.
- **Tenant identity resolves in almost no deployment.** `identity.rs` asks
  `GET /auth/me` per credential and keys groups and the queue cache by the
  answer, but the broker answers a bearer only with `JWT_ENABLED` unset and the
  proxy's endpoint reads a cookie, so everywhere else the key falls back to the
  hashed credential and one tenant's two credentials are still two groups over
  one offset namespace. The facade logs it and cannot do better from inside.
- **Raw-bytes payload mode.** Values are always the base64 `k`/`v`/`h`/`t`
  envelope; a native consumer of a Kafka-written queue sees the envelope.
- **Nothing expires or deletes offsets.** No DeleteGroups, no expiry.
- **Fetch does not rotate partition order**, so a saturated early partition can
  starve a later one across repeated fetches.
- **No container image and no Helm chart**, stated on `/deploy/kafka`. The crate
  is preview and builds on its own manifest.
- **The compat lane is out of release CI** on purpose (the proxyimpr lesson).
  `.github/workflows/tests.yml` runs the crate's unit tests, fmt and clippy only.
- **`/reference/kafka` still says the group registry is keyed by
  (credential, group id)**, which `identity.rs` changed to (tenant, group id)
  with the credential as fallback. `/deploy/kafka` describes the new behaviour
  correctly; the reference page needs the same paragraph.
- **M7 backlog**, unchanged from "Later, deliberately out of this plan" below:
  `queen.dedup=key` topic config, DLQ pseudo-topics, CreateTopics/DescribeConfigs,
  the idempotent-producer sequence window, the cycle re-key recipe.

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

- C1 SHIPPED: push responses carry the assigned absolute offset per result,
  including the pre-existing offset on a duplicate (`PushResult::offset`,
  `crates/queen-protocol/src/push.rs`).
- C2 SHIPPED: `POST /api/v1/fetch` is a batched multi-partition
  fetch-from-offset with long-poll on the has_pending gate, returning records +
  highWatermark + logStartOffset per entry, so an empty fetch is also the
  ListOffsets bounds probe. Read-only in `route_access_level`, in the generated
  route table and in the OpenAPI document. Its procedure
  (`032_log_fetch.sql`) rendered timestamps in the session timezone; fixed with
  `AT TIME ZONE 'UTC'` there and at 82 further sites across 13 procedure files
  plus `db.rs`, pinned by `server/tests/procedures_timezone.rs`.
- C3 NOT DONE (deferrable, as planned): cursor get/set at an absolute offset per
  (group, queue, partition). OffsetCommit/OffsetFetch use KV; graduating to the
  native cursor lights up console lag for Kafka groups.

## Milestones

M0 SHIPPED. `conn.rs` framing and serial dispatch, per-connection read
    timeouts, a connection cap (`QUEEN_KAFKA_MAX_CONNECTIONS`, default 4096) and
    a pre-auth frame ceiling; `versions.rs` is the one advertised table, with a
    walk test over every (key, version) it claims.

M1 SHIPPED. One broker, itself, at the advertised address; topics from the
    admin API through an LRU-cached per-credential catalog; auto-create at
    `QUEEN_KAFKA_DEFAULT_PARTITIONS` (1024). `QUEEN_KAFKA_ADVERTISED_ADDR` has
    no default and refuses a wildcard host, with the reason in the error.

M2 SHIPPED. RecordBatch v2 decode with all four codecs on a decompression
    budget (`decompress.rs`), push with C1 offsets in the response, acks 0/1/all
    handled, error codes audited in `compat/ERRORS.md`.

M3 SHIPPED. ListOffsets v1-v5 on the two sentinels, Fetch capped at v6 over
    C2's long poll rather than a poll loop; OFFSET_OUT_OF_RANGE below the
    watermark, UNKNOWN_TOPIC_OR_PARTITION for a name that is not there.

M4 SHIPPED. Actor per group (Empty, PreparingRebalance, CompletingRebalance,
    Stable) with the join window, session and rebalance timeouts, generation and
    member-id checks, and caps at 10k actors / 255-char ids / 1024 pending ids
    per group; OffsetCommit/OffsetFetch on Queen KV, write-through, percent-
    escaped `qk:group:...` keys. Tested under `tokio::time::pause` and live in
    the rig, restart included.

M5 SHIPPED. rustls listener with SNI capture and a handshake timeout,
    SASL/PLAIN in both flows with the password as the Queen bearer, optional
    SNI-to-Host forwarding for shared-host routing, and 429 mapped to
    `throttle_time_ms` on Produce, Fetch and Metadata. Boot refuses half a TLS
    pair, an unknown mechanism, or SNI forwarding without TLS.

M6 SHIPPED. Client matrix is franz-go, kafkajs, librdkafka (kcat and
    confluent-kafka) and Java, all green; error-code discipline audit written
    down in `compat/ERRORS.md`; differential runner against a KRaft Kafka
    3.9.1 with every divergence classified; the support matrix is generated from
    `versions.rs` by `webdoc/scripts/gen-kafka-apis.mjs`. The compat lane is in
    `compat/rig.sh` and stays out of release-day CI.

## Testing (how to run it)

- Unit: `cargo test --locked` in `queen-kafka/` (the coordinator FSM drives
  synthetic Join/Sync/Heartbeat/Leave under `tokio::time::pause`).
- Live: `queen-kafka/compat/rig.sh [--m5] -count=1`, which stands up a throwaway
  Postgres on 55432, a broker on 6699, a facade on 19092 (and under `--m5` a
  TLS/SASL facade on 19093 behind `compat/authgate` on 6698), runs the franz-go
  suite and tears everything down. Always pass `-count=1`.
- The other client rows run against a stack left up by
  `rig.sh --keep -run TestNothing`: `compat/js/run.mjs all`,
  `compat/librdkafka/kcat.sh`, `compat/librdkafka/confluent_group.py`, and the
  two `compat/java/*.java` scripts with kafka-clients + slf4j on the classpath.
  See `compat/README.md`.
- Differential oracle: `compat/differential/rig-diff.sh run`, then `down` (it
  does not tear itself down). Own ports and own containers, so it can run beside
  the main rig.
- The two DB-backed UTC pins are `#[ignore]`d: point
  `QUEEN_EMBEDDED_TEST_PG` at a throwaway Postgres and run with `--ignored`.

## Config surface (as shipped)

`QUEEN_URL` (default `http://localhost:6632`), `QUEEN_TOKEN` (optional, never
logged), `QUEEN_KAFKA_ADDR` (default `0.0.0.0:9092`),
`QUEEN_KAFKA_ADVERTISED_ADDR` (required, no default, no wildcard),
`QUEEN_KAFKA_DEFAULT_PARTITIONS` (default 1024, 1..=100000),
`QUEEN_KAFKA_MAX_CONNECTIONS` (default 4096, 16..=1000000),
`QUEEN_KAFKA_TLS_CERT` + `QUEEN_KAFKA_TLS_KEY` (both or neither),
`QUEEN_KAFKA_SASL` (`plain` or unset), `QUEEN_KAFKA_FORWARD_SNI_HOST`
(needs TLS), `QUEEN_KAFKA_GROUP_JOIN_DELAY_MS` (3000),
`QUEEN_KAFKA_GROUP_MIN_SESSION_TIMEOUT_MS` (6000),
`QUEEN_KAFKA_GROUP_MAX_SESSION_TIMEOUT_MS` (300000), plus `LOG_LEVEL` /
`RUST_LOG` and `QUEEN_LOG_JSON`. Every one is validated at boot and a bad value
is a boot failure, never a silent default.

## Later, deliberately out of this plan

`queen.dedup=key` topic config, DLQ pseudo-topics, CreateTopics/DescribeConfigs,
idempotent-producer sequence window, native cursor (C3), raw-bytes payload mode,
cycle re-key recipe (docs only, no facade work). Rough shape: M0-M3 in 2-3
weeks, M4 is the beast, M5-M6 turn the demo into a product over a quarter.
