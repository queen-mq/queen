# PLAN queen-kafka: Kafka wire-protocol front for Queen

Goal: unmodified Kafka clients (kcat, kafkajs, franz-go, librdkafka) produce and
consume against Queen by changing `bootstrap.servers` only. `queen-kafka` is a
separate binary deployed beside the broker (OSS) or beside the proxy (Cloud).

Non-goals, stated loudly in docs: transactions/EOS (excludes Kafka Streams apps),
log compaction, KIP-848 group protocol. Unsupported APIs fail with clear Kafka
error codes, never mysteriously.

## STATUS M7 (2026-08-29)

### F1 — topics admin: CreateTopics (19), DeleteTopics (20), DescribeConfigs (32)

The advertised table goes from 14 keys to 17. Windows and the reason each one
ends where it does are in `queen-kafka/src/versions.rs`; all three stop one
version below where a topic can be named by a UUID, which is the same boundary
Metadata stops at and for the same reason.

New: `handlers/create_topics.rs`, `handlers/delete_topics.rs`,
`handlers/describe_configs.rs` and `src/topic_config.rs` — the one config-name
mapping both the write side and the read side go through, so they cannot
disagree about what a Kafka config means here. `QueenApi` gains
`create_queue_with` and `delete_queue`; `create_queue` is untouched, so the
Metadata auto-create path is byte-identical.

Green: `queen-kafka` 567 tests (519 lib + 19 bin + 29 `tests/queen_http.rs`),
`clippy --all-targets -D warnings` and `fmt --check` clean. `compat/rig.sh --m5
-count=1 -v` PASS, 47/47 with zero skips (26 pre-existing + 21 new in
`compat/go/admin_topics_test.go`). Differential vs `apache/kafka:3.9.1`: 67
divergences, 46 deliberate, 21 accepted, **0 to classify**, across 10 scenarios
(the three new ones are `compat/differential/admin_topics.go`).

Measured against real clients, not against the advertisement:

- `kafka-topics.sh --create --partitions 4` prints `Created topic …`; `--list`,
  `--describe` and `--delete` all work, and the created queue is visible to a
  second client on a second facade and over the broker's own HTTP. A second
  create prints `TopicExistsException`, `__evil` prints `InvalidTopicException`,
  and `--config cleanup.policy=compact` prints `InvalidConfigurationException`.
- `kafka-configs.sh --describe` works for `--entity-type topics` and for
  `--entity-type brokers --entity-name 0`.
- **sarama `ClusterAdmin.ListTopics` works**, which was the campaign's decisive
  red: it issues one DescribeConfigs per topic after its Metadata, so the whole
  admin object was unreachable without key 32.

Two protocol facts were settled against the oracle before the handlers were
written, not after: Apache Kafka answers a topic name repeated in one
CreateTopics with INVALID_REQUEST and creates none of them, and an EMPTY (as
opposed to null) `configuration_keys` on DescribeConfigs means *every* key.

**What F1 does not unlock: Kafka Connect.** Its three internal topics are
created with `cleanup.policy=compact` and its config topic is a compacted log
used as a database. That setting is refused INVALID_CONFIG, so Connect fails at
startup instead of running and losing connector configuration on a restart.
`kafka-configs.sh --alter` also stays out: AlterConfigs is not advertised, which
is exactly why DescribeConfigs reports every key `read_only`.

New deliberate deviations, added to the list below: CreateTopics accepts
`num_partitions` and `replication_factor` and honours neither; its `timeout_ms`
is not acted on.

### F2 — groups admin: ListGroups (16), DescribeGroups (15), DeleteGroups (42)

The advertised table goes from 17 keys to 20. Unlike F1's trio these three stop
at three DIFFERENT boundaries, and `queen-kafka/src/versions.rs` argues each:
ListGroups at v4 because v5 adds the KIP-848 `group_type` and KIP-848 is
excluded by plan, DescribeGroups at v3 because v4 carries `group_instance_id`
(the same static-membership rule that caps JoinGroup), DeleteGroups at the end
of its schema.

New: `handlers/list_groups.rs`, `handlers/describe_groups.rs`,
`handlers/delete_groups.rs`, and the thing they all rest on — a DURABLE GROUP
INDEX in Queen's KV store, `qk:groups:<esc group>`, written by the commit path
and read by all three (`src/offsets.rs`). It exists because a group's actor is
reaped 300 s after its last member leaves, so a registry-only ListGroups shows
nothing for exactly the group an operator opens the tool to look at. It keeps
the facade's own split intact: **offsets and existence are Queen's, liveness is
this process's**. Note the `s`: `qk:groups:` is not a prefix of `qk:group:` and
a test pins that rather than leaving it to be re-derived.

The coordinator gains a second, protocol-shaped read of the same fields
(`Command::DescribeGroup`) rather than a widened `Snapshot`, a scope-FILTERED
`live()` (an enumeration would hand one tenant's tool another's group ids), and
`discard_if_empty` for the delete's last step. `Conn` carries the peer address
so a member has a HOST column; it is the only new thing on the connection and
nothing routes, authorizes or rate-limits on it.

Green: `queen-kafka` 604 tests (556 lib + 19 bin + 29 `tests/queen_http.rs`),
`clippy --all-targets -D warnings` and `fmt --check` clean. `compat/rig.sh --m5
-count=1 -v` PASS, 58/58 with zero skips (47 pre-existing + 11 new in
`compat/go/admin_groups_test.go`). Differential vs `apache/kafka:3.9.1`: 79
divergences, 58 deliberate, 21 accepted, **0 to classify**, across 11 scenarios
(the new one is `compat/differential/admin_groups.go`).

Three protocol facts were measured against the oracle BEFORE the handlers were
written, and one of them is counter-intuitive enough that the design would have
been wrong without the run: **a group Apache Kafka has never heard of is
DescribeGroups error 0 with state `Dead`, not an error** — which is what
`kafka-consumer-groups.sh` turns into `Consumer group 'g' does not exist.` The
other two: a group whose last member left is `Empty` and KEEPS its protocol
type, and DeleteGroups answers NON_EMPTY_GROUP (68) for a group with members and
GROUP_ID_NOT_FOUND (69) for one that does not exist.

Measured against real clients, not against the advertisement:

- **`Confluent.Kafka AdminClient.ListConsumerGroupsAsync` works and the process
  exits 0**, negotiating ListGroups v4. It used to abort with a glibc double
  free before a byte was sent, so merely advertising key 16 is the whole fix —
  and the facade was the only side it could be fixed from.
- `kafka-consumer-groups.sh --list`, `--list --state Stable|Empty`, `--describe`
  (with `--members --verbose`, `--state` and `--offsets`) and `--delete`, all
  against apache/kafka:3.9.1's own tooling. `--describe` prints the members x
  partitions x lag table with HOST `/127.0.0.1`, CLIENT-ID and CONSUMER-ID, and
  `--members --verbose` decodes the assignment the facade passed through
  untouched. `--delete` on a running group prints `GroupNotEmptyException` and
  moves no offset; once the session timeout has evicted both members it prints
  `Deletion of requested consumer groups ('qkx-f2-consumers') was successful.`,
  the offsets are gone, the group leaves the listing, and a second delete prints
  `GroupIdNotFoundException`.

**What F2 does not do.** No `OffsetDelete` (a partial reset with no membership
guard in front of it), no `AlterConsumerGroupOffsets`, no `DescribeCluster` (key
60 has no red behind it — Java's and librdkafka's `describeCluster()` both ride
Metadata and already work). Consumer lag in the Queen console for Kafka groups
is still C3, the native cursor. And per-member LIVENESS is node-local: see the
cluster note below.

**Cluster mode, and the one thing it must not pretend.** Existence is
cluster-correct for free, because the index is in Queen — a listing from any
node lists every group of the tenant. Liveness is not: state, generation and
members live in one process. So DescribeGroups and DeleteGroups consult the
existing ownership guard (`cluster::Cluster::group_guard`, which the sibling
cluster design already landed) and answer NOT_COORDINATOR for a group this node
does not own, while ListGroups keeps answering and marks such a group `Unknown`
— a real Kafka state string meaning exactly that. With `QUEEN_KAFKA_NODE_ID`
unset the guard is `None` without reading anything, nothing is `Unknown`, and
the bytes are what they always were.

**RATIFICATION REQUIRED (Alice).** DeleteGroups changes the deviation below from
"nothing removes offsets" to "DeleteGroups is the only thing that removes them",
and that needs an explicit yes to three things: that the facade offers an
irreversible delete of committed offsets to any authenticated Kafka client (the
blast radius is one tenant's own groups, and the same bearer can already delete
the same KV keys over HTTP, but this is the first time `--delete` reaches them);
that a merely STOPPED group loses its position and its next start runs
`auto.offset.reset`; and that offsets still never expire on their own. Until
that yes exists the bullet below is marked `(awaiting ratification)` rather than
removed.

### F3 — the idempotent producer: InitProducerId (22)

The advertised table goes from 20 keys to 21, and this is the row that removes
the largest onboarding papercut the facade had: `enable.idempotence` has
defaulted to true in the Java client since 3.0 and Spring Boot inherits it, so
before F3 a producer with NO configuration at all died on its first send.
**Nothing needs configuring any more.**

The window is `0..=4`, and v3 is load-bearing rather than a nicety: it is
KIP-360's epoch bump, without which a sequence window this facade LOSES (a
restart, an eviction) is a fatal `OutOfOrderSequenceException` instead of a
reset. The ceiling is v5, which exists for KIP-890's transaction protocol 2 —
advertising it would be advertising a refusal. Note the trap pinned by a test:
`ApiKey::InitProducerId.valid_versions()` answers `0..=6` because it takes the
wider of request and response, while the REQUEST decoder stops at 5.

New: `handlers/init_producer_id.rs` (which awaits nothing — a grant is minted
from process state, so it cannot fail for infrastructure reasons) and
`src/idempotent.rs`, the per-`(tenant, producer, topic-partition)` window.
`produce.rs`'s `refuse()` loses its producer-id arm to that window; the
transactional and control arms are untouched. `Facade` gains
`producers: Arc<idempotent::Producers>`.

The TOPIC is in the window's key and that is not over-specification: a Kafka
producer keeps one sequence per `TopicPartition` and every one starts at 0, so a
key without the topic would read a second topic's sequence 0 as a duplicate of
the first and silently drop records. A test pins it.

Green: `queen-kafka` 643 tests (595 lib + 19 bin + 29 `tests/queen_http.rs`),
`clippy --all-targets -D warnings` and `fmt --check` clean. `compat/rig.sh --m5
-count=1 -v` PASS, **69/69** with zero skips (58 pre-existing + 11 new in
`compat/go/idempotent_test.go`). Differential vs `apache/kafka:3.9.1`: 83
divergences, 61 deliberate, 22 accepted, **0 to classify**, across 13 scenarios
(the two new ones are `compat/differential/idempotent.go`).

The window's three promises were measured against the oracle side by side and
Apache Kafka answers identically: a duplicate batch is `error_code = 0` carrying
the ORIGINAL base offset with the high watermark unmoved on both; a gap is
OUT_OF_ORDER_SEQUENCE_NUMBER writing nothing on both; a stale epoch is
INVALID_PRODUCER_EPOCH on both.

Measured against real clients, not against the advertisement:

- **A stock Java producer with DEFAULT config sends.** kafka-clients 4.3.1,
  `enable.idempotence` untouched: `it SENT -> partition 0 offset 0`, 314 ms. The
  whole `java-matrix` scored suite (produce, four codecs, group consume, commit
  and resume, auto-create, offsets and seek) now runs on a producer with no
  overrides, and `InitProducerId advertised 0..4 … settled on v4`.
- **A wire-level duplicate is deduplicated.** One Produce frame with a fixed
  `(pid, epoch, base_sequence)`, sent twice: both answer error 0 with the same
  base offset, and the partition holds `[dup-a dup-b dup-c sentinel]` — the
  batch appears once.
- **sarama `Producer.Idempotent = true` produces**, on ONE InitProducerId v4 and
  ONE Produce v9 over one connection. It used to burn 51 connections in about
  five seconds and then EOF.
- **A facade SIGKILL under a live default-idempotence producer does not kill
  it.** The facade answers the lost window OUT_OF_ORDER, the producer recovers,
  and all 40 records are accounted for.

**What F3 does NOT fix, and the diagnosis is not the one that was assumed.**
`initTransactions()` still costs the whole of `max.block.ms` — measured 20 106 ms
with kafka-clients 4.3.1, identical to the pre-F3 measurement. A transactional
producer asks `FindCoordinator` for a TRANSACTION coordinator BEFORE
InitProducerId; the facade answers `COORDINATOR_NOT_AVAILABLE`, which is
RETRIABLE, and the client loops (~190 requests over 20 s, zero InitProducerId
requests — counted in the facade log). Advertising key 22 did not change it and
was never going to. The InitProducerId handler refuses a transactional id in
~10 ms when reached directly. **Follow-up, outside F3's files:** a fatal code
(TRANSACTIONAL_ID_AUTHORIZATION_FAILED, or INVALID_REQUEST) on
`handlers::find_coordinator`'s `KEY_TYPE_TRANSACTION` arm would make the whole
call fast. It is one line and it changes a verified contract, so it is flagged
rather than taken.

**The caveat, and it is the honest one.** A real Kafka broker persists producer
state in the log; this one does not. A restart, an eviction (`MAX_TRACKED`,
65 536 producer-partitions, LRU) or a connection landing on another facade
leaves an absent entry, answered OUT_OF_ORDER_SEQUENCE_NUMBER. The cost is
at-least-once for at most five batches. **Apache Kafka 3.9.1 ACCEPTS that case
(measured: error 0)** and the facade deliberately does not, because Kafka's
absent entry means "aged out", which is rare, while this facade's means "we
restarted", which is common — accepting would leave the window silently
unenforced for as long as a producer keeps running afterwards. That divergence
is the one row of F3's differential classification worth arguing with, and it is
reversible. UNKNOWN_PRODUCER_ID (59) is deliberately not used: some clients
answer it by reasoning about `log_start_offset`, which this facade answers -1.

New deliberate deviations, added to the list below: producer state is not
durable, and the KIP-360 bump answers the same id at a higher epoch where Kafka
allocates a fresh one at epoch 0.

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
facade restart is a broker restart. **M7 F1 adds three**: CreateTopics accepts
`num_partitions` and does nothing with it (Queen declares no width per queue, so
the width is the facade's `max(live, QUEEN_KAFKA_DEFAULT_PARTITIONS)` and the
v5+ response reports the real one, which the client's next Metadata agrees
with); it accepts any `replication_factor`, including -1, and reports 1 (one
logical broker, and refusing RF>1 would break every provisioner whose default is
3); and CreateTopics/DeleteTopics `timeout_ms` is not acted on, the same
deviation produce `timeout_ms` already carries.

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
- **The idempotent producer's sequence window is IN MEMORY and per facade**
  (M7 F3). A real Kafka broker persists producer state in the log and does not
  lose it on a restart; this one holds no durable state by design, so a restart,
  an LRU eviction or a connection landing on another facade leaves an absent
  entry — answered OUT_OF_ORDER_SEQUENCE_NUMBER (45), recovered by the client's
  KIP-360 epoch bump, at a cost of at-least-once for at most five batches.
  Apache Kafka ACCEPTS the same case; the divergence is deliberate and argued in
  `src/idempotent.rs` and in the differential classification. The durable cure
  exists and is recorded rather than taken: `PushItem.transactionId` would move
  the window into Queen, and needs three answers first (what a per-record dedup
  key costs on the write path, whether `dedupWindowSeconds` may be forced for
  Kafka-created queues, and what happens where an operator has turned dedup
  down).
- **InitProducerId answers a KIP-360 bump with the SAME producer id one epoch
  higher** (M7 F3), where Apache Kafka's non-transactional path blindly
  allocates a fresh id at epoch 0. The epoch is what discriminates one producer
  session from the next inside the window's key, so bumping it invalidates the
  old session while keeping the producer's identity stable and costs no entropy
  per recovery. Every client takes whatever the response says.
- **A transactional producer is refused, but not quickly** (M7 F3). Its
  `initTransactions()` still costs the whole of `max.block.ms` (measured 20 s),
  because the client loops on FindCoordinator's retriable
  COORDINATOR_NOT_AVAILABLE and never reaches InitProducerId — which refuses in
  ~10 ms. A fatal code on that FindCoordinator arm is the fix and was out of
  M7 F3's scope.
- **Offsets never expire on their own; DeleteGroups is the only thing that
  removes them** *(awaiting ratification — see STATUS M7 F2)*. M7 F2 adds
  `kafka-consumer-groups.sh --delete`, with Kafka's own rule that only an empty
  group is deletable, and adds no expiry policy: there is still no
  `offsets.retention.minutes` here. A topic deleted by M7 F1's DeleteTopics
  still leaves its committed offsets under `qk:group:*:<topic>:*` behind as
  orphans, and DeleteGroups is now a tool that exists for them.
- **A Kafka client can now delete a consumer group's committed offsets** (M7 F2).
  No new privilege — the same bearer can delete the same KV keys over
  `POST /api/v1/kv` — but a stopped group that is deleted resumes from
  `auto.offset.reset` on its next start, which is a full replay or a jump to the
  end. Exactly Kafka's behaviour, and still a footgun.
- **DescribeGroups answers `authorized_operations` as Kafka's own OMITTED
  sentinel**, whatever `include_authorized_operations` asked for. Kafka with no
  authorizer computes READ|DELETE|DESCRIBE instead. The facade has no ACL model:
  what a credential may do is Queen's to say, per call, and a bitfield computed
  here would be an invented permission set.
- **A group id is bounded at 255 characters and may not be empty**, answered
  INVALID_GROUP_ID by all six group-addressed APIs through one function. The
  protocol bounds it at nothing and Apache Kafka answers `Dead` /
  GROUP_ID_NOT_FOUND for these names; every copy of a group id here is this
  facade's.
- **A Kafka client can now delete a Queen queue** (M7 F1, `kafka-topics.sh
  --delete`). No new privilege — the same bearer can issue the same HTTP DELETE —
  but a new blast radius on a facade that until M7 could only create, and it
  reaches queues native Queen producers share. The mitigation is token scoping
  and the note on `/deploy/kafka`.
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
- **M7 backlog**, from "Later, deliberately out of this plan" below.
  CreateTopics/DeleteTopics/DescribeConfigs and
  ListGroups/DescribeGroups/DeleteGroups have LANDED (see STATUS M7 above);
  what is left is `queen.dedup=key` topic config, DLQ pseudo-topics, the
  and the cycle re-key recipe. The idempotent-producer sequence window has
  LANDED (see STATUS M7 F3 above).

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

`queen.dedup=key` topic config, DLQ pseudo-topics,
native cursor (C3), raw-bytes payload mode,
cycle re-key recipe (docs only, no facade work). Rough shape: M0-M3 in 2-3
weeks, M4 is the beast, M5-M6 turn the demo into a product over a quarter.
