# PLAN queen-kafka: Kafka wire-protocol front for Queen

Goal: unmodified Kafka clients (kcat, kafkajs, franz-go, librdkafka) produce and
consume against Queen by changing `bootstrap.servers` only. `queen-kafka` is a
separate binary deployed beside the broker (OSS) or beside the proxy (Cloud).

Non-goals, stated loudly in docs: log compaction (which is what excludes Kafka
Streams and Kafka Connect's exactly-once source support, and it is a stated
non-goal rather than a gap — accepting `cleanup.policy=compact` and compacting
nothing would eat a state store), and the KIP-848 group protocol. Unsupported
APIs fail with clear Kafka error codes, never mysteriously.

**Transactions were a non-goal until M9 and are now a bounded capability**, and
the boundary is the sentence that must travel with the word: a transaction here
is a STAGE held by one facade process, on the connection that opened it, and
committed by one `POST /api/v1/transaction`. That covers a transactional
producer and a same-process consume-transform-produce loop, which is Spring's
`KafkaTransactionManager` and every stock Java or franz-go EOS loop. It does not
cover a two-phase commit across a failover — Flink's `KafkaSink EXACTLY_ONCE`
and Spark's structured-streaming writer pre-commit at a checkpoint and commit
from a DIFFERENT process, and that `EndTxn` reaches a facade holding no stage —
and it does not bring Kafka Streams any closer, because Streams' dependency is
compaction. "Transactions landed" must never be said without that last clause.

## STATUS — Queen Cloud is reachable (2026-08-30)

The three Cloud entries that stood at the top of "Known open" are closed, and
each one is closed by a measurement rather than by a reading. **A real franz-go
client produces, consumes, forms groups, commits offsets, runs admin and runs
transactions through a whole cell — facade, proxy, broker — and two tenants on
one shared listener cannot see each other.** 16 of 16 scenarios green from a
clean machine, reproducible in about four minutes with
`compat/cloud/rig-cloud.sh`.

What made it reachable, in three pieces, none of which widened a permission:

1. **The consume path is routed.** `POST /api/v1/fetch` is classified `Consume`,
   which is exactly the authority of the pop it stands in for.
2. **Offsets are no longer behind the `kv` feature gate.** A KV batch touching
   only the reserved `qk:` prefix is reclassified `Consume` at the gateway
   (`proxy/src/kafka_kv.rs`); anything else in the body, or a body that cannot be
   read, fails closed. `classify()` is unchanged.
3. **Identity is real.** `GET /auth/me` answers an api-key bearer through the
   normal credential-resolution path, so one tenant's two keys are one group.

Two facts an operator meets that are decisions rather than defects: a Kafka Fetch
is metered as a **request and not as a delivery**, and a long-polling Fetch takes
**no parked slot**, so the proxy's parked-pop gauge cannot see Kafka consumers at
all. Both are printed by the suite and neither is asserted.

The routing choice worth knowing: the tenant of a Kafka connection is the tenant
of the **SASL password** and nothing else. `advertised_host` is per PROCESS, so
one facade hands every client the same bootstrap address whatever name it
dialled, and per-cluster SNI would need one facade process per cluster.

What remains is a list of ratifications, not a gap; it is in
`queen-kafka/compat/CLIENT_MATRIX.md` under "Open decisions". The sharpest is the
**transaction asymmetry**: a plain offset commit rides the `qk:`-prefixed KV
route and a transactional one rides `POST /api/v1/transaction`, classified
`Produce`, so the two spellings of "commit an offset" pass through different
route classes. Both work; the asymmetry is unratified.

## STATUS M9 — transactions (2026-08-30)

The advertised table goes from 28 keys to **32**: AddPartitionsToTxn (24),
AddOffsetsToTxn (25), EndTxn (26) and TxnOffsetCommit (28), all `0..=3`. Nothing
already advertised moved. InitProducerId keeps its `0..=4` and FindCoordinator
its `0..=3`; what changed on those two is the ANSWER, not the window.

**The shape, in one paragraph.** A `transactional.id` is claimed in Queen KV
under `qk:txn:<tenant>:<id>` by a compare-and-set, which is the fencing: a second
producer taking the same id bumps the epoch and the first one is answered
PRODUCER_FENCED (90) for ever after. A transactional Produce does not push — it
STAGES the records in this process and answers `base_offset = -1`.
AddOffsetsToTxn and TxnOffsetCommit stage offsets beside them.
`EndTxn(commit)` sends the whole stage as **one** `POST /api/v1/transaction`,
with the fence CAS as KV operation index 0 carrying `required: true`, so a
fenced producer's commit raises 23514 out of `kv_apply_v1` and the whole bundle
rolls back: zero records, zero offsets. `EndTxn(abort)` drops the stage and
writes nothing at all. That single atomic POST is the whole of exactly-once
processing here, and it is why the offsets and the records cannot disagree.

**What it costs.** The stage is a memory amplifier, so it is capped in five
places, none of which has a Kafka analogue: `QUEEN_KAFKA_TXN_MAX_BYTES`
(8 MiB), `QUEEN_KAFKA_TXN_MAX_RECORDS` (50 000),
`QUEEN_KAFKA_TXN_MAX_STAGED_BYTES` (128 MiB, the whole process),
`QUEEN_KAFKA_TXN_MAX_OPEN` (1024) and `QUEEN_KAFKA_TXN_MAX_TIMEOUT_MS`
(900 000, which is Kafka's own default for `transaction.max.timeout.ms`). Two
more are derived rather than chosen and cannot be set: at most 200 partitions per
transaction, and at most 62 offsets, which is `WIRE_KV_MAX_OPS − 1 fence − 1
group index`. A 1 s sweep expires a transaction past its timeout, and a
disconnect drops that connection's stage.

**What it unlocks, and what it does not.** Yes: the stock Java transactional
producer, Spring's `KafkaTransactionManager`, franz-go's `GroupTransactSession`,
and the librdkafka transactional API — all same-process. No: Flink's
`KafkaSink EXACTLY_ONCE` and Spark's structured-streaming writer, for the
two-phase reason in the non-goals above, and Kafka Streams and Connect's
exactly-once source, for the compaction reason. **Cluster mode refuses
transactions outright**: with `QUEEN_KAFKA_NODE_ID` set, FindCoordinator's
`key_type = 1` and InitProducerId's transactional branch both answer
TRANSACTIONAL_ID_AUTHORIZATION_FAILED (53) — fatal, so `initTransactions()`
returns in milliseconds instead of hanging. That is a refusal by CONFIGURATION,
not by capability: a stage held by one process cannot be honoured by a node the
client's next request is routed to.

**The 20 second hang is gone, and it was not where it looked.** The M7 F3 row
recorded `initTransactions()` costing the whole of `max.block.ms` and blamed the
missing InitProducerId. It was FindCoordinator: `key_type = 1` was answered
COORDINATOR_NOT_AVAILABLE (15), which is retriable, so the Java client re-enqueued
the lookup until `max.block.ms` and never sent key 22 at all. Measured after the
fix, over two clean runs of `compat/transactions/run.sh`: **471-557 ms cold**
and **112-119 ms warm** in single mode, **214-251 ms** and fatal in cluster
mode, against a 20 000 ms baseline.

New: `src/txn.rs` (the registry, the state machine, the caps, the sweep) and
`handlers/{add_partitions_to_txn,add_offsets_to_txn,end_txn,txn_offset_commit}.rs`.
`queen.rs` gains the `transaction` trait method and both implementations;
`conn.rs` gains a connection id and the teardown; `find_coordinator.rs`,
`init_producer_id.rs` and `produce.rs` gain their transactional branches.

Verified: `cargo test --locked` 786 + 20 + 34 = 840 passing, clippy
`-D warnings` and fmt clean; `compat/rig.sh --m5 -count=1` 91/91;
`compat/transactions/run.sh` 9 scenarios, 38 named checks, 0 FAIL, exit 0,
including a SIGKILL between the last send and the commit that leaves 200
records, 200 distinct keys, 0 duplicates and 0 missing; the differential runner
against apache/kafka:3.9.1 ends at **0 to classify** — 100 divergences (74
deliberate, 26 accepted) from a cold stack, 97 (72, 25) from a warm one, the
three-row difference being the oracle's own transaction-coordinator warm-up.

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
*(Superseded by F4, 2026-08-30: this paragraph used to end "`kafka-configs.sh
--alter` also stays out: AlterConfigs is not advertised, which is exactly why
DescribeConfigs reports every key `read_only`." Both halves are now false:
AlterConfigs and IncrementalAlterConfigs are advertised, and `read_only` is per
row. The Kafka Connect sentence above is unaffected: compaction is still
refused, and refusing it is still what keeps Connect out.)*

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

*(F4, 2026-08-30, answers the first two of those three. **OffsetDelete is
advertised**: the reason recorded here for leaving it out was answerable rather
than fatal, because Kafka's own guard for that API is not membership but
SUBSCRIPTION, and the coordinator keeps each member's JoinGroup metadata
verbatim, which is exactly a `ConsumerProtocolSubscription`. And
**AlterConsumerGroupOffsets never needed a key**: `KafkaAdminClient` sends it as
OffsetCommit at `generation_id = -1` with an empty member id, which is the
simple-consumer shape `offset_commit.rs` has served since M4, so it worked all
along and had only never been claimed. DescribeCluster stays out, on the same
measured evidence.)*

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

### F4: the remaining admin surface (2026-08-30)

The advertised table goes from 21 keys to **28**, and this is the wave that
finishes the admin surface rather than extending it: AlterConfigs (33) v0-v2,
IncrementalAlterConfigs (44) v0-v1, CreatePartitions (37) v0-v3, OffsetDelete
(47) v0, and the ACL family DescribeAcls (29) / CreateAcls (30) / DeleteAcls
(31), all v1-v3.

**Every one of the seven windows is the schema's whole window, and that is an
argument rather than laziness.** The table's ceiling rule is "stop where a
version starts asking for something the facade would have to invent". For six of
the seven, every field of both request and response schemas is marked with the
same version range, so nothing varies anywhere inside the window except the
flexible encoding: there is no version at which anything could be invented, and
capping lower would only make a modern client fall back for no reason.
OffsetDelete has exactly one version. The floors are the schema's own, which is
KIP-896's: `1` for the ACL trio because v0 was dropped from each, `0` for the
other four because nothing was dropped from them.

**The hole this closes: `retention.ms` becomes readable.** Since F1 it was
writable and not readable, so every admin UI's topic-settings tab showed a blank
where the retention it had just set should be. The fix is `src/topic_record.rs`:
the facade persists in Queen KV, under `qk:topiccfg:<topic>`, the options bag it
posted to `POST /api/v1/configure` for each topic it created, pinned to the
queue's `id`. DescribeConfigs reports retention out of that record; an alter is
`stored ∪ delta` sent as one whole bag, which is lossless. That indirection is
not elegance, it is forced: `/configure` is a whole-row upsert over nineteen
columns and **thirteen of them have no HTTP read at all**, so a partial alter on
a live queue would silently reset a tenant's lease time, retry policy and dedup
window. A topic the facade did not create has no record and is refused
INVALID_CONFIG with that whole sentence, which `kafka-configs.sh` prints
verbatim.

New: `handlers/acls.rs` (stateless; it does not even take a `Facade`),
`handlers/alter_configs.rs`, `handlers/incremental_alter_configs.rs`,
`handlers/create_partitions.rs`, `handlers/offset_delete.rs` and
`src/topic_record.rs`. `topic_config.rs`'s `READ_ONLY` module constant becomes a
per-row field, because the justification for it being `true` everywhere was
"AlterConfigs is not advertised" and that premise is gone;
`min.insync.replicas=1` becomes a no-op instead of a refusal, so setting a key to
the value the broker just reported no longer fails. `offsets.rs` gains
`delete_offsets`. `queen::Queue` gains one `id` field, which is what lets a
describe tell a live record from one belonging to a queue that was dropped and
recreated under the same name.

Green: `queen-kafka` **751 tests** (702 lib + 20 bin + 29 `tests/queen_http.rs`),
`clippy --all-targets -D warnings` and `fmt --check` clean. `compat/rig.sh --m5
-count=1 -v` PASS, **91/91** with zero skips (69 pre-existing + 22 new across
`compat/go/admin_configs_test.go`, `admin_offsets_test.go` and
`admin_acls_test.go`). Differential vs `apache/kafka:3.9.1`: 84 divergences, 62
deliberate, 22 accepted, **0 to classify**, across 16 scenarios (the three new
ones are `admin_acls.go`, `admin_partitions.go` and `admin_offsets.go`).

Six protocol facts were measured against the oracle before the handlers were
written, and two of them refuted what the design had written down:

- **The ACL family uses TWO sentences, not one.** `AclApis.handleDescribeAcls`
  sets *"No Authorizer is configured on the broker"* (no full stop) by hand,
  while CreateAcls and DeleteAcls raise
  `SecurityDisabledException("No Authorizer is configured.")`. The first
  implementation typed the design's single string in, and it cost seven
  unclassified differential keys against a zero-divergence bar. The verification
  pass found it and both literals are now pinned. This is the campaign's
  cleanest argument for recording a sentence off the wire instead of copying it
  out of a document.
- **CreatePartitions' sentences are KRaft's, not ZooKeeper's**, and there is **no
  separate "below 1" case**: `--partitions 0` answers the DECREASE sentence,
  because KRaft's own comparison catches every non-positive count first. The
  width comparison also runs BEFORE the replica-assignment check, which is the
  oracle's order.
- **Deleting the last offsets of an already-empty group makes it vanish from
  `--list` on the oracle**; here it stays listed. Deliberate: the alternative
  needs a prefix walk on every OffsetDelete and would make this API a second way
  to delete a group.
- CreateAcls and DeleteAcls carry the error **per element**, not at the top
  level, because Kafka's `getErrorResponse` maps over the request, so an empty
  `creations` list answers an empty result list and no error at all.

Measured against real clients, not against the advertisement:

- **`kafka-configs.sh --alter` works, and the round trip closes.**
  `--describe` shows `retention.ms=-1` sourced `DEFAULT_CONFIG`;
  `--add-config retention.ms=60000` then `--describe` shows `60000` sourced
  `DYNAMIC_TOPIC_CONFIG`; `--delete-config` puts it back. The value survives a
  facade SIGKILL, because it is in Queen KV and not in the process.
- **`kafka-acls.sh --list`, `--add` and `--remove` diff IDENTICAL** against
  apache/kafka:3.9.1 run from the same container, exit 1 on both.
- **`kafka-topics.sh --alter --partitions` refuses**, and a decrease and an equal
  count diff clean against the oracle apart from the tool's own timestamp. An
  increase is where they part: the facade names
  `QUEEN_KAFKA_DEFAULT_PARTITIONS` where the oracle widens the topic.
- **`kafka-consumer-groups.sh --delete-offsets`**: 8/8 `Successful`, the group
  still in `--list`, `GroupSubscribedToTopicException` with a live member and
  nothing deleted, and an unknown group byte-identical to the oracle.
- **kafka-ui renders a topic's Settings tab identically** against the facade and
  against the oracle, `retention.ms = 60000` / `DYNAMIC_TOPIC_CONFIG` /
  `readOnly = false` on both. That screen is where the F1 hole was visible to a
  human, and it is where the fix is.
- Java `AdminClient.incrementalAlterConfigs` and the deprecated `alterConfigs`
  both PASS on kafka-clients 3.6.2 and 4.3.1; sarama pins "exactly 28 APIs
  advertised"; `@platformatic/kafka` PASS.

**What F4 does NOT do.** No DescribeCluster (60), because every client in the
matrix answers `describeCluster()` from a plain Metadata request, so advertising it
would move five live suites onto a new code path for a measured gain of zero;
the trigger that flips that is the first client whose `describeCluster()` stops
falling back, and it is written into `compat/CLIENT_MATRIX.md`. No per-topic
partition width (see "Later" below). No differential scenario for the config
write half: it is covered over the wire by franz-go and by `kafka-configs.sh`
against the oracle, and the scenario is the one piece of F4's design left
unbuilt. And no ACL model, in any sense: the three keys answer a refusal.

**RATIFICATION REQUIRED (Alice), and it widens F2's.** OffsetDelete is a second
irreversible delete of committed offsets reachable from an admin CLI. It is no
new privilege (the same bearer can remove the same KV keys over
`POST /api/v1/kv`) and it keeps Kafka's subscription rule exactly, so a LIVE
group's subscribed topics are refused, but a STOPPED group's are not, and that
group runs `auto.offset.reset` on its next start. This is the same yes F2's
DeleteGroups is already waiting on, over one more API.

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
deviation produce `timeout_ms` already carries. **M7 F4 adds five**:
CreatePartitions refuses an INCREASE where a real broker widens the topic (Queen
declares no width per queue at all, and the refusal names
`QUEEN_KAFKA_DEFAULT_PARTITIONS` and the alternative); a tracked topic with no
retention set reports `retention.ms = -1` where Kafka's default is 604800000
(Queen's default is retention OFF, and OFF is Kafka's -1, so this is the facade
reporting its own truth rather than Kafka's number); `read_only` is per ROW
where a broker computes it per resource (`retention.ms` false on a tracked
topic, true otherwise, and the other two keys always true, so a UI greying out
an edit button on the flag is being told the truth); an alter on a topic this
facade did not create is refused INVALID_CONFIG where a real broker would apply
it (thirteen of `/configure`'s nineteen columns have no HTTP read, so a partial
alter would silently reset a tenant's dedup window); and OffsetDelete leaves the
group in `--list` after its last offset is removed, where the oracle drops an
already-empty group at that point (removing the index row would need a prefix
walk on every call and would make this API a second way to delete a group).

Known open, in the order they matter:

- ~~**The proxy blocks C2, so the Cloud shape cannot consume.**~~ **CLOSED
  2026-08-30.** Both halves of this entry are gone and both are measured.
  `classify` now names `POST /api/v1/fetch` and answers `Consume`, which is
  exactly the authority of the pop it stands in for. The KV half was closed
  differently and deliberately: rather than widen the `kv` feature gate, the
  gateway reclassifies a KV batch that touches **only** the facade's reserved
  `qk:` prefix as `Consume` (`proxy/src/kafka_kv.rs`), so a consumer commits and
  reads its offsets on a plan carrying no `kv` feature at all, and a tenant over
  its storage quota can still move its cursor. Refusing that read would strand a
  consumer at an offset it can never move past while the backlog it would drain
  keeps growing. The sniffer fails closed on an unreadable, empty, mixed,
  foreign-namespace or unknown-op body, and `classify()` itself is unchanged.
  Proven by `compat/cloud`: `TestOffsetsCommitForATenantWhosePlanHasNoKv` and
  `TestAPlainKvBatchIsStillGated`, the second being the one that says the gate
  was narrowed and not removed.
- ~~**C3, the native cursor.**~~ **CLOSED 2026-08-30.** Kafka consumer groups now
  appear beside native ones in all three consumer-group views. One `kafka_base`
  CTE per function in `010_log_admin.sql` reads the `qk:group:` rows out of KV and
  is `UNION ALL`'d ahead of the existing covering probe, so the probe, the lag
  arithmetic and the JSON shape are literally the same code for both engines;
  every row carries an additive `kind` of `queen` or `kafka`. Read only: KV stays
  the single source of truth for a Kafka offset and nothing on this path writes
  one. Proven at the SQL level by `server/tests/kafka_group_mirror.rs` (14 cases,
  against a real Postgres) and over the wire by
  `TestTheSmartMirrorShowsAKafkaGroupThroughTheProxy`, whose lag matches
  franz-go's own `ListConsumerGroupOffsets` number for number.
  **Still open, and it is documentation rather than code:** the
  `/reference/http/consumer-groups` page does not yet describe the `kind` field
  it now returns.
- **Tenant identity: resolved in Cloud, still open beside a bare broker.** The
  Cloud half is closed. `GET /auth/me` now answers an api-key bearer
  (`proxy/src/kafka_identity.rs`), resolved through
  `acting::resolve_route` -> `authenticate_for` / `resolve_from_credential` rather
  than a bare key lookup, so one tenant's two credentials are ONE group rather
  than two sharing one offset namespace. Proven by
  `TestTheFacadeResolvesItsTenantFromAuthMe`, which drives a group from two keys.
  What remains is the OSS shape: against a broker with `JWT_ENABLED` set,
  `/auth/me` still does not identify a bearer, so the key falls back to the hashed
  credential and the two-credentials-two-groups problem is unchanged there. The
  facade logs which of the two happened and cannot do better from inside.
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
- **A transactional produce answers `base_offset = -1`** *(awaiting
  ratification — see STATUS M9)*. Kafka appends the batch as it arrives and
  answers the real offset; here no offset exists until `EndTxn(commit)`
  allocates them all, and the client has to be answered first. The Java client's
  `RecordMetadata` keeps -1 unchanged rather than adding the batch index
  (verified in kafka-clients bytecode), so no fabricated offset ever reaches an
  application — what an application loses is `RecordMetadata.offset()` inside a
  transaction, and what it gains is that nothing partial is ever in the log.
  Every NON-transactional produce still answers a real offset.
- **A committed transaction advances the log end offset by N and not N+1**
  (M9). Kafka writes a commit or abort MARKER into the data partition; this
  facade writes records and nothing else. Nothing a client reads differs: the
  differential runner measures 10 records read at `read_committed` on both
  brokers for the same transaction. An ABORTED transaction advances it by 0
  here and by N+1 on Kafka, which also leaves the aborted records in the log for
  the client to filter — there is nothing to filter here because there is
  nothing there.
- **`read_uncommitted` sees LESS than Kafka does** *(awaiting ratification —
  see STATUS M9)*. An open transaction's records are invisible until commit, and
  aborted records are never visible at all. `read_uncommitted` is the DEFAULT, so
  an ordinary consumer sees the same records in the same order, later by the
  producer's own commit cadence; no client library exposes "records that may yet
  be rolled back" as a state an application can act on. The upside is measured:
  a `read_committed` consumer's lag here reaches 0, where against Kafka it stops
  at 1 per partition because of the marker.
- **The transaction stage's caps have no Kafka analogue** (M9). A Kafka
  transaction has no size, because its records are appended as they arrive; one
  here is held in this process until commit, so it is bounded by five knobs and
  two derived numbers (STATUS M9 lists them). Past a cap the transaction is
  answered MESSAGE_TOO_LARGE or INVALID_COMMIT_OFFSET_SIZE, becomes abortable,
  and the producer must abort it. Not retriable, deliberately: waiting does not
  make a 12 MiB transaction fit an 8 MiB stage.
- **Transactions are unavailable in cluster mode** (M9), by configuration and
  not by capability. With `QUEEN_KAFKA_NODE_ID` set, FindCoordinator `key_type=1`
  and InitProducerId's transactional branch both answer
  TRANSACTIONAL_ID_AUTHORIZATION_FAILED (53), which is FATAL, so
  `initTransactions()` returns in ~214 ms rather than looping on a retriable
  code. A stage lives in one process; a node that does not hold it cannot honour
  the commit, and a retriable refusal would send the client round a loop that
  cannot end. The durable-stage design that would lift this is a much larger
  project and is not started.
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
- **The config record is the facade's bookkeeping, not a read of the source of
  truth** (M7 F4). `retention.ms` round-trips out of `qk:topiccfg:<topic>`, the
  bag this facade last posted to `/configure`, because Queen exposes no HTTP
  read of a queue's config columns. The record is pinned to the queue's `id`, so
  a queue dropped and recreated under the same name is caught and the key is
  omitted rather than answered stale. What is NOT caught is an in-place edit:
  a retention changed in the Queen console between two facade writes is
  invisible, and the facade reports and would re-apply its own last value. **The
  clean fix is one field on `get_queue_detail_v2`** so the config columns can be
  read back, and that is a `server/` change this campaign could not make.
- **An alter is refused on every topic that predates F4**, because none of them
  has a record. There is no safe alternative: thirteen of `/configure`'s
  nineteen columns cannot be read back, so an "assume the defaults" mode would
  silently reset a live queue's dedup window. If an escape hatch is wanted it
  should be an explicit, default-off, loudly-logged `QUEEN_KAFKA_ALTER_UNTRACKED`
  and it is deliberately not in the shipped design.
- **A topic cannot be widened through CreatePartitions** (M7 F4), and the
  version that would make it real is recorded rather than taken: put a `width`
  in the same `qk:topiccfg:<topic>` record and have
  `metadata::advertised_partitions` take `max(live, record.width, default)`.
  That would also make CreateTopics' `num_partitions`, accepted and ignored
  today, real. It is not taken because `advertised_widths` runs on EVERY
  Metadata request, the hottest path the facade has, and a per-topic record read
  there means a batched KV call per Metadata plus a new cache with its own TTL
  and invalidation.
- **DescribeLogDirs (35) is the highest-value of the three documented absences,
  and the data for it exists.** `retainedBytes` is real and already on
  `GET /api/v1/resources/queues`, so a future version could answer honest sizes
  under one synthetic log dir. What is missing is a per-partition breakdown,
  which Queen reports only per queue. Until then kafka-ui's topic and broker
  "Size" columns read blank (it swallows the error and renders) and
  `kafka-log-dirs.sh` fails.
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
  CreateTopics/DeleteTopics/DescribeConfigs,
  ListGroups/DescribeGroups/DeleteGroups, and, since F4,
  AlterConfigs/IncrementalAlterConfigs/CreatePartitions/OffsetDelete and the ACL
  trio have all LANDED (see STATUS M7 above), which is the whole of the admin
  surface this plan set out to offer. What is left is `queen.dedup=key` topic
  config, DLQ pseudo-topics and the cycle re-key recipe. The
  idempotent-producer sequence window has LANDED (see STATUS M7 F3 above).

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

M7 SHIPPED in four waves (F1-F4). The admin surface, from 14 advertised keys to
    28: topics, groups, ACLs, configs, partitions, offset deletion, and the
    idempotent producer.

M9 SHIPPED. Transactions, 28 keys to 32, with the boundary stated in the
    non-goals above and the whole shape in STATUS M9. Its acceptance suite is
    `compat/transactions/run.sh`, which stands up its own stack on 32910-32914
    and runs nine scenarios against real Java and franz-go clients.

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
- Cluster mode: `compat/cluster/rig-cluster.sh`, which stands up its own
  Postgres, two mesh-wired brokers, three clustered facades and three unclustered
  ones on 32400-32419 and runs 11 scenarios. Every port is overridable as a block.
- Queen Cloud: `compat/cloud/rig-cloud.sh`, which stands up a whole cell
  (control-plane Postgres, cell Postgres, broker, proxy, facade on 33040-33044),
  issues the credentials through the proxy's own control plane, and runs 16
  scenarios with the PROXY on the path of every Kafka request.
  `compat/cloud/run.sh` is the same suite pointed at a cell that is already up,
  so it can be aimed at a real staging cell without editing a line of Go.
- Differential oracle: `compat/differential/rig-diff.sh run`, then `down` (it
  does not tear itself down). Own ports and own containers, so it can run beside
  the main rig.
- The two DB-backed UTC pins are `#[ignore]`d: point
  `QUEEN_EMBEDDED_TEST_PG` at a throwaway Postgres and run with `--ignored`. So
  is `server/tests/kafka_group_mirror.rs`, which boots a real `Broker` purely to
  apply the `include_str!`-embedded schema, and is therefore also what proves a
  `.sql` edit was actually rebuilt.

**Full gate, 2026-08-30, all from a clean machine.** `rig.sh --m5` **91/91**;
`cluster` **11/11**; `cloud` **16/16**; the differential **exit 0** with 100
divergences (74 deliberate, 26 accepted, **0 left to classify**);
`kafka_group_mirror` **14/14** against a real Postgres. Unit: queen-kafka 848,
server 1160, proxy 367, no failures in any of them.

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

Added by M9, all five bounding the transaction stage:
`QUEEN_KAFKA_TXN_MAX_BYTES` (8 MiB, 64 KiB..=64 MiB),
`QUEEN_KAFKA_TXN_MAX_RECORDS` (50000, 1..=5000000),
`QUEEN_KAFKA_TXN_MAX_STAGED_BYTES` (128 MiB, 1 MiB..=8 GiB, the whole process),
`QUEEN_KAFKA_TXN_MAX_OPEN` (1024, 1..=1000000) and
`QUEEN_KAFKA_TXN_MAX_TIMEOUT_MS` (900000, 1000..=7200000, which is Kafka's own
`transaction.max.timeout.ms` default). A per-transaction byte cap above the
process budget is a boot failure, because it is a knob that could never be
reached.

## Later, deliberately out of this plan

`queen.dedup=key` topic config, DLQ pseudo-topics,
native cursor (C3), raw-bytes payload mode,
cycle re-key recipe (docs only, no facade work). Rough shape: M0-M3 in 2-3
weeks, M4 is the beast, M5-M6 turn the demo into a product over a quarter.

Added by M7 F4, both recorded so the next campaign does not have to rediscover
them: a **per-topic partition width** carried in `qk:topiccfg:<topic>`, which
would make CreatePartitions a real API and CreateTopics' `num_partitions` real
too, at the cost of a batched KV read on the Metadata hot path; and
**DescribeLogDirs (35)**, which is answerable honestly from `retainedBytes` once
Queen reports it per partition rather than per queue. Both are argued at their
Known-open entries above.
