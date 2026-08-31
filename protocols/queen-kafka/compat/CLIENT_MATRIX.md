# queen-kafka client matrix

Which Kafka clients have been run against the facade, what each one needed, and
what each one cannot do. Every row is a real client against a real broker with
nothing faked in between, and every row has a suite in this directory that can
be re-run.

Three campaigns are recorded here. The M6 rows (franz-go, kafkajs, librdkafka via
kcat and confluent-kafka-python, Java kafka-clients 3.9.1) were verified while
the facade was built and are described in [README.md](README.md). The
**2026-08-29** rows were added by a campaign that went after the rest of the
usage-ranked client landscape: the other two Go clients, both pure-Python
clients, the newest and an old Java client, Spring Boot, .NET, the two Node
librdkafka bindings, Rust, Erlang, Ruby, PHP, and the one new pure-JavaScript
implementation.

A third campaign, the same day, changed what the facade IS rather than which
clients reach it: **M7** added the topics and groups admin APIs and the
idempotent producer, and **cluster mode** made two or three facades address one
group correctly. A fourth, overnight into **2026-08-30**, finished the admin
surface: M7 **F4** added the config WRITE half, CreatePartitions, OffsetDelete
and the ACL family, taking the advertised table from 21 keys to 28. **M9**, the
same night, added the four transaction APIs and took it to **32**: a
transactional producer and a same-process exactly-once loop work, and the
boundary that comes with them is stated in full under "What every client meets".
All have
their own sections below, and all changed rows in the table above. Which rows were re-measured afterwards and which were not is
stated explicitly, because "expected to work" and "measured" are different
claims and this file has only ever made the second one.

A fifth, on **2026-08-30**, changed what is BETWEEN the client and Queen rather
than either end: the **Cloud** shape, where the facade's `QUEEN_URL` is a cell's
proxy instead of a broker, so every Kafka request crosses authentication, tenant
scoping, quotas and metering. It is measured in [`cloud/`](cloud) and has its own
section, ["Reaching the facade in Queen Cloud"](#reaching-the-facade-in-queen-cloud).

Read this file with [ERRORS.md](ERRORS.md) (what the facade puts on the wire and
why) and with the deliberate-deviations list in `PLAN_QUEEN_KAFKA.md` STATUS. A
divergence on that list is a decision, not a defect, and this file says so where
it applies.

## The matrix

| Client | Versions tested | Verified | Result | Mandatory config | Main caveat | Suite |
| --- | --- | --- | --- | --- | --- | --- |
| Java kafka-clients | 3.9.1 | M6 | PASS | none (`enable.idempotence=false` was mandatory until M7 F3) | AdminClient is unusable beyond metadata | [`java/`](java) |
| Java kafka-clients | 4.3.1, 3.6.2 | 2026-08-29; transactions 2026-08-30 | PASS | none since M7 F3; the scored suite runs on a DEFAULT producer, and since M9 a `transactional.id` needs nothing either | 4.x negotiates every advertised API to the top of its window; KIP-896 raised no floor above a facade cap | [`java-matrix/`](java-matrix), [`transactions/`](transactions) |
| Spring Boot + spring-kafka | Boot 3.5.16, spring-kafka 3.3.16, kafka-clients 3.9.2 | 2026-08-29, re-measured after M7 | PASS | none (`NewTopic` beans work since M7 F1, as long as no bean asks for `cleanup.policy=compact`; the idempotence property is unnecessary since M7 F3). Since M9 a `KafkaTransactionManager` bean needs nothing either, and it is the same-process shape M9 supports — measured through the raw client in `transactions/`, not yet through Boot itself | `missing-topics-fatal=true` and Micrometer observation are safe; they only reach Metadata | [`spring-kafka/`](spring-kafka) |
| franz-go | (M6 pin); 1.21.x for the EOS lane | M6; transactions 2026-08-30 | PASS | none since M7 F3 (`kgo.DisableIdempotentWrite()` was mandatory before it); `kgo.TransactionalID` and `GroupTransactSession` need nothing since M9 | none beyond the shared list | [`go/`](go), [`transactions/eos/`](transactions/eos) |
| segmentio/kafka-go | v0.4.51 | 2026-08-29 | PASS | set `RequiredAcks` explicitly on a `&kafka.Writer{}` literal; configure TLS/SASL on both `Transport` and `Dialer` | its `Conn` path writes OffsetCommit v2 and OffsetFetch v1 with no negotiation, exactly on the advertised floors | [`kafka-go/`](kafka-go) |
| IBM/sarama | v1.60.2, v1.45.2 | 2026-08-29 | PARTIAL | nothing at v1.46.0 and above (`Producer.Idempotent = true` included, since M7 F3); **`Config.Version = sarama.V1_0_0_0` below v1.46.0** | below v1.46.0 the producer works while the consumer loops on EOF for ever | [`sarama/`](sarama) |
| librdkafka (C) | 2.15 via kcat 1.7 | M6 | PASS | none | declines zstd against this facade | [`librdkafka/`](librdkafka) |
| confluent-kafka-python | 2.15 | M6 | PASS | none | declines zstd; group offsets read back over OffsetFetch | [`librdkafka/`](librdkafka) |
| kafka-python (and kafka-python-ng) | 2.0.2, 2.3.2, 3.0.11; ng 2.2.3 | 2026-08-29 | PARTIAL | none, plaintext or SASL; **do not pin `api_version` above `(0, 11, 0)`** | 3.x loses a refusal's message to the immediate close and retries a bad password for ever; 2.0.2 needs Python 3.11 or older | [`kafka-python/`](kafka-python) |
| aiokafka | 0.14.0, 0.12.0 | 2026-08-29 | PARTIAL | **aiokafka 0.13.0 or newer** for consumer groups | 0.12 and older hang for ever on JoinGroup v5 | [`aiokafka/`](aiokafka) |
| Confluent.Kafka (.NET) | 2.15.0, 2.11.1, 2.6.1 | 2026-08-29 | PASS | none | `AdminClient.ListConsumerGroupsAsync` works since M7 F2 — it used to abort the process, and advertising key 16 is the whole fix | [`confluent-dotnet/`](confluent-dotnet) |
| kafkajs | 2.2.4 | M6 | PASS | none | unmaintained upstream since 2024 | [`js/`](js) |
| @confluentinc/kafka-javascript | 1.10.0 (librdkafka 2.15.0) | 2026-08-29 | PASS | compression is a producer property, not a `send()` argument | declines zstd | [`node-librdkafka/`](node-librdkafka) |
| node-rdkafka | 3.6.1 (librdkafka 2.12.0) | 2026-08-29 | PASS | none | cannot send a binary header value (client defect); its stock macOS build has no TLS | [`node-librdkafka/`](node-librdkafka) |
| @platformatic/kafka | 2.11.0 | 2026-08-29 | PASS | Node 22.22 or 24.6 and newer; `mode: 'earliest'` to read from the start | the only tested client that compresses zstd successfully, because it never consults the Fetch version | [`platformatic-kafka/`](platformatic-kafka) |
| rdkafka (Rust crate) | 0.39.0 (librdkafka 2.12.1) | 2026-08-29 | PASS | `allow.auto.create.topics=true` if the consumer is the party that must create the topic | declines zstd | [`rust-rdkafka/`](rust-rdkafka) |
| rdkafka (Ruby gem, karafka core) | 0.29.0 (librdkafka 2.14.2) | 2026-08-29 | PARTIAL | none | gzip, snappy and lz4 compress; zstd does not | [`rdkafka-ruby-php/`](rdkafka-ruby-php) |
| php-rdkafka | 6.0.5 (librdkafka 2.6.1) | 2026-08-29 | PARTIAL | none | **every codec is silently sent uncompressed** on librdkafka 2.11.0 and older | [`rdkafka-ruby-php/`](rdkafka-ruby-php) |
| brod (Erlang/Elixir) | 4.6.3 (kafka_protocol 4.3.6) | 2026-08-29 | PASS | none | pins Metadata at v2 and so cannot decline auto-create, which is on the deliberate list | [`brod/`](brod) |

Result meanings: **PASS**, the client works with the config in its row and every
divergence it meets is on the deliberate list. **PARTIAL**, the client works but
a real user meets something sharp: a version floor, a lane that is refused, or a
silent degradation. **FAIL**, the client cannot complete the basic path.

### What was re-measured after M7, and what was not

M7 F1 to F3 changed the advertised table from 14 keys to 21 and added cluster
mode; **F4 took it to 28** on 2026-08-30. Not every row above was run again
afterwards, and this table says which. A row that was not re-run keeps its M6 or
2026-08-29 result: nothing in M7 removed or narrowed an existing version window,
so no client can have lost a lane, but "expected to work" is not the same claim
as "measured".

The table below records the F1-to-F3 re-runs. **What was re-run for F4**, all
green: the whole franz-go suite (91 top-level tests), `java-matrix` on
kafka-clients 4.3.1 and 3.6.2 including a new `AdminClient.incrementalAlterConfigs`
probe, sarama (45 checks, and it asserts "exactly 28 APIs advertised" — see the
M9 follow-up below),
`@platformatic/kafka`, apache/kafka 3.9.1's own tooling for all four affected
CLIs, and kafka-ui against the facade and the oracle side by side. **What was
not:** `confluent-dotnet`, because no .NET SDK was available on the machine that
ran the verification and F4's only change to that suite was one string and one
comment. Treat that row as F3's measurement plus an unverified edit.

**M9 follow-up (2026-08-30).** M9 landed the four transaction keys but left the
sarama suite behind, so that assertion was measuring the wrong number for a day:
it now says **32**, and `wantAPIs` in `sarama/scenarios.go` carries rows for
AddPartitionsToTxn (24), AddOffsetsToTxn (25), EndTxn (26) and TxnOffsetCommit
(28), each `v0..v3`. The `versions` scenario is **49** checks rather than 45 —
the four new rows and nothing else. Re-measured against a live `rig.sh --keep`
facade: the whole suite PASS, 139 checks. The loop below `wantAPIs` still names
only the twelve keys M7 added, on purpose: it records an ABSENCE that M7
inverted, and the transaction keys were never asserted absent there.

| Client | Re-run after M7? | With what | Outcome |
| --- | --- | --- | --- |
| franz-go | **yes**, heavily | 65 new tests in `go/` (21 topics admin, 11 groups admin, 11 idempotence, and F4's 7 configs, 9 offsets/partitions, 6 acls) plus the whole pre-existing suite under `rig.sh` | PASS. `go/` now declares **91** top-level tests; the 4 that skip without `--m5` are the TLS and SASL lane |
| segmentio/kafka-go | **yes**, for cluster mode | `cluster/kafkago_test.go`: two members across two facades, on the OffsetCommit v2 / OffsetFetch v1 floors | PASS, 208 of 208 committed identically through all three nodes |
| apache/kafka 3.9.1 own tooling | **yes**, newly | `kafka-topics.sh` create/list/describe/delete, `kafka-configs.sh --describe` for topics and brokers, `kafka-consumer-groups.sh` list/describe/delete | all pass. This is also the only measurement of the Java **AdminClient** against the new APIs: no dedicated AdminClient probe was written, and `kafka-topics.sh` and `kafka-consumer-groups.sh` ARE that client |
| Java kafka-clients 4.3.1 / 3.6.2 | **yes** | the scored `java-matrix` suite on a DEFAULT (idempotent) producer, plaintext and SASL_SSL | PASS: 4.3.1 matrix 81 checks, 3.6.2 matrix 80 checks, `edges` rc=0 on both, SASL lane 81 checks on 4.3.1. Usable admin versions measured, not assumed: CreateTopics v6, DeleteTopics v5, DescribeConfigs v4, ListGroups v4, DescribeGroups v3, DeleteGroups v2 |
| IBM/sarama | **yes** | `ClusterAdmin` (`ListTopics`, `CreateTopic`, `DeleteTopic`, `DescribeConfig`) and `Producer.Idempotent = true`, plaintext and SASL/TLS | PASS, 126 checks, 0 FAIL |
| Confluent.Kafka (.NET) | **yes** | the whole suite: core, edges and SASL | PASS, 68 checks, 0 FAIL. `AdminClient.ListConsumerGroupsAsync` negotiates ListGroups v4; DescribeConfigs returns 7 broker entries |
| Spring Boot + spring-kafka | **yes** | the stock Boot bean set with `NewTopic` beans, over the TLS lane | PASS, 94 assertions, 0 FAIL. A stock-default (idempotent) producer landed at offset 96 |
| rust-rdkafka, rdkafka-ruby, php-rdkafka | **yes** | the three librdkafka bindings this directory owns, all scenarios plus SASL | PASS: rust 122 assertions, ruby 48 ok, php 50 ok. The Rust lane asserts librdkafka actually SENT InitProducerId v4 on the wire, because a client that self-disables idempotence would still deliver the records |
| @platformatic/kafka | **yes** | all 11 scenarios plus SASL | PASS, 90 ok, 0 FAIL |
| kcat, confluent-kafka-python, node-rdkafka, @confluentinc/kafka-javascript | **no** | idempotence was refused by an ApiVersions check on key 22, which is now advertised | expected to work, not re-measured |
| aiokafka | **no** | same ApiVersions mechanism | expected to work, not re-measured. Its suite still carries the stale "14 keys" text and a fail-open idempotence section |
| kafkajs, kafka-python, brod | **no** | these never send `InitProducerId` unless asked for transactions | unchanged by M7 |
| `apache/kafka:3.9.1` as a differential oracle | **yes** | `differential/rig-diff.sh`, 17 scenarios including `idempotent` and M9's new `transactions` | **0 to classify by hand** (2026-08-30, after M9): `100 divergence(s): 74 deliberate, 26 accepted` from a cold stack, `97: 72, 25` from a warm one. The three extra rows are the ORACLE warming up — its `__transaction_state` partitions are still loading for the first minute, so it answers NOT_COORDINATOR to the first transactional InitProducerId and NONE to the same question later |

The differential total floats from run to run, and the reason is worth knowing
before anyone reads a delta as a regression. After M9 the measurement is **100
divergences from a cold stack (74 deliberate, 26 accepted) and 97 from a warm
one (72, 25)**, and the count that matters is stable at both: **0 to classify by
hand**. The three-row difference between the two is the oracle warming up, as
the row above says. One more entry comes and goes independently of that:
`[ACCEPTED] deletetopics / delete.results_line_up`. The facade answers
DeleteTopics in REQUEST order, name for name; Kafka's controller answers in
whatever order it finishes, which sometimes happens to match the request and
sometimes does not. When it matches there is no divergence to report and the
total is one lower. Every per-name error code agrees either way, so nothing a
client reads differs.

The stale-FAIL counts this table used to carry are **gone**: the assertions that
still claimed CreateTopics, DeleteTopics, DescribeConfigs, ListGroups,
DescribeGroups and `ClusterAdmin.ListTopics` were refused have been inverted
into positive checks of the 21-key surface, and every suite above was re-run to
green against a live stack.

## What every client meets

These hold for all of them, and none is a client-specific defect.

**Idempotent produce works, since M7 F3 (2026-08-29).** `InitProducerId` (API
key 22) is advertised v0-v4 and the per-`(producer, topic-partition)` sequence
window is enforced in `protocols/queen-kafka/src/idempotent.rs`. **No producer needs any
configuration at all any more** — this was the single largest onboarding
papercut the facade had, because `enable.idempotence` has defaulted to true in
the Java client since 3.0 and Spring Boot inherits it.

What the facade actually promises, and it is Kafka's own promise:

* a batch resent after a lost response is answered `error_code = 0` with the
  offsets the original got, and is **not written twice**;
* a batch that would leave a gap in the sequence is refused
  `OUT_OF_ORDER_SEQUENCE_NUMBER` and **nothing is written**;
* an epoch below the highest seen is refused `INVALID_PRODUCER_EPOCH`.

All three were measured against `apache/kafka:3.9.1` side by side and the
answers are identical (`compat/differential`, scenario `idempotent`).

**The caveat, and it is real: the window is in memory and per facade.** A real
Kafka broker persists producer state in the log; this one does not. After a
facade restart, an eviction (65 536 tracked producer-partitions), or a
connection landing on another facade, the next batch of a producer that is not
at sequence 0 is answered `OUT_OF_ORDER_SEQUENCE_NUMBER`. The client's recovery
is KIP-360's epoch bump — which is why v3 is inside the advertised window — and
the cost is **at-least-once for at most the five in-flight batches**. Measured:
`compat/go/idempotent_test.go` SIGKILLs the facade under a live default
producer and the producer keeps running with every record accounted for.

What each client does with idempotence left ON, measured before F3 and after:

| Client | Before M7 F3 | After |
| --- | --- | --- |
| Java 3.6.2 / 4.3.1 / Spring Boot | fatal on the FIRST send after about 400ms; `UnsupportedVersionException: The node does not support INIT_PRODUCER_ID`, then `FATAL_ERROR` for the life of the bean | **sends.** The whole `java-matrix` scored suite now runs on a producer with no overrides; InitProducerId negotiates to v4 |
| franz-go | needed `kgo.DisableIdempotentWrite()` | **not needed.** A default `kgo.NewClient` produces and reads back in order |
| librdkafka family (.NET, Node, Rust, Ruby, PHP, Python) | fatal and immediate, before any wire traffic; "Idempotent producer not supported by any of the 1 connected broker(s)" | **measured for .NET, Rust, Ruby and PHP** (2026-08-29, after F3): all four produce with idempotence left on, and the Rust lane pins InitProducerId v4 on the wire rather than trusting the delivery. Node and Python are the same mechanism but were not re-measured |
| sarama | 51 `InitProducerId` attempts across 51 fresh connections in about 5 seconds, then EOF | **one** InitProducerId v4, one Produce v9, one connection. `Config.Producer.Idempotent = true` produces |
| aiokafka | `IncompatibleBrokerVersion: InitProducerIdRequest cannot be used if the API version is unknown` at `producer.start()` | expected to work, **still not re-measured**. Its own suite's idempotence section fails open, so running it green would not be evidence either |
| @platformatic/kafka | `UnsupportedApiError: Unsupported API InitProducerId`, off ApiVersions, no bytes sent | **measured** (2026-08-29, after F3): 90 ok across all 11 scenarios and SASL. Its idempotent scenario was informational and printed a success line unconditionally; it is now a measurement |
| kafka-go, kafka-python, brod | not reachable: these clients never send `InitProducerId` unless you ask for transactions | unchanged |

**Transactions work since M9 (2026-08-30), in one process, and the boundary is
the sentence that has to travel with the word.** A transaction here is a STAGE
held by one facade process, on the connection that opened it, committed by one
`POST /api/v1/transaction`. `beginTransaction` / `send` /
`sendOffsetsToTransaction` / `commitTransaction` on one producer in one process
is exactly that shape, so it works; a two-phase commit that finishes from
another process does not (see the Flink and Spark rows below).

`initTransactions()` was the 20 second hang F3 recorded, and the cause was not
the one everybody assumed. It was **FindCoordinator**, not InitProducerId: a
producer with `transactional.id` set asks for a TRANSACTION coordinator before
it sends key 22, the facade answered `COORDINATOR_NOT_AVAILABLE` (retriable), and
the Java client re-enqueued the lookup until `max.block.ms` — ~190
FindCoordinator requests over 20 s, zero InitProducerId requests. M9 answers
`key_type = 1` with this facade in single mode. Measured with kafka-clients
4.3.1 on 2026-08-30:

| | `initTransactions()` |
| --- | --- |
| before M9 | 20 000 ms, then `TimeoutException` |
| M9, single mode, cold JVM | **471 ms** and **557 ms** |
| M9, single mode, warm JVM | **119 ms** and **112 ms** |
| M9, cluster mode (`QUEEN_KAFKA_NODE_ID` set) | **214 ms** and **251 ms**, then a fatal `TransactionalIdAuthorizationException` |

Two numbers per row because it was measured on two clean runs of
`compat/transactions/run.sh` (scenarios s2 and s7) rather than once, and the
spread is the JVM's, not the facade's.

The cluster-mode row is a refusal by CONFIGURATION and not by capability, and it
is fast on purpose: 53 is fatal in the Java client, so the call returns instead
of looping on a code that will never change.

**Setting `isolation.level=read_committed` is correct here and costs nothing.**
Both isolation levels return the same records on this facade, because no
uncommitted record ever enters the log: an open transaction's records are staged
in the facade and an aborted transaction's are dropped. Set it anyway if the
application might ever run against a real Kafka. The spelling per family, and a
family with no spelling gets an explicit "not exposed" rather than a
plausible-looking key:

| Family | Spelling |
| --- | --- |
| Java kafka-clients | `isolation.level=read_committed` |
| Spring Boot | `spring.kafka.consumer.isolation-level=read_committed` |
| librdkafka family — confluent-kafka-python, Confluent.Kafka, node-rdkafka, @confluentinc/kafka-javascript, rust-rdkafka, rdkafka-ruby, php-rdkafka, kcat | `isolation.level=read_committed` |
| franz-go | `kgo.FetchIsolationLevel(kgo.ReadCommitted())` |
| segmentio/kafka-go | `ReaderConfig{ IsolationLevel: kafka.ReadCommitted }` |
| IBM/sarama | `Config.Consumer.IsolationLevel = sarama.ReadCommitted` |
| aiokafka | `AIOKafkaConsumer(isolation_level="read_committed")` |
| kafka-python | `KafkaConsumer(isolation_level="read_committed")` (default `"read_uncommitted"`, `consumer/group.py:322`) |
| kafkajs | **inverted, and read_committed is already the DEFAULT.** There is no `isolationLevel` on `ConsumerConfig`; the knob is `readUncommitted: boolean`, default `false`, which `src/index.js:152-155` maps to `ISOLATION_LEVEL.READ_COMMITTED`. Nothing to set |
| @platformatic/kafka | `new Consumer({ isolationLevel: FetchIsolationLevels.READ_COMMITTED })` — a number, from the `FetchIsolationLevels` enum in `dist/apis/enumerations.js` |
| brod | `isolation_level` in the consumer config, and like kafkajs its default is already `read_committed` (`brod_consumer.erl:221`). Nothing to set |

**Every spelling above was read out of the installed package**, not out of a
memory of the docs, because the four the design expected to be missing turned
out to be the four with the surprises: two of them default to `read_committed`
already and one takes a number rather than a string. Nothing here is guessed and
no client in the matrix lacks the control.

**The transaction stage is capped, and the caps have no Kafka analogue.** A
Kafka transaction has no size, because its records are appended as they arrive;
one here is held in memory until commit. Past `QUEEN_KAFKA_TXN_MAX_BYTES`
(8 MiB) or `QUEEN_KAFKA_TXN_MAX_RECORDS` (50 000) the produce is answered
`MESSAGE_TOO_LARGE`; past 62 offsets in one `sendOffsetsToTransaction` it is
`INVALID_COMMIT_OFFSET_SIZE`; a `transaction.timeout.ms` above 900 000 is
`INVALID_TRANSACTION_TIMEOUT`, which is Kafka's own code and Kafka's own default
bound. Each surfaces as a named exception in the Java client rather than a hang,
which is what `compat/transactions` scenario 6 measures.

**One divergence a transactional producer can see:
`RecordMetadata.offset()` is -1 inside a transaction.** No offset exists until
the commit allocates them, so the produce answer carries -1 rather than a
fabricated number. Verified in kafka-clients bytecode that `RecordMetadata`
keeps -1 unchanged rather than adding the batch index, so nothing invented
reaches an application. Every non-transactional produce still answers a real
offset.

**Both halves of the admin surface have landed: TOPICS in M7 F1, GROUPS in M7
F2, and the WRITE half in M7 F4 (2026-08-30).** The advertised table is 32 keys
since M9 added the four transaction ones.
The thirteen admin keys are CreateTopics (19), DeleteTopics (20),
DescribeConfigs (32), AlterConfigs (33), IncrementalAlterConfigs (44),
CreatePartitions (37), ListGroups (16), DescribeGroups (15), DeleteGroups (42),
OffsetDelete (47) and the ACL trio DescribeAcls (29), CreateAcls (30),
DeleteAcls (31).

**What a client's admin object can now do that it could not on 2026-08-29.**
`incrementalAlterConfigs` and the deprecated `alterConfigs` land on
`retention.ms`, and `describeConfigs` reads the value back, so the round trip
that was open all through F1 is closed for every topic this facade created.
`deleteConsumerGroupOffsets` works. `alterConsumerGroupOffsets` always worked
and simply had never been claimed: it sends OffsetCommit at generation -1, which
is the simple-consumer shape `offset_commit.rs` has served since M4, so it needs
no key of its own. `describeAcls`, `createAcls` and `deleteAcls` answer
`SecurityDisabledException` rather than `UnsupportedVersionException`, which is
what a real Kafka with no authorizer answers. `createPartitions` is advertised
and refuses, with the oracle's own sentence for a decrease and for an equal
count.

DescribeCluster (60) is still absent and does not need to be, because Java's
`describeCluster()` and librdkafka's both ride Metadata and already work. **The
trigger that would flip that decision, so nobody re-derives it:** the first
client in this matrix whose `describeCluster()` stops falling back to Metadata.
That is a Kafka-4.x-era client change, it shows up as a red in `java-matrix` or
`confluent-dotnet`, and the answer that day is the handler, not a redesign:
every field of it (`cluster_id`, the controller, the broker list, Kafka's
"omitted" sentinel for authorized operations) is already computed by Metadata
and DescribeGroups.

Measured against a live facade, with apache/kafka:3.9.1's own tooling and with
sarama:

* `kafka-topics.sh --create/--list/--describe/--delete` all work, and so does
  `kafka-configs.sh --describe` for both `--entity-type topics` and
  `--entity-type brokers --entity-name 0`. Since F4, `kafka-configs.sh --alter`
  works too: `--add-config retention.ms=60000` then `--describe` reports
  `60000` sourced `DYNAMIC_TOPIC_CONFIG`, `--delete-config retention.ms` puts it
  back to `-1` sourced `DEFAULT_CONFIG`, and the value survives a facade SIGKILL
  because it lives in Queen KV rather than in the process.
* `kafka-acls.sh --list`, `--add` and `--remove` print
  `Error while executing ACL command: No Authorizer is configured…` and exit 1,
  and the three outputs diff IDENTICAL against apache/kafka:3.9.1 run from the
  same container.
* `kafka-topics.sh --alter --partitions` refuses. A decrease and an equal count
  diff clean against the oracle apart from the tool's own timestamp; an increase
  is where the two part, and the facade's sentence names
  `QUEEN_KAFKA_DEFAULT_PARTITIONS` where the oracle simply widens the topic.
* `kafka-consumer-groups.sh --delete-offsets` deletes (8/8 `Successful`), leaves
  the group in `--list`, refuses with `GroupSubscribedToTopicException` while a
  member is live and deletes nothing, and answers an unknown group
  byte-identically to the oracle.
* sarama's `ClusterAdmin` works. `ListTopics` was the trap — it looks like pure
  Metadata and issues a DescribeConfigs per topic afterwards — and it now
  returns the topic map. `CreateTopic`, `DeleteTopic` and `DescribeConfig` work
  with it.
* Java's `AdminClient.listTopics()` and `describeCluster()` still ride Metadata
  and still work (`describeCluster` returns cluster id `queen`), and librdkafka
  still answers `DescribeCluster` from a plain Metadata request.
* Confluent.Kafka's `ListConsumerGroupsAsync` **works** and the process exits 0.
  It used to abort with a glibc double free before sending anything, so merely
  advertising key 16 is the whole fix; measured on 2.15.0, negotiating
  ListGroups v4.
* `kafka-consumer-groups.sh --list`, `--list --state Stable|Empty`, `--describe`
  (with `--members --verbose`, `--state` and `--offsets`) and `--delete` all
  work against apache/kafka:3.9.1's own tooling. `--describe` prints the members
  x partitions x lag table with HOST, CLIENT-ID and CONSUMER-ID; `--delete`
  answers `GroupNotEmptyException` for a running group and
  `Deletion of requested consumer groups ('g') was successful.` once it is
  empty; a group nobody has heard of prints
  `Consumer group 'g' does not exist.`
* `--reset-offsets` is untested and not claimed: it rides OffsetCommit from an
  admin client, which is advertised, but nothing has measured it.

Three things a create does NOT do, all deliberate and all answered honestly
rather than obeyed:

* **`--partitions N` is accepted and not honoured.** Queen declares no width per
  queue: `/configure` creates the queue row and a partition row materialises on
  the first push to that lane. The width is
  `max(live lanes, QUEEN_KAFKA_DEFAULT_PARTITIONS)`, which is a property of the
  facade, and the create's own answer reports it — so the number the create
  returns is the number the client's next Metadata agrees with.
  `kafka-topics.sh --create --partitions 4` prints `Created topic …` and the
  following `--describe` says `PartitionCount: 8` on a facade configured for 8.
* **`--replication-factor N` is accepted and reported back as 1.** One logical
  broker. Refusing RF>1 would break every provisioner whose default is 3.
* **`--config cleanup.policy=compact` is REFUSED**, INVALID_CONFIG, and this is
  the reason CreateTopics does not unlock Kafka Connect: Connect's three
  internal topics are compacted (hard-coded in
  `TopicAdmin.NewTopicBuilder.compacted()`) and its config topic is a compacted
  log used as a database. A facade that accepted the setting and compacted
  nothing would let Connect start and then lose the connector configuration on
  the first restart. It fails at startup instead. Any config name outside
  `cleanup.policy=delete` and `retention.ms` is refused for the same reason:
  accepting `min.insync.replicas=2` silently would be telling a client it got a
  durability setting it did not get.

**`retention.ms` round-trips, since M7 F4, and only for a topic this facade
created.** CreateTopics can set it (it becomes Queen's
`retentionEnabled`/`retentionSeconds`), IncrementalAlterConfigs can change it,
and DescribeConfigs reports it back. Queen still exposes no HTTP read of a
queue's configuration, so the value does not come from Queen's own record of it:
the facade persists the options bag it posted to `POST /api/v1/configure` under
`qk:topiccfg:<topic>` in Queen KV, pinned to the queue's `id`, and reports
retention out of that. A topic the facade did not create has no such record, and
for it a describe reports only two keys, `cleanup.policy=delete` and
`min.insync.replicas=1`, both true of every Queen queue by construction. This is
the asymmetry that used to read *"writable and not readable"* here; what remains
of it is the staleness window described under the deviations above.

**A Kafka client can now delete a Queen queue.** `kafka-topics.sh --delete`
reaches `DELETE /api/v1/resources/queues/:queue` under the connection's own
credential. That is no new privilege — the same bearer could always issue the
same HTTP call — but it is a new blast radius on a facade that until M7 could
only create, and it applies to queues native Queen producers share. Scope the
token accordingly. Committed offsets under `qk:group:*:<topic>:*` are not
removed with the topic; Kafka behaves the same way.

**Topics are created by producing to them.** Auto-create fires on the Metadata
request, at `QUEEN_KAFKA_DEFAULT_PARTITIONS`. From Metadata v4 up the client's
`allow_auto_topic_creation` flag is honoured, so `kafka.Client.Metadata` in
kafka-go (v8, flag false) never creates and answers UNKNOWN_TOPIC_OR_PARTITION
for ever, while `Dialer.LookupPartitions` (v6, flag true) does create. Below v4
the field does not exist on the wire and auto-create cannot be refused, which is
on the deliberate list; brod is the client where that bites, because it pins
Metadata at v2 and has no way to say no.

**zstd usually does not compress, and it is the client that decides.** librdkafka
gates zstd on the broker advertising Fetch v10; the facade caps Fetch at v6 on
purpose (v7 introduces fetch sessions). So every librdkafka binding logs
"Broker does not support compression type zstd: not compressing batch" and sends
the batch uncompressed. Records still round-trip byte-exact. The clients that
gate zstd on the Produce version instead (Java, Spring, @platformatic/kafka,
aiokafka, kafka-python, kafka-go, brod, sarama) compress zstd normally.

**Consumer groups cost the join window.** `QUEEN_KAFKA_GROUP_JOIN_DELAY_MS`
defaults to 3000, Kafka's `group.initial.rebalance.delay.ms`. Every group
formation in every suite measured between 3.0 and 3.3 seconds to the first
record. No suite shortens it.

**Session timeout must be inside 6000..300000 ms** or JoinGroup answers
INVALID_SESSION_TIMEOUT (26). Every client's default is inside that window, so
this only bites someone who tuned it.

**An out-of-window version on an advertised key closes the connection** rather
than answering an error code, which is what Apache Kafka does and what
[ERRORS.md](ERRORS.md) documents. The consequence for a user is that the failure
carries no error text: sarama below v1.46.0 and aiokafka 0.12 both fail as bare
EOF or as an unbounded reconnect loop, and the facade's log line is the only
place the reason exists.

**ApiVersions above v3 is answered, not closed.** A client that opens with v4
(Java 4.x, kafka-clients 3.9.2, kafka-python 2.3.2 and 3.0.11, sarama at
`Config.Version >= 3.9`) gets the v0-encoded UNSUPPORTED_VERSION body carrying
ApiVersions' own window, retries at v3, and proceeds. Every client that tried
this handled it correctly.

**SASL/PLAIN accepts an authorization identity equal to the username, and
refuses one that differs from it.** This is Apache Kafka's own `PlainSaslServer`
rule, and it is what makes the two pure-Python clients work: both build their
initial response by joining the username to itself. A differing authzid is a
request to act as somebody else, which the facade cannot grant and refuses with
error code 58 rather than ignoring. See the fixed-on-2026-08-29 section below
for the wire bytes on both sides of that line.

## Reaching the facade in Queen Cloud (2026-08-30)

Every other row in this file was measured with the facade wired straight to a
**broker**. In Cloud it is wired to the cell **proxy** instead, and that is the
whole of the difference: one variable, `QUEEN_URL`. What it buys is that a Kafka
client becomes a tenant of something. A facade pointed at the broker serves Kafka
clients with no quota, no metering and no isolation.

```text
Kafka client --TCP--> queen-kafka --QUEEN_URL--> queen-proxy --> queen
                       (facade)                  auth, tenant     (broker)
                                                 scoping, quotas,
                                                 metering
```

**Measured: franz-go, 16 of 16 scenarios green from a clean machine, 2026-08-30**
([`cloud/`](cloud)). That is one client row, not fifteen. The proxy sits on the
**HTTP** hop between the facade and Queen and never on the Kafka wire, so nothing
it does is client-specific and no row in the matrix above is expected to change.
But this file has only ever claimed what it measured, so: every other row's Cloud
behaviour is inferred, and the inference is stated here rather than hidden in a
PASS.

### What a client meets in Cloud that it does not meet beside a broker

- **The `read` scope is not optional.** The facade checks a credential with
  `GET /api/v1/resources/queues`, which the proxy classifies as a read route, and
  every Kafka client issues Metadata before anything else. A key scoped `consume`
  alone is refused `403` at SASL, the connection never opens, and the client never
  reaches Fetch. A consumer needs `consume` + `read`, a producer `produce` +
  `read`, a transactional producer `produce` + `consume` + `read`.
- **The tenant of a connection is the tenant of the SASL password, and nothing
  else.** One facade advertises one address to every client whatever name it
  dialled (`advertised_host` is per process), so the credential is the authority
  and per-cluster SNI would need one facade process per cluster.
- **Two credentials of one tenant are ONE consumer group.** The facade asks
  `GET /auth/me` who a credential is and files the group under the answer, so a key
  rotation or a per-service key does not fork a group's committed offsets.
- **A `403` carries the proxy's own sentence**, bounded and stripped of control
  bytes, in `error_message` wherever the response has that field. A `429` is not
  an error at all: it becomes `throttle_time_ms` on Produce, Fetch and Metadata,
  which every client obeys natively.

### Two deployment requirements that are easy to miss

Both of these cost real time before they were written down, and neither is
visible from the Kafka side until every request fails.

1. **The proxy must be told the listener's host is shared**
   (`QUEEN_PROXY_SHARED_HOSTS`), or else be given a per-cluster hostname to route
   on. On a shared host the proxy resolves the cluster from the credential; on an
   unknown host it resolves nothing.
2. **The BROKER has a second, independent KV gate.** With
   `QUEEN_TENANCY_HEADER=true`, which every cell runs, the broker derives
   `kv_require_grant`, and the **absence** of a `queen.kv_quota` row is a denial
   rather than a permission (`server/src/config.rs`, `server/src/quota.rs`).
   Without one row per broker tenant every offset commit is `403` at the broker,
   past everything the proxy already allowed. `cloud/rig-cloud.sh` inserts them
   and says why on the line.

## Deploying more than one facade: CLUSTER MODE (2026-08-29)

Until this date this file and the webdoc gave one answer to "can I run two
facades": no, a group whose members reach different facades will split. That
answer is now conditional, and the condition is one environment variable.

### The supported shapes

| Shape | Status | What a client sees |
| --- | --- | --- |
| **One facade**, one Queen deployment | Supported, and byte-identical to before | One broker, node id 0, `cluster_id` `queen`. With `QUEEN_KAFKA_NODE_ID` unset no cluster code is read, spawned, allocated or written. |
| **Several facades, disjoint work**: each with its own topics, or its own consumer groups | Supported, and always was | Each is its own broker. Nothing they share can conflict. |
| **Several facades, one consumer group, cluster config ABSENT** | **BROKEN, and still the default** | Every facade answers FindCoordinator with itself, so the group forms twice and each generation assigns every partition; and an offset commit is an unconditional upsert, so a racing commit rewinds a committed offset. Both reproduced on 2026-08-29, see below. |
| **Several facades, one consumer group, `QUEEN_KAFKA_NODE_ID=1..64` on each** | **Supported since 2026-08-29** | One cluster: every node lists every live node, all of them name the same coordinator for a given group, and a non-owner refuses the group RPCs with `NOT_COORDINATOR` (16), which every client answers by re-running FindCoordinator. Also needs `QUEEN_TOKEN`, all facades on ONE Queen tenant, and a distinct, individually reachable `QUEEN_KAFKA_ADVERTISED_ADDR` per node. |
| **Several facades behind ONE address** (a VIP, a Kubernetes `Service`, a load balancer) | **Anti-pattern. Cluster mode does not fix it and makes it worse** | Clients re-dial the address they are handed. If every node advertises one address, FindCoordinator says "node 2, at the VIP", the client dials the VIP, the balancer routes it to node 1, and node 1 answers `NOT_COORDINATOR` because it is not the owner. That is an infinite redirect, not a slow path, and nothing detects it: the node ids differ, so every boot claim succeeds. Give each facade its own address, as you would each broker of a real cluster. |

### The evidence

[`compat/cluster/`](cluster) stands up one Postgres, **two mesh-wired Queen
brokers on it**, **three clustered facades** (nodes 1 and 3 in front of broker A,
node 2 in front of broker B, so a cross-broker read is on the critical path of
every group assertion), **one facade with the cluster config absent** and **two
unclustered facades**, then runs nine scenarios. Nine pass, no skips. The
clients are real: franz-go, a `segmentio/kafka-go` second opinion, and raw
`kmsg` over TCP wherever a client would route around the thing under test.

The acceptance, three members of one group each bootstrapped at a **different**
facade:

```
the cluster says group qkc-g-…-310000 is coordinated by node 1 at 127.0.0.1:32410
JoinGroup at node 2 (non-owner): error 16 NOT_COORDINATOR
JoinGroup at node 3 (non-owner): error 16 NOT_COORDINATOR
all 3 members are in generation 1 with distinct member ids [...0000 ...0002 ...0001]
m1@127.0.0.1:32410 read partitions [0 1 2]
m2@127.0.0.1:32411 read partitions [6 7]
m3@127.0.0.1:32412 read partitions [3 4 5]
committed offsets through node 1 sum to 208 of 208 produced
```

208 records over 8 partitions: 208 distinct keys, zero duplicates, no partition
read twice, and the same final committed map read back through all three nodes.
Throughout the run a separate watcher polled the committed offsets every 250 ms
through a node deliberately chosen to be a **non-owner** (`cluster_test.go:60`),
and took 152 samples without one regression; the summed line above is the final
read, which the test takes through the owner.

An independent reproduction on a different port block picked a **different**
coordinator (node 2) and a **different** assignment (`[6 7]` / `[0 1 2]` /
`[3 4 5]` to the same three members). That is what a live rendezvous hash over a
live node set looks like, and what a replayed cache does not.

The rewind, and the fix, in one scenario:

```
SPLIT BRAIN, as documented: :32414 says the coordinator is :32414, :32415 says :32415
REWIND REPRODUCED on the unclustered pair: committed 50 through :32414, 16 through :32415,
  stored offset is now 16
the same sequence in cluster mode: 50 accepted at owner node 1, 16 REFUSED at node 2
  with error 16 NOT_COORDINATOR
all 3 nodes still read the committed offset as 50: the refused commit wrote nothing
```

Node death, the group owner SIGKILLed mid-consumption:

```
ownership moved from the dead node 2 to node 1 in 3.423s (budget: TTL 3s + join delay 3s)
node 2 has left every survivor's broker list
0 keys were redelivered after the failover
committed offsets sum to 128 of 128 produced, read back through node 3
```

Metadata, and the reason a non-leader still serves data: leadership here is an
**advertisement**, not an access control. Every node has the data, because there
is one shared Postgres underneath.

```
leaders spread over the live set as map[1:2 2:5 3:1]
a producer bootstrapped only at node 1 wrote all 8 partitions
every partition was fetched from a node that does not lead it, and answered
200 group ids, 3 nodes, zero disagreements
```

And the regression proof, the same acceptance body against a facade with the
cluster config absent: `one broker: node 0 at 127.0.0.1:32413, cluster_id
"queen", controller 0`, `JoinGroup at the unconfigured facade: error 0 (not a
routing refusal)`, 208 of 208.

### What cluster mode costs a client, and what it does not

- **No client configuration changes.** `NOT_COORDINATOR` (16) is a code every
  Kafka client has handled since 0.9: it re-runs FindCoordinator and retries.
  Two independent client implementations were used precisely so that the
  redirect dance is not proven by one library's quirk.
- **`replicas == isr == [leader]` and `leader_epoch == -1`**, on every partition,
  on every node. There is one copy of the data and no replication protocol; the
  broker list is a routing table, not a durability claim.
- **Only the group WRITES are gated.** JoinGroup, SyncGroup, Heartbeat,
  LeaveGroup, OffsetCommit, DescribeGroups and DeleteGroups are refused at a
  non-owner, before any group actor is spawned. **OffsetFetch, ListOffsets,
  Produce and Fetch are served by every node deliberately**: OffsetFetch reads
  shared state whose answer is identical everywhere, and an `assign()`-based
  simple consumer holding any connection would break if it were refused. The
  acceptance leans on that, sampling committed offsets through a non-owner
  throughout the run.
- **ListGroups can answer `Unknown`.** Group existence is durable and shared, so
  any node lists every group of the tenant; live membership is one process's, so
  a group whose members are attached to another facade is reported `Unknown`
  (a real Kafka state string) rather than the plausible wrong answer `Empty`.
  With the cluster config absent nothing is ever `Unknown`.
- **Ordering across a leadership move.** A producer with
  `max.in.flight.requests.per.connection > 1` and idempotence OFF can have two
  batches land out of order when its metadata moves, because every node accepts
  writes for every partition and refusing would make each membership change a
  metadata storm. Apache Kafka has the identical hazard on a leader change
  without idempotence, and the client-side fix is the same:
  `max.in.flight.requests.per.connection=1`, or leave idempotence on (it is
  advertised since M7 F3, though its window is per facade, see above).

### The caveats an operator meets, in the order they bite

1. **Rolling restarts are fixed and measured (2026-08-30).** A stopping node
   now deregisters on SIGTERM (its registry row deleted, fenced on the version
   it holds, measured 0.9 to 2.0 ms across four stops) and a replacement on the
   same node id adopts a dead holder's row instead of exiting FATAL, watching
   it for one TTL plus a heartbeat first; only a row whose version keeps moving
   stays fatal, which is the true duplicate-id case. The rolling-restart
   scenario in this suite moves every probe group in about 2.1 s at the
   product cadence; a SIGKILLed node (nobody ran the stop path) rebinds in
   about 10.3 s, the TTL backstop. The earlier text in this spot described the
   pre-fix behaviour and was superseded by `rolling_test.go`.
2. **The compare-and-set fence is proven at the KV layer, not from outside.**
   The acceptance shows the *ownership guard* refusing a non-owner. The deeper
   mechanism, a stale owner whose commit is refused by a compare-and-set on a
   fence key, is covered by unit tests and by a direct probe of the stored
   procedure's precondition contract. Forcing a stale view from outside needs a
   test-only switch the facade does not have.
3. **Nothing has run more than three nodes.** The registry ceiling is 64 and the
   hash is uniform, but three is what was measured.
4. **Two Queen tenants running a group of the SAME NAME are coordinated by the
   same node.** The ownership hash takes the group id and never the tenant,
   because the tenant key is seeded per process and a tenant-aware hash would
   never converge across facades. Harmless: they stay two coordinator entries
   over two `queen.kv` rows, with separate fences.

## The M7 admin APIs: acceptance and open decisions (2026-08-29, extended 2026-08-30)

What each new key is and what it answers is in [ERRORS.md](ERRORS.md), and what
a client can do with it is under [What every client meets](#what-every-client-meets)
above. This section is the evidence and the decisions still open. The last seven
rows are F4's and were added on 2026-08-30.

| API | Acceptance | The caveat a user meets |
| --- | --- | --- |
| CreateTopics (19) v2-v6 | 21 franz-go tests across the three topic APIs, 3 differential scenarios, `kafka-topics.sh --create`, sarama `ClusterAdmin.CreateTopic`, Spring `NewTopic` beans | `--partitions` and `--replication-factor` are accepted and NOT honoured; `cleanup.policy=compact` is refused, which is why this does not unlock Kafka Connect |
| DeleteTopics (20) v1-v5 | the same suites, `kafka-topics.sh --delete` | deletes the underlying Queen queue, which native non-Kafka producers may share |
| DescribeConfigs (32) v1-v4 | the same suites plus 6 new franz-go checks, `kafka-configs.sh --describe` for topics and for `--entity-type brokers --entity-name 0`, kafka-ui's topic Settings tab side by side with the oracle | a topic describe reports three keys: `cleanup.policy=delete`, `min.insync.replicas=1` and, **for a topic this facade created**, `retention.ms`. A topic created outside the facade still omits retention |
| ListGroups (16) v0-v4 | 11 franz-go tests, `kafka-consumer-groups.sh --list` with and without `--state`, Confluent.Kafka `ListConsumerGroupsAsync` | truncates at 10 000 groups with a log line, because the wire has no truncation flag; can answer `Unknown` in cluster mode |
| DescribeGroups (15) v0-v3 | the same suites, `kafka-consumer-groups.sh --describe --members --verbose --state --offsets` | none beyond the shared list |
| DeleteGroups (42) v0-v2 | 11 franz-go tests across the three group APIs, `kafka-consumer-groups.sh --delete` | **irreversibly removes committed offsets**, which otherwise never expire on their own. Refuses a group with members (`GroupNotEmptyException`), so a merely STOPPED group is deletable and loses its position |
| AlterConfigs (33) v0-v2 | 7 franz-go tests in `go/admin_configs_test.go`, Java `AdminClient.alterConfigs` on kafka-clients 3.6.2 and 4.3.1 | the deprecated FULL-REPLACEMENT form: a key the request does not name is reset to its default, so an alter naming only `cleanup.policy=delete` **turns retention off**. That is what a real broker does with key 33; prefer key 44 |
| IncrementalAlterConfigs (44) v0-v1 | the same suite, `kafka-configs.sh --alter --add-config` / `--delete-config`, Java `incrementalAlterConfigs`, kafka-ui's Settings tab | **only on topics this facade created.** Everything else is `INVALID_CONFIG` naming the reason. The vocabulary is three keys: `retention.ms`, `cleanup.policy` (delete only) and `min.insync.replicas` (1 only); APPEND and SUBTRACT are refused on the two scalar keys |
| CreatePartitions (37) v0-v3 | 6 franz-go tests, a differential scenario, `kafka-topics.sh --alter --partitions` diffed against the oracle | **an advertised refusal.** A decrease and an equal count answer Kafka's own sentences byte for byte; an increase answers the facade's own, naming `QUEEN_KAFKA_DEFAULT_PARTITIONS`. Queen declares no width per queue, so there is no write that widens one topic |
| OffsetDelete (47) v0 | 9 franz-go tests, a differential scenario, `kafka-consumer-groups.sh --delete-offsets` with and without a live member | **irreversibly removes committed offsets**, like DeleteGroups. Keeps Kafka's subscription rule: a live consumer group's subscribed topics answer `GROUP_SUBSCRIBED_TO_TOPIC`, everything else is deletable. The group stays in `--list` afterwards |
| DescribeAcls (29), CreateAcls (30), DeleteAcls (31) v1-v3 | 6 franz-go tests, a differential scenario at **zero divergence**, `kafka-acls.sh --list/--add/--remove` diffed IDENTICAL against the oracle | every call is `SECURITY_DISABLED`, at every version, for every filter. There is no ACL model to read or write: authorization is Queen's, over a bearer token |

### The protocol facts settled against the oracle before they were coded

Each was measured on `apache/kafka:3.9.1` first and implemented to match, and
each is pinned in the differential. The first two are F1's; the rest are F4's,
and two of them refuted what the design had written down:

- A topic name **repeated inside one CreateTopics request** is answered
  `INVALID_REQUEST` and **none of the request's topics are created**.
- An **empty** (as against null) `configuration_keys` in DescribeConfigs means
  **every key**, not none. This is load-bearing rather than pedantic:
  `kafka-protocol`'s `DescribeConfigsResource::default()` is `Some(vec![])`, so a
  hand-rolled client that never touches the field would otherwise be answered
  nothing at all.
- **The ACL family uses TWO different sentences, not one.**
  `AclApis.handleDescribeAcls` builds its response by hand and sets *"No
  Authorizer is configured on the broker"* with no full stop, while CreateAcls
  and DeleteAcls raise `SecurityDisabledException("No Authorizer is
  configured.")`. The first implementation used one string for all three and it
  cost seven unclassified differential keys against a zero-divergence bar. This
  is the clearest case in the campaign for the rule that the sentence is
  recorded off the wire and never typed in from a document.
- **CreatePartitions' refusal sentences are KRaft's, not ZooKeeper's.** The
  decrease reads *"The topic X currently has N partition(s); M would not be an
  increase."*, and the ZK-era broker phrased the same case differently, so
  recalling the wording would have produced a near-miss.
- **There is no separate "below 1" case.** `--partitions 0` against a topic of
  width 4 answers the DECREASE sentence on the oracle, because KRaft's
  `count <= current` comparison catches every non-positive count before any
  lower bound could. A dedicated branch would have answered a sentence the
  oracle never sends. For the same reason the width comparison runs BEFORE the
  replica-assignment check: a decrease carrying an assignment is
  INVALID_PARTITIONS on a real broker, not INVALID_REPLICA_ASSIGNMENT.
- **Deleting the last offsets of an already-empty group makes that group vanish
  from `--list` on the oracle**, while a partial delete leaves it listed. The
  facade keeps it listed either way; see the deviation below.

### Where the facade knowingly answers differently from apache/kafka:3.9.1

Two are shape-only and classified ACCEPTED in the differential, because no
client can read the difference: the facade answers **one CreateTopics result per
request entry** where Kafka collapses a duplicated name to one (both answer
`INVALID_REQUEST`, both create nothing, and every client keys results by name),
and it answers **DeleteTopics in request order** where Kafka's controller answers
in its own (every per-name code matches).

One is behavioural and is Alice's to ratify. **A batch at sequence 42 from a
producer the facade has never seen** is answered `OUT_OF_ORDER_SEQUENCE_NUMBER`
(45); Apache Kafka 3.9.1 **accepts it** with `error_code = 0`. The design
predicted the opposite and the measurement refuted it. The refusal was kept on a
different argument than the one the design gave: in Kafka an absent producer
entry means "aged out", which is rare, while here it means "this facade
restarted", which is common, so accepting would leave the sequence window
silently unenforced after every restart. The cost of refusing was measured
rather than assumed: the producer recovers via KIP-360's epoch bump and nothing
is lost. It is one line in `idempotent.rs::check()` to reverse.

F4 adds five more, all of them classified `deliberate` in the differential and
none of them a defect:

- **CreatePartitions refuses an INCREASE**, where the oracle widens the topic.
  This is the one genuine capability gap of the three cases: Queen declares no
  width per queue, a lane exists once something has been written to it, and the
  advertised width is `max(live lanes, QUEEN_KAFKA_DEFAULT_PARTITIONS)` whose
  second half is a broker start-up knob. The refusal names the knob and the
  alternative (produce to the higher lanes directly, which materialises them).
- **A tracked topic with no retention set reports `retention.ms = -1`**, where
  Kafka's own default is 604800000. Queen's default is retention OFF, and OFF
  IS Kafka's `-1`, so the facade is reporting its own truth rather than Kafka's
  number.
- **`read_only` is per row**, where a Kafka broker computes it per resource.
  `cleanup.policy` and `min.insync.replicas` are `true` because the only value
  either accepts is the one already reported; `retention.ms` is `false` on a
  tracked topic and `true` on an untracked one. A UI that greys out its edit
  button on this flag is being told the truth, which is the property the flag
  was reported for.
- **An alter on a topic this facade did not create is refused**
  `INVALID_CONFIG`, where a real broker would apply it. The reason is Queen's,
  not Kafka's: `POST /api/v1/configure` is a whole-row upsert over nineteen
  columns and thirteen of them have no HTTP read, so a partial alter would
  silently reset a tenant's lease time, retry policy and dedup window. On a
  deployment that predates F4 this is every topic. There is no safe alternative
  short of a Queen-side read of the config columns.
- **OffsetDelete leaves the group in `--list`** even after its last offset is
  removed, where the oracle drops an already-empty group at that point. Removing
  the index row would need a prefix walk on every OffsetDelete to find out
  whether anything is left, and would make this API a second way to delete a
  group. DeleteGroups stays the one and only thing that makes a group stop
  existing.

One more is not a divergence from the oracle but from the facade's own honesty
rule, and it is named here because it is the residual risk of the whole config
round trip. **A retention changed OUTSIDE this facade**, in the Queen console or
by another SDK, between two facade writes **is invisible**, and the value
`describeConfigs` reports is the one the facade last applied. The record is
pinned to the queue's `id`, so a queue dropped and recreated under the same name
IS caught and the key is omitted rather than answered stale. What is not caught
is an in-place edit. It is the same last-writer-wins two admins have against a
real Kafka, and the clean fix is a Queen-side read of the config columns.

### Open decisions, all of them Alice's

None of these is a defect; each is a shipped choice that nobody has ratified.

1. **DeleteTopics is advertised at all.** A Kafka client can now destroy a Queen
   queue that native producers share. It is no new privilege (the same bearer
   could always issue the same HTTP DELETE) but it is a new blast radius, and it
   is reachable from `kafka-topics.sh`. If the answer is no, the row comes out of
   the advertised table and nothing else depends on it.
2. **DeleteGroups deviates from a documented Queen promise.** "Committed offsets
   never expire on their own" is still true, and now there is a way to remove
   them irreversibly from an admin CLI. A merely stopped group loses its
   position. The plan carries the flag; the deviation is unratified.
3. **The admin APIs have no authorization model.** There is SASL and there is
   tenant scope, and inside a tenant any authenticated client can create topics,
   delete topics and delete groups. Apache Kafka has ACLs here. A default-OFF
   knob for the destructive half is the cheap posture until there is a role
   model.
4. **`num_partitions` and `replication_factor` are accepted and not honoured**,
   and the create's own answer reports the facade's real width and 1. Refusing
   would break every provisioner whose default is 3.
5. **`retention.ms` now round-trips, out of the facade's own record rather than
   out of Queen.** F4 closed the asymmetry this entry used to describe, and the
   way it closed it is the thing to ratify: the facade persists the options bag
   it posted to `/configure` in Queen KV under `qk:topiccfg:<topic>` and reports
   retention from there. That is bookkeeping the facade owns, not a read of the
   source of truth. The clean fix is one field added to `get_queue_detail_v2` so
   the config columns can be read back, and that is a `server/` change nobody in
   this campaign could make. Alice's call whether to ask for it.
6. **`cleanup.policy=compact` is refused**, which keeps Kafka Connect out. A
   facade that accepted the setting and compacted nothing would let Connect
   start and then lose its connector configuration on the first restart.
7. **OffsetDelete is advertised, and it is the second irreversible delete of
   committed offsets** reachable from an admin CLI (F4). No new privilege, since
   the same bearer can remove the same KV keys over `POST /api/v1/kv`, but a group
   that loses its position runs `auto.offset.reset` on its next start. Kafka's
   subscription rule is kept exactly, so a live group's subscribed topics are
   refused; a STOPPED group's are not. This is the same ratification DeleteGroups
   is already waiting on, widened by one API.
8. **The ACL family answers `SECURITY_DISABLED` rather than being absent**, and
   the message says nothing about Queen's real authorization. It is the oracle's
   own sentence, and diverging from it to say more would cost the zero-divergence
   acceptance. The fuller explanation lives in [ERRORS.md](ERRORS.md) and in this
   file. If Alice would rather the wire said something Queen-specific, the cost
   is that differential scenario's bar.
9. **`QUEEN_KAFKA_ALTER_UNTRACKED` does not exist, deliberately.** An escape
   hatch that let an alter proceed on a topic the facade has no record of would
   have to assume the other thirteen `/configure` columns are at their stored
   procedure defaults, and on a live queue that assumption silently resets a
   tenant's dedup window. If an escape hatch is wanted it should be explicit,
   default-off and loudly logged, and it should be a decision rather than a
   convenience.

The Cloud shape (2026-08-30) added four more, and they are Alice's in the same
sense: each one works, and each one is a choice nobody has ratified.

10. **The two spellings of "commit an offset" go through different route
    classes.** A plain offset commit is a `POST /api/v1/kv` batch under the
    reserved `qk:` prefix, which the proxy reclassifies as `Consume` so it passes
    on a plan with no `kv` feature. A TRANSACTIONAL offset commit rides
    `POST /api/v1/transaction`, which the proxy classifies as `Produce`, so it is
    authorized and metered as the push it is and never meets the `kv` gate its
    non-transactional twin was excused from. Both halves work and both are
    measured. The asymmetry is the thing to ratify, and it can be closed from
    either end.
11. **A Kafka Fetch is metered as a request and not as a delivery.** A tenant
    consuming a million records through the Kafka wire is billed for the requests
    that carried them, not for the records. That is a pricing decision, and it is
    written down here so it is not discovered from an invoice.
12. **The parked-pop gauge cannot see Kafka consumers.** A long-polling Fetch
    takes no parked slot at the proxy, so the gauge an operator watches for
    consumer pressure reads zero however many Kafka consumers are waiting. Either
    the gauge grows a Kafka lane or the dashboard says the gauge is native-only;
    today it does neither.
13. **Routing is by credential rather than by SNI**, which is a consequence of
    `advertised_host` being per process rather than per SNI lane. It is the shape
    a shared Kafka listener actually has in Cloud, so it is the one proven; a
    per-cluster SNI lane would mean one facade process per cluster and is not
    ruled out, only unbuilt.

## Fixed on 2026-08-29, in the working tree and NOT committed

The campaign above found two facade defects. Both were fixed the same day, both
were re-proved by real clients against a running facade, and neither is
committed yet: the changes live in `protocols/queen-kafka/src/` in Alice's working tree.
Anyone reading this from a fresh checkout will still see the old behaviour.

### brod could not produce at all (`produce.rs`): FIXED

Stock brod could not produce a single message. Every Produce was refused with
TRANSACTIONAL_ID_AUTHORIZATION_FAILED (53), which brod classes as not
retriable, so `brod_producer` exited on the first send and never recovered:

```
** {{not_retriable,{produce_response_error,<<"brod-main-full1">>,0,-1,
     transactional_id_authorization_failed}},
    [{brod_producer,handle_info,2,[{file,".../brod_producer.erl"},{line,377}]}
```

The facade logged eight of those, one per partition, and the value was empty:

```
WARN kafka: produce with a transactional id: transactions are out of scope transactional_id=
```

The cause is one field on one side of a contradiction inside kafka_protocol
4.3.6. Its schema declares the field nullable, `kpro_schema.erl:212`:

```erlang
req(produce, V) when V >= 3, V =< 8 -> [{transactional_id,nullable_string}, ...]
```

but the hand-rolled encoder at `kpro_req_lib.erl:308` encodes it as a plain
`string`, and `kpro_lib.erl:140` turns a null string into `""`. So a
non-transactional brod produce puts an empty string on the wire where the schema
says null, and `src/handlers/produce.rs` treated any present transactional id as
a transaction.

The fix reads an empty transactional id as the absent one it was meant to be:

```rust
if let Some(id) = req
    .transactional_id
    .as_ref()
    .filter(|id| !id.0.as_str().is_empty())
{
```

This matches Apache Kafka, which does not gate on the field at all: a produce is
transactional to a real broker when the RECORD BATCH carries the
`isTransactional` attribute bit. That bit is what `stage` reads, which is where
the decision always belonged. Verified with a hand-built raw Produce v7 frame,
no Kafka library involved, three sends differing in one field:

```
ok    transactional_id=absent (protocol null, -1)       error_code=0   (want 0)  baseOffset=0
ok    transactional_id=empty string (what brod sends)   error_code=0   (want 0)  baseOffset=0
ok    transactional_id=non-empty "tx-probe"             error_code=53  (want 53)  baseOffset=-1
```

**The third row is history since M9 (2026-08-30).** A non-empty
`transactional.id` is no longer a refusal: it is a stage, and the batch is
written by `EndTxn(commit)` at `base_offset = -1` — the same -1 in that column,
now for the opposite reason. What still answers 53 is a transactional id in
CLUSTER MODE, and what still answers `INVALID_TXN_STATE` is the
`isTransactional` bit on a request that carries no id at all. The empty-string
row, which is the one this section exists for, is unchanged and still the reason
every Elixir producer can produce here.

Stock brod, with `kpro_req_lib.erl:308` confirmed unpatched before the run, is
now 58 assertions and 0 failures: 512 messages over 8 partitions with keys and
four headers, all five codecs byte-exact including zstd,
`brod_group_subscriber_v2` group consume with per-partition order, OffsetCommit
and OffsetFetch, resume from committed with no replay and no loss, watermarks,
and a mid-log seek. Across that whole run the facade logged the refusal zero
times, so it is not merely survived, it is never reached.

The suite's opt-in `patch-kpro-txnid.sh` overlay is no longer needed against
this facade. It is kept as documentation of the upstream kafka_protocol
schema-versus-encoder disagreement, which this fix routes around rather than
cures.

brod is also the most valuable row in the matrix for a reason unrelated to the
defect: it keeps its own narrow version table
(`brod_kafka_apis:supported_versions/0`) and therefore sits at the BOTTOM of the
advertised windows where every other client sits at the top. Metadata v2,
ListOffsets v2, FindCoordinator v0 and OffsetFetch v2 are the least-exercised
part of the compat surface, and all of them work.

Recorded for the Elixir ecosystem specifically: broadway_kafka and kaffe both
sit on brod, so this row is most of Elixir production usage.

### SASL/PLAIN refused both pure-Python clients (`sasl.rs`): FIXED

kafka-python and aiokafka each build their PLAIN initial response by joining the
username to itself, putting the username in the authorization-identity field:
`'\0'.join([username, username, password])` in `kafka/sasl/plain.py`, and
`"\0".join([username, username, password])` at `aiokafka/conn.py:616`. The
facade refused any non-empty authzid, so both were locked out of every SASL
listener with no client-side workaround, because in those clients one config key
fills both fields: emptying the authzid empties the username too, and the
response is then refused for the missing username instead.

The fix accepts an authzid equal to the username and still refuses one that
differs:

```rust
if !authzid.is_empty() && authzid != username {
    return Err(PlainError::Impersonation);
}
```

RFC 4616 permits the authzid to be present, and Apache Kafka's own
`PlainSaslServer` refuses only one that DIFFERS from the authenticated username.
An authzid equal to the username asks for nothing; a differing one is a request
to act as somebody else, which this facade still cannot grant. The facade is now
that rule rather than being stricter than it.

Both Python suites pass stock, with no monkeypatch. The wire bytes and the
answer, captured on the SASL_SSL listener with hostname verification on:

```
kafka-python: b'kafka-python-compat\x00kafka-python-compat\x00<token>'
              facade error_message: None
aiokafka:     SaslAuthenticateRequest_v1(sasl_auth_bytes=
                b'aiokafka-compat\x00aiokafka-compat\x00<token>')
```

The whole of the new rule, proven live at both SaslAuthenticate versions with no
client library involved:

```
ok  empty authzid       \0probe\0TOKEN       -> error_code=0
ok  authzid == username probe\0probe\0TOKEN  -> error_code=0
ok  authzid != username boss\0probe\0TOKEN   -> error_code=58,
      "an authorization identity was requested that differs from the username"
ok  no username         \0\0TOKEN            -> error_code=58, "no username..."
```

Both flows got the fix together and cannot disagree: the v0 raw-token path and
the v1 SaslAuthenticate path reach it through the single `parse_plain`. The
connection label stays the USERNAME and not the authzid, so a log line means one
thing whichever framing a client uses, and SNI forwarding is untouched:

```
DEBUG kafka: sasl authenticated this connection user="kafka-python-compat" sni="localhost"
```

Clients that send an empty authzid are unaffected, checked on the same listener:
a real librdkafka (rust-rdkafka 0.39.0, librdkafka 2.12.1) and kafka-go both
still pass, and a wrong password is still refused as an authentication failure
rather than a timeout on every client tried.

`kafka-python/raw_sasl_probe.py` was the campaign's wire-level proof of this
defect and asserted the old rule; its section 2 has since been inverted to pin
the new one from both sides. It now proves, at SaslAuthenticate v0 AND v1, that
`probe\0probe\0token` is admitted with `error_code=0` and that
`boss\0probe\0token` is refused with `error_code=58` naming the differing
authorization identity. Full run: `RESULT: PASS`, all four sections, against a
live TLS+SASL lane. The facade labels the accepted-authzid connection with the
USERNAME (`user="probe"`), not the authzid, which is the module header's claim
and is only visible in the facade log.

## The non-PASS rows, with evidence

### IBM/sarama: PARTIAL

At v1.60.2 with the library's untouched defaults, sarama passes 107 checks: 512
keyed records with headers over 8 partitions, all four codecs each proven to
have actually compressed, a consumer group reading them back byte-exact in
per-partition offset order, a commit verified through OffsetFetch that a second
group instance resumes from, an auto-created topic, offset bounds and a seek,
and produce plus consume over SASL/PLAIN and TLS.

The reason the row is PARTIAL is a version cliff. sarama does not pick request
versions per API from ApiVersions the way franz-go, librdkafka, kafkajs and the
Java client do; it derives them from one knob, `Config.Version`, which is a
Kafka RELEASE number. What saves it is `restrictApiVersion` in
`sarama/api_versions.go`, which clamps every outgoing request down to the
broker's advertised maximum. That function does not exist before **sarama
v1.46.0**: v1.38.1, v1.40.1, v1.41.3, v1.42.2, v1.43.3, v1.44.0 and v1.45.2 were
each downloaded and grepped, and only v1.46.0 has it.

Below that boundary the failure takes the dangerous shape rather than the loud
one. At v1.45.2's own DefaultVersion (2.1.0), Produce goes out at v7 and lands,
then Fetch goes out at v10, the facade closes the connection, and sarama's
consumer retries for ever:

```
[sarama] consumer/broker/0 disconnecting due to error processing FetchRequest: EOF
facade: WARN kafka: connection closed suppressed=71
  error=Fetch v10 is outside the advertised window 4..=6
```

Writes keep working the whole time. `Config.Version = sarama.V1_0_0_0` fixes it
(Produce v5, Fetch v6, Metadata v5, verified end to end on v1.45.2). At
`Config.Version = 3.6.0` an old sarama cannot even bootstrap and fails in
seconds, which is the better of the two failures.

The same cliff exists on the current line through one setting:
`Config.ApiVersionsRequest=false` leaves nothing to clamp against and reproduces
it exactly. This matters because sarama's own `Validate()` refuses
`SASLHandshakeV0` (its default) together with `ApiVersionsRequest`, so anyone
reaching for SASL is pushed to either set `SASLHandshakeV1`, which is right, or
turn the handshake off, which breaks consuming.

Two other sarama facts worth writing down. `Config.Version = 0.10.2.0` sends
Produce v2 and is refused, and sarama's clamp formula never RAISES a version to
the broker's floor, so even v1.60.2 stays at v2 and fails; that is sarama's
arithmetic, not a facade gap. And `ClusterAdmin` used to be entirely
unusable, including `ClusterAdmin.ListTopics`, which issues a DescribeConfigs
per topic after its Metadata request. **M7 F1 fixed that**: measured against a
live facade at v1.60.2 with `Config.Version = V3_6_0_0` and
`ApiVersionsRequest = true`, `NewClusterAdmin`, `CreateTopic`, `ListTopics`
(21 topics, including the one just created, 8 partitions, rf 1),
`DescribeConfig` on a topic and on broker 0, and `DeleteTopic` — with a second
`DeleteTopic` answering `ErrUnknownTopicOrPartition` — all pass. Its GROUP half — `ListConsumerGroups`,
`DescribeConsumerGroups`, `DeleteConsumerGroup` — used to close the connection
because ListGroups was not advertised; **M7 F2 advertises all three**, and the
same object's group calls are now on a surface that answers. They are not
claimed measured against sarama specifically: the groups-admin trio was measured
with franz-go's `kmsg`, with apache/kafka's own `kafka-consumer-groups.sh` and
with Confluent.Kafka. `sarama.Client.Topics()` and `.Partitions()` are pure
Metadata and have always worked.

### kafka-python (and kafka-python-ng): PARTIAL

The plaintext lane passes on all four releases tested (2.0.2 on Python 3.9,
2.3.2 and 3.0.11 on Python 3.12, and the archived ng fork at 2.2.3) with stock,
zero-config clients, including the default `api_version` probe that was expected
to be a trap: 56 assertions covering 512 records over 8 partitions with keys and
three-header lists, all four codecs, a group reading them back byte-exact with
produce order preserved, commit and resume, auto-create, and seek.

The probe is safe for two different reasons. 2.0.x and ng infer "Kafka 2.4" from
the advertised Produce v8 but never emit above Fetch v4, so they stay inside the
window by accident. 2.1 and later clamp per API and ask for exactly Fetch v6.

The row is PARTIAL for two things. SASL/PLAIN used to be a third and is not any
more: it was fixed on 2026-08-29 and the suite now passes stock on 2.3.2, 3.0.11
and ng 2.2.3. See the fixed section above.

**Pinning `api_version` is the footgun, not the fix.** The usual advice for this
client is inverted here: an explicit hint switches the per-API clamp off.
`api_version=(1,1,0)` sends Fetch v7 and `(2,0,0)` sends Fetch v8, both outside
the advertised 4..6, and the facade closes the connection. The consumer then
makes no progress at all. `(0,11,0)` is the ceiling that still works.

**A refusal reaches 3.x as a retriable disconnect.** The facade answers with
error code 58 and a good message and then closes immediately. kafka-python 3.x's
async transport loses that frame to the close and reports
`KafkaConnectionError: socket disconnected`, which it treats as RETRIABLE, so it
retries a wrong password for ever. This was measured rather than inferred:
relaying the same listener through a proxy that holds the FIN for 300ms turns
the disconnect into a parsed `SaslAuthenticateResponse(error_code=58, ...)`.
kafka-python 2.3.2's synchronous transport is unaffected. Apache Kafka spends
100ms here (`connection.failed.authentication.delay.ms`) for this reason.

Also worth recording: kafka-python 2.0.2 fails at import on Python 3.12
(`No module named 'kafka.vendor.six.moves'`), before any wire traffic, so it
needs a 3.11 or older interpreter.

### aiokafka: PARTIAL

aiokafka 0.14.0 passes the whole plaintext bar with the default constructor and
no version hint: 34 assertions, 512 records over all 8 partitions with keys and
headers, all four codecs through cramjam, group consume byte-exact with
per-partition order, commit and resume with zero loss and zero duplicates,
auto-create, and seek. It negotiated Fetch v6, Produce v7, JoinGroup v2,
Metadata v5, OffsetCommit v3, OffsetFetch v3, FindCoordinator v1, SyncGroup v1,
LeaveGroup v1 and ListOffsets v3, every one inside the window, with no facade
errors and no reconnect loop.

**Consumer groups need aiokafka 0.13.0 or newer, and 0.12 fails as a silent
infinite hang.** 0.12's `check_version()` infers a Kafka release rather than
clamping per API; against this facade it concludes `(2, 4, 0)`, and
`group_coordinator.py:1313` then unconditionally sends `JoinGroupRequest_v5`.
The facade advertises JoinGroup 0 to 4 and closes. Over one 15-minute run: 17769
JoinGroup v5 requests, 5952 client-side closes, 90 facade warnings, and no
exception ever raised to the caller. Produce is unaffected, which is what makes
it look like a broker hang rather than a client fault. Facade line:

```
WARN kafka: connection closed suppressed=95
  error=JoinGroup v5 is outside the advertised window 0..=4;
  a client that read our ApiVersions answer never sends it
```

0.13.0 fixed this by making `api_version` a no-op and clamping per API. On an
older release, `api_version="2.2.0"` forces JoinGroup v2 and groups work; note
that this is the exact opposite of the kafka-python advice above, where pinning
is the footgun.

**SASL/PLAIN used to be refused for the same reason as kafka-python** and is
not any more. `aiokafka/conn.py:616` builds
`"\0".join([username, username, password])`, and between them these two clients
are the whole pip-installable Python Kafka ecosystem that is not librdkafka.
Since the 2026-08-29 `sasl.rs` fix the suite passes stock: a full produce, group
consume and commit over SASL_SSL with real hostname verification, and a wrong
password still correctly refused. See the fixed section above.

One ergonomic note for operators: a refusal reaches an aiokafka caller as a
generic `KafkaConnectionError: Unable to bootstrap from [...]`. The facade's
error code and message are only visible with DEBUG logging on the `aiokafka`
logger.

### rdkafka-ruby 0.29.0 and php-rdkafka 6.0.5: PARTIAL

Both clients work with zero non-default configuration, and every functional
assertion passed on both: 512 messages over 8 partitions with keys, headers and
binary payloads, consumer groups on librdkafka's stock defaults, byte-exact key,
payload and header round-trip, per-partition order, commit, stop and resume in
the same group with no loss and zero duplicates, watermarks, seek, auto-create,
and a SASL/PLAIN over TLS lane including a correctly refused wrong password.

The row is PARTIAL for one silent caveat that is the facade's to decide about.
**The advertised Produce FLOOR of v3 makes librdkafka 2.11.0 and older silently
drop gzip, snappy AND lz4**, not just the documented zstd. Every batch goes out
uncompressed, announced once per DAY per broker at notice level.

Measured side by side against the same facade:

| librdkafka | gzip | snappy | lz4 | zstd |
| --- | --- | --- | --- | --- |
| 2.14.2 (Ruby gem 0.29.0) | compressed | compressed | compressed | downgraded |
| 2.6.1 (php-rdkafka 6.0.5) | downgraded | downgraded | downgraded | downgraded |

php-rdkafka is the worst case in the whole matrix because it links the SYSTEM
librdkafka, and Debian bookworm ships 1.9.2. The Ruby gem ships a precompiled
2.14.2 and is on the fixed side, so the two suites in one directory capture both
sides of the boundary live.

The mechanism, from librdkafka's own source:
`rd_kafka_msgset_writer_select_MsgVersion()` maps each codec to a required
Produce version, with gzip and snappy left at zero, then calls
`rd_kafka_broker_ApiVersion_supported(rkb, Produce, 0, 0, NULL)`, which ends in
`else if (ret.MinVer > maxver) return -1;`. MinVer 3 is greater than maxver 0,
so the codec is dropped. Real Kafka advertises Produce from v0 and never trips
it. librdkafka 2.11.1 replaced that call with
`rd_kafka_broker_ApiVersion_at_least()`, which is why 2.14.2 is unaffected. The
boundary was bisected over twelve release tags.

This is not a correctness bug and not a decompression bug: records land
byte-exact, and 200 records produced with `compression.type=lz4` from
`apache/kafka:3.9.1`'s own console producer (which has no such gate) were read
back through the facade intact. The cost is bandwidth, on exactly the
deployments that went to the trouble of configuring compression.

One further librdkafka-family ergonomic, seen most clearly here: a wrong SASL
password reaches Ruby as a delivery TIMEOUT rather than an auth exception,
because librdkafka treats SASL failure as retriable and re-bootstraps. The
facade's real reason is in the client's log stream, verbatim. PHP sees it
immediately through `setErrorCb`; Java and kafkajs fail fast.

## Client defects found on the way

These are not the facade's, and each was proved not to be. They are recorded
because a queen-kafka user will meet them and read them as broker problems.

**Confluent.Kafka (.NET): `AdminClient.ListConsumerGroupsAsync` aborts the
process** against any broker whose ApiVersions omits ListGroups. Exit 134
(`free(): double free detected in tcache 2`) or exit 139, reproduced identically
on 2.6.1, 2.11.1 and 2.15.0. A `debug=protocol` trace of the whole run is
ApiVersions plus Metadata on two connections and nothing else: the ListGroups
request is never sent, so the facade never sees it. No try/catch helps, because
a glibc abort is not a .NET exception.

**FIXED FROM THIS SIDE since M7 F2**, which is the only side it could be fixed
from: the facade advertises key 16, so the call is made, answered and returns a
`ListConsumerGroupsResult`. The defect is still in the client and still bites
against any other broker without KIP-518; the probe in
[`confluent-dotnet/Edges.cs`](confluent-dotnet/Edges.cs) is now a positive check
so that a regression here is caught rather than silently skipped.

**node-rdkafka cannot send a binary header value.** A 2x2 cross-binding probe
(write with each binding, read each topic with both) shows the losses are a ROW,
not a column: `410042` arrives as `41`, `feff80` as `efbfbdefbfbdefbfbd`,
`00010203` as empty, for both readers including node-rdkafka itself, while the
same bytes written by `@confluentinc/kafka-javascript` come back exact through
the same facade. The cause is `node-rdkafka/src/producer.cc`, which coerces
every header value through `Nan::To<v8::String>` and then builds a `std::string`
from a `char*`. This also proves the facade stores and returns arbitrary header
bytes.

**segmentio/kafka-go: a `&kafka.Writer{}` composite literal defaults to
acks=0.** Only `kafka.NewWriter(cfg)` rewrites 0 to `RequireAll`. Against this
facade acks=0 writes no response frame, so such a Writer can never see an error
and the offsets it reports are invented from its own counter. The records do
land, verified byte-exact, but the write is unverified.

**kafka-clients below 3.9 cannot do SASL on JDK 24 or newer.**
`SaslClientCallbackHandler` calls `Subject.getSubject(AccessControlContext)`,
whose removal JEP 486 finalised in JDK 24, and throws
`UnsupportedOperationException: getSubject is not supported`. The channel is
never built, the client retries for ever, and the facade logs
`TLS handshake eof`, which looks exactly like a TLS bug and is not one. The
documented `-Djava.security.manager=allow` workaround is itself rejected by JDK
24. Proved environmental by running the same client and the same facade on JDK
21: clean 66-check PASS. Fixed upstream in kafka-clients 3.9.

## The landscape: what was tested, what was not, and why

Usage tiers below come from live registry data pulled on 2026-08-29 (npm, PyPI,
crates.io, RubyGems, Packagist and NuGet download counts, pkg.go.dev
imported-by, GitHub push dates), plus the Apache clients wiki and Confluent's
client-update posts. Where no download signal exists, the row says so.

The organising fact of the whole landscape is that most clients are not
independent: **librdkafka is the wire engine for Python, .NET, Go (Confluent),
JavaScript (Confluent and node-rdkafka), Rust, Ruby, PHP, Haskell and the C++
wrappers**, and **kafka-clients is the wire for every JVM framework**. Testing a
wrapper over a proven core tests the wrapper's defaults and packaging, not the
protocol. That is why the campaign spent its effort on independent
implementations and on the two wrappers whose defaults are known to be
load-bearing (Spring Boot, and the Confluent Node client's KafkaJS
compatibility surface).

### Independent protocol implementations

Each of these has its own wire code, so each is a real oracle.

| Client | Ecosystem | Usage | Status |
| --- | --- | --- | --- |
| Java kafka-clients | JVM | top | tested (3.9.1, 3.6.2, 4.3.1) |
| kafka-python | Python | top (about 24.3M/month) | tested |
| aiokafka | Python | high (about 14.2M/month) | tested |
| IBM/sarama | Go | top (2696 importers under the IBM path alone) | tested |
| segmentio/kafka-go | Go | top (4085 importers, the most-imported Go package) | tested |
| twmb/franz-go | Go | high (646 importers, but the client inside Redpanda Connect) | tested (M6) |
| kafkajs | JS | top (about 14.3M/month, unmaintained since 2024-08) | tested (M6) |
| @platformatic/kafka | JS | high and rising (about 0.98M/month) | tested |
| brod | Erlang/Elixir | high (the BEAM default; broadway_kafka and kaffe sit on it) | tested |
| librdkafka | C | top (the engine under nine ecosystems) | tested (M6) |
| kafka-node | JS | medium by inertia (about 0.52M/month) | not tested: unmaintained since about 2020, pinned to very old API versions, and nobody starts a project on it in 2026 |
| rskafka | Rust | medium (0.41M recent downloads) | not tested: no consumer groups by design, so it exercises strictly less of the facade than the covered clients |
| kafka_ex | Erlang/Elixir | medium | not tested: genuinely separate wire code from brod, and the best stretch target if the matrix extends |
| longlang/phpkafka | PHP | medium (about 80k installs/month) | not tested: the only PHP client with wire code of its own, and the PHP row to add next |
| ruby-kafka | Ruby | medium by legacy (85.5M lifetime) | not tested: deprecated by Zendesk in favour of the rdkafka stacks |
| kafka-rust | Rust | niche | not tested: last release 2023 |
| pykafka, kafka-python-ng, nmred/kafka-php, klife | mixed | niche | not tested: dead, archived, or producer-only |

### librdkafka bindings

The wire is librdkafka's, so the value of each row is its binding layer,
defaults and packaging.

| Client | Ecosystem | Usage | Status |
| --- | --- | --- | --- |
| confluent-kafka-python | Python | top (about 59.0M/month) | tested (M6) |
| Confluent.Kafka | .NET | top (263M total NuGet; a monopoly in .NET) | tested |
| @confluentinc/kafka-javascript | JS | high (about 3.8M/month, the sanctioned kafkajs exit) | tested |
| rdkafka crate | Rust | high (6.3M recent downloads; the client inside Vector) | tested |
| rdkafka-ruby / karafka / WaterDrop | Ruby | high (karafka 28.4M, WaterDrop 31.5M downloads) | tested |
| php-rdkafka | PHP | high (the PHP standard, distributed by PECL so no Packagist signal) | tested |
| node-rdkafka | JS | medium (about 0.27M/month, superseded by the Confluent client) | tested |
| kcat | C | medium | tested (M6), and worth keeping as its own row: it is what an operator reaches for first |
| confluent-kafka-go | Go | high (972 importers) | not tested: cgo over librdkafka, and it cannot fail the wire in a way kcat and confluent-kafka-python would not have caught |
| KafkaFlow, MassTransit, Silverback | .NET | medium | not tested: wrappers over Confluent.Kafka. KafkaFlow's topic provisioning uses CreateTopics, which M7 F1 advertises; the group-admin half it also reaches for is still absent |
| racecar | Ruby | medium (16.3M) | not tested: 2.x runs on rdkafka-ruby |
| laravel-kafka | PHP | medium (about 180k installs/month) | not tested: Laravel integration over php-rdkafka |
| modern-cpp-kafka, cppkafka, hw-kafka-client | C++, Haskell | niche | not tested: thin API layers over librdkafka |
| quix-streams | Python | medium | wire covered by confluent-kafka; its stateful mode is out of scope (changelog topics, optional EOS) |

### kafka-clients wrappers

| Client | Usage | Status |
| --- | --- | --- |
| Spring Kafka | top: how most enterprises actually touch Kafka | tested |
| Spring Cloud Stream Kafka binder | high | not tested: sits on spring-kafka. Its provisioner is admin-heavy and the CreateTopics wall is gone since M7 F1, but it also alters configs and reads groups, so it is not claimed working until it is run |
| Quarkus / SmallRye Reactive Messaging | high | not tested: wraps kafka-clients; its health checks and topic verification touch admin surfaces |
| Micronaut Kafka, Vert.x Kafka, Camel kafka | medium | not tested: annotation or route layers with no protocol code |
| Pekko/Alpakka, fs2-kafka, zio-kafka, jackdaw, kinsky | medium to niche | not tested: streams and effect-system wrappers. Their transactional sources and sinks are out of scope |
| kafka-console-producer.sh / kafka-console-consumer.sh | medium | not tested directly, but they ship inside kafka-clients and speak the tested wire. They should work |
| kafka-topics.sh, kafka-configs.sh | medium | MEASURED WORKING since M7 F1: `--create`, `--list`, `--describe`, `--delete`, and `kafka-configs.sh --describe` on topics and on broker 0. Since M7 F4 `kafka-configs.sh --alter` works too, on a topic this facade created: `--add-config retention.ms=…` and `--delete-config retention.ms` both land and both read back. `kafka-topics.sh --alter --partitions` REFUSES, with the oracle's own sentence for a decrease and for an equal count |
| kafka-consumer-groups.sh | medium | MEASURED WORKING since M7 F2: `--list` (with `--state` and `--state Stable`), `--describe` (`--members --verbose`, `--state`, `--offsets`) and `--delete`. Since M7 F4, `--delete-offsets` too, with Kafka's subscription rule. `--reset-offsets` rides OffsetCommit, which is advertised, but nothing has measured it and it is not claimed here; `--to-datetime` and `--by-duration` will NOT work regardless, because they send ListOffsets with a concrete timestamp and this facade answers -1 for every one of those (no time index in Queen) |
| kafka-acls.sh | medium | MEASURED since M7 F4: `--list`, `--add` and `--remove` all print `Error while executing ACL command: No Authorizer is configured…` and exit 1, and each output diffs IDENTICAL against apache/kafka:3.9.1 run from the same container. It degrades exactly as it does against a Kafka with no authorizer, which is the honest answer: there is no ACL model here to query |
| kafka-delete-records.sh, kafka-log-dirs.sh, kafka-leader-election.sh, kafka-reassign-partitions.sh, kafka-delegation-tokens.sh, kafka-metadata-quorum.sh, kafka-transactions.sh | low to medium | will NOT work, each for a reason in the long-tail list below. Every one fails client-side with `UnsupportedVersionException` before a byte is sent, which is a named failure and not a hang |
| KNet | niche | not tested: drives the real Java client over JNI, so its wire is byte-identical to a tested row |
| faust-streaming | medium | wire covered by aiokafka; its stateful tables need compacted changelog topics, which are out of scope |

### Out of scope by capability, not by effort

None of these is a client row that failed. Each needs a facility the facade
deliberately does not have, and each should be reported as known-unsupported
rather than as untested.

**Transactions are NO LONGER on this list, and what replaced them is a
boundary rather than a refusal.** `AddPartitionsToTxn`, `AddOffsetsToTxn`,
`EndTxn` and `TxnOffsetCommit` are advertised v0-v3 since M9 (2026-08-30), and a
transactional producer works: Java, Spring's `KafkaTransactionManager`,
franz-go's `GroupTransactSession` and librdkafka's transactional API all commit.
What is out of scope is the SHAPE below, and each row says which half it fails
on.

**Two-phase commit across a process boundary: Flink `KafkaSink EXACTLY_ONCE`
and Spark's structured-streaming EOS writer.** Both pre-commit at a checkpoint
and call `commitTransaction()` on checkpoint-completion, which after a failover
happens in a DIFFERENT process, from a producer reconstructed out of the saved
`(transactional.id, producer id, epoch)`. That `EndTxn` reaches a facade holding
no stage and is answered `INVALID_TXN_STATE` — fatal, so the job cannot recover.
The happy path works and the recovery path is the entire point of the feature,
so the honest answer is **no**. This is a correction of the pre-M9 text, not an
upgrade of it: the reason has changed from "transactions are absent" to "a
transaction here lives in one process", and the verdict has not.

**Kafka Streams.** Still no, and **not for the transaction reason**. Streams
additionally needs `cleanup.policy=compact` for its changelog and repartition
topics, which the facade refuses with `INVALID_CONFIG` and refuses deliberately:
accepting it and compacting nothing would eat the state store. The dependency is
compaction, compaction is a stated non-goal, and **"transactions landed" must
never be said as though it brought Streams closer.** It brought it no closer at
all.

**Kafka Connect, MirrorMaker 2, Debezium, ksqlDB.** The Connect worker needs an
AdminClient surface to provision its config, offset and status topics, and all
three of those are compacted — the same non-goal. Connect's exactly-once source
support has the two-phase shape on top of that. MirrorMaker 2 has ListGroups and
DescribeGroups since M7 F2 and still needs AlterConsumerGroupOffsets to mirror
offsets.

**Flink and Spark, at-least-once.** Both resolve partitions and initial offsets
through AdminClient, which has worked since M7. The at-least-once paths might
work and are unproven, and should not be claimed either way.

**Admin and monitoring UIs**: kafbat/kafka-ui, AKHQ, Conduktor, Redpanda
Console, Cruise Control, Burrow, kafka-lag-exporter. These used to fail at
connect; since M7 F1, F2 and F4 the calls they open with (Metadata,
DescribeConfigs, IncrementalAlterConfigs, ListGroups, DescribeGroups,
OffsetFetch, ListOffsets, DescribeAcls) are all advertised. **None of them is
claimed working**, and the reason is that each fans out over the long tail
below: a tool that renders a tab per call will show broken tabs. Two of the
seven were re-measured on 2026-08-30 and are worth stating individually:

- **kafka-ui renders a topic's Settings tab identically against the facade and
  against apache/kafka:3.9.1**, `retention.ms = 60000` sourced
  `DYNAMIC_TOPIC_CONFIG` with `readOnly = false` on both. That tab was the hole
  F4 set out to close, and it is closed. Its ACL tab is hidden on BOTH clusters
  (neither reports `ACL_VIEW`), and its `/acls` route answers 500 on both, which
  is kafka-ui's own guard and not a facade behaviour.
- **kafka-lag-exporter works today**: `listConsumerGroups` (16),
  `listConsumerGroupOffsets` (9) and a consumer's `endOffsets` (2) are all
  advertised, and it needs nothing else.
- **Burrow cannot work, for a reason no API addition changes**: it consumes
  `__consumer_offsets` as a topic, and every `__` name is invisible here by rule.
- **Cruise Control needs three absences closed, not one**: `DescribeLogDirs`,
  `AlterPartitionReassignments`, and a metrics-reporter topic. Do not describe
  any single one of them as the blocker.

The honest claim is the measured one, that a client connects, lists topics,
lists groups, sees a group's members and its lag, and now reads and writes a
topic's retention. Anything beyond that is unmeasured. Queen's own console remains
the answer, and mirroring Kafka group offsets into Queen's native cursor (C3 in
the plan) is what makes those groups visible in it.

### The long tail, key by key, with what each one costs a tool

Nineteen API keys are deliberately not advertised. Each is a decision with a
reason, and each is pinned by `classify_the_absent_admin_apis` in
`src/versions.rs`, so advertising one of them by accident fails a test rather
than shipping. A client that sends one anyway has its connection closed, which
is Apache Kafka's answer to an unparseable request and is unreachable for any
client that read the ApiVersions response.

| Key | API | Why it is absent | What it costs |
| --- | --- | --- | --- |
| 21 | DeleteRecords | Queen has no truncate-to-offset primitive: `log_start` moves by retention and by dropping the queue. Answering would mean reporting a `low_watermark` that did not move | `kafka-delete-records.sh` fails; kafka-ui's and AKHQ's "Clear messages" button errors. The workaround is DeleteTopics then CreateTopics, which both work |
| 23 | OffsetsForLeaderEpoch | Every leader epoch this facade reports is -1, so a consumer's `SubscriptionState` never holds one and `validateOffsetsAsync` short-circuits | Nothing measurable. **No client ever sends it** |
| 35 | DescribeLogDirs | Queen's storage is Postgres segments; there are no log directories, and answering would mean inventing a path and per-partition byte sizes | kafka-ui's topic and broker "Size" columns read blank (it swallows the error and renders); `kafka-log-dirs.sh` fails. **The best future candidate of the three absences**: `retainedBytes` is real and already on `GET /api/v1/resources/queues`; what is missing is a per-partition breakdown |
| 38-41 | Create/Renew/Expire/DescribeDelegationToken | A delegation token is derived from a SCRAM principal and signed by the broker. This facade mints no credentials; Queen does | `kafka-delegation-tokens.sh` fails. Nothing in this matrix uses it |
| 43 | ElectLeaders | One logical broker and no replicas: every Metadata answer is `replicas=[0], isr=[0]`. In cluster mode a partition's leader is a rendezvous hash, deterministic and not movable | `kafka-leader-election.sh` fails; Cruise Control's self-healing cannot run |
| 45, 46 | Alter/ListPartitionReassignments | There are no replicas to move; durability is Postgres's. A reassignment API over one logical broker would accept a plan and have nothing to do with it | `kafka-reassign-partitions.sh` fails; Cruise Control and Confluent's auto-balancer cannot run. No UI calls `listPartitionReassignments` on a render path |
| 48, 49 | Describe/AlterClientQuotas | The facade DOES have quotas, namely Queen's 429 with `Retry-After`, but they are the Cloud proxy's, per TENANT, and not expressible in Kafka's `(user, client-id)` model. Altering must never work for a second and independent reason: it would let a tenant raise its own rate cap | `kafka-configs.sh --entity-type clients --describe` fails; kafka-ui's quotas tab is absent. The one absence with a real future story: a READ-ONLY 48 mapping the tenant onto a `user` entity, once the proxy exposes the cap |
| 50, 51 | Describe/AlterUserScramCredentials | SASL here is PLAIN only and the credential is a Queen bearer verified by Queen. There is no local user store. Supporting SCRAM would make this facade a credential store with its own secrets at rest | `kafka-configs.sh --entity-type users --alter --add-config 'SCRAM-SHA-256=[…]'` fails; kafka-ui's user management tab is absent. This is a security posture change, not a protocol gap |
| 55 | DescribeQuorum | No Raft log and no voters; every field would be invented. The one thing UIs actually render, a controller id, is already in every Metadata answer | `kafka-metadata-quorum.sh describe` fails; kafka-ui's KRaft panel feature-detects and hides |
| 60 | DescribeCluster | Answerable, truthfully, and deliberately not answered: every client in this matrix already answers `describeCluster()` from a plain Metadata request, so advertising it moves five live suites onto a new code path for a measured gain of zero | Nothing today. See the trigger sentence above for the day that changes |
| 61 | DescribeProducers | The idempotence window is PROCESS state, deliberately lost on restart. Answering from it would advertise durable producer state the facade does not have; answering empty would say "nothing is producing" while producers produce | `kafka-transactions.sh find-hanging` unavailable. No client path needs it |
| 65, 66 | Describe/ListTransactions | The registry they would read does not exist, and after M9 it still would not be answerable well: transaction state is per process and transactions are refused in cluster mode, so a listing would be per node exactly where a listing matters | `kafka-transactions.sh list` / `describe` fail. Nothing else. M9's call if anyone's |

**KIP-848 next-generation consumer groups** (`group.protocol=consumer`).
`ConsumerGroupHeartbeat` is not advertised; the facade implements the classic
coordinator only. State the window honestly: classic is still the default
through Kafka 4.x, 4.3 only logs an upgrade nag (KIP-1274), and the default
flips in 5.0 with removal targeted for 6.0. A 4.x client asking for it fails in
70ms with an error that names the fix verbatim
(`Set group.protocol=classic on the consumer configs`). This is the facade's
expiry date, not merely a missing feature.

**KIP-932 share groups.** `ShareFetch` and friends are not advertised. Ironic
given what Queen is underneath, but it is a separate protocol surface.

**Static membership** (`group.instance.id`, KIP-345), deliberately excluded by
capping JoinGroup at v4 and SyncGroup, Heartbeat and LeaveGroup at v2, so that a
client configured for static membership cannot silently receive dynamic
behaviour. This affects Spring and Flink deployments that set it; report it as a
configuration to remove, not as a client failure.

**Schema Registry and any compacted-topic-as-database pattern.** Log compaction
is a stated non-goal, so a system that stores state in a compacted topic cannot
be hosted regardless of which client it uses.

## How these suites are run

Every suite takes its stack from the environment and starts nothing of its own,
so it can be pointed at `rig.sh --keep` or at any facade:

```sh
protocols/queen-kafka/compat/rig.sh --keep -run TestNothing    # stack on 19092, no suite
```

Each one prints one `ok` or `FAIL` line per assertion, ends with a `RESULT:`
line, exits non-zero on failure, and reports the API versions its client
NEGOTIATED, read out of that client's own debug stream rather than assumed.
Suite-specific notes (toolchains, images, environment variables, and the
proposed `rig.sh` wiring for each) are in the README inside each directory.

Two environment facts that cost several agents real time:

- A client running in a CONTAINER needs the facade booted with
  `QUEEN_KAFKA_ADVERTISED_ADDR=host.docker.internal:PORT`, because every client
  re-dials the advertised address after Metadata and FindCoordinator. A facade
  advertising `127.0.0.1` is unusable from a container whatever bootstrap you
  pass. The Ruby and PHP suite checks this at startup and refuses with the fix
  rather than timing out.
- The rig certificate carries SANs for `kafka.example.com`,
  `shared.queenmq.cloud`, `localhost` and `127.0.0.1`, and NOT for
  `host.docker.internal`. A HOST client can therefore keep certificate and
  hostname verification fully on, which most of these suites do; a CONTAINER
  client dialling `host.docker.internal` must disable hostname verification.
