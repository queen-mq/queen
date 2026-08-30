# queen-kafka

A Kafka wire-protocol front for Queen. Point an unmodified Kafka client at it
(change `bootstrap.servers`, nothing else) and it translates to a Queen broker
over plain HTTP. By default it advertises itself as a single logical broker;
two or three facades can also present themselves to clients as **one cluster**
(see [Cluster mode](#cluster-mode)). It keeps no durable state of its own, and a
restart behaves like a Kafka broker restart: clients rejoin and resume from
their committed offsets, which live in Queen.

## What it can do

- **Produce**: acks 0/1/all, gzip/snappy/lz4/zstd, keys, headers, timestamps.
  Topics auto-create on first use.
- **Idempotent produce**: `InitProducerId` and the per-`(producer,
  topic-partition)` sequence window. No producer needs any configuration; read
  the in-memory caveat under [What it refuses](#what-it-refuses-loudly).
- **Consume**: Fetch with long-poll (capped at v6, so fetch sessions never
  exist), ListOffsets earliest/latest.
- **Consumer groups**: the classic protocol (JoinGroup/SyncGroup/Heartbeat),
  rebalancing, offsets durable in Queen KV.
- **Admin**: CreateTopics, DeleteTopics, DescribeConfigs, AlterConfigs,
  IncrementalAlterConfigs, CreatePartitions, ListGroups, DescribeGroups,
  DeleteGroups, OffsetDelete, and the ACL trio. `kafka-topics.sh`,
  `kafka-configs.sh --describe` and `--alter`, and `kafka-consumer-groups.sh`
  all work against it.
- **Transactions**: a transactional producer and a same-process
  consume-transform-produce loop, atomic across records and offsets. Read the
  boundary under [Transactions](#transactions-what-works-and-the-boundary)
  before you plan around it.
- **Cloud fit**: TLS with SNI, SASL/PLAIN (the password is your Queen token),
  429s mapped to `throttle_time_ms`. Point `QUEEN_URL` at a cell's proxy instead
  of at a broker and every Kafka request crosses authentication, tenant scoping,
  quotas and metering; a real client proves it end to end in
  [`compat/cloud/`](compat/cloud).

The advertised table is 32 API keys. The thirteen admin keys,
`InitProducerId` and the four transaction keys landed on 2026-08-29 and
2026-08-30:

| API | key | versions | Notes |
| --- | --- | --- | --- |
| CreateTopics | 19 | v2-v6 | `--partitions` and `--replication-factor` are accepted and reported back as the facade's own width and 1; `cleanup.policy=compact` is refused |
| DeleteTopics | 20 | v1-v5 | deletes the underlying Queen queue, which native producers may share |
| DescribeConfigs | 32 | v1-v4 | topics and this broker; `retention.ms` round-trips for topics this facade created |
| AlterConfigs | 33 | v0-v2 | the deprecated FULL-REPLACEMENT form: a key the request does not name is reset to its default. Prefer key 44 |
| IncrementalAlterConfigs | 44 | v0-v1 | the delta form, and the one `kafka-configs.sh --alter` sends. Only on topics this facade created |
| CreatePartitions | 37 | v0-v3 | an advertised refusal: Queen declares no width per queue. A decrease and an equal count answer Kafka's own sentences |
| ListGroups | 16 | v0-v4 | live membership merged with a durable index of every group that ever committed |
| DescribeGroups | 15 | v0-v3 | members, host, client id and the assignment |
| DeleteGroups | 42 | v0-v2 | irreversibly removes committed offsets; refuses a group with members |
| OffsetDelete | 47 | v0 | `--delete-offsets`, with Kafka's subscription rule: a live group's subscribed topics are refused |
| DescribeAcls | 29 | v1-v3 | `SECURITY_DISABLED`, the answer of a Kafka with no authorizer |
| CreateAcls | 30 | v1-v3 | the same, one result per creation |
| DeleteAcls | 31 | v1-v3 | the same, one result per filter |
| InitProducerId | 22 | v0-v4 | the idempotent producer, and since M9 the transactional one; refused in cluster mode |
| AddPartitionsToTxn | 24 | v0-v3 | enrols a partition in the open transaction. v4 is a broker-to-broker request, not a client one |
| AddOffsetsToTxn | 25 | v0-v3 | one group per transaction, not several |
| EndTxn | 26 | v0-v3 | commit sends the whole stage as ONE `POST /api/v1/transaction`; abort drops it and writes nothing |
| TxnOffsetCommit | 28 | v0-v3 | v3 is the FLOOR and it is mandatory: kafka-clients throws below it whenever group metadata is set |

## Transactions: what works, and the boundary

Since M9 a transactional producer works. `initTransactions`,
`beginTransaction`, `send`, `sendOffsetsToTransaction`, `commitTransaction` and
`abortTransaction` all do what they say, the records and the consumer offsets
commit atomically in one Postgres transaction, and a second producer taking the
same `transactional.id` fences the first with zero of its records in the log.
Measured against real clients in [`compat/transactions`](compat/transactions):
Java kafka-clients 4.3.1 and franz-go, including a SIGKILL between the last send
and the commit that leaves 200 records, 200 distinct keys, 0 duplicates and 0
missing.

**The boundary is one sentence: a transaction here is a STAGE held by one
facade process, on the connection that opened it.** That is enough for a
transactional producer and for a consume-transform-produce loop in one process,
which is Spring's `KafkaTransactionManager` and every stock Java or franz-go EOS
loop. It is not enough for a two-phase commit that finishes somewhere else:
Flink's `KafkaSink EXACTLY_ONCE` and Spark's structured-streaming writer commit
after a failover from a DIFFERENT process, that `EndTxn` reaches a facade
holding no stage, and it is answered `INVALID_TXN_STATE` — fatal, so the job
cannot recover. **Transactions also change nothing for Kafka Streams**, whose
dependency is log compaction and not transactions.

Three things to know before you meet them. Records are STAGED in memory until
the commit, so a transaction is capped in five places with no Kafka analogue
(`QUEEN_KAFKA_TXN_MAX_*`, defaults 8 MiB / 50 000 records / 128 MiB per process
/ 1024 open / 900 s), and past a cap the producer must abort. A transactional
produce answers `base_offset = -1`, because no offset exists until the commit
allocates them. And **cluster mode refuses transactions outright**: with
`QUEEN_KAFKA_NODE_ID` set, `initTransactions()` raises
`TransactionalIdAuthorizationException` in about 250 ms rather than hanging.

## What it refuses, loudly

Log compaction (which is what keeps Kafka Streams and Kafka Connect's
exactly-once source support out), KIP-848 and static membership. Unsupported
requests fail fast with clear error codes, never hangs.

The ALTER half of the admin surface is no longer on that list, and the shape of
what is left changed on 2026-08-30. AlterConfigs and IncrementalAlterConfigs
land; AlterConsumerGroupOffsets always worked, because it rides OffsetCommit and
needs no key of its own; the ACL APIs answer `SECURITY_DISABLED` exactly as an
Apache Kafka with no authorizer does. What stays absent is the surface with no
Queen primitive behind it: DeleteRecords (nothing truncates to an offset),
DescribeLogDirs (Postgres segments, not log directories), partition reassignment
and leader election (one logical broker, no replicas), delegation tokens and
SCRAM (the facade mints no credentials; Queen does), and client quotas (the
quota is per tenant, and altering one would let a tenant raise its own cap).
Each of those closes the connection if sent anyway, which is Apache Kafka's own
answer to an unparseable request and unreachable for a client that read
ApiVersions. Two consequences worth knowing before you meet them:

- **The idempotence window is in memory and per facade.** A facade restart, an
  eviction (65 536 tracked producer-partitions) or a connection landing on
  another facade costs at-least-once for at most the five in-flight batches.
  Real Kafka persists producer state in the log; this does not.
- **A config alter needs a topic this facade created.** Queen's
  `POST /api/v1/configure` is a whole-row upsert over nineteen columns and
  thirteen of them have no HTTP read, so the facade keeps its own record of the
  bag it posted and alters only topics it has one for. Every other topic is
  refused `INVALID_CONFIG` with that sentence. `retention.ms` reads back out of
  the same record, which is what makes the round trip work at all.

Verified live against kafkajs, librdkafka (kcat and confluent-kafka), Java
kafka-clients 3.9 and 4.3, franz-go, segmentio/kafka-go, sarama, Spring Boot,
.NET, Rust, Ruby, PHP, Erlang and both pure-Python clients, and differentially
against Apache Kafka 3.9.1. The full per-client table, with the mandatory config
and the caveat for each, is [compat/CLIENT_MATRIX.md](compat/CLIENT_MATRIX.md).

## Run

```
cargo build --release
QUEEN_URL=http://localhost:6632 \
QUEEN_KAFKA_ADVERTISED_ADDR=localhost:9092 \
./target/release/queen-kafka
```

`QUEEN_KAFKA_ADVERTISED_ADDR` is required: it is the address clients are told
to connect back to, and getting it wrong is the classic Kafka footgun.

## Cluster mode

**One facade needs none of this and is unchanged by all of it.** With
`QUEEN_KAFKA_NODE_ID` unset, nothing in `src/cluster/` is read, spawned,
allocated or written, and the bytes on the wire are what they always were. That
is asserted, not assumed: `TestSingleNodeRegression` runs the whole acceptance
body against a facade with the cluster config absent.

### The problem it solves

Two facades in front of one Queen deployment already share everything durable:
offsets are in Queen's key/value store and the log is in Postgres. What they did
not share was **who arbitrates a group**, and that produced two distinct
defects. Both are fixed, and both are reproduced side by side in
[`compat/cluster/`](compat/cluster):

- **Double delivery.** Every facade answered FindCoordinator with itself, so one
  group formed twice and each generation assigned all eight partitions.
- **Offset rewind.** An offset commit was an unconditional upsert, so the loser
  of a race silently overwrote the winner: a commit of 50 followed by a commit of
  16 through the other facade left 16.

Set `QUEEN_KAFKA_NODE_ID` on each facade and the group RPCs become a redirect
(`NOT_COORDINATOR`, which every client answers by re-running FindCoordinator) to
the one node a shared rendezvous hash names as owner, with a compare-and-set
fence on the commit itself for the window in which a node's view is stale.

### The config surface

| Variable | Default | What it does |
| --- | --- | --- |
| `QUEEN_KAFKA_NODE_ID` | unset | **The one switch.** An integer in `1..=64`. Unset means single mode. 0 is reserved for the single-node identity, so "am I clustered" is never ambiguous; Apache Kafka is 0-based and this deviates deliberately. |
| `QUEEN_KAFKA_CLUSTER` | `queen` | The cluster name: the registry key prefix and the `cluster_id` clients see. 1 to 64 characters of `[A-Za-z0-9._-]`. Setting it without a node id is a boot failure rather than a silent second axis. |
| `QUEEN_KAFKA_CLUSTER_HEARTBEAT_MS` | `2000` | How often a node refreshes its registry row and re-reads the live set. Range 500 to 30000. |
| `QUEEN_KAFKA_CLUSTER_TTL_MS` | `10000` | How long a node stays live after its last successful write, and therefore the failover budget. Range 3000 to 120000, and **never below three times the heartbeat**: under 3x one slow KV call evicts a healthy node and moves every group it coordinates. |

Two requirements the boot check enforces and one it does not:

- `QUEEN_TOKEN` is **required** in cluster mode. The registry is written with
  this process's own credential, not a client's, because the broker list every
  client is handed has to be one list.
- All facades of one cluster must present credentials of **one Queen tenant**.
  `queen.kv` is keyed by tenant, so two tenants are two registries and each
  facade would see only itself.
- Not enforced: each node's `QUEEN_KAFKA_ADVERTISED_ADDR` must be **its own**,
  individually reachable by every client. See the anti-pattern below.

### The single-address anti-pattern, which cluster mode does not fix

**Do not put the facades behind one VIP, one Kubernetes Service or one load
balancer address.** This was already wrong and cluster mode makes it worse.
Every client re-dials the address Metadata and FindCoordinator hand it. If all
the nodes advertise one address, FindCoordinator answers "the coordinator is
node 2, at the VIP", the client dials the VIP, the balancer routes it to node 1,
and node 1 answers `NOT_COORDINATOR` because it is not the owner. That is an
infinite redirect loop, not a slow path. Nothing in the facade detects it: the
node ids differ, so the boot claim succeeds.

Give each facade its own DNS name or address, exactly as you would give each
broker of a real Kafka cluster one. A headless Service with stable per-pod
addresses is the Kubernetes shape that works.

### Operating it

- **Leadership is an advertisement, not an access control.** Every node serves
  Produce, Fetch, ListOffsets and OffsetFetch for every partition, whatever
  Metadata said the leader was. A non-leader has the data here (it is one shared
  Postgres), so refusing would cost availability for nothing. What IS gated at a
  non-owner is JoinGroup, SyncGroup, Heartbeat, LeaveGroup, OffsetCommit,
  DescribeGroups and DeleteGroups. OffsetFetch is deliberately not: it reads
  shared state whose answer is the same at every node, and an `assign()`-based
  consumer holding any connection would break if it were refused.
- **Failover is one TTL plus the join delay.** Measured at TTL 3000 in two
  independent runs: ownership moved from a SIGKILLed node to a survivor both
  survivors agreed on in 3.4 s and 3.2 s, with no loss and no redelivery, and
  the committed offsets never rewound. On the product defaults that step is
  budgeted at 10 s, plus `QUEEN_KAFKA_GROUP_JOIN_DELAY_MS` for the group to
  re-form.
- **A stop hands the node id back.** SIGTERM and Ctrl-C are handled: the
  listener closes, the connections already being served drain, and this node's
  registry row is deleted, fenced on the version this process holds it with,
  inside a two second budget. Peers drop the node on their next registry read
  instead of one TTL later, and a replacement claims the id at once. The TTL
  stays as the backstop for the stop nobody got to run (SIGKILL, OOM kill, a
  severed node), where the survivors do keep advertising the dead node and do
  keep pointing FindCoordinator at its address until the row expires.
- **A replacement that meets its predecessor's row waits it out rather than
  exiting.** It watches that row's version for one TTL plus one heartbeat. If
  the version MOVES somebody is refreshing it, which is a second live facade on
  this node id: FATAL, with the observation that proved it in the message. If
  the row expires or the version never moves, the replacement adopts the id. So
  a pod restarting on the same StatefulSet ordinal inside the TTL costs a wait
  and not a crash loop.
- **A registry that cannot be reached is not fatal.** The facade logs an ERROR
  naming the consequence and serves produce and fetch; every group RPC is
  refused `COORDINATOR_NOT_AVAILABLE` (retriable) until a heartbeat succeeds.
- **Two Queen tenants running a group of the same name are coordinated by the
  same node.** The ownership hash takes the group id and never the tenant,
  because the tenant key is seeded per process and a tenant-aware hash would
  never converge across facades. It is harmless: they remain two coordinator
  entries over two `queen.kv` rows.
- **Ordering across a leadership move.** A producer with
  `max.in.flight.requests.per.connection > 1` and idempotence off can have two
  batches land out of order when its metadata moves. Apache Kafka has the
  identical hazard on a leader change; the client-side fix is the same one.

### Proving it in your own deployment

[`compat/cluster/rig-cluster.sh`](compat/cluster) stands up one Postgres, two
mesh-wired Queen brokers on it, three clustered facades (two in front of one
broker and one in front of the other, so a cross-broker read is on the critical
path), one facade with the cluster config absent and two unclustered facades,
then runs nine scenarios and tears it all down. `run.sh` in that directory
points the same suite at a stack you already have.

## Tests

`cargo test`. Live end-to-end (Docker + Go): `compat/rig.sh --m5`, passing
`-count=1`. Cluster mode: `compat/cluster/rig-cluster.sh`. Queen Cloud, with a
whole cell and the proxy on the path: `compat/cloud/rig-cloud.sh`. Behaviour
against a real broker: `compat/differential/rig-diff.sh` diffs every answer
against `apache/kafka:3.9.1`. Full support matrix and config reference: the
webdoc pages `/reference/kafka` and `/deploy/kafka`. Plan and status:
[../PLAN_QUEEN_KAFKA.md](../PLAN_QUEEN_KAFKA.md).

Measured on 2026-08-30, all from a clean machine: `rig.sh --m5` **91/91**,
`cluster` **11/11**, `cloud` **16/16**, the differential **0 divergences left to
classify** (100 found: 74 deliberate, 26 accepted).

Status: preview. Not in release CI, and no published image carries the facade
yet, though the repository's `Dockerfile` builds it beside the broker binary for
`QUEEN_KAFKA_EMBEDDED=true` (server/src/kafka_facade.rs). Queen Cloud is
**reachable**: produce, consume, groups, committed offsets, admin and
transactions all cross the cell proxy, and two tenants on one shared listener are
isolated from each other. What is still open there is a short list of
ratifications rather than a gap, and it is in
[compat/CLIENT_MATRIX.md](compat/CLIENT_MATRIX.md) under "Open decisions". See
the plan's known-open list for the rest.
