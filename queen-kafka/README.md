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
- **Admin**: CreateTopics, DeleteTopics, DescribeConfigs, ListGroups,
  DescribeGroups, DeleteGroups. `kafka-topics.sh`, `kafka-configs.sh --describe`
  and `kafka-consumer-groups.sh` all work against it.
- **Cloud fit**: TLS with SNI, SASL/PLAIN (the password is your Queen token),
  429s mapped to `throttle_time_ms`.

The advertised table is 21 API keys. The six admin keys and `InitProducerId`
landed on 2026-08-29:

| API | key | versions | Notes |
| --- | --- | --- | --- |
| CreateTopics | 19 | v2-v6 | `--partitions` and `--replication-factor` are accepted and reported back as the facade's own width and 1; `cleanup.policy=compact` is refused |
| DeleteTopics | 20 | v1-v5 | deletes the underlying Queen queue, which native producers may share |
| DescribeConfigs | 32 | v1-v4 | topics and this broker; `retention.ms` is writable and not readable |
| ListGroups | 16 | v0-v4 | live membership merged with a durable index of every group that ever committed |
| DescribeGroups | 15 | v0-v3 | members, host, client id and the assignment |
| DeleteGroups | 42 | v0-v2 | irreversibly removes committed offsets; refuses a group with members |
| InitProducerId | 22 | v0-v4 | idempotence only, never transactions |

## What it refuses, loudly

Transactions and EOS (so no Kafka Streams apps), log compaction, KIP-848,
static membership, and the ALTER half of the admin surface (AlterConfigs,
AlterConsumerGroupOffsets, the ACL APIs). Unsupported requests fail fast with
clear error codes, never hangs. Two consequences worth knowing before you meet
them:

- **`initTransactions()` is still slow, not fast-failed.** A transactional
  producer asks FindCoordinator for a TRANSACTION coordinator before it sends
  `InitProducerId`; that answer is retriable, so the client loops until
  `max.block.ms`. Set no `transactional.id`.
- **The idempotence window is in memory and per facade.** A facade restart, an
  eviction (65 536 tracked producer-partitions) or a connection landing on
  another facade costs at-least-once for at most the five in-flight batches.
  Real Kafka persists producer state in the log; this does not.

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
- **A restart on the same node id inside the TTL exits FATAL.** The dead node's
  registry row lives until it expires, the replacement loses its boot claim to
  it, and two processes on one node id would advertise one address for two
  facades. There is **no SIGTERM deregistration yet**, so a rolling restart must
  either wait the TTL out between pods or move to a fresh node id. Budget for it
  before you automate a deploy. Meanwhile the survivors still advertise the dead
  node and still point FindCoordinator at its address until the row expires.
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
`-count=1`. Cluster mode: `compat/cluster/rig-cluster.sh`. Behaviour against a
real broker: `compat/differential/rig-diff.sh` diffs every answer against
`apache/kafka:3.9.1`. Full support matrix and config reference: the webdoc pages
`/reference/kafka` and `/deploy/kafka`. Plan and status:
[../PLAN_QUEEN_KAFKA.md](../PLAN_QUEEN_KAFKA.md).

Status: preview. Not in release CI, no container image yet, and behind the
Cloud proxy the consume path is not routed yet (see the plan's known-open list).
