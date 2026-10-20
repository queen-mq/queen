# Kafka facade 2: a translation layer

Decided with Alice on 2026-10-20: the first facade had become a second system
(its own node registry, fences, producer windows, group coordinator,
transaction stages, config mirror). It is replaced by a layer that turns every
Kafka request into operations of the Queen broker it runs inside, and keeps
nothing that outlives a connection or a leader's term.

## The mapping

| Kafka | Queen core | Facade state |
|---|---|---|
| Produce | push to queue `T`, partition `"p"`; one ordered push per request (`in_log`) | none |
| Fetch | read at an offset (`fetch_records`), long poll | none |
| ListOffsets | partition bounds; `POST /api/v1/fetch/offsets` for times | none |
| OffsetCommit | consumer-group positions, one transaction | none |
| OffsetFetch | `POST /api/v1/consumer-groups/positions` | none |
| Metadata | queues; raft voters are the brokers; the raft LEADER leads every partition | topic list cache, 1 s |
| FindCoordinator | the raft leader | none |
| Join / Sync / Heartbeat / Leave | in memory on the raft leader | members, generation |
| ListGroups / DescribeGroups / DeleteGroups | the same memory; delete forgets the positions | — |
| CreateTopics / CreatePartitions | width row in KV (`queen-kafka` / `qk:topiccfg:<topic>`), then create queue | none |
| DeleteTopics | delete queue, drop width row | none |
| DescribeConfigs | existence only, no values | none |
| InitProducerId (idempotent) | id from clock + node + counter, epoch 0; KIP-360 bumps keep the id | none |
| Idempotent records | each record's Queen `transactionId` is `k<producer id>.<epoch>.<sequence>`: a retried batch is a duplicate in the queue's dedup window and answers its original offsets | none |
| InitProducerId (transactional), AddPartitionsToTxn, AddOffsetsToTxn, TxnOffsetCommit, EndTxn | records and offsets held on the raft leader; EndTxn(commit) = ONE `POST /api/v1/transaction` (pushes + positions + a fence on the KV row `qk:txn:<id>`) | the open transaction, on the leader |
| Group presence (console) | KV rows `qk:groups:<g>` and `qk:live:<g>` written by the leader every 15 s and on every membership change, in the group's own tenant | none |
| SASL/PLAIN | the password is the Queen token, the bearer of every call | per connection |

Why a Fetch is not a pop: a Kafka Fetch names no consumer group, consumers read
ahead of their commits (fetch 100, 200, 300, commit 100 later) and seek. Pop and
ack are Kafka's share groups (KIP-932, ShareFetch / ShareAcknowledge), a
possible later addition.

## Rules that make it one system

1. **The raft leader is the Kafka cluster's only leader.** It leads every
   partition and coordinates every group. A follower answers produce, fetch,
   list-offsets and group requests with `NOT_LEADER_OR_FOLLOWER` /
   `NOT_COORDINATOR`, and the client moves, as it does on any Kafka leader
   election. No facade registry, no node ids of its own, no fence: raft already
   elects one leader.
2. **Groups live only on the leader.** When leadership moves, the old leader
   drops every group (pending joins answered `NOT_COORDINATOR`); the new one
   starts with none, and members rejoin on their next heartbeat or commit
   (`UNKNOWN_MEMBER_ID` / `ILLEGAL_GENERATION`). Committed offsets are Queen
   positions and do not move.
3. **Order per connection.** A produce request is one ordered push; the next
   request is read once it is in the raft log, so anything sent after it lands
   after it. If it is not in the log, the next request waits for its outcome.
4. **Writes go through core operations only**: push, positions transaction,
   KV, configure, delete queue. Nothing else.

## Compatibility with what the first facade wrote

- Records: the same envelope (`src/records.rs`), so existing topics read back.
- Widths: the same KV key and `partitions` field; `QUEEN_KAFKA_DEFAULT_PARTITIONS`
  is still 1024, so keys hash to the same partitions as before.
- Offsets: the first facade's in-process default already kept them as Queen
  positions (`QUEEN_KAFKA_OFFSET_STORE=positions`). The `kv` store is gone.
- API versions: the same windows, minus the keys of phase 2.

## The dashboard

A Kafka topic is a queue and a Kafka group a Queen group, so the console reads
them as it reads its own, with three things the facade adds:

- a record a Kafka client fetches counts as a pop (rate, pop lag, the node's
  throughput), and a commit that moves a group past N records counts N acks
  and adds N to the group's `totalConsumed` (core: consume/txn.rs, positions).
  A group's FIRST commit on a partition counts nothing, because where a Kafka
  consumer started is not known; lag, pending and completed are exact anyway;
- the presence rows above make a live group `kind: kafka`, Stable, with its
  live members, instead of a dead group that never consumed;
- every topic Kafka writes to or auto-creates gets a width row, which is what
  the console reads as "a Kafka topic".

## Transactions: what is guaranteed

- A commit is all or nothing: one raft entry. A fenced commit (a newer producer
  took the `transactional.id`) writes nothing and answers PRODUCER_FENCED.
- An open transaction lives on the leader only. When leadership moves (or the
  node dies) its records are lost; the row says `open`, so the producer's
  commit is answered INVALID_TXN_STATE and nothing partial is ever written. The
  producer aborts and runs the transaction again.
- A commit whose answer was lost is recognized on the retry by its token.
- A transaction past its `transaction.timeout.ms` is aborted and its producer
  fenced (epoch + 1), as Kafka's coordinator does.
- Caps: 8 MiB and 50,000 records per transaction, 1,000 partitions, 256 MiB
  held per node, timeout at most one hour (Flink's default ask).

## Not done

1. **Gap detection for idempotent producers.** Retries are deduplicated, and
   order holds per connection (`in_log`); a producer that has batches in flight
   on two connections at once (Java across a leader move) is not ordered
   across them.
2. Topic configs (`retention.ms`, `cleanup.policy=compact`): accepted, not
   applied, not reported. AlterConfigs / IncrementalAlterConfigs, OffsetDelete,
   ACLs, DescribeLogDirs: not advertised.
3. TLS on the Kafka listener, the standalone binary, Cloud through the proxy,
   the cargo-fuzz targets (they called the first facade's decoders).
4. Group membership survives a leader change only by rejoin. If the pause
   measured on a leader change hurts, snapshot membership into KV.
5. The facade's own writes (presence rows, transaction rows, the timeout
   sweep) use the token of the clients they are about, so they land in those
   clients' tenant; a broker without auth needs none.

## Configuration

`QUEEN_KAFKA_EMBEDDED=true`, `QUEEN_KAFKA_ADVERTISED_ADDR` (required, this
node), `QUEEN_KAFKA_ADDR` (default `0.0.0.0:9092`),
`QUEEN_KAFKA_DEFAULT_PARTITIONS` (1024), `QUEEN_KAFKA_GROUP_JOIN_DELAY_MS`
(3000), `QUEEN_KAFKA_AUTO_CREATE` (true), `QUEEN_KAFKA_DEDUP_WINDOW_SECONDS`
(900: the window of a topic the facade creates, and of a topic with none when
an idempotent producer first writes to it; 0 leaves topics alone), `QUEEN_KAFKA_SASL=required`,
`QUEEN_KAFKA_MAX_CONNECTIONS`, `QUEEN_KAFKA_MAX_REQUEST_BYTES`,
`QUEEN_KAFKA_THREADS`. Other nodes are advertised at the host of their raft
HTTP address (`QUEEN_RAFT_PEERS`) with this node's Kafka port.
`QUEEN_KAFKA_NODE_ID` and `QUEEN_KAFKA_CLUSTER*` are ignored.

## Measured, 2026-10-20 (Alice's Mac, debug build)

- franz-go compat suite (`compat/go`), one node: every failure is a phase-2
  feature above (ACLs, config alters, OffsetDelete, transactions, idempotent
  sequence checks, topic configs) or a test that dials the first facade's
  broker id 0. Produce, consume, groups (rejoin storm included), metadata and
  the native interop tests pass.
- Java kafka-clients 3.9.2 (`compat/java-matrix`): every functional check
  passes (512 records byte-exact with headers, order per partition, commits,
  gzip/snappy/lz4, seeks).
- Three nodes, 3000 records at 100/s, a two-member group, the raft leader
  stopped at 10 s and back at 18 s:

| client, stop | missing | written twice | per-key order breaks | redelivered | longest consumer pause |
|---|---|---|---|---|---|
| franz-go, SIGTERM | 0 | 1 | 0 | 12 | 4.5 s |
| franz-go, SIGKILL | 0 | 1 | 0 | 5 | 7.5 s |
| librdkafka (kcat), SIGKILL | 0 | 0 | 0 | 463 | 5.8 s |

  "Written twice" is the retry of an unknown outcome (phase 2: writer dedup).
  The librdkafka redeliveries are what it consumed since its last commit
  (5 s auto-commit): the new leader knows no group, so commits fail until the
  members rejoin. End-to-end latency, librdkafka producer to consumer: p50 44 ms.
