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
| InitProducerId | id from clock + node + counter, epoch 0; KIP-360 bumps keep the id | none |
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

## Not in phase 1

1. **Idempotent sequences are not checked.** Producers with
   `enable.idempotence=true` work and never get a fatal error, but a retry after
   an unknown outcome can write a batch twice. Phase 2: writer dedup in the
   broker (per partition and writer, the last ~10 batches with their offsets,
   in the raft state; a gap answers a retriable error, never 45).
2. **Transactions** (`transactional.id`): refused at InitProducerId, as cluster
   mode did. Phase 2: the stage on the leader, one core transaction at EndTxn.
3. Topic configs (`retention.ms`, `cleanup.policy=compact`): accepted, not
   applied, not reported. AlterConfigs / IncrementalAlterConfigs, OffsetDelete,
   ACLs, DescribeLogDirs: not advertised.
4. TLS on the Kafka listener, the standalone binary, Cloud through the proxy.
5. Group membership survives a leader change only by rejoin. If the pause
   measured on a leader change hurts, snapshot membership into KV.
6. The console's Kafka presence rows (`qk:live:*`) are no longer written.

## Configuration

`QUEEN_KAFKA_EMBEDDED=true`, `QUEEN_KAFKA_ADVERTISED_ADDR` (required, this
node), `QUEEN_KAFKA_ADDR` (default `0.0.0.0:9092`),
`QUEEN_KAFKA_DEFAULT_PARTITIONS` (1024), `QUEEN_KAFKA_GROUP_JOIN_DELAY_MS`
(3000), `QUEEN_KAFKA_AUTO_CREATE` (true), `QUEEN_KAFKA_SASL=required`,
`QUEEN_KAFKA_MAX_CONNECTIONS`, `QUEEN_KAFKA_MAX_REQUEST_BYTES`,
`QUEEN_KAFKA_THREADS`. Other nodes are advertised at the host of their raft
HTTP address (`QUEEN_RAFT_PEERS`) with this node's Kafka port.
`QUEEN_KAFKA_NODE_ID` and `QUEEN_KAFKA_CLUSTER*` are ignored.
