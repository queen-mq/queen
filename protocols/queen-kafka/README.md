# queen-kafka

The Kafka wire protocol for Queen. Since 2.6 it is a translation layer: the
broker runs it in-process (`QUEEN_KAFKA_EMBEDDED=true`), every Kafka request
becomes operations the broker already has, and nothing the facade knows
outlives a connection or a raft leader's term. A topic is a Queen queue, Kafka
partition `n` is the Queen partition named `"n"`, and a Kafka group is the Queen
consumer group of the same name, so Kafka and Queen clients read and write the
same records.

How to use it and what it does are on the docs site:

- [Kafka clients](https://queenmq.com/guides/kafka/): how it works, how Kafka
  maps onto Queen, groups, idempotent producers, transactions, a cluster, limits.
- [Kafka reference](https://queenmq.com/reference/kafka/): every setting, topic
  configs, APIs and versions, where it differs from Apache Kafka, the clients
  run against it, security and ceilings.
- [Kafka Streams, Connect and Flink](https://queenmq.com/guides/kafka-frameworks/).

[REDESIGN.md](REDESIGN.md) records the design of the rewrite (2026-10-20) and
why the first facade, the one 2.0 to 2.5 shipped, was replaced.

## Layout

| File | Kafka requests | What it does |
|---|---|---|
| `src/lib.rs`, `src/server.rs` | | The listener, and what every connection shares |
| `src/conn.rs` | | One connection, answered in order; a produce lets the next request in once its push is in the raft log |
| `src/core.rs` | | The `Broker` trait, the facade's only door into Queen, implemented by `server/src/kafka_inproc.rs` |
| `src/handlers/produce.rs` | Produce | One ordered push per request |
| `src/handlers/fetch.rs` | Fetch, ListOffsets | Reads at an offset, with the long poll |
| `src/handlers/offsets.rs` | OffsetCommit, OffsetFetch | The Queen consumer group's positions; a commit is one transaction |
| `src/handlers/group.rs`, `src/groups.rs` | JoinGroup, SyncGroup, Heartbeat, LeaveGroup, ListGroups, DescribeGroups, DeleteGroups | The classic group protocol, in memory on the raft leader |
| `src/handlers/txn.rs`, `src/txn.rs` | AddPartitionsToTxn, AddOffsetsToTxn, TxnOffsetCommit, EndTxn | Records and offsets held on the raft leader, committed as one Queen transaction |
| `src/handlers/meta.rs` | ApiVersions, Metadata, FindCoordinator, InitProducerId | The raft cluster as the Kafka cluster |
| `src/handlers/admin.rs`, `src/topics.rs` | CreateTopics, DeleteTopics, CreatePartitions, DescribeConfigs | A topic is a queue, with a width kept in KV |
| `src/handlers/sasl.rs`, `src/sasl.rs` | SaslHandshake, SaslAuthenticate | SASL/PLAIN: the password is a Queen token |
| `src/presence.rs` | | The KV rows the console reads to show a Kafka group as live |
| `src/records.rs` | | One Kafka record as one Queen payload, and back |
| `src/wire.rs`, `src/decompress.rs` | | The records section of a RecordBatch, and decompression with a memory ceiling |
| `src/versions.rs` | | The API versions it answers |
| `src/config.rs`, `src/errors.rs` | | The settings read at boot, and Kafka's error codes |

## Tests

`cargo test --manifest-path protocols/queen-kafka/Cargo.toml` runs the unit
tests, as CI does in the `kafka-facade` job of `tests.yml`, with rustfmt and
clippy. The broker's half, `server/src/kafka_inproc.rs`, is tested with the
server.

`compat/` holds the client suites that the reference's client table was run
with, by hand, against 2.6 on 2026-10-20, on one node and on three. Their
READMEs and rigs were written for the first facade: `compat/rig.sh` and the
`cluster/` and `transactions/` rigs start its standalone binary, which 2.6 no
longer builds. Start a node with the facade on and point a suite at it instead;
the Go suites read `QUEEN_KAFKA_BOOTSTRAP`.
