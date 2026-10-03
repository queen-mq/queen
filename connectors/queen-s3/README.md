# queen-s3

A data-lake sink for Queen. It writes each queue's log into any S3-compatible
bucket as JSONL or Parquet, under a Hive-partitioned layout
(`tenant=…/queue=…/dt=…/hour=…`) that DuckDB, Spark, Trino, Athena, ClickHouse
and Snowflake read with nothing in front of them. Data leaves Queen's format
entirely: nothing has to read it back.

It is a library linked into the broker (feature `s3`, on by default) and run
in-process on **every node** of the cluster with the same configuration. Each
node reads its own applied copy of the log through the broker's in-process
twins of `POST /api/v1/fetch` and `POST /api/v1/partitions/changed`; nothing
connects to it and it opens no listener. Per queue it keeps three small
documents in Queen's key/value store — the window intent, the commit pointer
and the ownership lease — and the rest of its state is in the bucket.

## The guarantees, in three sentences

**The commit unit is a time window on the broker's log clock**, `[T_{k-1},
T_k)` over the stamp each record's append got in the replicated log, closed
only at or below the `safeTime` of the node that reads it — the greatest stamp
that node has applied, below which nothing can still become visible there.
Stamps strictly increase in log order, so a window is a deterministic set:
rebuilding it produces the same records in the same order, and with pinned
writer settings, the same bytes. So a crash, a retry, a restart or a handover
to another node **rewrites the identical object under the identical key**
rather than adding a second copy of a row, and exactly-once needs no
conditional PUT, no LIST and no offset ranges in an object name. Windows are
per **queue** rather than per partition, which is what makes a queue with a
million lanes one object an hour instead of a million objects.

Two things the word "sink" makes people assume, that are not true here: the lake
mirrors the **log** and not the outcome of processing, so a record a consumer
group later nacked or dead-lettered is in it too; and the lake is **plaintext**,
so a queue encrypted at rest inside Queen needs encryption on the bucket to stay
encrypted.

## What identifies a record

Every record carries `partition`, `offset`, `transactionId`, `ts` and
`payload`. Within one incarnation of a partition, `(partition, offset)` is
unique. But a partition can be deleted — by retention, once it has been idle
and empty for `PARTITION_CLEANUP_DAYS`, or with its queue — and created again
under the same name, and the new incarnation restarts at offset 0. So across
the lake the unique key of a record is **`(partition, offset, ts)`**: `ts` is
the stamp of the append that wrote the record, and it strictly increases from
one append to the next across the whole broker, so every record of a later
incarnation is stamped after every record of the one before. Within a
partition, ordering by `(ts, offset)` is log order.

## One sink per tenant, one bucket per tenant

Every broker tenant has a sink of its own, with its own bucket and
credentials: the default tenant's from the `QUEEN_S3_*` environment, every
other tenant's from the control plane, as a JSON document of the same
settings in camelCase (`Config::from_tenant_doc`; `validate_tenant_doc` is the
same check without the secret key, which the document never holds). Every key
carries its tenant first — `<prefix>/tenant=<id>/queue=<name>/…`, and
`<prefix>/_queen/tenant=<id>/queue=<name>/…` for the manifests and checkpoints
— so two tenants that share a bucket and a prefix never share a key, and a
reader gets a `tenant` column beside `queue` from Hive discovery. Each manifest
names its tenant, and a Parquet footer carries `queen.tenant` beside
`queen.queue`. The KV documents of a tenant's queues live in that tenant's own
key/value store, where its retention hold reads them.

What is node-wide stays in the environment and is shared by every sink of the
node (`NodeKnobs`, `SinkShared`): the memory budget — one budget, so when it
is reached the largest buffer of any tenant's queue closes first — the fetch
concurrency, the discovery interval, the safe guard, the lease TTL, the
multipart threshold, the checkpoint cadence, the node's lease instance, and
one metric registry, rendered once, each tenant's series labelled `tenant`
(the default tenant's carry no label).

## One cluster, one writer per queue

Every node has a task for every configured queue, and each task claims the
queue's lease (`s3:<sink>:<queue>:lease`, a TTL'd KV row): one node wins and
runs the queue, the others read who holds it and look again after the TTL. A
node claims a free queue after a wait that grows with the queues it already
runs, of every tenant (a second each, plus a jitter, at most half the TTL), so
the least-loaded node claims first and the queues spread over the nodes; a row
an earlier life of the same node left behind is taken back at once. A read or
a claim that fails for a reason that passes — no leader yet at a cold start, a
503, a timeout — is tried again after about a second, doubling up to the TTL:
only a queue another node holds waits a whole TTL. Every intent and every
commit carries a conditional write of that lease row at index 0 with
`required`, so a node that lost its lease — stalled, or cut off from the leader
— cannot commit anything: its batch is rolled back whole. The refresh and those
batches take turns on the row, so a node never fences itself. When the owner
stops it gives the lease back — retrying for up to ten seconds when the KV
store cannot answer, as on a leader that is handing its leadership over while
it drains; when it dies another node takes the queue after the TTL and starts
from the latest commit pointer (KV reads are linearizable).
A window that was in flight is finished from its intent: committed from its
manifest if its upload had finished, otherwise rebuilt — and a rebuild uploads
nothing until the new node has applied the whole window. A queue named in
`QUEEN_S3_QUEUES` that does not exist yet is looked for again every five
seconds.

Every line the sink logs is inside a `sink` span, which carries `tenant` for
a sink with a tenant label (every tenant but the broker's default one, the same
rule as the metrics), and every line of one queue's task inside a
`queue{queue=…}` span below it: `sink{tenant=acme}:queue{queue=orders}:
queen-s3: window committed queue=orders k=1 …`.

The lease refresh is a log entry, so it also keeps `safeTime` moving: a broker
with no traffic writes nothing, and its applied clock would otherwise stand
still and hold the last window open.

## Queues spread over the nodes, and stay put

A node that starts alone — a Kubernetes StatefulSet starts its pods one after
another — claims every queue, so claiming in turn is not enough: the sink
rebalances. Each node keeps a presence row (`s3:<sink>:node=<instance>`,
written at the lease's cadence with the lease's TTL, deleted on a clean stop)
and counts the live ones, N; its fair share is `ceil(Q / N)` for the Q queues
the sink runs. A node that holds more than its share for a whole TTL gives one
queue back — the least buffered — by draining that queue exactly as a stop
drains every queue, and gives back the next only once the last is held by
another node (or a TTL has passed). A node at its share waits a TTL more before
it claims a free queue and looks at the queues others hold only once a TTL,
while a node below its share looks every third of a TTL and claims at once;
the node that gave a queue back leaves it alone for two TTLs. So a queue given
back goes to a node that is short, and in a steady state nothing moves: a
staggered start of three nodes over six queues ends at two each, a node that
dies has its queues taken a TTL or so after its lease expires, and one that
comes back gets its share again within two TTLs. There is no knob beyond the
lease TTL. `Sink::status()` reports the node's view (`placement`: nodes, queues,
share, held, the queue being given back), and
`queen_s3_queues_given_back_total` counts the hand-offs.

## Run

```
QUEEN_S3_EMBEDDED=true \
QUEEN_S3_QUEUES=orders \
QUEEN_S3_ENDPOINT=https://s3.eu-central-1.amazonaws.com \
QUEEN_S3_REGION=eu-central-1 \
QUEEN_S3_BUCKET=my-lake \
QUEEN_S3_ACCESS_KEY=… QUEEN_S3_SECRET_KEY=… \
./queen
```

`QUEEN_S3_EMBEDDED=true` is the broker's switch; every other variable is read
once, at the broker's boot: the node-wide ones by `NodeKnobs::from_env_with`,
the default tenant's sink by `Config::from_env`. `QUEEN_S3_QUEUES` is what turns
that sink on — without it the node runs only the sinks the control plane
configures, and a bucket or a key set without it is refused. With it,
`QUEEN_S3_ENDPOINT`, `_REGION`, `_BUCKET` and the keypair have no defaults:
there is no bucket that is right more often than it is wrong. Every other knob
does, and the full table with its ranges is on the deploy page below.
`QUEEN_S3_INSTANCE`, the name a node holds leases under, defaults to the
broker's node identity. `QUEEN_S3_MEMORY_MB`, the buffer budget across every
queue of every sink a node runs, defaults to 512 because the buffers live in
the broker's own process. A bad value fails the broker's boot with one line
naming the variable. A bucket that does not answer at boot does not: the queues wait, the
probe is retried behind a backoff, and the status says so.

To keep records in Queen until the lake has them, configure the queue with
`retentionSinkHold=<sink>`: retention then keeps every record younger than the
committed window's `tEnd` minus a minute, capped by
`retentionSinkHoldMaxSeconds` so that a stopped sink cannot hold retention for
ever (server/src/rsm/maintenance.rs `sink_floor`).

The broker reports the sink through `Sink::status()` — per queue: owned here or
held by which node, the state, the last committed window and its `tEnd`,
`completeThrough`, the lag, the last error — and appends `Sink::prometheus()`
to its metrics. `completeThrough` is the stamp the lake is complete through:
every record stamped below it is in a committed window or provably absent. It
is `tEnd`, or later when the queue is read to the end with nothing pending, so
it follows the clock on an idle queue. The lag is `safeTime − completeThrough`,
and the health verdict is red when a queue a node runs lags more than three
times `QUEEN_S3_MAX_WINDOW_MS` (30 s at the least), because the failure policy
is one sentence: **the sink never drops, it only lags.** An idle queue stays
green; a queue with records it cannot ship goes red, and so does a backfill
until it is back inside the budget. `queen_s3_lag_seconds` is the number to
alarm on; a node exports it, like every per-queue gauge, only for the queues it
runs.

## Tests

`cargo test --manifest-path connectors/queen-s3/Cargo.toml`, and the job that
runs it is in `.github/workflows/tests.yml`. The S3 client can also be pointed
at a real gateway:

```
QUEEN_S3_TEST_ENDPOINT=http://127.0.0.1:17070 QUEEN_S3_TEST_ACCESS_KEY=… \
QUEEN_S3_TEST_SECRET_KEY=… QUEEN_S3_TEST_BUCKET=queen-s3-test \
cargo test --manifest-path connectors/queen-s3/Cargo.toml \
  --test infra_s3_versitygw -- --nocapture
```

versitygw is the gateway that lane runs against, and it is also the awkward end
of the compatibility range: path-style addressing, no virtual-host DNS, and a
multipart implementation that is not AWS's. Without those four variables the test
skips, so `cargo test` stays hermetic.

## Documentation

Running it, every variable, the bucket policy and the retention hold:
[deploy/s3](https://queenmq.com/deploy/s3). What the bucket holds, the record
envelopes, the sidecars, the window commit and the reader recipes:
[reference/s3](https://queenmq.com/reference/s3).
