# queen-s3

A data-lake sink for Queen. It reads a queue's log through the broker's own HTTP
API and writes it into any S3-compatible bucket as JSONL or Parquet, under a
Hive-partitioned layout (`queue=…/dt=…/hour=…`) that DuckDB, Spark, Trino,
Athena, ClickHouse and Snowflake read with nothing in front of them. Data leaves
Queen's format entirely: nothing has to read it back.

It is **not a wire-protocol facade**. Nothing connects to it. It is a client of
Queen like any SDK is, holding no database connection and nothing durable of its
own except three small documents per queue in Queen's key/value store (the window
intent, the commit pointer and the ownership lease) and what it puts in the
bucket.

## The guarantees, in three sentences

**The commit unit is a time window on PostgreSQL's clock**, closed only at or
below a watermark below which no record can still become visible, which makes a
window a deterministic set: rebuilding it produces the same records in the same
order, and with pinned writer settings, the same bytes. So a crash, a retry or a
restart **rewrites the identical object under the identical key** rather than
adding a second copy of a row, and exactly-once needs no conditional PUT, no LIST
and no offset ranges in an object name. Windows are per **queue** rather than per
partition, which is what makes a queue with a million lanes one object an hour
instead of a million objects.

Two things the word "sink" makes people assume, that are not true here: the lake
mirrors the **log** and not the outcome of processing, so a record a consumer
group later nacked or dead-lettered is in it too; and the lake is **plaintext**,
so a queue encrypted at rest inside Queen needs encryption on the bucket to stay
encrypted.

## Run

```
cargo build --release
QUEEN_URL=http://localhost:6632 \
QUEEN_S3_QUEUES=orders \
QUEEN_S3_ENDPOINT=https://s3.eu-central-1.amazonaws.com \
QUEEN_S3_REGION=eu-central-1 \
QUEEN_S3_BUCKET=my-lake \
QUEEN_S3_ACCESS_KEY=… QUEEN_S3_SECRET_KEY=… \
./target/release/queen-s3
```

`QUEEN_S3_ENDPOINT`, `_REGION`, `_BUCKET`, `_QUEUES` and the keypair have no
defaults: there is no bucket that is right more often than it is wrong. Every
other knob does, and the full table with its ranges is on the deploy page below.
A bad value exits 2 with one line naming the variable.

The same binary also runs **inside the broker**: `QUEEN_S3_EMBEDDED=true` makes
the broker spawn and supervise it as a child process, wired to its own listener
over loopback (`server/src/s3_sink.rs`). The repository's `Dockerfile` builds it
beside the broker binary in `/app/bin`, which is what makes embedded mode need no
configuration at all, and `docker run … queen-mq ./bin/queen-s3` runs it alone.

`/healthz` and `/metrics` are served on `QUEEN_S3_LISTEN`, loopback by default.
The probe is green while every queue this process owns has committed a window
inside three times `QUEEN_S3_MAX_WINDOW_MS`, because the failure policy is one
sentence: **the sink never drops, it only lags.** `queen_s3_lag_seconds` is the
number to alarm on.

## Tests

`cargo test`, and the job that runs it is in `.github/workflows/tests.yml` from
this crate's first commit. The S3 client can also be pointed at a real gateway:

```
QUEEN_S3_TEST_ENDPOINT=http://127.0.0.1:17070 QUEEN_S3_TEST_ACCESS_KEY=… \
QUEEN_S3_TEST_SECRET_KEY=… QUEEN_S3_TEST_BUCKET=queen-s3-test \
cargo test --test infra_s3_versitygw -- --nocapture
```

versitygw is the gateway that lane runs against, and it is also the awkward end
of the compatibility range: path-style addressing, no virtual-host DNS, and a
multipart implementation that is not AWS's. Without those four variables the test
skips, so `cargo test` stays hermetic.

## Documentation

Running it, every variable, the API key scopes, the bucket policy and the
`pg_read_all_stats` requirement: [deploy/s3](https://queenmq.com/deploy/s3).
What the bucket holds, the record envelopes, the sidecars, the window commit and
the reader recipes: [reference/s3](https://queenmq.com/reference/s3). Plan and
status: [../../PLAN_S3_SINK.md](../../PLAN_S3_SINK.md).
