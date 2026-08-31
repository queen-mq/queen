# compat/rust-rdkafka — the `rdkafka` crate against queen-kafka

The Rust binding to librdkafka, driven through `FutureProducer` and
`StreamConsumer`, against a running queen-kafka facade.

```
QUEEN_KAFKA_BOOTSTRAP=127.0.0.1:19092 QUEEN_KAFKA_PARTITIONS=8 ./run.sh
```

Nothing here starts or stops a stack. That is `rig.sh`'s job, or yours.

| | |
|---|---|
| crate | `rdkafka` 0.39.0, `default-features = false`, features `cmake-build,libz,ssl,zstd,tokio` |
| C library | librdkafka **2.12.1** (`rdkafka-sys` 4.10.0+2.12.1), vendored and compiled by the build |
| built | `cargo build` — no system librdkafka, no pkg-config for it, nothing to install |
| entry point | `./run.sh [scenario]`, or `./target/debug/compat <bootstrap> <runId>` |

## Why this row exists beside `compat/librdkafka`

`compat/librdkafka` drives the same C library through **kcat** and through
**confluent-kafka-python**. This one drives it through the **Rust** binding,
which is a different surface with different defaults:

* `FutureProducer` hands the caller one future per record carrying the
  **partition and offset the broker assigned**. kcat and the Python consumer
  throw that away. If the facade ever invented an offset instead of reporting
  the one Queen assigned (C1, `PushResult::offset`), this is the row that sees
  it — and it asserts on it: on a fresh 8-partition topic the n-th record
  acknowledged on a partition must come back at offset n.
* `StreamConsumer` is an async stream over the same rebalance callbacks, so the
  suite can assert what the coordinator *handed out*, not just which records
  turned up.
* It carries a **different librdkafka**: 2.12.1, vendored and pinned by
  `rdkafka-sys` 4.10.0, against the 2.15 the librdkafka row gets from Homebrew's
  kcat. One version behind, not ahead — which is the more useful side to be on,
  because the older client is the one still deployed, and two points on the
  version curve are what tell you whether a negotiation result is a property of
  the facade or of one client build. Both send the same versions here
  (Produce v9, Fetch v6, Metadata v9).

## What it proves

| Scenario | What it asserts |
|---|---|
| `metadata` | one broker; a producer's bare Metadata auto-creates at `QUEEN_KAFKA_DEFAULT_PARTITIONS`; a consumer's does **not** (librdkafka defaults `allow.auto.create.topics=false`) and is answered UNKNOWN_TOPIC_OR_PARTITION; `allow.auto.create.topics=true` makes it; a `__`-prefixed name is never created |
| `roundtrip` | 512 records over 8 partitions at `acks=all` with keys, six headers and CreateTime; acknowledged offsets are 0..63 per partition; consumed back through a **consumer group** byte-exact and in per-partition order |
| `codecs` | the same round trip under `none`, `gzip`, `snappy`, `lz4` and `zstd`, and whether librdkafka actually *sent* each one |
| `resume` | a manual `OffsetCommit`, the consumer closed, a **new consumer in the same group** resuming at exactly the committed offset, with no loss and no overlap |
| `autocreate` | producing to a topic nobody has ever named |
| `offsets` | `fetch_watermarks` earliest/latest, assign-at-offset, `seek(Beginning)`, `seek(End)`, `offsets_for_times` |
| `idempotence` | the failure mode with `enable.idempotence=true`, which is unsupported on purpose (M7) |
| `sasl` | SASL/PLAIN over TLS, produce + group consume, and a wrong password being refused *as an authentication failure* |

Every record is a pure function of its sequence number, so the consumer side
recomputes what it expects rather than remembering what was sent. Three details
in the corpus are aimed at the payload envelope (`protocols/queen-kafka/src/records.rs`)
rather than at the transport:

* the **value is not UTF-8** — it carries `00 ff fe 80 7f`, which no JSON string
  can hold, so a value that survives proves the base64 envelope;
* the headers carry an **empty value and a null value**, which Kafka
  distinguishes and the envelope writes as `""` and `null`;
* one header **name appears twice**, which Kafka allows and an `IndexMap`
  cannot hold — the case `wire::header_lists` exists for.

## Config that is not the default, and why

Three settings in `src/clients.rs` are deliberate; everything else is
librdkafka's default, because a suite that tunes its way to a pass has proved
something about the tuning.

* **`enable.idempotence=false`** — librdkafka already defaults it off, so today
  this line changes nothing. It is written down because it is the one setting
  that breaks every other Kafka client against this facade, and the day
  librdkafka flips its default the way the Java client did in 3.0, this line is
  what keeps the suite honest.
* **`debug=protocol` + log level `Debug`** — not for troubleshooting. It is the
  only way to read which API versions were actually negotiated. The suite prints
  them as a table, parsed out of librdkafka's own `Sent …Request (v…)` lines.
* **`session.timeout.ms=10000`** — inside the facade's
  `QUEEN_KAFKA_GROUP_MIN/MAX_SESSION_TIMEOUT_MS` window (6000..300000). Outside
  it the join is answered INVALID_SESSION_TIMEOUT (26).

## Three things in the transcript that are the CLIENT's, not the facade's

1. **`zstd` is downgraded to uncompressed.** librdkafka gates zstd *produce* on
   the broker advertising Fetch v10; queen-kafka caps Fetch at v6 on purpose
   (`versions.rs` — v7 introduces fetch sessions), so librdkafka logs
   `Broker does not support compression type zstd: not compressing batch` and
   sends it plain. The records land byte-exact. `gzip`, `snappy` and `lz4` are
   sent compressed and decompressed correctly.
2. **`enable.idempotence=true` fails.** InitProducerId is M7 and not advertised.
   librdkafka is the well-behaved case: it reads ApiVersions and fails the send
   up front rather than dying mid-batch the way the Java producer does.
3. **A `Connect to ipv6#[::1]:… failed` on a hostname bootstrap.** The facade
   binds exactly what `QUEEN_KAFKA_ADDR` says; `0.0.0.0` is IPv4 only, and a
   name like `localhost` resolves to `::1` first on macOS. librdkafka tries it,
   is refused, and falls back to IPv4 — one wasted connect per connection, and
   a `BrokerTransportFailure` surfaced through the consumer stream on the way.
   The suite reports these and keeps polling, which is what a real consumer
   does; only failing to get the records by the deadline is a failure.

A fourth is a property of the Rust binding worth knowing before you debug
something else: rdkafka forces `log.queue=true` and routes librdkafka's log
stream to the main queue (`rdkafka-0.39/src/client.rs:257`). **A consumer that is
never polled has an empty log**, so an error you are sure happened can look
absent. `scenarios::sasl_tls` polls while it waits for exactly this reason.

## Wiring it into rig.sh

Not done here — nothing in `compat/` was edited. The row would be one block
beside the librdkafka one, needing no new ports and no new containers:

```bash
if [ "$RUST_RDKAFKA" = 1 ]; then
  say "rdkafka (Rust) against $KAFKA_HOST:$KAFKA_PORT"
  QUEEN_KAFKA_BOOTSTRAP="$KAFKA_HOST:$KAFKA_PORT" \
  QUEEN_KAFKA_PARTITIONS="$PARTITIONS" \
  RUN_ID="$RUN_ID" \
  ${M5:+QUEEN_KAFKA_TLS_BOOTSTRAP="localhost:$KAFKA_TLS_PORT"} \
  ${M5:+QUEEN_KAFKA_SASL_TOKEN="$TOKEN"} \
  ${M5:+QUEEN_KAFKA_TLS_CA="$LOGDIR/tls.crt"} \
    "$HERE/rust-rdkafka/run.sh" all || FAIL=1
fi
```

Two notes for whoever adds it:

* the first run compiles librdkafka (~15s from cold, then nothing), so a CI lane
  wants the target directory cached — `**/target/` is already gitignored;
* pass `localhost:$KAFKA_TLS_PORT`, not `127.0.0.1:$KAFKA_TLS_PORT`, for the M5
  lane. librdkafka's default `ssl.endpoint.identification.algorithm=https`
  checks the dialled name against the certificate's **DNS** SANs, and the rig
  cert's `IP:127.0.0.1` SAN does not satisfy it.

## Layout

```
Cargo.toml     the pin and the feature set, with the reasoning
run.sh         the entry point; reads the environment, builds, runs
src/main.rs    argv/env, scenario dispatch, the RESULT line
src/clients.rs every producer and consumer, and the exact config
src/records.rs the corpus and what byte-exact is checked against
src/probe.rs   the ClientContext that reads the negotiated versions out of
               librdkafka's own debug stream, plus rebalance and commit callbacks
src/scenarios.rs the scenarios
src/harness.rs   ok/FAIL/RESULT, and the deadlines
```

There is no build system beyond cargo, nothing is published, and the crate
declares an empty `[workspace]` so it is never absorbed into `server/` or
`protocols/queen-kafka/` — this repository has no cargo workspace and this row must not
become the first one.
