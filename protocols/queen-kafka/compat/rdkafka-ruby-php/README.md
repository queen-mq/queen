# rdkafka-ruby-php — the `rdkafka` gem and php-rdkafka against queen-kafka

Two thin librdkafka bindings that no other row of the M6 matrix covers:

| | library | version tested | librdkafka | how it gets librdkafka |
|---|---|---|---|---|
| Ruby | `rdkafka` (karafka's core) | 0.29.0 | **2.14.2** | **precompiled** in the `aarch64-linux-gnu` platform gem |
| PHP | `php-rdkafka` (pecl) | 6.0.5 | **2.6.1** | **links the system library** — whatever the image/distro provides |

The librdkafka C core is already proven by `compat/librdkafka`. Re-proving the wire
protocol here would be theatre. What these two add is the thing the core cannot
tell you: **packaging and defaults**. They are also, deliberately, on opposite
sides of a librdkafka behaviour change that turns out to matter a great deal here —
see [Compression](#compression-the-finding).

Both suites pass end to end: 512 messages over 8 partitions with keys, headers and
binary payloads, consumer groups, commit/resume across a restart, watermarks,
repositioning, auto-create, and a SASL/PLAIN-over-TLS lane.

## Running

The stack is `compat/rig.sh`'s job, or yours. This directory starts nothing.

```sh
KAFKA_BOOTSTRAP=host.docker.internal:19092 ./run.sh          # both suites
./run.sh ruby | php | probe | build
```

| env | meaning |
|---|---|
| `KAFKA_BOOTSTRAP` | plaintext facade (default `host.docker.internal:19092`) |
| `RUN_ID` | topic/group suffix so reruns never collide (default: epoch seconds) |
| `KAFKA_PARTITIONS` | expected topic width (default 8) |
| `KAFKA_SASL_BOOTSTRAP` | SASL listener; **unset skips the SASL lane entirely** |
| `KAFKA_SASL_PROTOCOL` | `sasl_ssl` (default) or `sasl_plaintext` |
| `KAFKA_SASL_TOKEN` | the Queen bearer token — this is the SASL **password** |
| `KAFKA_SSL_CA` | PEM to verify the listener with (mounted into the container) |
| `KAFKA_SSL_INSECURE=1` | skip certificate verification |
| `TRACE_DIR` | where each run's librdkafka protocol trace is kept |
| `CONTAINER_PREFIX` | `docker --name` prefix (default `qkcompat-ruby-php`) |
| `REBUILD=1` | force a `docker build` |

## The one thing that is different about this directory

Every other suite in `compat/` runs its client **on the host**. These two cannot:
the `rdkafka` gem needs Ruby 3.x (macOS ships 2.6) and php-rdkafka needs a pecl
build against a system librdkafka. Both therefore run **inside a container**, from
the official `ruby:3.3-bookworm` and `php:8.3-cli-bookworm` images.

That changes exactly one thing: **the address the facade advertises**. A container
cannot reach a facade advertising `127.0.0.1` — it completes the bootstrap Metadata
and then dies re-dialling itself. The facade must be started with

```
QUEEN_KAFKA_ADDR=0.0.0.0:<port>                       # bind
QUEEN_KAFKA_ADVERTISED_ADDR=host.docker.internal:<port>   # what clients re-dial
```

`run.sh` reads the advertised address off the wire before it runs anything and
exits `BLOCKED` with that instruction rather than letting every suite time out on
it. That failure looks exactly like a facade bug and is not one.

## Compression: the finding

`compat/README.md` documents one compression downgrade — zstd, because librdkafka
gates the zstd feature on Fetch v10 and the facade caps Fetch at v6 on purpose
(v7 is fetch sessions, KIP-227). That is real. It is not the whole story.

**On librdkafka ≤ 2.11.0, gzip, snappy AND lz4 are silently downgraded to
uncompressed as well**, for an unrelated reason. From
`rdkafka_msgset_writer.c`, `rd_kafka_msgset_writer_select_MsgVersion()`:

```c
static const struct { int feature; int16_t ApiVersion; }
compr_req[RD_KAFKA_COMPRESSION_NUM] = {
        [RD_KAFKA_COMPRESSION_LZ4]  = {RD_KAFKA_FEATURE_LZ4,  0},
        [RD_KAFKA_COMPRESSION_ZSTD] = {RD_KAFKA_FEATURE_ZSTD, 7},
        /* gzip and snappy are absent, so their entries are zeroed: {0, 0} */
};
if (msetw->msetw_compression &&
    (rd_kafka_broker_ApiVersion_supported(
         rkb, RD_KAFKAP_Produce, 0,
         compr_req[msetw->msetw_compression].ApiVersion, NULL) == -1 || ...))
        msetw->msetw_compression = RD_KAFKA_COMPRESSION_NONE;
```

For gzip, snappy and lz4 that asks *"does this broker support Produce somewhere in
`[0, 0]`?"*, and `rd_kafka_broker_ApiVersion_supported()` answers with
`if (ret.MinVer > maxver) return -1;`.

queen-kafka advertises **Produce 3..=9** (`versions.rs` — v0–v2 are the legacy
message sets nothing has sent in a decade). `MinVer 3 > maxver 0`, so the call
returns −1 and the codec is dropped. It is the Produce **floor** that does this,
not any missing feature. Real Kafka advertises Produce from v0, so no real broker
ever trips it.

librdkafka **2.11.1** replaced that call with `rd_kafka_broker_ApiVersion_at_least()`,
which asks the sane question (*is MaxVer ≥ N?*), and the problem disappears.
Measured, both against this facade:

```
librdkafka 2.14.2 (rdkafka gem)   gzip COMPRESSED  snappy COMPRESSED  lz4 COMPRESSED  zstd DOWNGRADED
librdkafka 2.6.1  (php-rdkafka)   gzip DOWNGRADED  snappy DOWNGRADED  lz4 DOWNGRADED  zstd DOWNGRADED
```

`probe_compression.rb` / `probe_compression.php` are that table, reproducible.

This is **not** a correctness problem — every codec still delivers, byte-exact —
and the facade itself is innocent: a Java producer with `compression.type=lz4`
sends a genuinely compressed batch to the same facade and all records read back
fine, so `decompress.rs` works. What is lost is the compression, silently, on
exactly the deployments that asked for it, mentioned once per **day** per broker at
`LOG_NOTICE`. It affects every librdkafka binding older than 2.11.1 — and
php-rdkafka is the worst case, because it links the **system** library and Debian
bookworm ships librdkafka 1.9.2.

Suggested fix, for whoever owns `versions.rs`: lowering the advertised Produce
**floor** to 0 while still refusing v0–v2 requests at dispatch would satisfy the
client-side check without accepting a legacy message set. That is a contract change
and explicitly not made here.

## Other client-side behaviours worth knowing

* **php-rdkafka 6.0.5 has no `KafkaConsumer::seek`.** The Ruby gem has both `seek`
  and `seek_by`; the extension has neither. Repositioning is
  `assign([new TopicPartition($topic, $p, $offset)])`, which issues the same Fetch.
  Section 6 asserts the gap so it stays visible if a later version closes it.
* **A wrong SASL password reaches Ruby as a delivery *timeout*.** librdkafka treats
  SASL failure as retriable: it logs the facade's real reason
  (`Queen refused this credential (HTTP 401)`) and starts a re-bootstrap loop, so
  the gem raises `Rdkafka::AbstractHandle::WaitTimeoutError` after
  `message.timeout.ms`. PHP sees it immediately because it can install an
  `setErrorCb`. Java fails fast with `SaslAuthenticationException`. The suites print
  the real reason out of the trace.
* **Idempotence must stay off** — `InitProducerId` is unimplemented (M7). librdkafka
  defaults it off, but both suites set `enable.idempotence=false` explicitly so a
  run never depends on a default holding.
* **The rdkafka gem ships precompiled.** `bundle install` resolved
  `rdkafka 0.29.0 (aarch64-linux-gnu)` — a platform gem with librdkafka 2.14.2
  already built in. The `libzstd-dev` / `libsasl2-dev` headers in `ruby/Dockerfile`
  are therefore belt-and-braces for the source-gem fallback, not what produced this
  run's core. Section 0 prints the gem platform so this is never guesswork.
* **Every group formation costs 3 s** (`QUEEN_KAFKA_GROUP_JOIN_DELAY_MS`, the
  default). Each suite forms four groups, so budget ~40 s per language.

## Layout

```
run.sh                     entry point; assumes a running stack, reads env, has the preflight
ruby/Dockerfile            ruby:3.3-bookworm + bundle install
ruby/Gemfile               `gem "rdkafka"`, deliberately unpinned
ruby/compat.rb             the suite: sections 0-9
ruby/probe_compression.rb  the codec table above, from the new side of 2.11.1
php/Dockerfile             php:8.3-cli-bookworm + librdkafka from source + pecl rdkafka
php/compat.php             the suite: sections 0-9
php/probe_compression.php  the codec table above, from the old side of 2.11.1
```

Both suites follow the `compat/` conventions: `[bootstrap] [runId]` positionally,
`=== ` section headers, one `  ok  ` / `  FAIL ` line per assertion, a final
`RESULT: PASS` / `RESULT: FAIL (n)`, non-zero exit on failure, every blocking call
under a deadline, and the negotiated API versions read out of librdkafka's own
`debug=protocol` stream rather than assumed.

### Wiring into rig.sh

`rig.sh` is not edited by this directory. To add it, the shape would be: a `--ruby-php`
flag that, after the facade is up, runs

```sh
KAFKA_BOOTSTRAP="host.docker.internal:$KAFKA_PORT" \
KAFKA_SASL_BOOTSTRAP="${M5:+host.docker.internal:$KAFKA_TLS_PORT}" \
KAFKA_SASL_TOKEN="$SASL_TOKEN" KAFKA_SSL_CA="$LOGDIR/tls.crt" \
RUN_ID="$RUN_ID" TRACE_DIR="$LOGDIR/rdkafka-ruby-php" \
  compat/rdkafka-ruby-php/run.sh all
```

with **one prerequisite the rig does not currently meet**: the facade it starts
advertises `127.0.0.1`, which no container can re-dial. Either start the
plaintext facade with `QUEEN_KAFKA_ADVERTISED_ADDR=host.docker.internal:$KAFKA_PORT`,
or add a third facade on its own port that does — one broker can carry several,
which is exactly what `--m5` already does. Offsets are shared (they live in Queen
KV); **group membership is not** (in-memory, per facade), so never split one
consumer group across two facades.

If the SASL lane is wired with `KAFKA_SSL_CA` rather than `KAFKA_SSL_INSECURE=1`,
note that the rig's self-signed certificate has SANs for `kafka.example.com`,
`shared.queenmq.cloud`, `localhost` and `127.0.0.1` but **not**
`host.docker.internal`, so the suites also set
`ssl.endpoint.identification.algorithm=none`. Adding `host.docker.internal` as a
SAN to that certificate would remove the workaround for every containerised client.
