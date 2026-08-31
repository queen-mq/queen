# brod (Erlang/OTP) vs queen-kafka

The brod row of the M6 client matrix. brod is the client under essentially all
Erlang and Elixir Kafka traffic, directly or through kaffe.

**Headline: stock brod cannot produce to queen-kafka at all.** Everything else
about brod works, including all four compression codecs, group consumption with
offset commit, resume-from-committed, and SASL/PLAIN over TLS. One field on the
Produce request is the whole gap.

## What was run

| | |
| --- | --- |
| brod | 4.6.3 (hex) |
| kafka_protocol | 4.3.6 |
| Erlang | OTP 27, erts 15.2.7.12, arm64 Linux in a container |
| codec libs | snappyer 1.2.10, lz4b 0.1.0, ezstd 1.2.4 |
| facade | `QUEEN_KAFKA_DEFAULT_PARTITIONS=8`, group join delay left at the default 3000ms |

## The blocker, in one paragraph

kafka_protocol builds the Produce request by hand instead of from its own
schema, and the hand-rolled encoder types `transactional_id` as `string` rather
than `nullable_string`:

```erlang
kpro_req_lib.erl:308   [encode(string, transactional_id(TxnCtx)) || Vsn > 2]
kpro_req_lib.erl:593   transactional_id(false) -> ?kpro_null.
kpro_lib.erl:140       encode(string, ?null) -> encode(string, "").
```

So a **non-transactional** produce puts a zero-length string on the wire where
the schema says null. kafka_protocol's own schema disagrees with its encoder —
`kpro_schema.erl:212` reads `{transactional_id, nullable_string}`.

queen-kafka refuses any *present* transactional id
(`src/handlers/produce.rs:195`, `if let Some(id) = &req.transactional_id`) and
answers `TRANSACTIONAL_ID_AUTHORIZATION_FAILED` (53). brod treats 53 as
`not_retriable`, so `brod_producer` exits on the first send and never recovers:

```
** Reason for termination ==
** {{not_retriable,
       {produce_response_error,<<"brod-main-full1">>,0,-1,
           transactional_id_authorization_failed}}, ...
```

**Apache Kafka 3.9.1 accepts the identical bytes.** That is measured here, not
assumed: the same container, the same brod build, pointed at a KRaft broker,
produces 512 messages and consumes them back with every assertion green.

Two independent one-line fixes, either of which unblocks brod:

* **facade** — treat an empty transactional id as absent, e.g.
  `req.transactional_id.as_ref().filter(|id| !id.0.as_str().is_empty())`.
  Apache Kafka effectively keys transactional behaviour off the RecordBatch
  `isTransactional` attribute bit, not off the presence of this field.
* **upstream kafka_protocol** — use `nullable_string`, matching its own schema.
  `patch-kpro-txnid.sh` in this directory applies exactly that, to the copy
  under `_build/`, and is what `BROD_PATCH_TXNID=1` runs.

The facade side is the one worth doing: it costs nothing, it is
Kafka-compatible, and it does not require every Erlang shop to wait for a
kafka_protocol release.

## Running it

The suite starts **nothing**. Point it at a running stack.

```sh
# against a facade advertising host.docker.internal (the containerised runner)
KAFKA_BOOTSTRAP=host.docker.internal:31182 ./run.sh all

# with the one-field overlay, to exercise everything past the blocker
BROD_PATCH_TXNID=1 KAFKA_BOOTSTRAP=host.docker.internal:31182 ./run.sh all

# SASL/PLAIN over TLS
BROD_PATCH_TXNID=1 KAFKA_TLS_BOOTSTRAP=host.docker.internal:31183 \
  QUEEN_KAFKA_SASL_TOKEN=<bearer> ./run.sh sasl
```

Environment: `KAFKA_BOOTSTRAP`, `RUN_ID`, `KAFKA_TLS_BOOTSTRAP`,
`QUEEN_KAFKA_SASL_TOKEN`, `QUEEN_KAFKA_SASL_USER`, `BROD_RUNNER`, `BROD_IMAGE`,
`BROD_PATCH_TXNID`.

Scenarios: `versions`, `produce`, `codecs`, `offsets`, `resume`, `probes`,
`sasl`, `all`.

### Why there is a container here when no other compat suite has one

There is no Erlang on the host, and "install Erlang" is a bigger ask than
`docker run erlang:27` — which is also closer to what an Erlang shop's CI does.
`BROD_RUNNER=host` runs the identical Erlang source natively if you do have
rebar3.

The image is `erlang:27` **plus cmake**, built from the `Dockerfile` here on
first use. That is not a nicety: brod pulls `crc32cer`, whose NIF is built by a
Makefile that shells out to cmake, and `erlang:27` ships gcc but not cmake, so a
bare `rebar3 compile` dies before a single byte of Kafka protocol is exchanged.

A container cannot reach a facade advertising `127.0.0.1`, and macOS cannot
resolve `host.docker.internal`. `run.sh` checks the bootstrap against the runner
and refuses the impossible combination up front, rather than letting brod fail
with a name-resolution error that looks like a facade bug.

## What brod actually negotiates

Read from `kpro_connection:get_api_vsns/1` — the client's own parsed table — and
crossed with `brod_kafka_apis:supported_versions/0`, which is brod's own
deliberately narrow window (`%% Do not change range without verification.`) and
NOT kpro's. Using kpro's range gives a table that looks right and is wrong.

```
api                  broker     brod       will use  src
api_versions         0..3       0..3       3         kpro
fetch                4..6       0..10      6         brod
find_coordinator     0..3       0..0       0         brod
heartbeat            0..2       0..4       2         brod
join_group           0..4       0..6       4         brod
leave_group          0..2       0..4       2         brod
list_offsets         1..5       0..2       2         brod
metadata             0..9       0..2       2         brod
offset_commit        2..6       2..8       6         brod
offset_fetch         1..7       1..2       2         brod
produce              3..9       0..7       7         brod
sasl_authenticate    0..1       0..1       1         kpro
sasl_handshake       0..1       0..1       1         kpro
sync_group           0..2       0..3       2         brod
```

**brod sits at the BOTTOM of the facade's windows where every other client in
the matrix sits at the top**: Metadata v2, ListOffsets v2, FindCoordinator v0,
OffsetFetch v2. Those low versions are the least-exercised part of the compat
surface and they all work. That is the most useful thing this row adds.

One consequence is product-visible. `allow_auto_topic_creation` is a Metadata
**v4** field. brod pins Metadata at v2, so a brod client configured with
`allow_topic_auto_creation => false` has no way to say so on the wire and a bare
Metadata request creates the topic anyway. The facade's documented "auto-create
cannot be refused on Metadata v0-v3" deviation is not theoretical for brod — it
is the only behaviour brod can get. The `probes` scenario demonstrates it.

## Client-side facts that are brod's, not the facade's

* **`begin_offset` defaults to LATEST.** A group subscriber with no committed
  offset and no `begin_offset` reads nothing and looks like a broken broker.
  Every consumer config here sets `begin_offset => earliest`.
* **`unknown_topic_cache_ttl` defaults to 120000ms.** brod caches an
  unknown-topic error for two minutes, which outlives any sane retry loop on a
  broker whose auto-create is asynchronous. Set to 1000 here.
* **brod ships gzip and gzip only.** snappy, lz4 and zstd go through
  `kpro_compress`, which calls `snappyer` / `lz4b_frame` / `ezstd` only if the
  app is loaded. They are named in this project's `rebar.config`; a stock
  `{deps,[brod]}` project cannot send them.
* **No idempotence trap.** brod never sends `InitProducerId` unless you build a
  transactional producer explicitly, so the failure mode that kills the Java
  console producer at its 3.9 defaults does not exist on brod's default path.
* **Auto-create shape differs and a client must cope.** Apache Kafka creates the
  topic as a side effect of Metadata and answers that same request
  `UNKNOWN_TOPIC_OR_PARTITION`; queen-kafka creates it synchronously and the
  first attempt wins. The suite retries and reports the attempt count rather
  than pretending either is the only correct behaviour.

## Probes (recorded, never asserted)

Printed as `note`, because what the facade does with them is a product decision:

* an empty produced key comes back as `<<>>`
* duplicate header keys survive, both copies, in order
* `acks=0` returns `ok` with no response frame in existence to carry an error
* a bare Metadata request auto-creates, unrefusably, at brod's v2

## Wiring into rig.sh

`rig.sh` is not edited by this directory. The row it would gain, beside the
other client suites:

```sh
KAFKA_BOOTSTRAP="$KAFKA_HOST:$KAFKA_PORT" RUN_ID="$RUN_ID" BROD_RUNNER=host \
  compat/brod/run.sh all
```

with `BROD_RUNNER=host` if the CI image has rebar3, or `BROD_RUNNER=docker` and
a facade advertising `host.docker.internal` if it does not. Until the
transactional-id gap closes, the row is expected to fail; `BROD_PATCH_TXNID=1`
is what makes it green, and it should be removed the moment either fix lands.
