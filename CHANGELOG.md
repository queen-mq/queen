# Changelog

Release history for the Queen MQ server and client SDKs. Full release notes live on
[GitHub Releases](https://github.com/queen-mq/queen/releases).

## 1.4.1 — 2026-09-02

**A Kafka topic can declare its own partition count.** Until now the width every topic was
advertised at came from one broker-wide knob: `max(live lanes, QUEEN_KAFKA_DEFAULT_PARTITIONS)`,
where the second term is a start-up setting. A create's `num_partitions` was accepted and
discarded, so a low-volume topic and a high-volume one could not differ, and changing either meant
restarting the broker for all of them. `CreateTopics` now stores the number it was asked for as
that topic's own width **floor**, and the topic is advertised at `max(live lanes, its floor)`.
Two topics on one broker can be 8 and 512 lanes wide, declared by the clients that made them.

**`CreateTopics` is the only writer.** Both alter paths carry an existing floor through untouched,
so a `retention.ms` change cannot silently narrow a topic, and there is no config key that sets
one. `CreatePartitions` still refuses, in the same words a real broker uses for the two cases a
real broker also refuses; its message now says what to do instead. Changing a declared width means
deleting the topic and creating it again. A `num_partitions` above 100,000 is refused
`INVALID_PARTITIONS` rather than clamped, so the facade never stores a number it would then
quietly answer as something else.

## 1.4.0 — 2026-08-31

**Kafka and SQS clients connect directly.** Two wire-protocol facades now ship inside
`ghcr.io/queen-mq/queen`, beside the broker binary. `queen-kafka` advertises 32 Kafka API keys,
transactions included, so an unmodified producer or consumer moves over by changing
`bootstrap.servers`; `queen-sqs` answers the SQS and SNS protocols, so an unmodified AWS SDK moves
over by changing `endpoint_url`. Both are inert until `QUEEN_KAFKA_EMBEDDED=true` or
`QUEEN_SQS_EMBEDDED=true`, where the broker spawns and supervises the facade as a child process on a
backoff — neither holds a database connection or stores anything durable of its own, so they are
Queen clients like any SDK. Kafka adds a two or three node cluster mode with its node registry in
Queen's key/value store; SQS is stateless behind an ordinary load balancer given a shared
`QUEEN_SQS_HANDLE_SECRET`. What each protocol covers, and every place it deviates, is
[reference/kafka](https://queenmq.com/reference/kafka) and
[reference/sqs](https://queenmq.com/reference/sqs); running them is
[deploy/kafka](https://queenmq.com/deploy/kafka) and [deploy/sqs](https://queenmq.com/deploy/sqs).

**Laravel queue driver, worker supervisors and a dashboard.** A native queue driver, supervisors
with prefetch and lease renewal, and a supervisor dashboard, hardened for production supervision and
qualified against a benchmark harness. The PHP client is packaged for Packagist as
`queen-mq/php-client` (registered; no version tagged yet) and mirrored to its own repository by CI.
Guides are under [use/laravel](https://queenmq.com/use/laravel).

**Proxy: `kv`, `timers` and `ephemeral` are base surfaces, not upsells.** The plan seed predated all
three families, and a plan row that does not name a feature is a plan that does not have it, so every
plan answered `403 feature_gated` on those routes from the day each shipped. Migration
`009_default_families` grants them on every existing plan and defaults new plans the same way.
`streams` and `traces` stay per-plan.

**Fixes.** A partial ack no longer erases the redelivery marker: `attempt_offset` follows the first
uncommitted frame of a live lease, so when a worker dies after acking a prefix, the tail is
recognised as a redelivery rather than as fresh work, and retry budgets and DLQ routing see the true
`deliveryAttempt`. A DLQ replay now provably mints a transaction id different from the original, so
a replay can never be mistaken for the frame it was quarantined from; it is still not idempotent, on
the terms 1.3.0 spells out. The JavaScript stream runner fails closed on a bootstrap error instead of
running a partially initialised chain.

## 1.3.0 — 2026-08-28

**Streams are tenant-scoped.** `queen_streams` was the last name- and pid-addressed surface without
tenant scoping, and on a shared cell it was unsafe: a query name was globally unique, so `reset:true`
could take over another tenant's query and hand back their uuid; the cycle resolved sink queues by
bare name, which can multi-match on `(tenant_id, name)`; and state operations ran with no ownership
check at all. `queen_streams.queries` now carries `tenant_id` with uniqueness on `(tenant_id, name)`,
the cycle gates the source partition and the query before taking the advisory lock, and all four sink
resolves are scoped. The new `queen_streams.quota` is the grant: for a non-default tenant, absence is
a denial (`403`, distinct from the `config_hash` `409`), checked on the fresh-insert path only, so
revoking a grant stops new queries without stranding a Runner that is still draining. Tenant purge
covers `queries` and `quota`, and empties `state` through the query FK in a bounded phase before any
query row is deleted.

**Cloud must-builds (proxy).** `QUEEN_PROXY_SHARED_HOSTS`: on a shared host the cluster resolves from
the credential rather than the hostname, and the whole listener answers `401` instead of `421` when
any shared host exists; host canonicalisation closes trailing-dot, case and port bypasses. Tenant
wipe lands as `queen_proxy.delete_tenant` (requires `status=deleting`, redacts the outbox) plus
`queen.delete_tenant_data_v1`, which walks every tenant-carrying table under per-table row budgets.
Proxy boot now fails fast when JWT material is supplied but unusable, and answers a precise `503` at
login when there is none at all. `PG_SSL_ROOT_CERT` and `PXDB_SSL_ROOT_CERT` take PEM content, not a
path, so a private CA can be verified without turning authentication off.

**DLQ retry reaches the SDKs.** `queenctl dlq retry <partitionId> <transactionId>` replays one
dead-lettered message, and Go's `RetryMessage` went from a stub that refused to a working call. The
route is **not idempotent** — the replay is minted with a fresh transaction id, so two replays are two
distinct messages that nothing collapses — so it is sent with the new `WithoutFailoverRetry` option
and must not be retried on failure without re-reading the DLQ first.

**`JWT_ALGORITHM` accepts the whole HMAC family.** HS384 and HS512 were implemented by the
request-time verifier but refused at boot, which made pinning either impossible and pushed operators
onto `auto`, the strictly wider posture. One `SUPPORTED_JWT_ALGORITHMS` constant now drives the boot
check and the error message it prints, with a test that fails if boot and verifier ever disagree
again.

**Version-line alignment.** Every client and SDK moves to 1.3.0 with the broker and proxy, which a
MINOR requires. Only two changed content since 1.2.0: `client-go` and `queenctl`, both above. The
rest move because a client version says which broker line it was released against.

## 1.2.0 — 2026-08-25

**Pop autopilot (server-advised pop sizing).** New SDKs omit `partitions`/`batch` and send
`autopilot=true`; the broker sizes the sweep from hot-list state (ready count, ready-age,
burst bypass under `QUEEN_POP_AUTOPILOT_BURST_CAP`). An explicit client value is a hard pin,
old clients are byte-identical, kill switch `QUEEN_POP_AUTOPILOT=on|shadow|off`, and an
`autopilot` echo + divergence log make the choices observable. Ported to all seven SDKs
(Go, JS, Python, Rust, PHP/Laravel, C++, queenctl) plus `queen-protocol`.

**Retention is now O(deletable), not O(partitions).** Per-partition watermarks
(`oldest_live_at`/`oldest_txn_at`, maintained by the push allocator and the retention steps)
turn the work list into per-queue indexed probes; a batched daily safety walk doubles as the
one-time backfill. Measured on an 827k-partition cell: 20 → 663 seg/s, cycle 172 s → 0.7 s.
Knobs: `QUEEN_RETENTION_DUE_CAP`, `QUEEN_RETENTION_SAFETY_WALK_MS`. The cycle's advisory
lock is transaction-scoped on a dedicated holder that takes no table locks, so a timed-out
or dying cycle can never leave retention deadlocked cluster-wide.

**Boot-time schema apply is safe on a busy cluster.** One statement per transaction
(dollar-quote-aware splitting), 2 s `lock_timeout` slices with jittered bounded retries
(~3 min patience, never >2 s of traffic stall), `CREATE INDEX CONCURRENTLY` support, and —
on exhaustion — a diagnostic that names the blocking sessions (robust to `pg_stat_activity`
privilege masking). Proven live: full apply in 443–724 ms against 1000+ tx/s.

**Hot-list memory is bounded.** A broker serving no pops for a queue (standby, failover
leftover) drops that ring after `QUEEN_HOTLIST_UNSERVED_TRIM_MS` and releases the pages
(`malloc_trim`); the partition intern went from four heap allocations per entry to one.
Measured: standby broker 2.1 GB → tens of MB; active plateau roughly halved.

**Observability.** Burst-resolved pop telemetry on the rates line (`ring_depth_max`,
`ring_oldest_max_ms`, `max_lane_ready`, `pop_wait_max`, ready-entry provenance).

Operational note for rolling upgrades from ≤1.2.0-beta.2 under heavy load: apply new
hot-table DDL manually first (single-statement `ALTER` with `lock_timeout`, indexes via
`CREATE INDEX CONCURRENTLY`) or boot once with `QUEEN_APPLY_SCHEMA=false`; from this
release onward the applier handles it unaided.

## 1.1.0 (2026-08-21) — conflation

**Last-value delivery, as a consumer-group policy.** `conflation=true` on any pop route makes a
pop of a partition deliver exactly one message — the newest visible one — and commit everything
behind it when the handler acks. It is for command-style queues where one partition is one
logical key and only the freshest "recompute X" is worth running: under a backlog the handler
runs once per partition instead of once per message. Nothing on disk is touched; retention still
governs storage. The guarantee it keeps is that after the last push to a partition, at least one
run of that partition's handler *starts* after that push commits — the broker never commits past
an offset it did not observe at pop time.

Conflation is a property of the **group on the queue**, not of the call: the first pop that
registers the group persists it, and from then on the stored value wins for every consumer of
that group. That is what lets `workers` conflate while `audit` on the same queue reads
everything. A consumer that declares the opposite of the stored policy is not rejected — the
stored policy is applied and the response says `"conflationConflict":true`, so the consumer keeps
running and warns once. Rejecting would take down the already-correct half of a rolling deploy.
Two combinations ARE refused, with a `400` that names the reason: `conflation=true` without a
`consumerGroup`, and `conflation=true` with `autoAck=true`.

**Degrade loudly, in every SDK.** No SDK negotiates a version with the broker, so a 1.1.0 client
against an older one would have the unknown query parameter ignored and quietly drain the whole
backlog message by message. Instead the broker echoes `"conflation":true` on every conflating
response *including empty ones*, and an SDK that asked and did not get the echo raises on its
first round trip, before a single message is handled: *"conflation was requested but this broker
did not apply it — requires broker >= 1.1.0"*. Because those keys have to reach the client, a pop
whose answer has anything to say about conflation is a `200` with a body even when it delivered
nothing, pop maintenance included. Every response that never mentions conflation is byte-identical
to 1.0.6, `204` included.

Also in this release: `subscriptionMode`, `subscriptionTimestamp` and `subscriptionCreatedAt` are
real values on `GET /api/v1/consumer-groups` instead of hard-coded `null`, next to the new
`conflation` field; `GET /api/v1/resources/queues/:queue/depth` gains `partitionsPending`,
`conflation` and `effectivePending`, where a conflating group's `pending` is log depth and
`effectivePending` is the handler runs that remain (`pending: 4000000, effectivePending: 12` is
healthy under conflation and an incident without it); and
`queen_queue_conflated_per_minute{queue}` counts the positions conflation retired without a
handler invocation. The dead `queen_queue_depth_total` and `queen_queue_depth_pending` families
were removed: they read an aggregator key no stored procedure has ever produced, so they were
never in the exposition, only in the generated reference.

The C++ SDK's push buffer catches up to the 1.0.6 contract the other five SDKs got on
2026-08-20: `max_size` is now a blocking backpressure bound (unbounded is deliberately not
expressible), a batch whose POST fails is re-queued at the front of the buffer, in order, and
retried until it lands or an explicit `flush_buffer`/`flush_all_buffers` deadline expires — at
which point `BufferFlushError` says how many messages are still buffered, none of them dropped —
and `close()` flushes under the same 30 s deadline and reports what is left. Before this, the C++
buffer grew without limit and dropped a failed batch after a log line.

**Rollout.** Default off: a group created without the flag behaves exactly as before, and there
is deliberately no `QUEEN_CONFLATION_ENABLED`. Ship the broker first (the columns are additive
and defaulted, old SDKs are unaffected), then the SDKs; no coordinated cutover and no migration
for existing groups. The broker rolls back cleanly, with one caveat worth saying out loud: a
group already registered with `conflation=true` is served full batches by an older binary, which
ignores the column. Rolling back turns conflation off, it does not turn it into an error.

## 1.0.6 (2026-08-20) — clients only

**The client-side push buffer is now bounded, and a failed flush no longer loses messages.** In
every SDK (Go, JavaScript, Rust, Python, PHP). Before this release the buffer grew without
limit — a producer filling faster than the flush pipeline drains was measured accumulating 20.9M
messages (11.7 GB of RSS) in 45 seconds and losing every one of them at process exit, with zero
client-side errors reported — and a batch whose POST failed was dropped after a log line. Now
`maxSize` (default `4 × messageCount`; unbounded is deliberately not expressible) makes the add
path wait for the flusher in each language's idiom, and a failed batch is re-queued at the front
of the buffer, in order, and retried every `retryDelay` until it lands. A broker outage shows up
as blocked producers and bounded memory instead of silent loss. The formerly inert `maxSize` and
`retryDelay` knobs now do exactly what they say; `close()` flushes under a 30 s deadline and
reports anything left unsent. Same measured workload after the fix: 881k msg/s sustained with
exact send/receive parity and 71 MB of RSS.

The server stays at 1.0.5; this release bumps only the client packages.

## 1.0.3 (2026-08-18)

**Key/value state and timers are part of the engine, not features to switch on.** There is no
`QUEEN_KV_ENABLED` and no `QUEEN_TIMERS_ENABLED` — the broker reads neither, and setting them
does nothing. The reason is the one that governs every other surface: there is no
`QUEEN_PUSH_ENABLED` either. A boot flag is the claim that a thing is optional, and a cell where
`/api/v1/kv` might or might not exist is a cell no client can be written against. From the moment
this binary lands, both surfaces are live and the sweeper is running on every cell.

**Upgrading breaks a broker started with `QUEEN_TENANCY_HEADER=1` unless you also set
`QUEEN_KV_TRUSTED_PROXY=1`.** This is the one change that needs an edit before the upgrade, and
the failure is loud: the process exits at boot with the reason in its last log line, so a
Kubernetes rollout crash-loops rather than serving. Add the variable wherever the tenancy header
is on:

```
QUEEN_TENANCY_HEADER=1
QUEEN_KV_TRUSTED_PROXY=1     # new, and now mandatory alongside the line above
```

Set it only where the claim it makes is true: **a proxy in front sets `x-queen-tenant` and strips
whatever the client sent.** The interlock is not about KV being dangerous. With the header on, the
tenant identity is opaque and validated against nothing, and any caller who can reach the broker
directly can name another tenant — KV was simply the first surface addressable purely *by name*,
which is what made it visible. The requirement existed before; it was conditional on the KV flag,
and with that flag gone it is unconditional for anyone running with the header. If you cannot make
that affirmation truthfully, the answer is to stop running with `QUEEN_TENANCY_HEADER=1`, not to
set the new variable: there is no longer a flag that could withhold the surface, and there should
not be — a fleet where the engine is missing on some cells is worse than a boot that names the
variable to set.

**Nothing ships dark any more.** The rollout plan for these surfaces used to begin by installing
the complete broker with both flags false, so the routes were not even registered, and enabling
them later cell by cell. That step no longer exists. Anything to be watched has to be watched on a
cell that already answers. The instrument for a cell in trouble is the runtime kill switch —
`POST /api/v1/system/kv-timers` with `kvEnabled`, `timersScheduleEnabled` or `timersFireEnabled`
set false — which pauses a surface that exists, answers 503 with `Retry-After` while paused, takes
effect on the next call rather than the next restart, and is expected to be flipped back. Same
class as maintenance mode. It is not a rollout gate and does not make an unvalidated tenant header
safe.

**Wire and metric consequences of the flags being gone.** A kv or timer route can no longer answer
`404 not_found` with reason `kv_not_enabled` or `timers_not_enabled`, and a transaction carrying a
`kv` or `timers` rider can no longer be refused with `400 bad_request` for reaching a cell without
the surface — a 404 from those routes now means a wrong URL or an older image. `GET`/`POST
/api/v1/system/kv-timers` no longer return `kvEnabledByConfig` and `timersEnabledByConfig`; every
other field, including the three switch states, the mirror status and the quotas, is unchanged.
The `queen_kv_*`, `queen_timers_*` and `queen_sweeper_*` metric families are now exported by a
broker that has never served a kv call, so a dashboard can be built before any traffic exists.
`queen_kv_read_rejected_total{reason="disabled"}` now means the operator's kill switch and nothing
else; refusals that used to land in `reason="pool"` because they were classified from the HTTP
status are attributed correctly.

## 1.0.2

**A new logo.** The duck gives way to a geometric mark — a ring with an exit port and the piece
that left through it — across the dashboard, the docs, the sign-in page and the README.

## 1.0.1

**The hot-list reseed asks a bounded question.** The reseed is how a broker rebuilds its
in-memory candidate ring: for a (queue, group) it enumerates the partitions that still hold
unconsumed data. It did that by walking every partition of the queue, once per ring per 30
seconds per broker, and on a production database of 51,552 partitions that had become the
single largest consumer of the whole instance — measured on one 9,563-partition queue, 49 ms
per call to return zero rows, 8.2 calls a second, 0.58 cores, more total database time than
the entire pop path at 24x the call count. The cost was never the partition scan; it was the
one `log_consumers` primary-key probe the join pays per partition, 38,292 of the query's
39,700 shared buffers.

It now walks only the partitions written in the last `QUEEN_HOTLIST_RESEED_WINDOW_MS`
(default: four reseed intervals, floored at 120s), driven by an index that already existed.
Same question, same answer, 0.375 ms. That bound is sound because a partition can only
*become* pending by being written, and every push stamps `last_write_at`; acks and retention
only ever remove pendingness.

**What a window cannot see is a cursor moving backwards**, which is why the full walk stays,
at `QUEEN_HOTLIST_RESEED_FULL_MS` (default 300s; `0` pins every reseed to the full walk and
restores the previous behaviour without a rebuild). **This is the one behavioural change to
weigh before upgrading**: the worst case for repairing a ring that lost a partition with no
write behind it moves from roughly 45 seconds to roughly 6 minutes. Nothing is lost or
duplicated by that — the reseed is a cache over PostgreSQL and errs toward over-inclusion —
but a stall that used to heal itself inside a minute can now take five.

The operations that genuinely move a cursor backwards no longer wait for it. A consumer-group
delete and a seek both force the full walk on the broker that served them and publish a
durable repair marker in their own transaction; every other broker applies it on its next
reconcile pass. Measured on a two-broker stage cluster: 18 seconds from the seek to the peer's
ring being repaired.

**A ring on a `windowBuffer` or `delayedProcessing` queue can be reclaimed again.** One
ordinary push was enough to arm an entry that could never return to idle, so the queue's state
was pinned for the lifetime of the process and burned about 18 empty claims a second forever.
No seek, no mesh and no replication were required to reach it. On those queues a partition
that becomes visible again with no new write behind it — a retry delay longer than the
visibility cut — is now recovered by the reseed floor rather than by that loop, which is the
exposure plain queues have always had.

**The reseed says what it is doing.** The periodic floor line now separates full from windowed
passes, counts the ones that failed, and reports each ring's age since its last full walk, so
"which mode is this ring in, and when was it last repaired" is answerable without arithmetic.

`queen.log_hotlist_reseed_window_v1` gains an absolute cutoff. The argument is appended after
the tenant rather than beside the window it pins, so a 1.0.1-beta.1 replica keeps resolving
its call while a rolling upgrade replaces it, and the schema is safe to apply under a running
previous release.

## 1.0.0

First stable release. The broker, the proxy, the dashboard and the operator console all carry
`1.0.0`. The prerelease numbers had drifted apart — the proxy, the dashboard and the console
never moved past `1.0.0-beta.1`, because the later betas were broker-only — and this release
puts them back on one number.

**The broker is embeddable as a Rust library.** `queen-engine` publishes the same engine the
container runs, importable as `queen::Broker`: `Broker::start` applies the schema, starts the
background machinery and hands back typed operations — push, pop with long-poll, ack, leases,
transactions, configure, delete, the DLQ, metrics. Each one invokes the handler functions the
HTTP router dispatches to, so behaviour and defaults are the broker's by construction rather
than a reimplementation's. `default-features = false` drops the `server` feature, which is
the axum serve stack, the embedded dashboard and the process tracing subscriber; with the
default features the `queen` binary is byte-identical to the pre-feature layout. The package
publishes as `queen-engine` because the bare crates.io name `queen` belongs to an unrelated
crate, while the library still imports as `queen`. Measured MSRV is 1.88. The Rust surface is
published as **beta**: the HTTP API remains the stable compatibility contract.

**`RETENTION_PARALLELISM` does something again.** It was parsed and ignored through the
log-engine port, which pinned deletion at one partition at a time — a measured ceiling of
~13.8k step rows/s against the ~14.6k that 1M msg/s produces, so the database grew without
bound at 1M and held fine at 600k. Retention phases 1-3 now fan out over that many workers,
each on its own pooled connection and its own maintenance-lane admission slot, pulling
partitions off a shared cursor rather than a static split, so one deep backlog cannot leave a
single worker running alone. Phase 4, partition cleanup, stays serial on the cycle's own
connection: it is the one step that is not per-partition, so it is the one that can hold more
than one lock at a time. The value is clamped to 16 and defaults to 1 — the historical serial
cycle — so upgrading changes nothing until it is set.

Raising `RETENTION_BATCH_SIZE` is not a substitute. The step cost is per row, and the step
takes the same `log_partitions` row lock the push allocator takes, so at 1M msg/s a batch of
8000 pushed client p99 from 0.6 s to 20 s and absorbed no more rows.

**The admission controller can be told a lane's concurrency instead of discovering it.** A
lane's cap only widens on a probe, and a probe needs a minimum number of completions inside
one tick, which a lane running ~250 ms transactions never reaches: it decays to the global
minimum and stays there. Measured, retention with 4 fan-out workers sat at cap 2 with 4
waiters for a whole run — no faster than the serial cycle it replaced. Both the binary and
the embedded boot path now state the maintenance lane's floor as `RETENTION_PARALLELISM + 1`.
Raise `QUEEN_ADMISSION_SHARE_MAINT` along with the parallelism.

## 1.0.0-beta.4

**Google sign-in through the proxy could never complete.** The token exchange died with
`bad_gateway` / "google token exchange failed", and the underlying error was a TLS one:
`peer closed connection without sending TLS close_notify`. The proxy's self-contained HTTP
client sends `Connection: close` and reads to end-of-stream, but a peer that closes the TCP
connection without a TLS close_notify surfaces through rustls as `UnexpectedEof`, and the
read loops treated any read error as a failure — so a response that had already arrived in
full was thrown away. Google's token endpoint closes exactly that way. Both read loops now
treat it as the end of the message, which also unblocks the JWKS fetch that verifies the
returned id_token; the JWKS path would have failed a step later for the same reason.

Tolerating an abrupt close is only safe if a genuinely truncated response is still
rejected, so response parsing now validates the body against `Content-Length` and returns
`truncated body` when it is short. The chunked path already required its terminating
zero-size chunk. `Transfer-Encoding: chunked` takes precedence over `Content-Length`, per
RFC 9112.


**The dashboard reported messages as pending after they had been consumed and
acknowledged.** Two independent accounting defects, both around the group-less
`__QUEUE_MODE__` cursor, both visible only to grouped ("bus mode") consumers — the data
plane was never affected, and no message was ever delivered or retained wrongly.

- **The Messages list showed `pending` forever.** `list_messages_v1` derived a frame's
  status from a join pinned to the `__QUEUE_MODE__` cursor alone. A partition consumed by
  named consumer groups has no such row, so the join produced all NULLs, neither the
  completed nor the processing branch could fire, and every frame fell through to the
  `pending` fallback. The same select was already counting named groups two lines below,
  which is why a row could render `pending` next to `1/1 groups`. The status is now derived
  the way the message-detail endpoint already derived it: named groups decide when the
  partition has any, and the group-less cursor decides only when it has none.

- **A single group-less pop could pin a queue's pending count high forever.** The stats
  refresh took the worst cursor across every consumer group without distinguishing them.
  Popping without a `consumerGroup` seeds a `__QUEUE_MODE__` cursor at the head of the
  backlog — on every partition of the queue for a wildcard pop, and even when that pop
  returns nothing — so one debug pop or load-test run left a permanent floor under
  `pending_messages` that no amount of real consumption could drain. The same precedence
  rule now applies: named groups when they exist, the group-less cursor only otherwise.
  Applied identically in the queue-detail and queue-list paths.

Retention deliberately keeps the old across-all-groups watermark: there, including the
group-less cursor is the conservative choice, because deleting a segment a consumer might
still want is not recoverable.

## 1.0.0-beta.3

**A push was rejected whenever a JSON escape appeared in `queue`, `partition` or
`transactionId`.** Those three fields deserialized into borrowed `&str`, and serde cannot
borrow a string literal that needs unescaping, so the parse failed for the whole request
body: HTTP 400, every item in the batch discarded. The trigger was any escape at all, not
one bad character. Go's `encoding/json` escapes `&`, `<` and `>` by default, so a
transaction id like `Bed&Breakfast-771` from the Go SDK was rejected; Guzzle escapes `/`
and all non-ASCII, so `2026/07/BK-11` was rejected from PHP; and `"` and a backslash are
escaped by every JSON encoder, so they failed from every client including Queen's own Rust
one. The C++ broker copied these fields into owned strings and had none of this, which made
it a regression introduced with the Rust push path.

The fields now deserialize through a `Cow` newtype that still borrows when the literal
needs no unescaping, so the push path keeps its per-item allocation count. Control
characters remain rejected, now deliberately and with an error that says so: the layer-1
dedup key and the fusion group key are both composed by joining these fields on `0x1F`, an
invariant that until now held only because every escape happened to fail.

Three defects found alongside it are fixed in the same pass:

- **Error bodies were not valid JSON.** Fourteen handlers built `{"error":"..."}` with a
  raw `format!`, and a serde error embeds the offending value in quotes, so the response to
  a malformed request could not itself be parsed. They now route through the existing
  escaping helper.
- **An over-long `transactionId` was silently truncated.** The segment frame codec
  length-prefixes it with a `u16` while computing the body length from the full size, so a
  transaction id above 65535 bytes produced a frame whose declared length disagreed with
  its contents. The limit is now enforced at the HTTP boundary and asserted in the codec.
- **Dead-lettered messages could be unreplayable.** DLQ replay rebuilds a push body with
  `serde_json` and feeds it back through this parser, so a message on a queue or partition
  whose name contained a quote or a backslash could never be replayed. Existing stuck rows
  become replayable.

**A new consumer group now starts at the tail, not at the beginning of the backlog.** The
C++ broker honored `DEFAULT_SUBSCRIPTION_MODE` and the shipped charts set it to `new`; the
Rust port dropped the variable and hardcoded the per-request default to `all`. A chart that
still said `new` was therefore serving `all`, and the first group created after traffic
resumed replayed the whole retained log. The variable is honored again and its default is
`new`. Set `DEFAULT_SUBSCRIPTION_MODE=all` to restore the previous behavior without a
rebuild.

This changes only grouped consumers on their first contact. Existing groups resume from
their stored cursor as before, and a pop with no `consumerGroup` is unaffected: the SQL
pins those to `all` regardless.

**`subscriptionMode=new-only` did the opposite of what it advertised.** The Go SDK exports
it as an alias of `new`, the CLI offers it in `--from-mode`, and the JS README shows it in
use, but the SQL compares the mode literally, so `new-only` missed the `new` branch and
replayed the entire backlog. The broker now normalizes it. Any unrecognized value still
resolves to `all`, which is the safe direction.


**Two fixes in the Rust SDK, both found by putting its test suite under audit.**

- **A dead-lettered message's reason was never readable.** `DlqMessage.error` deserialized
  the key `error`, while `queen.get_dlq_messages_v1` projects the stored reason as
  `errorMessage`. The field was therefore always `None` and the reason sat unnoticed in the
  struct's `flatten`ed `rest`. It now reads the wire key, and accepts the plain one as an
  alias. The test that was meant to cover this asserted only that the DLQ had one row.
- **A `gate` after a stateless operator acked the wrong messages.** The gate settles by
  offset commit — *n* messages ending at a transaction id — but counted the records the
  gate had allowed. A `filter` before the gate leaves fewer records than messages, so the
  commit landed short and the remainder was consumed but never acked, redelivered on lease
  expiry, filtered out again: a partition that never drains. A `flat_map` leaves more
  records than messages, and the count indexed past the end, panicking inside the spawned
  loop task while `stop()` still reported a clean shutdown. Records are now grouped back
  onto their source message, and a message is settled only when every record it produced
  was allowed. State mutations from a denied message are rolled back, which is what
  `Stream::gate` already documented.

`TxnAckOperation` gained the `error` field the broker reads as the dead-letter reason, so a
transactional failure can explain itself; `TransactionBuilder::nack` and `ack_with_reason`
expose it.

The SDK's tests also now run in CI, which they did not before: `rust-client` is in the
suite matrix, and a native job runs the client's and `queen-protocol`'s unit tests, clippy,
rustfmt and the declared MSRV.

## 1.0.0-beta.2

**One admission arbiter replaces the two Vegas limiters.** Concurrency against PostgreSQL
was governed by a pair of TCP-Vegas-style controllers, one per lane, each inferring
queueing from per-operation round-trip time. On a WAL-bound commit path most of the excess
over the observed minimum is the group-commit flush wait: intrinsic cost that admission
cannot remove. The estimator counted that floor as congestion and backed off from it, and
because its grow and shrink thresholds were absolute the dead band widened as the limit
fell, making low limits an attractor. The broker left cores idle while the connection pool
reported no waiters at all, and nothing exported the controller's inputs, so the
disagreement was invisible.

`server/src/admission.rs` replaces it with a single arbiter for every write transaction:

- **The sensor is passive.** Group commit clusters write completions in time, so grouping
  the broker's own commit completions measures the flush pipeline with no PostgreSQL-side
  telemetry. Train size is amortisation measured rather than assumed; train cadence is the
  flush rate; the gap between train starts is the flush cycle.
- **One budget, four lanes.** Push, Pop, Ack and Maint share a work-conserving budget.
  Guarantees act on the wake order when the budget is exhausted, acks first because they
  unblock lanes. The budget never rises above `DB_POOL_SIZE` minus a reserve, so admitted
  work cannot starve on the pool.
- **Slots are RAII.** Dropping a slot releases it. The previous limiter leaked its
  in-flight counter at eight call sites that took a permit and returned early, until its
  own anti-ramp guard was permanently disabled.
- **A degraded mode is detected, not guessed.** With `synchronous_commit = off` commit
  waits collapse and the trains carry no signal; the arbiter pins a static budget and says
  so in telemetry (`adm_mode`).

**The pop claim path is set-based.** `queen.log_pop_list_v1` used to call `log_pop_v1` once
per candidate partition, about six statement executions each, inside the admission permit
and the committing transaction. It now does the same work in roughly six statements total,
independent of the candidate count. Measured on the sparse-partition shape, the per-pop
cost was 3.42 ms of which 0.17 ms was data work.

**New defaults.** `QUEEN_V2_FUSION_HOLD_MS` is 3, down from 15: the hold is paid twice per
flow, on ingress and on the derived republish. `QUEEN_ADMISSION_MIN` and
`QUEEN_ADMISSION_INIT` now derive from the pool, two thirds of
`DB_POOL_SIZE - QUEEN_ADMISSION_POOL_RESERVE`, which is 96 on the defaults. They are
derived rather than fixed so a small deployment cannot admit more concurrent transactions
than it has connections for. On the 2000 ev/s / 1000-lane shape, raising the floor alone
took p50 from 1143 ms to 240 ms.

**Validated by a soak.** 600,000 msg/s for 3 h 10 m with full production semantics (leases,
explicit async acks, 60 s dedup window, retention active): **6.82 billion messages, zero
push, pop and ack errors, flat lag**, p50 120 ms and p99 297 ms, with the median steady
inside ±8% across the run.

**Known weakness, stated because this is a beta.** The shrink signal is the age of the
oldest admitted slot, which conflates statement execution with commit wait: a transaction
doing genuine heavy work looks like one waiting behind a queue. On workloads whose write
transactions routinely run long the budget hunts rather than settles. This is documented in
`admission.rs` and on [flow control](https://queenmq.com/internals/flow-control), and the
replacement signal is named there.

### Breaking

- **Prometheus metric names changed.** `queen_seg_push_vegas_limit` and
  `queen_seg_pop_vegas_limit` no longer exist. The replacements are
  `queen_admission_budget`, `queen_admission_inflight{lane}`,
  `queen_admission_waiting{lane}`, `queen_admission_trains_per_s`,
  `queen_admission_txn_per_train` and `queen_admission_cycle_ms`. **Dashboards and alerts
  referencing the old names need updating.**
- **The `rates` log line changed shape.** `vegas_push` and `vegas_pop` are replaced by
  `adm_budget`, `adm_mode` and `adm_lanes`.
- **Vegas-era variables are no longer read.** `QUEEN_SEG_{PUSH,POP}_{INIT,MIN,MAX}`,
  `QUEEN_VEGAS_ALPHA` and `QUEEN_VEGAS_BETA` are accepted by the environment and ignored;
  the broker logs a warning at boot for each one it finds set, rather than letting a
  deployment tune a control loop that is not there. The Helm chart no longer sets the eight
  libqueen-era `QUEEN_{PUSH,ACK,POP}_MAX_CONCURRENT` / `QUEEN_VEGAS_*` /
  `QUEEN_PUSH_*_BATCH` variables, none of which the Rust broker ever read.

## 1.0.0-beta.1

First beta of the 1.0 line: the Rust broker on the segment storage engine described under
[1.0.0](#100) below, plus the tenancy surface built on top of it.

- **Multi-tenancy and the gateway.** Native tenant scoping in the broker behind
  `QUEEN_TENANCY_HEADER` (off by default), and `queen-proxy`, a separate Apache-2.0 gateway
  carrying API keys, plans, quotas, rate limiting, metering and a console.
- **One webapp behind the proxy.** Auth, roles, tenant selector and the operator surface in
  a single application, re-keyed onto the documentation site's palette.
- **The migration tool is gone.** It shelled out its PostgreSQL connection parameters,
  which was a remote-code-execution path; the fix landed first and the tool was then
  removed rather than kept.
- **Tenancy correctness and cost.** Each tenant gets its own discovery wake gate, so one
  tenant's pushes stop waking every other tenant's parked consumers. Confirmed
  partition-to-tenant ownership is cached, dropping a round trip per ack.
- **Hot-list lease revisit is bounded**, which removes a stall that could hold a single
  partition indefinitely.

## 1.0.0

**A Rust broker on a new storage engine.** The 0.x line was a C++ implementation
(libqueen, uWebSockets, libpq) storing one row per message in `queen.messages`. 1.0.0
replaces both halves: the broker is a single stateless Rust binary (`queen-seg`, axum and
tokio over a pooled `libpq`), and the storage engine is a log of compressed segments.

### The storage engine

A message's position is now a single monotone per-partition offset. A segment is one row
holding many length-prefixed frames, packed and zstd-compressed, so a push writes segments
rather than rows. Consumption state is one cursor per (partition, consumer group) in
`queen.log_consumers`: there is no per-message delivery state anywhere in the schema, which
is what makes tens of thousands of partitions cheap.

- **Acknowledgement is an offset commit.** Acking a message commits the cursor past it and
  implicitly completes everything before it in that partition for that group. Acks still
  arrive addressed by `transactionId`; the broker resolves them through a 16-byte-per-frame
  hash sidecar (`queen.log_txns`).
- **Two honesty guarantees, both contract-tested.** An explicit `failed`, `dlq` or `retry`
  is never skipped by a later `completed` ack in the same call: the cursor clamps at the
  lowest signal. An ack that lands below the cursor or outside the hash window is reported
  as a no-op rather than silently succeeding.
- **Deduplication is exact and enforced in SQL.** The probe of the transaction-id sidecar
  happens under the partition row lock, before an offset is allocated, so a duplicate writes
  nothing. The broker-side cache can only narrow that probe, never change its verdict.
- **Commits are amortised.** A fusion layer groups pending writes by partition and bundles
  disjoint partitions into one transaction, so N segments cost one commit and one fsync.
- **Pop candidates come from memory.** An in-process hot-list ring replaces the SQL
  candidate scan on the wildcard pop path, with a deferred-visibility wheel for leases and
  timed retries.

### Elsewhere

- **Multi-tenancy.** Native tenant scoping in the broker behind `QUEEN_TENANCY_HEADER`
  (off by default), plus `queen-proxy`, a separate Apache-2.0 gateway with API keys, plans,
  quotas, rate limiting, metering and a console. Documented under
  [Self-hosting](https://queenmq.com/selfhost).
- **Multi-broker coordination is framed TCP**, not UDP. The old `QUEEN_UDP_*` variables are
  accepted as aliases for `QUEEN_MESH_*`. Everything the mesh carries is a best-effort hint;
  PostgreSQL remains the only source of truth. **The mesh port must be firewalled.**
- **The dashboard is compiled into the binary** (`rust_embed`). There is no
  `QUEEN_STATIC_DIR` and no on-disk assets at runtime.
- **The dashboard works broker-direct.** With auth off (the default) the broker answers
  `GET /auth/me` itself with a standalone operator identity, so the full dashboard runs
  with no proxy: every view live, session controls hidden. With `JWT_ENABLED=true` the
  broker serves an explanation page at `/auth/login` instead; a dashboard with logins and
  roles remains `queen-proxy`'s. Documented at
  [queenmq.com/selfhost/dashboard](https://queenmq.com/selfhost/dashboard).
- **New documentation.** [queenmq.com](https://queenmq.com) is rewritten from the current
  source. Its route table, environment-variable reference, metric list, proxy route classes,
  OpenAPI documents and benchmark figures are generated from the code and the archived
  benchmark artifacts, and CI fails when any of them falls behind.

### Breaking

- **The migration tool is gone.** `POST /api/v1/migration/*` and its handler were removed.
  Back up with `pg_dump` over the `queen` and `queen_streams` schemas.
- **The retired engine's objects are dropped at boot.** Applying the 1.0.0 schema removes
  the previous engine's tables and procedures. This is not a data migration: messages stored
  by a 0.x broker do not carry over. Drain a queue before upgrading, or start fresh.
- **Prometheus names moved.** The in-process counters are `queen_process_*`; the
  `queen_cluster_*` namespace now means database-backed lifetime totals, identical on every
  instance. Per-queue series sum across tenants, so the endpoint is not a per-tenant surface.
- **`QUEEN_STATIC_DIR` no longer exists**, and several 0.x tuning variables
  (`NUM_WORKERS`, `QUEEN_*_SLOTS`, `SIDECAR_*`, `RESPONSE_BATCH_*`) have no equivalent: the
  Rust broker's concurrency is adaptive and sized from the connection pool.

### Compatibility

The HTTP message plane keeps the 0.16.0 contract: push, pop, ack, transaction and lease
extension are unchanged on the wire, and an existing SDK keeps working against a 1.0.0
broker for those calls. The SDKs are version-aligned at 1.0.0. Details, including which
client methods target routes that no longer exist, are in
[the compatibility reference](https://queenmq.com/reference/compatibility).

Measured behaviour, with the configuration of every run attached, is at
[queenmq.com/benchmarks](https://queenmq.com/benchmarks).

## Release History

> Every row below **0.16.0 and including it** describes the retired C++ implementation and
> its row-based storage engine. Those measurements and architecture notes do not describe
> 1.0.0.

**JS clients from version 0.12.0 can be run inside a browser**

| Server Version | Description                                                                                                                     | Compatible Clients                                          |
| -------------- | ------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------- |
| **1.0.0**      | **Rust broker on a segment-based log engine.** Offsets instead of row cursors, acknowledgement as an offset commit, exact windowed deduplication enforced before offset allocation, amortised commits via request fusion, an in-memory pop candidate ring, framed-TCP mesh, native multi-tenancy and a separate multi-tenant gateway. See the section above. | SDKs are version-aligned at 1.0.0; the message plane keeps the 0.16.0 wire contract |
| **0.16.0**     | **Push-serialization architecture + SIMD JSON.** New function-split libqueen engine cluster (3 shared engines — push/ack, pop, rest — decoupled from `NUM_WORKERS`, sized by per-function connection slots). Per-partition push serialization (in-memory in-flight gate + `pg_advisory_xact_lock` + `clock_timestamp()`) makes `messages.created_at` **commit-ordered**, eliminating the cursor-skip where a message could commit behind an already-advanced pop cursor under high concurrent push. Data-path concurrency (push/pop/ack) is now **static** (Vegas retained for auxiliary lanes), and `partition_lookup` maintenance is coalesced Nagle-style. Hot-path JSON is parsed and assembled with **simdjson** (nlohmann/json fallback), cutting broker CPU on push result fan-out. Balanced ~110–120k msg/s push & pop concurrently, ~190k push-only, on a 32-core host. Validated by a 24-hour soak: **10.4 billion messages, ~119k msg/s balanced, zero loss, flat ~400 MB broker**. [See soak →](https://queenmq.com/benchmarks-0.16-soak.html) | All ≥0.14.0 clients work unchanged — HTTP contract is identical; 0.16.0 SDKs are a version-aligned release |
| **0.15.5**     | Resolves **#30** (proxy compatible with Traefik forward-auth middleware), **#31** (write-only access role), **#32** (hardened Node base image). Robustness: malformed payloads can no longer crash a worker. Invalid UTF‑8 bytes and unpaired UTF‑16 surrogates — in a request body or in DB‑returned data — are serialized leniently and rejected with a clean **400** instead of throwing out of the event loop. Previously‑unguarded admin/metrics routes wrapped in error handling. | All ≥0.14.0 clients work unchanged |
| **0.15.0**     | Cross-language streaming SDK: fluent `Stream` builder + `.gate()` rate limiter + tumbling/sliding/session/cron windows + event-time + watermarks shipping in `queen-mq` (JS), `queen-mq` (Python), and `client-go` — all backed by the same `/streams/v1/*` endpoints and three new stored procedures (`streams_register_query_v1`, `streams_cycle_v1`, `streams_state_get_v1`). Identical SHA-256 `config_hash` across runtimes so a query registered by one client can be resumed by a worker written in another. UUIDv7 stamping in `streams_cycle_v1` push items to preserve FIFO order in batched sink emits. | All ≥0.14.0 clients work unchanged — upgrade clients to 0.15.0 to use the streaming SDK |
| **0.14.3**     | Improved frontend. | All ≥0.14.0 clients work unchanged |
| **0.14.1**     | Updated frontend: new metrics views and embedded developer guide; Google OAuth on the proxy; Prometheus metrics route (`/metrics`); significantly optimized lease renewal (reduced lock contention and DB round-trips); delete partition and delete messages API. | All ≥0.14.0 clients work unchanged |
| **0.14.0**     | Major release: new dynamic libqueen loop; rewritten `push_messages_v3`, `pop_unified_batch_v4`, `ack_messages_v2`, and `stats` stored procedures; `maxPartitions` on all clients (JS, Python, Go, Laravel, C++); new frontend. Benchmarked on real hardware: **104k msg/s** push (batch=100), **165k msg/s** fan-out across 10 consumer groups, pop throughput **+80–90%** vs 0.12 under partition contention, 52 MB server RSS at peak, zero message loss across 1.6B events. [See benchmarks →](https://queenmq.com/benchmarks.html#version) | All ≥0.13.x clients work unchanged — upgrade clients to gain `maxPartitions` support |
| **0.13.0**     | Major release: new libqueen with adaptive batch/concurrency/scheduling engine (S1 ~2x, S3 ~3x push throughput), new `push_messages_v2` stored procedure (temp-table + batched-insert pipeline), new Vue 3 dashboard, and server-stamped `producerSub` from the JWT on every message (closes #23) | All ≥0.12.x work unchanged — 0.13.0 pop responses add a new `producerSub` field that older clients silently ignore. Upgrade to 0.13.0 clients only if you want typed access to `producerSub` (Go struct field, Python TypedDict hint) |
| **0.12.19**    | Fix bug that on seek or cg delete do not deleted the watermark                                                                  | JS ≥0.7.4, Python ≥0.7.4                                    |
| **0.12.18**    | Improved charts and filters                                                                                                     | JS ≥0.7.4, Python ≥0.7.4                                    |
| **0.12.17**    | Improved stats                                                                                                                  | JS ≥0.7.4, Python ≥0.7.4                                    |
| **0.12.13**    | Added watermark tracking for efficient wildcard POP discovery. x20 faster pop on high partition count queues                    | JS ≥0.7.4, Python ≥0.7.4                                    |
| **0.12.12**    | Built-in database migration (pg_dump \| pg_restore, no temp file, selective table groups, row count validation)                  | JS ≥0.7.4, Python ≥0.7.4                                    |
| **0.12.10**    | Fixed JWKS fetch over HTTPS (cpp-httplib TLS support)                                                                           | JS ≥0.7.4, Python ≥0.7.4, 0.12.0 if needs to use            |
| **0.12.9**     | Fixed server crash (SIGSEGV) on lease renewal, added EdDSA/JWKS auth, fixed examples                                            | JS ≥0.7.4, Python ≥0.7.4, 0.12.0 if needs to use            |
| **0.12.8**     | Added single partition move to now to frontend                                                                                  | JS ≥0.7.4, Python ≥0.7.4, 0.12.0 if needs to use            |
| **0.12.7**     | Optimized cg metadata creation for new consumer groups                                                                          | JS ≥0.7.4, Python ≥0.7.4, 0.12.0 if needs to use            |
| **0.12.6**     | Improved slow cg discovery when there are tons of partitions                                                                    | JS ≥0.7.4, Python ≥0.7.4, 0.12.0 if needs to use            |
| **0.12.5**     | Fixed cg lag calculation for "new" cg at first message                                                                          | JS ≥0.7.4, Python ≥0.7.4, 0.12.0 if needs to use            |
| **0.12.4**     | Fixed window buffer debounce behavior                                                                                           | JS ≥0.7.4, Python ≥0.7.4, 0.12.0 if needs to use proxy auth |
| **0.12.3**     | Added JWT authentication                                                                                                        | JS ≥0.7.4, Python ≥0.7.4, 0.12.0 if needs to use proxy auth |
| **0.12.x**     | New frontend and docs                                                                                                           | JS ≥0.7.4, Python ≥0.7.4, 0.12.0 if needs to use proxy auth |
| **0.11.x**     | Libqueen 0.11.0; added stats tables and optimized analytics procedures, added DB statement timeout and stats reconcile interval | JS ≥0.7.4, Python ≥0.7.4                                    |
| **0.10.x**     | Total rewrite of the engine with libuv and stored procedures, removed streaming engine                                          | JS ≥0.7.4, Python ≥0.7.4                                    |
| **0.8.0**      | Added Shared Cache with UDP sync for clustered deployment                                                                       | JS ≥0.7.4, Python ≥0.7.4                                    |
| **0.7.5**      | First stable release                                                                                                            | JS ≥0.7.4, Python ≥0.7.4                                    |

**[Full Release Notes →](https://github.com/queen-mq/queen/releases)**

---

## Latest bug fixing and improvements

- Server 0.16.0: **Push-serialization architecture (commit-ordered `created_at`).** Under high concurrent push to the same partitions, `created_at` (transaction-*start* time) could be assigned out of commit order, letting the wildcard pop cursor `(created_at, id)` advance past a not-yet-committed message — silent loss. The push path now serializes **per partition** (an in-memory "≤1 in-flight push transaction per partition" gate plus a Postgres `pg_advisory_xact_lock` in a dedicated two-int keyspace) and stamps `created_at = clock_timestamp()` under the lock, so per-partition `created_at` is monotonic in commit order. POP and ACK are unchanged. Deterministic repro + proof in `benchmark-queen/2026-06-06-engine-scaling/cursor-repro.sh`.
- Server 0.16.0: **Function-split engine cluster.** libqueen now runs as 3 process-global engines (push/ack, pop, rest) shared by all HTTP workers via `QueenCluster`, replacing the previous one-engine-per-worker model. `NUM_WORKERS` sizes only HTTP I/O; DB concurrency is sized by per-function slots (`QUEEN_PUSH_SLOTS` / `QUEEN_POP_SLOTS` / `QUEEN_REST_SLOTS`). The push engine forms disjoint-partition batches that run concurrently (`QUEEN_PUSH_MAX_PARTITIONS_PER_BATCH`).
- Server 0.16.0: **SIMD JSON on the hot path.** The engine parses Postgres stored-procedure results and assembles batch payloads with **simdjson** (on-demand + DOM parsers), keeping the previous nlohmann/json path as a fallback. Lower CPU and tail latency on push/ack/pop result demultiplexing at 100k+ msg/s.
- Server 0.16.0: **Static data-path concurrency.** push/pop/ack default to static limits (24/16/16). Vegas (RTT-adaptive) mis-reads the hot path — it under-shoots push (high per-commit RTT) and collapses pop (long-poll parking read as PG queuing) — so it is retained only for the auxiliary lanes. Override per lane with `QUEEN_<TYPE>_CONCURRENCY_MODE`.
- Server 0.16.0: **`partition_lookup` coalescing.** Post-push lookup maintenance is batched Nagle-style (at most one flush in flight) instead of one call per push batch, clearing a backlog that appeared under sustained high push.
- Docs 0.16.0: **Architecture docs refreshed.** The developer guide and website now describe the new engine topology, push serialization, and concurrency model (`developer/02-architecture.md`, `04-libqueen.md`, `05-database-schema.md`, `docs/architecture.html`, `server/README.md`, `server/ENV_VARIABLES.md`).
- Proxy 0.15.5: **Traefik forward-auth compatibility (#30).** The Queen proxy can now be used as a Traefik external/forward-auth middleware, not only behind its own login flow.
- Server 0.15.5: **Write-only role (#31).** A new write-only access level lets producers push without being able to read or consume.
- Build 0.15.5: **Hardened Node base image (#32).** The proxy and dashboard images now build on a hardened, minimal Node base.
- Server 0.15.5: **Malformed‑payload hardening.** A request body — or DB‑returned content — containing invalid UTF‑8 or an unpaired UTF‑16 surrogate no longer throws out of the worker event loop. JSON is now serialized with a lenient error handler on every HTTP response and on the libqueen result/callback path, so such input is rejected with a clean **400** instead of crashing the worker (previously a `json.exception.type_error.316` could take a worker down and loop on retry).
- Server 0.15.5: **Defensive error handling on admin/metrics routes.** The shared‑state, partition‑seek, migration‑reset, and `/metrics/prometheus` handlers (including the deferred callback that runs on the event loop) are wrapped in try/catch, so an unexpected exception returns a 500 instead of killing a worker.
- Clients 0.15.0: **Streaming SDK on every runtime.** Ships a fluent `Stream` builder + composable operators (`.map`, `.filter`, `.flat_map`, `.key_by`, `.window_tumbling`, `.window_sliding`, `.window_session`, `.window_cron`, `.reduce`, `.aggregate`, `.gate`, `.to`, `.foreach`) and helper factories (`token_bucket_gate`, `sliding_window_gate`) in JS, Python, and Go. All three packages export the SDK from the same package as the broker client (`queen-mq` on npm/PyPI; `client-go/streams` subpackage in Go) — one install, one import.
- Clients 0.15.0: **Exactly-once cycles via `/streams/v1/cycle`.** State mutations + sink emissions + source acks commit in a single PostgreSQL transaction. On commit failure the entire cycle rolls back; Queen redelivers via the existing lease/retry path.
- Clients 0.15.0: **`.gate()` rate limiter with FIFO preservation.** New per-message ALLOW/DENY operator with persistent per-key state, a partial-ack on deny, and `release_lease=false` so the un-acked tail of the batch is redelivered in original order when the lease expires — no deferred queue, no reordering. The `tokenBucketGate` and `slidingWindowGate` helpers cover all four canonical rate-limit shapes (req/s, msg/s, cost-weighted, sliding-window quota) on every language.
- Clients 0.15.0: **Tumbling, sliding, session, and cron windows.** Per-window `gracePeriod`, `idleFlushMs`, optional `eventTime` extractor with per-partition watermarks (stored under the reserved `__wm__` state key), `allowedLateness`, and `onLate: 'drop' \| 'include'`. The runner emits closed windows on idle partitions via a per-window flush timer.
- Server 0.15.0: **Three new streaming stored procedures.** `streams_register_query_v1`, `streams_cycle_v1`, and `streams_state_get_v1` route through libqueen's existing async pipeline — same uvloop, same connection pool, same metrics attribution. Streaming cycles increment `record_ack_request` / `record_ack_messages` / `record_push_messages_with_queue` so the dashboard's per-queue Ack/s and Push/s charts include streaming throughput.
- Server 0.15.0: **UUIDv7 message IDs in streaming push.** `/streams/v1/cycle` stamps every sink push item with a UUIDv7 server-side, matching the `/api/v1/push` route. Time-ordered IDs preserve partition FIFO order even when batched inserts share a `created_at` timestamp.
- Tests 0.15.0: **75 Python streams tests, 33 Go subtests, 45 JS unit tests pass live.** Plus 11 examples per language ported 1:1 from the JS reference, including a "rate-limiter all canonical models" stress test (100 tenants × 10k messages, 4 runners, ~360 msg/sec aggregate sustained).
- Docs 0.15.0: Added [`use-cases.html`](https://queenmq.com/use-cases.html) landing page and [`use-case-rate-limiter.html`](https://queenmq.com/use-case-rate-limiter.html) with verified end-to-end snippets in JS, Python, and Go.
- Server/App 0.14.3: **Improved frontend.** Further refinements to the dashboard UI and user experience.
- Server/App 0.14.1: **Updated frontend.** New metrics views and an embedded developer guide surfaced directly in the dashboard.
- Proxy 0.14.1: **Google OAuth support.** The proxy now supports Google as an OAuth provider for end-to-end authentication without a custom identity server.
- Server 0.14.1: **Prometheus metrics route.** A `/metrics` endpoint exposes standard Prometheus-compatible metrics for scraping.
- Server 0.14.1: **Significantly optimized lease renewal.** Reduced lock contention and database round-trips on the hot lease-renewal path, lowering tail latency under high consumer concurrency.
- Server/App 0.14.1: **Delete partition and delete messages.** New API and dashboard actions to delete individual partitions or bulk-delete messages from a queue.
- Server and clients 0.14.0: **New dynamic libqueen loop.** Full rewrite of the core scheduling engine — adaptive concurrency controller (TCP-Vegas-style) now drives push, pop, ack, and stats independently. Active DB connections stay at ~2.5 even with a pool of 50 under 104k msg/s peak load. Largely eliminates the PG deadlock mode that appeared under heavy fan-out at high partition counts on 0.12 (occasional deadlocks still observed at 10 001 partitions, all absorbed by file-buffer failover — see [benchmarks](https://queenmq.com/benchmarks.html)).
- Server 0.14.0: **Rewritten stored procedures.** `push_messages_v3`, `pop_unified_batch_v4`, `ack_messages_v2`, and stats procedures redesigned around the new loop. PG memory usage 30–70% lower for equivalent workloads vs 0.12. Pop throughput +80–90% under partition contention.
- Clients 0.14.0: **`maxPartitions` on all clients.** JS, Python, Go, Laravel, and C++ clients expose `maxPartitions` on queue creation and configuration.
- Server 0.14.0: **New frontend.** Redesigned dashboard for the new stats model.
- Server 0.13.0: **New libqueen with adaptive engine.** Per-worker push/ack drain factored into three independently-tuned concerns — batching, concurrency, scheduling — glued by an event-driven orchestrator. Fixes two long-standing bottlenecks: per-commit overhead amortization on small-batch workloads, and the single-slot-per-drain cap on high-fanout workloads. Perf harness numbers: S1 ~6.2k → ~13k pg_ins/s, S3 ~4.7k → ~20k pg_ins/s, PG pinned instead of idle. Design notes in `cdocs/LIBQUEEN_IMPROVEMENTS.md`.
- Server 0.13.0: **New push stored procedure.** `queen.push_messages_v2` rewritten around a temp-table + batched-insert pipeline that feeds cleanly into the adaptive engine. HTTP contract (queued/duplicate/failed) unchanged.
- Server 0.13.0: **New Vue 3 dashboard.** Reworked queues, analytics, DLQ management, and maintenance-mode views. Served by the same C++ acceptor at `/`.
- Server 0.13.0: Added server-stamped `producerSub` to close the impersonation vector from GitHub issue #23. When JWT auth is enabled the server stamps the validated `sub` claim on every pushed message; clients cannot set this field and it is exposed on pop responses and admin message APIs. Schema migration is additive and metadata-only (no table rewrite), safe on tables with millions of rows.
- Clients 0.13.0: All clients (JS, Python, Go, Laravel, C++) expose `producerSub` on popped messages; Go adds a typed `Message.ProducerSub` field.
- Server 0.12.19: Fix bug where seek or cg delete did not delete the watermark.
- Server 0.12.13: Added watermark tracking for efficient wildcard POP discovery — x20 faster pop on high partition count queues.
- Server 0.12.12: Added built-in database migration — stream pg_dump | pg_restore directly from the dashboard, no temp file, selective table groups, row count validation.
- Clients 0.12.2: Added custom `headers` option to JS, Python, and Go clients for API gateway authentication.
- Server 0.12.9: Fixed server crash (SIGSEGV) on lease renewal; added native EdDSA and JWKS JWT authentication (auto-discovery via `JWT_JWKS_URL`).
- Server 0.12.3: Added JWT authentication.
