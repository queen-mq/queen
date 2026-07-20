# RUSTFIX — production-parity fix plan for the Rust queen server

Audience: an agent working on branch `rustserverandstorage`. This branch replaces the
C++ v0.16.0 server (`server/src/*.cpp` on the old branch) with a Rust broker
(`server/src/*.rs`) and a new segment storage engine
(`server/sql/procedures/023_*.sql` … `034_*.sql`, `099_retire_rows.sql`).
A full parity audit against v0.16.0 found the gaps below. Each item states the
problem, where the code lives on both sides, the required change, and acceptance
criteria. Work through them in the order given inside each priority band.

## How to get the C++ reference

The old implementation is the source of truth for behavior. Create a worktree:

```bash
git worktree add /tmp/queen-v0160 v0.16.0
```

C++ paths cited below (`server/src/routes/*.cpp`, `server/src/services/*.cpp`,
`server/include/queen/*.hpp`, `lib/queen*.hpp`) are relative to that worktree.
Rust paths are relative to this repo. The legacy row-engine SQL procedures
(`server/sql/procedures/001_push.sql`, `002d_pop_unified_v4.sql`, `003_ack.sql`,
`004_transaction.sql`, `005_renew_lease.sql`, `006_has_pending.sql`,
`012_configure.sql`) are byte-identical to v0.16.0 and also serve as reference.

General rule for every item: **the client-observable contract of v0.16.0 wins.**
When in doubt, replicate old behavior exactly, even where the new behavior seems
"better", unless this plan explicitly says otherwise.

---

## P0 — Critical (data loss, security, broken clients)

### 1. Re-implement the file buffer (DB-outage durability)

**Problem.** C++ spooled pushes to disk when the DB was unavailable and replayed
them FIFO on recovery (`services/file_buffer.cpp`; config `FILE_BUFFER_DIR`
default `/var/lib/queen/buffers`, `FILE_BUFFER_FLUSH_MS`=100,
`FILE_BUFFER_MAX_BATCH`=100, `FILE_BUFFER_EVENTS_PER_FILE`=10000 —
`config.hpp:356-392`). Clients got HTTP 201 with per-item `status:"buffered"`
(`routes/push.cpp:148-214`). The Rust broker has nothing: when the DB is down,
fusion resolves every frame to `"error"` and **still returns 201**
(`fusion.rs:504-536`, `handlers/data.rs:191-194`) — silent message loss for any
client that only checks the status code.

**Required.** Port the file buffer:

- Disk spool of failed push items (length-prefixed or JSONL event files, rotating),
written when the fusion bundle fails with a connection/timeout error (hook the
error path at `fusion.rs:504-536`).
- Background drain task: replay oldest-first to the DB in batches, preserving the
original `transactionId`s (dedup makes replay idempotent), circuit breaker on
consecutive failures (C++: 10 failures / 5 s cooldown, `file_buffer.hpp:150-151`).
- Startup recovery: drain leftover buffer files before serving (C++ blocked worker 0,
capped 3600 s, `file_buffer.cpp:77-85, 206-245`).
- Response contract: buffered items must be reported `status:"buffered"` in the
201 body, exactly like C++.
- Read the four `FILE_BUFFER_`* env vars with the C++ names and defaults.

**Acceptance.** Kill Postgres mid-push-load: no message lost, clients see
`buffered`, everything lands in the DB after Postgres returns, restart during the
outage does not lose spooled data.

### 2. DLQ default flipped — restore "always dead-letter"

**Problem.** Old ack always inserted exhausted messages into
`queen.dead_letter_queue`, ignoring the queue flags (`003_ack.sql:148-160`). The
segment engine dead-letters only when
`COALESCE(qq.dead_letter_queue,false) OR COALESCE(qq.dlq_after_max_retries,false)`
(`024_storage_v2_pop_ext.sql:354-363, 532-541`) and otherwise **silently drops**
the poison message (`024:386-439, 567-588`).

**Required.** Make `deadLetterQueue` and `dlqAfterMaxRetries` default to **true**
so unconfigured queues keep the old behavior:

- Change the `COALESCE(..., false)` defaults to `true` in `024_storage_v2_pop_ext.sql`
(both the durable path ~354-363 and the legacy path ~532-541) and anywhere else
the flags are read (grep `dead_letter_queue` and `dlq_after_max_retries` across
`server/sql/procedures/02*.sql`, `025_storage_v2_dlq.sql`).
- Change the column defaults in `server/sql/schema.sql` for `queen.queues` to true,
and make `012_configure.sql` default them to true when the caller omits them
(careful: don't override an explicit `false` from the client).

**Acceptance.** On a queue configured with no DLQ options, a message that exhausts
retries lands in the DLQ (visible via `GET /api/v1/dlq`), never silently dropped.
An explicit `deadLetterQueue:false` still disables it.

### 3. Remove `max_queue_size` enforcement entirely

**Problem.** Old SQL ignored the cap. The new engine enforces it via
`queen.seg_evict_v1` (`026_storage_v2_maintenance.sql:217-301`), which deletes
oldest whole segments — documented data loss including in-flight leased batches.

**Required.**

- Delete `seg_evict_v1` from `026_storage_v2_maintenance.sql` (or reduce it to a
no-op returning 0) and remove its invocation from the Rust retention loop
(`retention.rs`, and the eviction helpers in `db.rs` — grep `evict`).
- Keep `maxSize` accepted and persisted by `/api/v1/configure` (API compat: the
option is echoed back), just never enforced — this matches v0.16.0.
- Also remove the broker-side `max_wait_time_seconds` eviction if it deletes data
(`db.rs` ~719-783) — v0.16.0's SQL never implemented it either; check what the
C++eviction service actually did before deciding (if C++ enforced it
server-side, keep parity; the audit found the old SQL had no implementation).

**Acceptance.** A queue past its configured `maxSize` keeps accepting pushes and
never loses segments to eviction. `configure` still echoes `maxSize`.

### 4. Accept both `PG_DB` and `PG_DATABASE`

**Problem.** C++ read `PG_DB` (`config.hpp:86`); Rust reads only `PG_DATABASE`
(`config.rs:213`). Existing deployments silently connect to database `postgres`.

**Required.** In `config.rs`, resolve the database name as
`PG_DATABASE` → fallback `PG_DB` → fallback `postgres`. Apply the same in the
migration handlers (`handlers/migration.rs:128-138`).

**Acceptance.** Setting only `PG_DB=foo` connects to `foo`; setting both prefers
`PG_DATABASE`.

### 5. Postgres TLS

**Problem.** `PG_USE_SSL` / `PG_SSL_REJECT_UNAUTHORIZED` (`config.hpp:90-91`) are
unread; the pool is hard-wired `NoTls` (`db.rs`), migration handlers too
(`handlers/migration.rs:8-25`), and the migration `ssl` body param is ignored
(C++ honored it via `sslmode=require`, `migration.cpp:47-55, 182`).

**Required.**

- Read `PG_USE_SSL` (default false) and `PG_SSL_REJECT_UNAUTHORIZED`
(default true).
- Wire a TLS connector (e.g. `tokio-postgres-rustls` or `postgres-native-tls`)
into the deadpool pool when enabled; `PG_SSL_REJECT_UNAUTHORIZED=false` must
disable cert verification (many managed PGs use self-signed chains).
- Honor `ssl:true` in the migration test-connection/start/validate handlers.

**Acceptance.** Connects to an SSL-required Postgres with `PG_USE_SSL=true`;
plain deployments unaffected.

### 6. Boolean env parsing — match C++ exactly

**Problem.** C++treats only the literal string `true` as truthy
(`config.hpp:14`). Rust accepts `1/true/yes/on` case-insensitively
(`config.rs:200-205`). `JWT_ENABLED=1` was a no-op on C++ but enables auth on
Rust. Additionally Rust treats empty-string env values as unset
(`config.rs:191-196`), so `JWT_SKIP_PATHS=""` silently restores the default skip
list instead of clearing it; C++ used the empty value verbatim.

**Required.**

- Boolean parser: only `"true"` is true, everything else false (exact C++ parity).
- String vars: an env var that is set-but-empty is an **empty string value**, not
unset (audit every `filter(|v| !v.is_empty())` in `config.rs`).

**Acceptance.** `JWT_ENABLED=1` leaves auth off; `JWT_SKIP_PATHS=""` results in an
empty skip list; unset vars still get defaults.

### 7. Restore the full auth system (RS256 / EdDSA / JWKS)

**Problem.** Rust verifies HS256 only, by hand (`auth.rs:113-134`); any other
`alg` is 401. C++supported HS256, RS256/RS384/RS512, EdDSA (Ed25519) and
`auto`-dispatch on the token header (`auth/jwt_validator.cpp:88-116`), plus a full
JWKS client: startup pre-fetch, kid-keyed cache, refresh-on-unknown-kid,
`JWT_JWKS_URL` / `JWT_JWKS_REFRESH_INTERVAL`(3600 s) / `JWT_JWKS_TIMEOUT_MS`(5000)
(`jwt_validator.cpp:64-74, 277-287, 547-692`; `config.hpp:493, 525-526`). C++ also
validated the auth config at startup (`config.hpp:539-561`); Rust boots with a
missing secret and 500s per request.

**Required.**

- Replace the hand-rolled verifier with a real JWT library (`jsonwebtoken` crate)
supporting HS256, RS256/384/512, EdDSA, and `JWT_ALGORITHM=auto`.
- Static key support: `JWT_PUBLIC_KEY` (PEM) — currently loaded but dead
(`config.rs:11-15`).
- JWKS: fetch on startup, cache keys by `kid`, refresh on unknown kid and on the
refresh interval, honor the timeout env. RSA `n`/`e` and OKP `x` JWK forms.
- Startup validation: fail fast (or at minimum log loudly) when `JWT_ENABLED=true`
but the configured algorithm has no usable key material.
- Keep the existing claim handling (issuer, audience, clock skew 30 s, roles
claims, producer_sub stamping) — that part is already at parity.
- Route-level map (`auth.rs:220-285`): remove the Rust-only PUBLIC `/status`
exposure (make it READ_ONLY or keep public deliberately — C++ had no `/status`;
do not leak broker info unauthenticated by default).

**Acceptance.** Tokens signed RS256 via a JWKS endpoint validate; rotating the
JWKS key is picked up without restart; HS256 deployments unchanged; boot fails
loudly on `JWT_ENABLED=true` with no key.

### 8. Restore at-rest payload encryption

**Problem.** Entirely absent in Rust. C++: AES-256-GCM via
`QUEEN_ENCRYPTION_KEY` (64 hex chars → 32-byte key), triggered per queue by
`queen.queues.encryption_enabled`; payload replaced by a
`{"encrypted": <b64>, "iv": <b64>, "authTag": <b64>}` envelope with
`is_encrypted=true`; decrypted on pop and on management reads
(`services/encryption.cpp:100-277`, `routes/push.cpp:382-405`,
`routes/pop.cpp:27-47`, `routes/messages.cpp:17-58`). Rust writes every frame
`encrypted:false` (`fusion.rs:401`, `data.rs:1419`, `streams.rs:229`) and never
decrypts; migrated old encrypted messages are served as raw ciphertext envelopes.

**Required.**

- Encryption module (`aes-gcm` crate): same envelope format, same env var, same
16-byte IV / 16-byte tag, so old messages decrypt and old servers could decrypt
new ones.
- Encrypt at push when the queue's `encryption_enabled` is true and the key is
set. The flag must be read from queue config — add it to the broker's queue
config cache (see also item 16's cache work). Set `FLAG_ENCRYPTED` on the frame
(`frames.rs:10, 84-85`).
- Decrypt on pop (`handlers/data.rs` pop response assembly ~629-644), on
`GET /api/v1/messages` list enrichment and `GET /api/v1/messages/:pid/:txn`
(`handlers/messages.rs:85-105`), envelope-sniffing like C++ (decrypt anything
that looks like the envelope, regardless of current flag state).
- Transaction pushes and streams sink pushes must encrypt too
(`data.rs:1419`, `streams.rs:229`).
- Match C++'s failure mode: if the key is missing/encryption fails, store
plaintext **with a warning log** (do not fail the push).

**Acceptance.** Round-trip on an encrypted queue returns plaintext to consumers;
DB rows contain only envelopes; messages encrypted by v0.16.0 (after migration)
decrypt correctly; queues without the flag are untouched.

### 9. Migration endpoints: ADMIN auth + ssl param

**Problem.** All five C++ migration routes required ADMIN
(`migration.cpp:172, 245, 434, 448, 591`); Rust's `route_access_level` has no
migration branch so they fall to READ_WRITE (`auth.rs:220-285`) — a read-write
token can `pg_dump | pg_restore` the whole DB to an arbitrary host. The `ssl`
body param is also ignored (covered by item 5).

**Required.** Add `/api/v1/migration` prefix → ADMIN in `auth.rs`. Honor `ssl`.

**Acceptance.** READ_WRITE token gets 403 on every migration route; ADMIN works.

### 10. Ack semantics — restore v0.16.0 behavior exactly (CRITICAL)

**Problem.** The segment engine changed ack semantics in several
client-observable ways. Old behavior (`003_ack.sql`):

- A `completed` ack of message N set the cursor to N — **implicitly completing
every unacked message before it** (`003:88-92`). "Ack the last message of the
batch" completed the batch.
- Retry state was a single `batch_retry_count` per (partition, group), incremented
once per `failed` ack, reset on full batch completion (`003:109-147`); lease
expiry did **not** consume retry budget.
- `status:'retry'` released the lease without touching cursor or retry count —
budget-free explicit retry (`003:193-203`).
- `status:'dlq'` force-dead-lettered immediately, bypassing remaining retries
(`003:123-133`).

New behavior (`023_storage_v2.sql:416-485`, `024_storage_v2_pop_ext.sql:207-618`):
contiguous-prefix cursor advancement (gaps redeliver, including already-acked
positions above a gap), per-batch-start-position `attempt_count` charged on every
redelivery (lease expiry now consumes budget), no `retry` status, no forced-DLQ
status.

**Required.** Make the segment ack path behave like the old one:

- `seg_ack_by_txn_v1` / `seg_ack_segments_v1`: an ack of position P advances the
cursor past P unconditionally (clamped to the leased batch range), regardless of
gaps — restore implicit-ack. Release the lease when the cursor reaches the batch
end. The `batch_positions` contiguous-prefix machinery
(`025_storage_v2_dlq.sql:250-255`, `024:271-349`) can be removed or bypassed.
- Restore per-(partition, group) retry counting semantics: increment only on an
explicit failed/nack ack, reset when a batch fully completes, never charge on
lease expiry or plain lease release. The `attempt_seq/attempt_off/attempt_count`
columns (`025:21-25, 231-241`) should be reworked to count failed acks, not
deliveries.
- Re-add the `retry` ack outcome (release lease, no cursor move, no retry charge)
and the client-forced `dlq` outcome, and thread both through the broker ack
handler (`handlers/data.rs` ack path) and `seg_transaction_wire_v1`
(`026:432-464`).
- Keep the DLQ handoff mechanics (payload snapshot into `queen.seg_dlq`) — only
the *decision* logic changes, per item 2's defaults.

This is the largest SQL rework in the plan. Write pgTAP-style or scripted tests
against the old engine's documented cases before changing anything: (a) ack last
message only → whole batch completed; (b) ack out of order → earlier unacked
skipped, never redelivered; (c) `retry` N times → no DLQ ever; (d) forced `dlq`
→ immediate DLQ entry; (e) lease expiry then redelivery → retry count unchanged.

**Acceptance.** The five cases above behave identically to v0.16.0 (verify the
same scripts pass against a v0.16.0 row-engine database).

### 11. Lease validation on ack — restore old leniency

**Problem.** New engine hard-fails every ack whose worker/lease doesn't match or
whose lease expired (`023:444-447`, `024:254-257`, `025:388-391`). Old engine
validated **only when a non-empty leaseId was supplied** (`003:52-59`,
`004:172-182`): lease-less acks and post-expiry acks without a leaseId still
advanced the cursor.

**Required.** In the seg ack functions, skip the worker/expiry check when the
caller passes an empty/NULL worker id; keep validation when one is supplied.
Thread "no lease supplied" from the broker handler down to SQL (the broker
currently always sends its worker id — it must forward the client's `leaseId`
presence/absence instead).

**Acceptance.** An ack with no leaseId succeeds and moves the cursor (old
contract); an ack with a wrong or expired leaseId still fails.

### 12. Wildcard pop watermark — stop stranding backlog (CRITICAL)

**Problem.** `seg_pop_wildcard_wire_v1` and the discover variant advance
`last_empty_scan_at` unconditionally whenever zero partitions yielded frames
(`024:178-185`, `033_seg_pop_discover.sql:199-210`). If a partition with backlog
was skipped only because another worker held its lease, the watermark jumps
anyway; the candidate filter `p.last_write_at >= watermark - 2min` (`024:143`)
then hides that partition forever if no new push arrives. Old engine re-verified
(30 s throttled) that no pending data existed **ignoring the lease filter**
before advancing (`002d:395-426`).

**Required.** Port the old guard: before advancing the watermark, re-check for
any partition with pending data for this group with **no lease condition**; if
found, do not advance. Throttle the re-check (old: 30 s) to keep empty polls
cheap. Apply to both `024` and `033`. Also fix the related bootstrap-marker
collision noted in item 13.

**Acceptance.** Scenario test: two workers, worker A holds the only partition's
lease, worker B polls empty repeatedly (watermark churn), A's lease expires
without acking, **no more pushes arrive** → B's next wildcard pop still delivers
the backlog.

### 13. Consumer-group `subscriptionMode:'new'` on late-created partitions (CRITICAL)

**Problem.** Old engine recorded the subscription timestamp durably in
`queen.consumer_groups_metadata` at registration; a partition created later
seeded its cursor from that timestamp, so messages pushed after subscription were
always delivered (`002d:211-271`). New engine bulk-seeds existing partitions once
(marker = first `consumer_watermarks` insert, `024:63-118`) and lazily seeds
late-created partitions at first contact with `next_seq = last_seq + 1`
(`025:165-195`) — everything pushed to that partition before the group first
touches it is **skipped**. Two adjacent bugs: (a) the empty-pop path also inserts
the `consumer_watermarks` marker row (`024:178-185`), so a group that first polls
without a subscription never bulk-seeds later; (b) the lazy seed in `025:165-167`
does not exclude `__QUEUE_MODE__`, so a queue-mode pop carrying `sub_mode='new'`
skips backlog.

**Required.**

- Persist the subscription (mode + timestamp) durably per (group, queue) — either
reuse `queen.consumer_groups_metadata` or add columns to the group's watermark
row. Registration time = first pop carrying `sub_mode`/`sub_from`, and the
`POST /consumer-groups/:group/subscription` endpoint must update it.
- Lazy per-partition seeding must consult the stored subscription: seed from the
subscription timestamp (map to first segment with `created_at >= ts`), not from
`last_seq + 1`.
- Use a dedicated "subscription registered" marker instead of the
`consumer_watermarks` row existence, fixing collision (a).
- Exclude `__QUEUE_MODE__` from subscription seeding in `025` (parity with
`002d:211` and `024:76`).
- This same durable record should feed the consumer-group observability endpoints
(item 22: `subscriptionMode`/`subscriptionTimestamp` fields).

**Acceptance.** Group subscribes with `subscriptionMode:'new'`; a new partition is
created afterwards and receives pushes; the group's first pop of that partition
delivers those messages. Queue-mode consumers never skip backlog.

### 14. Dedup hash collision — investigate, decide, document

**Problem.** Old dedup was an exact-string unique constraint on
`(partition_id, transaction_id)`. New dedup keys on
`hashtextextended(txn, 0)` — 64-bit — and does not store the raw txn
(`023:93-101, 236-248`). Two distinct transaction ids colliding within the same
partition's live dedup window cause the second message to be silently dropped as
a "duplicate".

**Required.** This is an investigation task, not necessarily a code change:

- Quantify: collision probability among N live dedup entries per partition is
≈ N²/2⁶⁵ (birthday bound). At 1 M live entries per partition-window it is
~5×10⁻⁸ per window; at 10 k entries ~5×10⁻¹². Estimate against realistic
production rates (entries = messages per partition per `dedup_window_seconds`,
default 3600 s).
- If accepted: document the risk and the window semantics in `DEVELOPING.md` /
release notes, and move on.
- If not accepted: store the raw `transaction_id` alongside the hash in
`queen.seg_dedup` and compare it on hash match before declaring a duplicate
(turns false positives into correct inserts at the cost of one text column).
- Note the second-order issue either way: txn-based acks resolve through
`seg_dedup` (`024:470-489`), so expired dedup entries already degrade acks —
keep that in mind when tuning window defaults.

**Acceptance.** A written decision (in the PR description or `DEVELOPING.md`)
with the math; if the fallback is chosen, a test that two colliding-hash distinct
txns both insert.

### 15. Mixed C++/Rust cluster wire format — accepted, no code change

The UDP payload codec differs (C++msgpack vs Rust JSON; framing and HMAC are
identical). Decision: **mixed clusters are out of scope** (already documented at
`udp.rs:26-31`). Only action: make sure the deployment/upgrade docs say a rolling
C++→Rust migration runs without cross-replica sync (wakeups, maintenance flags,
cache invalidation) and that C++ nodes will report Rust peers as dead. No code.

### 16. Maintenance flags must converge after UDP loss

**Problem.** UDP is best-effort. C++ re-read both maintenance flags (and queue
configs) from the DB every 60 s (`shared_state_manager.cpp:458-492, 610-680`), so
a lost `MAINTENANCE_MODE_SET` packet healed in ≤60 s. Rust seeds flags only at
boot (`main.rs:108-116`) and then trusts UDP (`main.rs:151-158`) — a single lost
packet leaves a replica divergent indefinitely. Same root cause leaves the
`lease_cache` entry stale forever when a `QUEUE_CONFIG_SET` invalidation is lost
(`handlers/mod.rs:37-40`).

**Required.**

- Add a periodic reconcile task (default 60 s, honor
`QUEEN_CACHE_REFRESH_INTERVAL_MS`, which is parsed but deliberately unused —
`config.rs:87-92`): re-read `queen.system_state` maintenance + pop-maintenance
flags and overwrite the in-process atomics.
- Give the queue-config `lease_cache` a TTL (or refresh it in the same loop).
- Make `GET /api/v1/system/maintenance` read fresh from the DB like C++ did
(`maintenance.cpp:22`), or accept the ≤60 s staleness and document it.

**Acceptance.** Flip pop-maintenance on replica A with replica B's UDP blocked;
within the refresh interval B pauses pops too. Same for un-flip.

### 17. Push maintenance mode must buffer, as in C++ (CRITICAL)

**Problem.** C++ maintenance mode diverted all pushes to the file buffer and
returned 201 `status:"buffered"`; disabling maintenance drained the buffer into
the DB (`routes/push.cpp:307-358`, `maintenance.cpp:31-37, 73-76`). Rust only
sets a flag; pushes keep flowing to the DB
(`handlers/maintenance.rs:36-39`, `handlers/mod.rs:41-45`), and
`bufferedMessages` is always 0.

**Required.** Depends on item 1. When maintenance mode is on, route pushes into
the file buffer instead of fusion; report real `bufferedMessages`/`bufferStats`
in `GET /api/v1/system/maintenance`; drain on disable. Restore the C++ response
message text ("Maintenance mode ENABLED. All PUSHes routing to file buffer.") —
some tooling greps for it.

**Acceptance.** With maintenance on: pushes return `buffered`, nothing is written
to `queen.seg_segments`; on disable everything drains; consumer sees all
messages exactly once (dedup handles replay).

---

## P1 — High (behavior/API regressions)

### 18. Lease defaults and per-request override

**Problem.** Old effective lease: `COALESCE(request.leaseSeconds, queue.lease_time, 60)`
(`002d:182, 510`). New: no per-request field at all (`PopParams`,
`handlers/data.rs:199-216`), `seg_queues.lease_time` defaults **300**
(`023_storage_v2.sql:32`), and the discover pop ignores the passed parameter
entirely (`033:154-156` — `COALESCE(sq.lease_time, p_lease_seconds)` with a
NOT NULL column is dead code).

**Required.** Change `seg_queues.lease_time` default to 60 (schema + `023`);
re-add a `leaseSeconds` request parameter on pop endpoints, threaded through
`seg_pop_wildcard_wire_v1` / `seg_pop_segments_v1` / discover with old COALESCE
precedence; fix the discover dead code so per-request wins over queue config.

**Acceptance.** Unconfigured queue leases 60 s; `?leaseSeconds=120` yields 120 s;
queue-configured `leaseTime` used when no request override.

### 19. Long-poll: restore 30 s default and exponential backoff

**Problem.** Old server default wait 30000 ms (`DEFAULT_TIMEOUT`,
`config.hpp:228`) with exponential re-check backoff 100 ms → ×2 after 3 misses,
capped 1000 ms (`config.hpp:238-241`, `lib/queen.hpp:2282-2296`). Rust:
`POP_DEFAULT_TIMEOUT_MS`=2000 and a fixed 25 ms poll (`config.rs:229-230`,
`handlers/data.rs:312-320`) — 40 DB polls/s per idle waiter.

**Required.** Honor `DEFAULT_TIMEOUT` (fall back to `POP_DEFAULT_TIMEOUT_MS`,
then 30000). Implement exponential backoff between re-queries using the four old
env knobs (`POP_WAIT_INITIAL_INTERVAL_MS`, `POP_WAIT_BACKOFF_THRESHOLD`,
`POP_WAIT_BACKOFF_MULTIPLIER`, `POP_WAIT_MAX_INTERVAL_MS`) with the old defaults;
a `Notify` wakeup resets the backoff to immediate re-query (that part already
works — keep it).

**Acceptance.** Idle long-poll waits ~30 s by default and its DB re-query rate
decays to ~1/s; a push still wakes it within milliseconds.

### 20. Retention/metrics background-job knobs

**Problem.** `RETENTION_BATCH_SIZE`, `RETENTION_PARALLELISM`,
`METRICS_RETENTION_DAYS` (90), `PARTITION_CLEANUP_DAYS` (30) are unread
(`config.hpp:319-323`). Metrics tables grow unboundedly. (Eviction cadence
concerns disappear once item 3 removes eviction.)

**Required.** Read the vars with C++ names/defaults. Implement a metrics purge in
the retention loop (`retention.rs`): delete `worker_metrics` / metrics-history
rows older than `METRICS_RETENTION_DAYS`. Apply `RETENTION_BATCH_SIZE` to the
sweep's batched deletes; `RETENTION_PARALLELISM` may be a no-op if the segment
sweep is already fast — decide and document.

**Acceptance.** Metrics rows older than the window are purged on the retention
cadence; knobs change behavior.

### 21. `GET /metrics` — restore JSON shape

**Problem.** C++ returned a JSON object
`{uptime, requests{total,rate}, messages{...}, database{poolSize,idleConnections,waitingRequests}, memory{...}, cpu{...}}`
(`metrics.cpp:11-46`). Rust repurposed the route to Prometheus text
(`status.rs:32-45`), breaking JSON consumers.

**Required.** Restore the JSON shape at `/metrics` (populate from the process
metrics + deadpool pool status). Prometheus stays at `/metrics/prometheus` only.

**Acceptance.** `/metrics` returns the old keys as JSON; `/metrics/prometheus`
unchanged.

### 22. Consumer-group observability endpoints

**Problems** (all in `099_retire_rows.sql` redefinitions):

- `GET /consumer-groups/:group` returns `{}` for every group: `099:265-267` joins
`partition_consumers` to `queen.partitions`, but segment cursors reference
`queen.seg_partitions` (independent UUID space, FK dropped at `023:157-160`).
- `GET /consumer-groups/lagging` is a stub: `099:186-240` hardcodes
`lag_seconds = NULL` then filters `lag_seconds IS NOT NULL` — always `[]`.
Also the handler default changed: C++ `minLagSeconds` 3600
(`consumer_groups.cpp:99`) vs Rust 60 (`consumer_groups.rs:51`).
- Group list: `subscriptionMode`/`subscriptionTimestamp`/`subscriptionCreatedAt`
hardcoded `null` (`099:378-380`).

**Required.** Fix the details join to `seg_partitions`; implement real lag in the
lagging SP (pending frames via `last_seq`/`next_seq` and segment `msg_count`,
time lag from segment `created_at`); restore the C++ 3600 default; populate the
subscription fields from item 13's durable subscription store.

**Acceptance.** Details endpoint returns per-queue/per-partition cursor data for a
segments consumer group; lagging returns rows for a genuinely lagging group;
webapp drill-downs render.

### 23. `GET /api/v1/messages/:pid/:txn` — restore full shape

**Problem.** Old response (`010_messages.sql:194-271`) had ~20 fields incl.
`queue`, `queuePath`, `namespace`, `task`, `status`, `errorMessage`,
`retryCount`, `leaseExpiresAt`, `queueConfig{...}`, `mode{...}`,
`consumerGroups[]`. Rust synthesizes only 10 fields (`messages.rs:94-105`).
Additionally the lookup resolves via `seg_dedup`, so messages older than the
dedup window 404 even though they still exist (`db.rs:326-338`), and payloads are
not decrypted (item 8).

**Required.** Rebuild the missing fields: queue/namespace/task from
`seg_partitions`→`seg_queues`→`queen.queues`; `status` derived from cursor
position vs message position per group; `queueConfig` from `queen.queues`;
`consumerGroups` from `partition_consumers`; `leaseExpiresAt` from the consumer
row. Add a fallback lookup path that scans the partition's segments for the txn
when the dedup entry has expired (bounded scan; acceptable for a management
endpoint). Decrypt payloads once item 8 lands.

**Acceptance.** The webapp message-detail view renders all fields for a segment
message, including one older than the dedup window.

### 24. Prometheus endpoint fixes

**Problems** (`status.rs:135-224`, `metrics.rs:140-168` vs `prometheus.cpp`):

- No `# HELP`/`# TYPE` lines at all.
- `queen_cluster_{push,pop,ack}_{requests,messages}_total{scope="cluster"}` keep
their C++names but are now per-process counters that reset on restart; the
C++ versions were DB-backed cluster-lifetime totals.
- The new `queen_db_*_total` family reads from `queen.worker_metrics_summary`,
which the Rust broker never populates — always 0 (`status.rs:140-143`).
- ~30 C++ families dropped (per-queue `queen_queue_*_per_minute`, per-worker
`queen_worker_`*, pool/threadpool/file-buffer/maintenance gauges).

**Required.**

- Emit HELP/TYPE for every family.
- Resolve the `queen_cluster_`* semantic lie: either populate
`worker_metrics_summary` from the Rust stats path and emit DB-backed values
under the old names, or rename the in-process counters to
`queen_process_*` and keep `queen_cluster_*` DB-backed. Old dashboards use
`max(queen_cluster_*)` — pick the option that keeps them correct.
- Restore the families that still make sense: `queen_db_pool_{size,idle,active}`
(deadpool status), `queen_maintenance_mode_enabled`, `queen_file_buffer_*`
(after item 1), and the per-queue minute-rate family if the stats tables can
supply it (034 path). Per-worker/threadpool/sidecar families may be declared
obsolete — document which are intentionally gone.
- Add `Cache-Control: no-cache` like C++ (`prometheus.cpp:664`).

**Acceptance.** A v0.16.0 Grafana dashboard's core panels (cluster totals, DLQ
depth, pool, maintenance, per-queue rates) show correct data against the Rust
broker.

### 25. SP-level errors must map to HTTP 500/404

**Problem.** C++ uniformly mapped an SP-returned `{"error": ...}` to HTTP 500,
or 404 when the message contains "not found" (`status.cpp:47-51`,
`resources.cpp:49-53`, `messages.cpp:91-95`, `consumer_groups.cpp:45-48`). Rust
mostly forwards SP error JSON at **200** (exceptions already correct: queue
get/detail 404, streams register 409).

**Required.** Add one helper in `handlers/mod.rs` — `sp_result_to_response(json)`:
if the object has an `error` key → 404 when the text contains "not found", else
500; otherwise 200. Apply it to every handler that returns an SP result verbatim
(consumer-groups details/list, status/analytics family, messages list, dlq,
traces reads, configure, resources). Keep the streams `state/get` 400-on-
`success:false` C++ behavior (`state_get.cpp:97-101`) while you're there.

**Acceptance.** Requesting a nonexistent consumer group returns 404/500 per the
old contract, not 200 + error body.

### 26. Push-only queues must get a `queen.queues` row

**Problem.** Old push created the `queen.queues` config row (deriving
namespace/task from the dotted queue name — `001_push.sql:192-209`). Segment push
creates only `seg_queues`/`seg_partitions` (`023:204-213`), so a never-configured
queue: runs with fallback defaults (retry 3, no DLQ, no retention — those reads
COALESCE from a missing row: `025:141-146`, `024:354-363`), and is invisible to
namespace/task discovery pops, which inner-join `queen.queues`
(`033:89-93, 154-163`).

**Required.** In `seg_push_segment_v1` (and the multi-push `032:59-69` and
transaction pre-pass `026:388-398`), upsert the `queen.queues` row on queue
creation, deriving `namespace`/`task` via `split_part` exactly like
`001:196-200`, with `storage='segments'`.

**Acceptance.** Pushing to a brand-new dotted queue name (`ns.task.foo`) makes it
appear in `/api/v1/resources/queues` with namespace/task set, discovery pop finds
it, and retention/DLQ config later applied via `/configure` takes effect.

---

## Suggested execution order and dependencies

1. Quick config wins, independent: **4, 5, 6, 9, 18, 20** (small, unblock deployments).
2. **1 (file buffer)** then **17 (maintenance buffering)** — 17 depends on 1.
3. **2 (DLQ defaults)** and **3 (remove eviction)** — small SQL changes, do before 10.
4. **10 (ack semantics)** + **11 (lease leniency)** together — same SQL functions.
5. **12 (watermark)** and **13 (sub mode)** — same pop SQL area; 13 also feeds 22.
6. **7 (auth)** and **8 (encryption)** — self-contained Rust modules; 8 touches
  push/pop/messages handlers.
7. **16 (reconcile loop)**, **19 (long poll)** — broker-side, independent.
8. Observability batch: **21, 22, 23, 24, 25, 26**.
9. **14 (dedup investigation)** any time; **15** is docs-only.

## Testing notes

- The JS test suite (`clients/`?) was previously used for parity ("js suite seems
ok" in commit history) — run it before and after each band.
- For SQL semantic changes (items 2, 3, 10, 11, 12, 13, 26) write direct SQL
scenario tests first, and where possible run the same scenario against a
v0.16.0 database (worktree + old schema) to pin expected behavior.
- Multi-replica items (16) need a 2-process test with UDP dropped (e.g. block the
port with a firewall rule or run peers pointing at a black-hole address).

