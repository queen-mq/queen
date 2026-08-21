# EPHEMERAL_QUEUES — RAM-class queues behind three new routes, PG only as config

Rev 1.1 — 2026-08-21. Grounded in a read of the live tree at branch `conflation` (1.0.6, 97aa5e98). Every file:line reference below is from that tree. **Design only — no code was changed.**

Rev 1.1 changes (same day, after review with Alice): the eight §10 questions are resolved. The largest consequence is Q1: **there is no queue-level `mode`** — consumption semantics come from the pop's consumer group exactly as on the durable engine, placement is always single-owner in v1, and replicated local reads are deferred (§9). Dashboard integration (§5.3) and client-side buffering (§4.1) moved INTO scope.

Thesis: a per-queue storage class whose contents live in broker RAM and survive nothing, behind its own route family `/api/v1/ephemeral/*` and SDK namespace `queen.ephemeral.*`. The durable engine is not edited — not its handlers, not its SPs, not its fusion — and the claim "durable hot path unchanged" is enforced by construction and pinned by a perf gate, not promised.

Name: **ephemeral**, everywhere. Not "memory", not "transient", not "volatile". The age knob is **`ttlSeconds`**, never "retention": durable retention cleans consumed history and never touches pending; this knob drops unconsumed messages. One word per contract.

Target workloads, in order of expected value: req/reply inboxes (today partition-bound at ~12 ms p50; see `benchmark-queen/2026-08-20-gate/` and `examples/rrl.js`), presence/typing/cache-invalidation fan-out, progress events, best-effort work distribution. Common shape: high queue cardinality, small payloads, immediate consumption, worthless history — exactly the shape that is disproportionately expensive on the log engine (per-partition serial claim tx; idle metric churn measured in the 1.0.4 tenants rerun).

---

## 0. Ground truth — what a read of the tree changes about the sketch

- **M1 — the mesh is framed TCP with drop-on-full peer queues, not UDP.** `mesh.rs:1-41` (module header: replaces `udp.rs`, which is gone), frame = `u32 BE len | u8 type | JSON` (`mesh.rs:645,658`), `MAX_FRAME_LEN = 16 MiB` (`mesh.rs:78`), per-peer bounded queue `PEER_QUEUE = 1024` with **drop, never block** (`mesh.rs:83,227-235`). Consequence (now deferred with replication, §9): bulk payload frames must never share the wake lane — a 16 MiB frame ahead of a `T_MESSAGE_AVAILABLE` is ~128 ms of head-of-line at GbE. v1 mesh traffic for this feature is control-only.
- **M2 — there is no broker→broker HTTP client.** The only HTTP client in `server/src` is `httpget.rs` (single-shot GET for JWKS, `httpget.rs:1-27`). The in-house template for a pooled client is the proxy's `hyper_util::client::legacy::Client` (`proxy/src/main.rs:151-160`, `proxy/Cargo.toml:20-21`, "No reqwest by design"). Forwarding needs a new small module copying that pattern (§3.6).
- **M3 — membership exists but is not consumable.** Liveness is a private map keyed by peer `server_id`, fed by HELLO and inbound frames (`mesh.rs:150-153,441,453`); `stats()` exposes only counts, and the static peer list carries no `server_id` (`mesh.rs:578`). There is no `server_id → dialable HTTP address` mapping. The rendezvous hash needs a public live-members accessor and a HELLO that advertises `{http_addr, epoch}` (§3.5).
- **M4 — no boot epoch exists anywhere.** No incarnation id in `server/src`; nearest things are `lease::holder_id = "{server_id}#{pid}"` (`lease.rs:42`) and the DB-side `maintenance_leases.fence` (`lease.rs:70-80`), both wrong for this. A restarted peer is indistinguishable in `liveness` (`mesh.rs:684` HELLO has `server_id + nonce` only). The engine introduces `eph_epoch` at boot (§3.2) and puts it in message ids and HELLO.
- **M5 — the parked-pop machinery is pure RAM and reusable as-is.** `notify::Notifier` (`notify.rs:59-89`): gates keyed by the composite `tenant_queue_key(tenant, queue)` = `tenant \x1f queue` (`handlers/mod.rs:186`), `wait_queue`/`wake_local_hint`/`drain_hints` touch no pool. The mesh splits the key on the FIRST `\x1f` and rejoins on receive (`mesh.rs:240,262`, `main.rs:706`), so a namespace prefix must live in the queue half: ephemeral qkeys are `tenant \x1f eph:<name>`. Gates are evicted when idle (`notify.rs:158`, driven by `reconcile.rs:203-226`) — gate presence is a fast path, never state. A durable queue literally named `eph:x` would only cross-WAKE (wakes are hints; the woken side checks its own store and re-parks), so no durable-side name guard is needed; stated here as deliberate.
- **M6 — the proxy fails closed on the new family.** `is_upstream_path` already forwards `/api/` (`proxy/src/webapp.rs:55-63`), but `classify` returns `RouteClass::Blocked` for unknown API paths (`proxy/src/routes.rs:244-246`) → 404 `route_blocked` (`gateway.rs:87-90`). `routes.rs` is **owned by the orchestrator** (`routes.rs:6-7` — agents must not edit; report desired changes): §5.1 is written as that report. Webdoc mirrors are fingerprint-guarded and must be updated in the same change: `webdoc/scripts/gen-proxy-routes.mjs:32-33,173-174`, `gen-routes.mjs:60,163-165`, `gen-openapi.mjs:363-378`.
- **M7 — the quota house pattern fits, minus one loop.** Three-rung ladder `switches::decide` = kill switch → grant → occupancy (`switches.rs:291-322`), verdict→HTTP in one place (`quota.rs:138-149`: 403 quota / 403 `feature_gated` / 429 / 503), `require_grant` defaulting to `tenancy_header` (`config.rs:1210`), charge-local-delta / measurement-only-releases (`quota.rs:34-48`). Ephemeral copies the ladder with new `Surface` variants — but its occupancy is measured **in-process** (the broker IS the meter), so unlike KV there is no PG usage table and no refresh SP. Only the grant rows live in PG (§2).
- **M8 — auth fallthrough is `ReadWrite` and the GET-trap is documented.** `auth.rs:472` (fallthrough), `auth.rs:425-436` (GET arms must live inside the GET block), `auth.rs:461-468` (state levels explicitly; the fn is mirrored in webdoc generators behind fingerprints). New routes get explicit arms (§3.9).
- **M9 — no boot enable flag, by house rule.** One un-`mut` route chain, no conditional registration (`main.rs:811-816`); management routes registered unconditionally so a 404 never means "feature off" (`main.rs:957-961`); PLAN_CONFLATION.md:804-807 states the principle. Ephemeral routes always exist; pausing is a runtime kill switch (rung 1 of the ladder), and cloud gating is the grant (rung 2), which with `require_grant ← tenancy_header` means: OSS on by default, cloud off until the plan grants it — the exact KV posture.
- **M10 — lease expiry has two in-house shapes.** Opportunistic TTL sweep on touch (`ack_registry.rs:215-217,286`) and the sweeper's `Gate` sub-cadence + process-global `Wake::hint_in_ms` (`sweeper.rs:502-525,339-346`). §3.2 uses opportunistic-on-touch with a sweeper gate as backstop, ringing `hint_in_ms` for the next known expiry.
- **M11 — the durable A/B gate already exists as a convention.** `benchmark-queen/kv-timers-gates/perf-gate-hotpath.sh`: CPU µs/msg from cgroup `cpu.stat`, fixed work, fresh DB per rep, `GATE_REPS>=3` else UNRESOLVED, `TOLERANCE_PCT=1.0`; plus the reusable pin "no new `pg_stat_statements` entry with calls>0 on a load that doesn't use the feature" (`perf-gate-hotpath.sh:20-41`). §7.5 reuses it verbatim.
- **M12 — hot path zero-PG is achievable exactly.** All engine tables are boot-created (`schema.rs:23,48-97`); ephemeral adds one config table family + SPs (§2) touched only by `configure`/`delete`/boot — push/pop/ack never construct SQL. The mechanical guard pattern for "handlers never name tables" already exists (`server/tests/kv_handler_isolation.rs:22,41`) and is extended to the new tables (§7.2).
- **M13 — the SDK buffer machinery is target-agnostic in shape but not in wiring.** `clients/client-js/client-v2/buffer/BufferManager.js` keeps one `MessageBuffer` per `queue/partition` address with exactly one drain loop per buffer (header, `BufferManager.js:1-22`); a failed batch returns to the FRONT and retries until it lands or a `deadlineMillis` expires (the 2026-08-20 lossless-flush fix); backpressure is blocking at `maxSize` (`addMessage` must be awaited). Enabled per chain via `queue(name).buffer(options)` (`builders/QueueBuilder.js:158-159`), threaded to `PushBuilder` (`QueueBuilder.js:199`). The drain currently posts only the durable push wire — §4.1 parametrizes the sink instead of duplicating the machinery.
- **M14 — the dashboard is a Vue 3 app with one data layer.** `app/src/api/index.js` (the only fetch layer; already speaks `resources/queues`), `stores/queuesStore.js`, views in `app/src/views/` (`Queues.vue`, `QueueDetail.vue`, `Dashboard.vue`, …), health grid component `components/QueueHealthGrid.vue`. §5.3 adds one view + two fetchers rather than threading ephemeral rows into durable tables whose columns (pending/retained/DLQ) would lie about the class.

---

## 1. Semantics — the contract, before any code

### 1.1 Existence: two tiers
- **Implicit**: created by the first `push`/`pop` that names it, with tenant defaults; lives only in RAM; garbage-collected after `QUEEN_EPHEMERAL_IMPLICIT_IDLE_S` (default 300 s) of being empty and unpolled. Never touches PG. This tier is mandatory, not a convenience: req/reply inboxes are thousands of short-lived per-client queues, and one PG row per inbox would rebuild the partition-churn cost this class exists to kill.
- **Declared**: created/updated by `configure`; options persisted in PG (§2); survives restarts **as configured but empty**; listed in the dashboard; the object quotas count (`max_queues`) applies to declared + currently-live implicit.

### 1.2 Loss contract
Contents survive nothing: process exit (clean or crash), ownership movement (membership change), deploys. Failover framing for the docs: *treat it like a Redis restart*. Declared configuration survives everything (it is durable). There is no replay, no history, no `subscriptionMode` — the concepts have no referent here.

### 1.3 Delivery
The class picks what can be lost; the ack mode picks the guarantee:
- `autoAck=true` → at-most-once. Cursor advances at delivery; no lease bookkeeping at all.
- ack mode (default) → **at-least-once while the owning incarnation lives**: undelivered acks redeliver on lease expiry (`leaseSeconds`, default 30), `status ∈ {completed, failed, retry}` as on the durable wire; `failed`/`retry` redeliver with `attempts+1`; attempts exhausted at `retryLimit` (default 5) → **dropped and counted** (`eph_dropped_retry`). DLQ-to-PG is deliberately out of v1 (§9).
Consumers that ack still need idempotency, exactly as on durable queues. The docs must not describe the class as "at most once".

### 1.4 Ordering
FIFO per `(queue, partition)` within one ownership incarnation. Across an incarnation boundary the question is empty (contents are gone). The few seconds of membership disagreement during a topology change may blur order and duplicate or lose messages — inside this contract, where on a durable queue it would be a correctness bug. That legality is the deep reason cheap distribution works here.

### 1.5 Consumption semantics come from the group; placement is not a semantic (Q1, resolved)
Exactly as on the durable engine, the pop's `group` parameter is the whole story:
- **Competing** consumers = the same `group` (or no group at all — the groupless queue-mode, mirroring the durable `__QUEUE_MODE__` sentinel, `data.rs:2762`).
- **Fan-out** = each subscriber pops with its **own** group; every group has its own cursor over the one ring and receives everything.
There is **no queue-level `mode`**: rev 1.0 had one, and it was conflating consumption semantics (the group's job) with data placement (the engine's job). Placement in v1 is always **single-owner** (§3.7): one ring per `(queue, partition)` on the rendezvous owner, all groups' cursors on it — which also gives fan-out *consistent* cursors (no per-broker divergence, no double-consume). The cost is that all of a queue's traffic lands on its owner and remote consumers pay one forwarded hop; high queue cardinality spreads owners evenly, and §7.6 measures the hop. Replicating a queue to every broker for local reads is a placement *optimization*, deliberately deferred (§9) until the hop measurably hurts.

### 1.6 Bounds and pressure
Three nested budgets, checked on push in order, charge-before-append with refund on failure (the `quota.rs:34-48` discipline):
1. per-queue `maxBytes` (default 16 MiB) / `maxLength` (default 10 000) → policy `reject` (429 `queue_full`, same shape the 1.0.6 SDK backpressure already handles) or `dropOldest` (feed semantics; `eph_dropped_bounds` counted);
2. per-tenant `max_bytes` / `max_queues` / msgs-per-sec from the grant row → 403 `ephemeral_quota_exceeded` / 429 `rate_limited` via the ladder;
3. broker-global `QUEEN_EPHEMERAL_MAX_BYTES` (default 256 MiB) → 503 `ephemeral_unavailable` (NoRoom), mirroring `quota.rs:138-149`.

### 1.7 Delivery shaping
- `ttlSeconds`: head-drop of messages older than the limit, enforced lazily on push/pop plus the sweeper backstop; `eph_dropped_ttl` counted.
- `windowBuffer {ms, count}`: a waiting pop returns when `count` messages are ready or `ms` elapsed since the first, bounded by the pop's own `timeout`. Delivery-side batch fattening only.

---

## 2. Storage/SQL — one cold-path table family, SP-only

New file `server/sql/procedures/030_ephemeral.sql`, registered in the apply list (`schema.rs:48-97` pattern):
- `queen.ephemeral_queues (tenant_id UUID, queue TEXT, options JSONB, updated_at, PRIMARY KEY (tenant_id, queue))` — declared configs only.
- `queen.ephemeral_quota (tenant_id UUID PRIMARY KEY, enabled BOOL, max_bytes BIGINT, max_queues INT, max_msgs_per_sec INT)` — the grant rows, same shape and provisioning story as `queen.kv_quota` (`024_kv.sql:243-253`); the orchestrator writes them.
- SPs: `queen.eph_config_set_v1 / get_v1 / delete_v1 / list_v1(tenant)`, `queen.eph_quota_list_v1`. Handlers never name the tables (M12; enforced §7.2). No usage table, no refresh SP — occupancy is in-process (M7).

Touched at: boot (load declared configs + grants before the listener, like `main.rs:277-278` does for KV), `configure`, `delete`, and a slow grant refresh reusing the `kv_quota_refresh_ms` cadence knob family. Never on push/pop/ack.

**Not touched**: `schema.sql`, every `00x_log_*.sql`, `024_kv.sql`, `log_dlq`, retention, stats SPs.

---

## 3. Broker (Rust)

### 3.1 Wire
All bodies JSON; tenant from `Extension<Tenant>` (never the body — `kv.rs:23-24` rule); errors in the house `{error, code}` envelope.

| Verb | Route | Body / params | Success |
|---|---|---|---|
| push | `POST /api/v1/ephemeral/push` | `{queue, partition?, messages:[{payload}...]}` | 201 `{pushed}` — all-or-nothing per request |
| pop | `GET /api/v1/ephemeral/pop` | `?queue&partition?&batch?&wait?&timeout?&group?&autoAck?` | 200 `{queue, messages:[{id, partition, payload, attempts}]}` (empty array on timeout) |
| ack | `POST /api/v1/ephemeral/ack` | `{queue, group?, acks:[{id, status?, error?}]}` | 200 `{results:[{id, outcome}]}` — `outcome ∈ {acked, redelivered, stale, unknown}` |
| configure | `POST /api/v1/ephemeral/configure` | `{queue, options:{maxBytes, maxLength, policy, ttlSeconds, leaseSeconds, retryLimit, windowBuffer}}` | 201 echo |
| reset | `POST /api/v1/ephemeral/reset` | `{queue}` | 200 `{dropped}` |
| delete | `DELETE /api/v1/ephemeral/queue/:queue` | — | 200 |
| status | `GET /api/v1/ephemeral/queues` and `GET /api/v1/ephemeral/queues/:queue/depth` | tenant-scoped | 200 gauges |

Message id: `e:<owner_epoch_hex>:<partition>:<seq>` — opaque to clients (the durable wire's §11 stance), self-describing to the broker. An ack whose epoch is not the current owner's returns `stale`, never an error — the fencing that survives restarts and ownership moves (M4).

`reset` = drop all messages + void leases + rewind group cursors; legal only because of §1.2. `reset`/`delete` broadcast over the mesh (§3.5) and are backstopped by the config reconcile (§10 Q4).

Registration: appended in the single chain `main.rs:817-1048`, static segments before `:param` siblings (matchit rule restated at `main.rs:819-822`), inheriting body-limit → tenant → auth layers (`main.rs:1060-1079`). Unconditional (M9).

### 3.2 `server/src/ephemeral.rs` — the engine
- `pub struct Ephemeral { queues: Mutex<HashMap<QKey, Arc<EqQueue>>>, global_bytes: AtomicI64, tenants: Mutex<HashMap<Tenant, TenantUsage>>, epoch: u64, knobs }` — `QKey` built with `tenant_queue_key(tenant, "eph:" + name)` (M5; single-key discipline of `handlers/mod.rs:178-192`).
- `EqQueue { config, partitions: Mutex<HashMap<String, Ring>> }`; `Ring { deque: VecDeque<Msg>, bytes, next_seq, groups: HashMap<Group, Cursor>, leases: BTreeMap<(deadline, id), Lease> }` — one ring, N group cursors (§1.5). Payloads copied out of the request buffer (a 1 KB slice must not pin a 64 MiB body allocation), stored raw — no zstd, no frame pack: the segment codec serves durable storage economics that do not apply. Ring eviction (ttl/bounds) advances past the minimum needed cursor; a group parked below an evicted range skips forward and the skip is counted per group.
- `eph_epoch`: random `u64` drawn once at boot next to `server_id` resolution (`config.rs:165-169`), logged in the boot block, carried in ids and HELLO.
- Lease expiry: opportunistic sweep on every touch of a ring (M10) plus one sweeper `Gate` (`sweeper.rs:558-559` siblings) as backstop; next-expiry rings `sweeper::wake().hint_in_ms` (`sweeper.rs:346`).
- Implicit GC: same sweeper gate frees empty+idle implicit queues past `IMPLICIT_IDLE_S` and returns their bytes to the budgets.
- AppState wiring is the 4-edit Hotlist/AckRegistry recipe: `mod ephemeral;` in **both** crate roots (`main.rs:1-39`, `lib.rs:29-59` — twin-list rule `lib.rs:20-23`); construct in `main()` (`main.rs:317-366` neighborhood) with a boot `info!(target:"boot")`; `pub ephemeral: Arc<Ephemeral>` on `AppState` (`handlers/mod.rs:25`) filled in **both** constructors — `main.rs:377-415` **and** `embedded/boot.rs:390` (the embedded `queen::Broker` gets the feature for free and is by definition single-broker).

### 3.3 `server/src/handlers/ephemeral.rs`
Handler family in the house signature (`data.rs:203-208`): `State(st), Extension(authed), Extension(tenant), Bytes`; `json`/`json_err` helpers (`handlers/mod.rs:426,445`); registered via `mod ephemeral; pub use ephemeral::*;` (`handlers/mod.rs:477-512`). One `gated()`-style choke point calling `switches::decide` with the new surfaces before any work (`kv.rs:110-127` model), so the error ladder names the outermost reason (`switches.rs:277-282`).

### 3.4 Wake integration — why long-poll costs nothing
Push (after append): `st.notifier.notify_pushed_batch(&[(qkey, partition)])` — the hotlist-OFF direct path (`notify.rs:352`, call-site model `data.rs:432`). Ephemeral never enters the hotlist or its 5 ms coalescing tick; its wakes are direct. Pop with `wait=true`: `notifier.wait_queue(qkey, remaining)` (`notify.rs:202`) → on wake, claim from the ring under the mutex; losers of a multi-waiter wake re-park (wake-all + mutex is accepted; hints are unused here). **No poll loop and no `pop_backoff_interval`** — there is no DB to re-query; the wait is purely event-driven, which is the structural reason the pop floor drops to transport time. Parked gauge via `metrics.parked.enter` (`metrics.rs:352`) as on the durable path. Cross-broker wakes ride the existing `T_MESSAGE_AVAILABLE_BATCH` with the `eph:`-prefixed queue half surviving the split/rejoin (M5).

### 3.5 Mesh extensions (`mesh.rs`) — control-plane only in v1
- New frame type `T_EPH_ADMIN` (`{op: reset|delete|configure_invalidate, tenant, queue}`), broadcast fire-and-forget, backstopped by the config reconcile (§10 Q4).
- **HELLO v2**: add `{http_addr, eph_epoch}` to the payload (`build_hello_payload` `mesh.rs:684`); absent fields ⇒ old peer ⇒ not ephemeral-capable. New accessor `MeshTransport::eph_members() -> Vec<Member{server_id, http_addr, epoch, alive}>` built from `liveness` + HELLO data (M3). Unknown frame types on old peers must be ignored, not fatal — verify (§11).
- No payload frames in v1. When replication lands (§9), its `T_EPH_DATA_BATCH` gets a second per-peer bounded queue drained at strictly lower priority than control frames, chunked at ≤ 256 KiB so a wake is never head-of-line-blocked more than ~2 ms at GbE (M1). The analysis stays here so the deferred feature inherits it.

### 3.6 `server/src/peerclient.rs` — forwarding
Pooled `hyper_util` legacy client copied from `proxy/src/main.rs:151-160` (nodelay, `pool_max_idle_per_host`; no reqwest — M2). Non-owners forward push/pop/ack to the owner's `http_addr` on the same public paths, adding `x-queen-eph-fwd: 1` (a forwarded request is never re-forwarded: on owner mismatch after a re-hash the owner answers 503 `owner_moved` and the first broker retries the hash once) and the tenant header. Forwarded `wait=true` pops are **held open at the owner for the full timeout** (Q5, resolved) — one hop, parked at the owner, woken by the owner's gate.

### 3.7 Ownership — rendezvous, no leases
Owner of `(tenant, queue, partition)` = HRW hash over `eph_members() ∪ self` filtered to alive-and-capable, keyed by the qkey + partition. Every broker computes the same answer from the same membership; there is no lease row, no heartbeat write, no fencing protocol — membership churn moves partitions and empties their rings, which is already the §1.2 contract; stale acks die on epoch (§3.1). Membership flaps therefore cost content, not correctness. Single-broker (no peers ⇒ `mesh_active()` false, `config.rs:184`) short-circuits: self is always owner — every free-tier cell and the embedded API run this path.

### 3.8 Config knobs — `config.rs`
House three-part pattern (field with doc-comment default; read in `load()` with inline clamps; one `info!(target:"boot", ..., "config: ephemeral")` block in `log_effective`, `config.rs:796-994`). Knobs: `QUEEN_EPHEMERAL_MAX_BYTES` (256 MiB), `QUEEN_EPHEMERAL_QUEUE_MAX_BYTES` (16 MiB), `QUEEN_EPHEMERAL_QUEUE_MAX_LENGTH` (10 000), `QUEEN_EPHEMERAL_LEASE_S` (30), `QUEEN_EPHEMERAL_RETRY_LIMIT` (5), `QUEEN_EPHEMERAL_IMPLICIT_IDLE_S` (300), `QUEEN_EPHEMERAL_REQUIRE_GRANT` (default `tenancy_header`, the `config.rs:1210` posture), rate/burst pair. **No `QUEEN_EPHEMERAL_ENABLED`** (M9). `gen-config.mjs` documents them automatically. Runtime pause: a new `/api/v1/system/ephemeral` GET+POST mirroring `handlers/maintenance.rs:214,270`, feeding `switches` rung 1 with new `Surface::{EphPush, EphPop, EphAck, EphAdmin}` variants (`switches.rs:217-224`).

### 3.9 Auth
Explicit arms in `route_access_level` (`auth.rs:364-473`), mirroring the levels the durable pop/ack routes carry today (verify exact levels at implementation, §11): push `WriteOnly` (as `auth.rs:448`), pop/status inside the GET block (the `auth.rs:425-436` trap), ack/configure/reset/delete `ReadWrite`, `/api/v1/system/ephemeral` under the existing `Admin` prefix rule. Update the fingerprinted mirrors (`gen-routes.mjs`, `gen-openapi.mjs`) in the same commit.

---

## 4. SDKs

JS first, then the others in the cross-SDK per-feature-file convention (`clients/client-go/{conflation.go, conflation_wire_test.go}` model).

- **JS** (`clients/client-js/client-v2/`): new `ephemeral/Ephemeral.js` mirroring `kv/Kv.js` (including its header doc stating the wire rule); lazy singleton getter on `Queen` exactly like `get kv()` (`Queen.js:249-253`), private `#ephemeral` field, import at top, export from `index.js:18-20`; all HTTP through the one `HttpClient` (`http/HttpClient.js:541-553`). Surface: `configure/reset/delete/push/pop/ack` + `queues()/depth()`.
- **Go / Py / Rust / C++ / Laravel / CLI**: per-feature file + wire test each, following their conflation/kv precedents. `client-rust` gains typed structs in `crates/queen-protocol` (Q7, resolved: yes).
- **Examples**: `examples/35-ephemeral-basics.js`, `examples/36-ephemeral-reqreply.js` (numbered next in line, house rule quoted in PLAN_CONFLATION.md:833-835). The req/reply example is the flagship: same rendezvous, sub-ms reply leg.
- Old-broker behavior: 404 on the family (and 404 `route_blocked` from an old proxy) — SDKs map both to one clear "broker/proxy does not support ephemeral queues (>= 1.1)" error.

### 4.1 Client-side buffering (in scope — rev 1.1)
The existing machinery is reused, not duplicated (M13). One refactor makes it possible: `MessageBuffer`'s drain gains a **sink** — `{path, format(queue, partition, batch) -> body}` — with the durable sink being today's behavior byte-for-byte (its wire tests pin that), and an ephemeral sink posting `/api/v1/ephemeral/push` with the flat `{queue, partition, messages}` body. Buffer addresses are namespaced `eph:<queue>/<partition>` so the two families never share a buffer or a drain.

- **API**: `queen.ephemeral.push(queue, msgs, {buffered: {intervalMillis, messageCount, maxSize}})` — same option names and same semantics as `QueueBuilder.buffer()` (`QueueBuilder.js:149-159`): blocking backpressure at `maxSize` (the awaited `addMessage` contract, `BufferManager.js:40-47`), failed batches back to the front and retried until they land or a flush `deadlineMillis` expires, `queen.close()` drains through the same `flushAllBuffers` deadline path (`Queen.js:611`).
- **Semantics note for the docs**: buffering is a *client-side* latency/efficiency trade, not a durability change — a buffered ephemeral message not yet flushed dies with the client process, which is already inside the class contract. The lossless-until-close retry discipline is kept anyway (consistency with 1.0.6 across the SDK, and a full ring answering 429 drains correctly through it).
- **Cross-SDK**: every SDK got the bounded-buffer discipline in 1.0.6; each ports the sink parametrization the same way (Go/Py/Rust/C++/Laravel), with wire tests proving the durable sink unchanged. JS ships in phase 1, the rest with their §4 ports.

---

## 5. Touchpoints

### 5.1 Proxy and multi-tenancy — the report for the orchestrator (`routes.rs` is orchestrator-owned, M6)
Requested classification (new `Feature::Ephemeral` in `routes.rs:10-19`, parsed default-false from the plan JSONB — `cache.rs:750-765` — so cloud is grant-gated at the proxy too; `state::Features` + `gateway.rs:99-104` arms):

| Method, path | RouteClass | Notes |
|---|---|---|
| POST `/api/v1/ephemeral/push` | `Gated(Ephemeral, Grow)` | Deliberate over-blocking: `Grow` also inherits the retained-storage push-block, which is PG storage, not RAM. Safe direction; a `GrowVolatile` op that checks monthly/message quota but not storage is the refinement, orchestrator's call. Items must flow through `limits.check_msgs` and meter as `OpClass::Push` (Q6, resolved) — do not inherit the Gated-meters-as-Read gap flagged at `gateway.rs:1104-1110`. |
| GET `/api/v1/ephemeral/pop` | `Gated(Ephemeral, Open)` | Extend `is_wait_pop`/`poll_timeout_ms` (`routes.rs:251-262`) so `wait=true` gets the long-poll upstream timeout and `limits.parked_slot` accounting (`limits.rs:277`); meter as `OpClass::Delivery`. |
| POST ack, configure, reset; DELETE queue | `Gated(Ephemeral, Open)` | Write-authz (produce‖consume keys; users not Viewer — `auth.rs:327-341`), never storage-blocked. |
| GET queues, depth | `Gated(Ephemeral, Read)` | Never quota-blocked. |

Tenant flow is untouched and sufficient: proxy strips-then-injects `x-queen-tenant` unconditionally (`gateway.rs:287-300`), broker middleware stamps `Extension<Tenant>` (`tenant.rs:94-123`), and every engine key passes through `tenant_queue_key` — the isolation rules of `handlers/mod.rs:178-192` and the hotlist cross-tenant tests are the model. The cell's `base_url` is one k8s Service with no proxy stickiness (`cache.rs:557-585`, `state.rs:122-124`): requests land on an arbitrary broker of the cell, and §3.6-3.7 is precisely what makes that correct — **no proxy routing changes are needed for multi-broker cells**. Grant provisioning = the orchestrator inserts `queen.ephemeral_quota` rows exactly as it does `kv_quota`.

### 5.2 Not touched
`admission.rs`, `fusion.rs`/`pop_fusion.rs`/`ack_fusion.rs`, `ack_registry.rs`, every log-engine SP, `file_buffer.rs` (ephemeral push never spools; DB-down does not affect it), `hotlist.rs` (ephemeral wakes bypass it by construction, §3.4), `retention.rs`, `stats.rs`, Gate (dogfooding mid-hops is future work, §9), durable handlers in `data.rs`.

### 5.3 Dashboard (webapp) — in scope (rev 1.1)
The webapp is Vue 3 with one data layer (M14). Integration is additive and honest — ephemeral queues are never mixed into durable tables whose columns (pending, retained, DLQ, lag-from-PG) would lie about the class:

- `app/src/api/index.js`: two fetchers over the §3.1 status endpoints (`ephemeralQueues()`, `ephemeralDepth(queue)`), plus `ephemeralReset(queue)` / `ephemeralDelete(queue)` for the actions.
- New view `app/src/views/Ephemeral.vue` + router entry (`app/src/router/`), following the `Queues.vue` + `stores/queuesStore.js` store pattern: columns are the class's truth — depth, bytes, groups (cursor per group), drops (bounds/ttl/retry), declared-vs-implicit badge, owner broker (from the status payload). Actions: reset and delete, each behind an explicit confirm that restates the loss contract.
- `Queues.vue` gets a cross-link ("N ephemeral queues" chip), not merged rows; `QueueHealthGrid.vue` untouched in v1.
- Polling is free by construction: the status endpoints read in-process gauges, zero PG — unlike the durable meter (whose 1 s poll is load-bearing on the DB), the dashboard can poll ephemeral at 1-2 s with no cost anywhere.
- Dark-only styling as the rest of the app; webdoc screenshot pipeline regenerates when the view stabilizes (phase 4).
- Standalone mode (`standalone` flag) serves the view identically — the endpoints are broker-local and need no proxy.

---

## 6. Metrics and logging

- Counters as `AtomicU64` on `Metrics` (`metrics.rs:73-113`, conflation-pair precedent): `eph_pushed/popped/acked`, `eph_dropped_{bounds,ttl,retry}`, `eph_forwarded`, `eph_wipes` (ownership moves observed); gauges `eph_bytes`, `eph_queues`.
- Rates: new fields on the **existing** `"broker rates"` block (`obs.rs:391-441`) and bytes on `"broker sizes"` (`obs.rs:518-540`) + per-tenant top-N (`obs.rs:555-567`). The rule is explicit and binding: no third periodic target (`obs.rs:378-386`), no per-message lines (PLAN_CONFLATION.md:433-437).
- Prometheus: additions in `Metrics::prometheus` (`metrics.rs:1187`).
- The status endpoints (§3.1) read the same gauges — the dashboard never invents semantics: depth = ring length, lag = head−cursor, both labeled ephemeral (§5.3).

---

## 7. Test plan

- **7.1 Server semantics** — `server/tests/ephemeral_semantics.rs` (the `kv_semantics.rs` shape): FIFO per partition; lease expiry redelivery with `attempts`; `retry`/`failed`/`completed`; retryLimit exhaustion counted; autoAck at-most-once; multi-group fan-out on one ring (each group sees everything; groupless queue-mode competes); ttl head-drop; bounds × both policies; cursor skip past evicted ranges; windowBuffer timing; implicit GC; reset/delete; epoch-stale acks answer `stale`. Pure logic (HRW determinism and stability under member add/remove, id mint/parse, budget arithmetic) in `server/src/tests_unit/`.
- **7.2 Mechanical guards** — extend the `kv_handler_isolation.rs:22` pattern: the strings `queen.ephemeral_queues` / `queen.ephemeral_quota` never appear under `server/src/handlers/`; plus a unit asserting every engine map key transits `tenant_queue_key`.
- **7.3 SDK wire suites** — `test-v2/ephemeral-unit/` (JS plan-server): verbs, buffered push draining to the ephemeral sink (batch shape, front-of-buffer retry on failure, flush deadline), and a pin that the **durable** sink's bodies are byte-identical before/after the sink refactor (M13). Then `ephemeral_wire_test.go` and siblings per SDK.
- **7.4 E2E lane** — `test/runners/ephemeral/` cloned from the conflation runner (4 files: alpine+bash+curl+jq Dockerfile, `.dockerignore`, `entrypoint.sh` with `wait-for-broker`, one check script with named scenarios and EXIT-trap cleanup); registered in `ALL_SUITES` (`run.sh:48`) with an `add_job` branch. **Unlike conflation, this lane runs `single`, `ha`, and `tenanted`**: cross-broker is the point. On `ha` (`docker-compose.ha.yml`, queen-a/queen-b full mesh with `QUEEN_A_URL`/`QUEEN_B_URL`): push on A / pop on B (forwarding); two groups popping from different brokers each receive everything (fan-out through the owner); `docker restart queen-b` mid-traffic → rings empty + stale acks + durable queues unaffected; wait-pop on B woken by push on A; reset/delete propagation. On `tenanted`: default-tenant results identical to `single` (the compose header invariant), plus grant-denial 403 `feature_gated` when `require_grant` is forced on.
- **7.5 Durable A/B gate — WAIVED (Alice, 2026-08-21, during implementation).** With separate routes the durable request path has zero edits, so a CPU-per-message A/B measures noise. Replaced by the mechanical equivalent, asserted on the branch diff: `git diff conflation..HEAD` touches none of the §5.2 not-touched files (verified: 0 files). The `kv-timers-gates` harness (M11) remains the tool of choice if a future change ever does edit a shared hot file.
- **7.6 Ephemeral perf snapshot** (ships with the feature, dated campaign folder convention): pop RTT p50/p99 local and forwarded (the hop cost that would justify deferred replication); req/reply e2e vs the 2026-08-20 12 ms baseline; fan-out delivery rate per owner at N groups; buffered vs unbuffered push efficiency; RAM accounting accuracy under churn.
- **7.7 Dashboard** — no automated webapp lane (none exists today); acceptance is manual against the `single` stack plus the screenshot regeneration in phase 4. The view consumes only §3.1 endpoints, so 7.1/7.4 already pin its data contract.

---

## 8. Rollout

- **Versioning**: server minor (1.1.0 — new surface, no wire changes to existing routes); SDK minors; `queen-protocol` minor for the typed structs (Q7).
- **Defaults**: routes always registered (M9); OSS/self-hosted usable out of the box; cloud gated by grant (`require_grant ← tenancy_header`) + proxy `Feature::Ephemeral` default-false — double-gated until the plan says otherwise.
- **Order**: (1) engine + routes + switches/quota rungs + JS SDK (incl. §4.1 buffering + sink refactor) + 7.1/7.2/7.3-js + single-broker lane; (2) mesh HELLO v2 + members accessor + epoch + forwarding + ha/tenanted lanes + 7.5 gate; (3) proxy classification/Feature/metering + grants (orchestrator change) + limits extensions; (4) remaining SDKs + examples + webdoc + dashboard view (§5.3) + screenshots; (5) drain-to-peer on SIGTERM, replicated local reads, consumer-anchored inbox routing, SSE transport — each optional and independently shippable.
- **Compat matrix**: old SDK × new broker — unaffected. New SDK × old broker/proxy — 404, mapped to one clear SDK error (§4). Mixed-version mesh during rolling deploy — old peers lack HELLO v2 fields ⇒ excluded from the ephemeral ring, never forwarded to; old peers must ignore unknown frame types (§11). Rollback — kill switch stops the surface instantly; nothing durable depends on it; the two PG tables are inert.
- **Docs**: `gen-config.mjs` picks knobs automatically; `gen-routes.mjs` / `gen-openapi.mjs` / `gen-proxy-routes.mjs` mirrors + fingerprints updated deliberately; webdoc gets one concept page (the ladder: what survives what, the §1.3 matrix, "treat failover like a Redis restart", groups = semantics as on durable) and per-verb reference; worked examples in `examples/` (§4).

---

## 9. Explicitly out of scope (v1)

- **Replicated local reads** (was "fanout mode") — a placement optimization: every broker holds a copy, pops are local, at the price of per-broker cursors (dupes on reconnect, double-consume for cross-broker groups) and mesh payload traffic needing the M1 bulk-lane treatment (§3.5 keeps the design). Deferred until §7.6 shows the forwarded hop or owner concentration actually hurting.
- **DLQ-to-PG hybrid** — coupling to `log_dlq`'s log-engine row shape is unproven; retry exhaustion drops and counts instead. Revisit with a shape read (§11).
- **Transactions** — the 1.0.5 txn wire is PG-native; faking atomicity in RAM would lie. The verbs simply don't exist in the family.
- **subscriptionMode / replay / traces / encryption at rest** — no history exists; payloads never touch disk (TLS in flight unchanged).
- **Drain-to-peer on SIGTERM** — v1 documents that deploys wipe; graceful handoff is phase 5.
- **Consumer-anchored inbox routing** — v2 latency refinement; rendezvous serves v1 correctly.
- **Gate mid-hop dogfooding** — promising (mid-hops were DB-contention-bound in the 2026-08-20 bench) but a separate plan.
- **SSE/WebSocket subscribe transport** — long-poll works day one; streaming is an efficiency follow-up.

---

## 10. Decisions (resolved 2026-08-21, Alice)

**Q1 — why a configure-time mode instead of the pop's consumer group?** *Resolved: no mode.* The question exposed a conflation in rev 1.0: fan-out vs competing is consumption semantics, and queen already expresses it with groups at pop — the RAM engine follows the durable model exactly (§1.5). What the old "mode" was actually choosing (single-owner vs replicated placement) is not a semantic and moved to §9 as a deferred optimization. v1 is always single-owner; groups on one ring give fan-out with consistent cursors for free.

**Q2 — budget defaults.** *Resolved: global 256 MiB, queue 16 MiB / 10 000 msgs* (~3% of a 2c/8 GB free cell at full burn; env knobs raise it on dedicated cells).

**Q3 — grants table.** *Resolved: separate `queen.ephemeral_quota`*, same provisioning story as `kv_quota`.

**Q4 — reset/delete propagation.** *Resolved: mesh broadcast + config-reconcile backstop* — a lost frame leaves a ghost ring for at most one reconcile interval; implicit rings die of idle GC anyway.

**Q5 — forwarded `wait=true`.** *Resolved: hold open at the owner* for the full timeout — one hop, real parking, proxy parked-slot accounting already per-request; fast-empty would turn remote inboxes into poll loops.

**Q6 — cloud metering.** *Resolved: meter push as `OpClass::Push`, pop as `Delivery` from day one*, explicitly not inheriting the Gated-as-Read gap (`gateway.rs:1104-1110`).

**Q7 — protocol crate.** *Resolved: yes* — typed ephemeral wire structs enter `crates/queen-protocol` for client-rust.

**Q8 — same name on both engines.** *Resolved: allowed silently* — distinct namespaces by construction (`eph:` key half); the only interaction is a harmless cross-wake (M5); the docs state they are unrelated objects.

---

## 11. Implementation-time verifications (small, listed so they are not lost)

1. Exact `AccessLevel` of durable `GET /api/v1/pop*` and `POST /api/v1/ack*` in `auth.rs:364-473` — mirror them (§3.9).
2. `MeshTransport::dispatch` behavior on an unknown frame type (`mesh.rs:495-560`) — must skip, not close the connection; fix if it closes (rolling-deploy requirement, §8).
3. `log_dlq` row shape (`005_log_ack.sql:60`) — feeds the v2 DLQ decision (§9).
4. Queue-name validation rules on the durable configure path — mirror the same charset for ephemeral names.
5. `embedded/boot.rs:390` parity checklist — the embedded API memory rule ("KEEP IN SYNC with main.rs") applies to every §3.2 wiring edit.
6. Where `limits.check_msgs` is invoked in `gateway.rs` for Produce — the smallest edit that also counts ephemeral push items (§5.1).
7. The `MessageBuffer` drain's exact POST site — the seam for the §4.1 sink parameter; the durable-sink byte-identity pin (§7.3) guards the refactor.
8. Durable `__QUEUE_MODE__` sentinel semantics (`data.rs:2762`) — the groupless ephemeral pop must match them observably.
