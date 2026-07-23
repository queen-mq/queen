# QueenMQ — Proxy-Fold + Multi-Tenancy: Integrated Implementation Plan

**Branch:** `rustserverandstorage` · **Broker:** `server/` · **Status:** master document for review
**Provenance:** synthesized from the multi-tenant feasibility study + proxy inventory, then adversarially audited (blocker-coverage, enable-order, backward-compat, effort). All the audit fixes are folded in and marked ⟶ where they changed the draft.

---

## 0. TL;DR — what the review must decide

1. **Two release trains, not one.** **v1 = Isolation** (identity + tenant isolation, *no quotas*). **v1.1 = Fairness** (the rate limiter). Cutting the limiter out of v1 removes the entire coordinated-client-release risk from the first increment — 429 never arrives without the limiter.
2. **This is a DAG, not a line.** Deleting the Node proxy is *not* a prerequisite for tenancy — only the broker *owning identity* (users table + asymmetric minting) is. Critical path: **P1 → P3a → T2a → T3**. `P4` (proxy decommission) can come after the tenancy code.
3. **Three product decisions for you** (details in §10): (a) is v1 a *fairness guard, not a billing meter* — acceptable? (b) cut Google OAuth to a fast-follow, keeping only local login + your existing IdP-JWKS path in v1? (c) drop JWKS *publishing* from v1 (asymmetric *minting* alone fixes the trust blocker)?

---

## 1. Central thesis (holds up against the code)

Fold the proxy **first**, because the broker owning identity *dissolves* four of the hardest tenancy blockers instead of merely enabling them:

| Fold effect | Blocker dissolved |
|---|---|
| Broker owns `queen.users` → `tid` read from the authenticated user row, never projected from a client bearer | **S1** (tid forgeable) — the single worst finding across all 5 reviews |
| `queen.users` gives `users.tenant_id` a home + a CRUD surface | **G2** (onboarding has no schema column) |
| Internal(no-auth)/external(force-auth) listener split is the tenant-optional mechanism | **B1** (enabling tenancy forces a global 401) |
| External listener is the single req/s choke point | **G5** (proxy/hybrid limiter alternative) |
| Broker mints with an asymmetric key it can publish but sidecars can't sign | **S3** (shared HS256 verify==mint) becomes *satisfiable* |

**But** (⟶ effort audit): "fold first" means *identity* first, not *delete the proxy* first. `queen_proxy` can stay live until after tenancy lands.

---

## 2. Scope & non-goals

### v1 (Isolation) ships
Node proxy self-auth reimplemented in Rust: **local login + session cookie**, internal-JWT **minting** (asymmetric), users → `queen.users`; two-listener broker; `queen.tenants`; server-authoritative `tid`; DB-side quota *columns* (unenforced in v1); **read + write + consumer-group cross-tenant isolation**; tenant/user CRUD; revocation (auth-layer + durable deny-list); per-tenant observability skeleton.

### v1.1 (Fairness) ships
The rate limiter (req/s + msgs/s, default-off), partition cap, and the coordinated js/go/py 429-backoff client release.

### Explicitly OUT (state in release notes)
1. **Generic Traefik ForwardAuth gateway (#30) — deprecated, not ported.** Built for external user *xmlking*, disabled in your own prod/stage helm, Traefik-shaped while you run GKE Gateway API. Point him at `oauth2-proxy`.
2. **Cluster-global exact req/s** — no shared counter substrate; a PG counter reproduces the `Lock:transactionid` stall. v1.1 is **per-instance**.
3. **Total-stored-messages / bytes quotas** — `max_msgs_per_sec` is a **rate**; retention moves `log_start`, so a live-count cap is a moving target.
4. **Per-tenant queue-name namespaces** (`UNIQUE(tenant_id,name)`) — cascades into `partition_lookup` FK, `consumer_watermarks`, every name-resolve SP. v1 keeps global names + first-creator-owns.
5. **Per-tenant signing keys** — v1 trust model: "the broker external listener is the one trusted issuer."
6. **Durable per-tenant billing/audit trail** — v1.1 is a *fairness guard, not a metering-of-record* (buckets reset on restart, per-instance). **Your sign-off required.**
7. **Google OAuth** — recommend fast-follow (P3b), not v1 (⟶ effort audit; see §10-b).
8. **C++ client tenancy** (no bearer field).

---

## 3. Target architecture — THREE router planes

⟶ *backward-compat audit: the draft had only 2 planes and couldn't prove the default deploy is unchanged. There are three.*

**One binary, one shared `Arc<AppState>` (`main.rs:192`), up to three router shapes:**

| Plane | Built when | Routes | Auth |
|---|---|---|---|
| **Legacy/Full** | `QUEEN_EXTERNAL_PORT` **unset** (default) | today's exact router (data + status + streams + consumer-group + `handle_static` SPA fallback), **no** login/OAuth/tenant routes | `auth_middleware` in pass-through (per `JWT_ENABLED`) — **byte-for-byte today** |
| **Internal** | external port set | data API only, no login/SPA/tenancy | none — trusted plane, `TenantCtx(None)` always |
| **External** | external port set | data API + login/OAuth/JWKS/tenant-CRUD + SPA | **force-auth by construction** (see invariant) |

**Force-auth invariant (⟶ enable-order audit, BLOCKER):** the External plane rejects any non-PUBLIC route without a valid token **as a property of the plane**, *independent of `JWT_ENABLED`* (which defaults false, `config.rs:42`). PUBLIC on External = `{login, logout, auth/config, oauth callbacks, JWKS, static}` only. Without this, moving the HTTPRoute onto the broker and scaling the proxy to 0 would expose the entire data API unauthenticated. This is a cutover blocker, gated in §9.

**Request flow (External):**
```
GKE HTTPRoute ─▶ :EXT ─▶ [auth (force)] ─▶ [tenant_resolve?] ─▶ [limiter?] ─▶ handler[isolation]
                              │                  │                   │              │
                         validate JWT/      tid from token      req/s bucket   queue.tenant_id
                         cookie; revoked;   → TenantCtx(slug);   (v1.1,          == tid || NULL
                         deny-list          quota cache          default OFF)    (always when tid present)
```
Auth stays **outermost** (`main.rs:464`). `tenant_resolve` and `limiter` layers are **conditionally wired** (⟶ backward-compat audit): present only on the External plane **and** only when their feature flag is on — never an always-present frame on the default hot path (the B3 rule, applied to *both* layers, not just the limiter).

**Prod topology:** 3 replicas, GKE Gateway API, `sessionAffinity: ClientIP`. HTTPRoute → broker External service; Internal service = ClusterIP-no-route + NetworkPolicy default-deny external ingress (needs Dataplane-V2 enforcement — a hard cutover gate).

---

## 4. Dependency DAG & build order

⟶ *effort audit: the draft's strict serial line overstated the critical path.*

```
        P1 ─────────────┬────────────► T1 (parallel; additive DDL)
   (users schema)       │
        │               └► P3a ──► T2a ──► T3 ──► [v1 SHIP]     ← CRITICAL PATH
        │               (asym mint) (tid auth) (isolation)
        ▼
        P2 ──► P3b (Google OAuth, fast-follow, optional v1)
   (2-listener+login)
        │
        └► P4 (proxy decommission) — after tenancy lands, NOT before

   T2b (tenant/user CRUD)  ── parallel off T2a
   T5 (observability)      ── parallel, lands with/just after T4a
   T7 (client 403+429)     ── ZERO server dep, start day one

   v1.1: T4a → T4b → T6 → [enable via runbook]
```

**Critical path to v1:** `P1 → P3a → T2a → T3`. Everything else parallelizes.

---

## 5. Phased plan

Legend: **Size** S/M/L. **BC** = what stays byte-for-byte unchanged. Every PR keeps the default deploy (external unset, tenancy off, limiter off) unchanged unless stated.

### ══ TRACK P — identity foundation ══

#### PR-P1 — `queen.users` schema move + `user create` subcommand · **M**
- **DDL (`schema.sql`, ~after line 45):** `queen.users` with UUID PK (matches every `queen.*` PK), `username UNIQUE`, nullable `password_hash`, `role` CHECK, `email`, `google_sub`, `auth_provider`, **`tenant_id UUID` (multi-tenant hook)**, timestamps; partial-unique indexes on `email`/`google_sub`.
- **⟶ backward-compat audit (MAJOR):** the legacy-user copy MUST be a plpgsql guard, not a comment — a bare `INSERT … FROM queen_proxy.users` bricks greenfield boot (the whole `schema.sql` batch aborts under the advisory lock, `schema.rs:106`):
  ```sql
  DO $$ BEGIN
    IF to_regclass('queen_proxy.users') IS NOT NULL THEN
      INSERT INTO queen.users (username,password_hash,role,email,google_sub,auth_provider,created_at,updated_at)
      SELECT username,password_hash,role,email,google_sub,auth_provider,created_at,updated_at
      FROM queen_proxy.users ON CONFLICT (username) DO NOTHING;
    END IF;
  END $$;
  ```
- **New:** `bcrypt` crate (verify migrated hashes). `server/src/users.rs` (DB accessors). `queen-seg user create <username> --role <r>` subcommand (port of `create-user.js`).
- **BC:** broker does not yet authenticate on this table; proxy still authoritative. Additive, catalog-only.
- **Exit:** **fresh DB with no `queen_proxy`** boots clean (new gate); DB with `queen_proxy.users` copies rows; `user create` row verifies under bcrypt; `run.js human` 117/117.

#### PR-P2 — two-listener skeleton + local login/session + CSRF · **L**
- **`main.rs`:** extract `build_router(state, auth, plane)` with the **three** plane shapes from §3. Default (external unset) builds **Legacy/Full**. Spawn a 2nd `axum::serve` on `QUEEN_EXTERNAL_PORT` when set.
- **⟶ backward-compat audit:** legacy plane must preserve `tcp_nodelay(true)`, `DefaultBodyLimit::max(64MiB)`, auth-outermost, layer order (`main.rs:465-480`). Add a **route-set diff test** (legacy plane == pre-refactor route list) + a `pushLargePayload` test.
- **New routes (External only):** `POST /api/login`, `/api/logout`, `GET /api/me`, `/api/auth/config`. Session = **stateless signed JWT in an httpOnly cookie**; `queen_proxy.sessions` stays dropped (dead schema). `login.html` via rust-embed.
- **⟶ effort audit (CSRF, MAJOR):** cookie auth adds a CSRF surface bearer-in-header didn't. Scope in: `SameSite=Lax/Strict` + double-submit CSRF token (or strict Origin check) on cookie-authenticated mutating routes; bearer API clients exempt.
- **⟶ effort audit (P2/P3 coupling):** the session cookie needs a signing strategy now — scope **HS256 dev-only cookie signing** into P2 with a note that P3a upgrades the same signer to asymmetric.
- **⟶ coverage audit (B1, MAJOR):** state the **External tokenless policy** explicitly: External is mandatory-auth; the precondition that makes it safe is *the proxy already authenticated all external traffic, so no anonymous external client exists post-fold*. Anonymous in-cluster workloads must target `QUEEN_INTERNAL_PORT`. **Verify on the cluster that nothing hits the broker tokenless before cutover.**
- **New config:** `QUEEN_EXTERNAL_PORT` (unset=legacy), `QUEEN_INTERNAL_PORT` (6632), `SESSION_COOKIE_NAME/TTL`, `COOKIE_DOMAIN/SECURE`.
- **BC:** external unset ⇒ legacy plane ⇒ today. **Document:** enabling external flips `:6632` to the no-auth internal plane (fine for you — the proxy already fronts unauth `:6632`).
- **Exit:** external unset → 117/117 + Go/Py green + route-diff test passes; external set → migrated user logs in, `/api/me` returns their row, mutating route rejects a forged cross-site request.
- **Biggest risk:** sharing the one `Arc<AppState>` across both listeners (fusion shards/notifier/file_buffer are singletons — a second AppState corrupts the waker/drain).

#### PR-P3a — asymmetric internal-JWT minting + key loading · **M**
- `jsonwebtoken::encode`; HS256 for dev, **RS256/EdDSA when a private key is configured** — the verify key can't mint (this is what makes S3 satisfiable). Dual-verifier support (accept **both** the old HS256 secret and the new asymmetric key) for rollover — see §9.
- **New config:** `INTERNAL_JWT_PRIVATE_KEY/_ALGORITHM/_ISSUER/_AUDIENCE/_TTL/_KID`.
- **Exit:** login yields a broker-minted token `auth.validate` (`auth.rs:502`) accepts; a holder of only the public key cannot mint.

#### PR-P3b — Google OAuth/OIDC · **L** · *recommend fast-follow, not v1 (§10-b)*
- Port `google-auth.js` (329 lines: authorize-URL, signed-state CSRF, code-exchange POST, id_token verify against Google's **rotating** JWKS with kid selection, domain+email allowlist, `resolveUser`). **Extend `httpget.rs` to POST** over `tokio-rustls` — **no `reqwest`**.
- **⟶ effort audit:** the real risk is id_token verification against a third-party rotating JWKS, *not* the httpget POST. Keep as its own PR.
- **JWKS publishing** (`/.well-known/jwks.json`): **⟶ effort audit — `jsonwebtoken` cannot serialize a public key to JWK.** Either (a) **cut publishing from v1** (asymmetric *minting* already fixes S3; publishing is only needed if *other* services verify broker tokens without calling it) — **recommended**, or (b) add a JWK-serialization crate (`jsonwebkey`/`rsa`/`spki`) and re-size. Do **not** ship "JWKS publish, no new dep" — it's false.

#### PR-P4 — proxy decommission + helm cutover · **M** · *after tenancy lands*
- Move HTTPRoute → broker External service; Internal → ClusterIP-no-route + NetworkPolicy. Delete `proxy/`. Deprecate #30 → oauth2-proxy note.
- **`DROP SCHEMA queen_proxy CASCADE`** only as a **separate manual post-soak** step, never auto-applied.
- **⟶ enable-order audit (two HARD pre-cutover gates):** (1) `curl` a protected data route on External with no token → **401** (proves force-auth); (2) confirm NetworkPolicy is actually enforced so unauth `:6632` is unreachable externally. A NetworkPolicy failure means exposure has existed since this cutover — treat as immediate-remediation, not a clean stop.
- **Exit:** stage with proxy scaled to 0 for 24h, all client suites green, both gates pass; then delete.

### ══ TRACK T — tenancy ══

#### PR-T1 — tenancy schema (DDL only) · **S** · *parallelizable*
- **`schema.sql`:** `queen.tenants` (id, `slug UNIQUE` = JWT `tid`, display_name, `max_req_per_sec/max_msgs_per_sec/max_partitions INTEGER NULL` = unlimited **never 0**, `partition_count BIGINT` denormalized, `revoked BOOL`, timestamps); `ALTER queen.queues ADD tenant_id UUID`.
- **⟶ B2 (the #1 cold-start footgun):** `ALTER queen.log_queues ADD tenant_id` goes in **procedure `041_log_schema.sql` after the CREATE at `:21-28`** — NOT in `schema.sql` (log_queues doesn't exist at schema.sql time; the ALTER there bricks boot).
- Also add **`queen.revoked_tokens`** (jti/sub + expiry for self-GC) here — the durable deny-list store (⟶ coverage audit, S4).
- No inline FKs (avoid ACCESS EXCLUSIVE on the push provisioning path). Write `tenant_id` to both queue tables at provisioning.
- **BC:** all nullable/`IF NOT EXISTS` = catalog-only (same as `producer_sub`). All-NULL = inert. `cargo build` (include_str!).

#### PR-T2a — server-authoritative `tid` + revocation core · **L** · *critical path*
- **`tid` minted server-side from `users.tenant_id`** (S1) — never copied from an inbound bearer. `Claims.tid: Option<String>` (`auth.rs:47`), config `JWT_TENANT_CLAIM` (default `tid`).
- **`TenantCtx(Option<String>)`** stamped on all auth exit branches. **⟶ backward-compat audit:** the resolve layer is **conditionally wired** — only External plane + `QUEEN_TENANCY_ENABLED` — never an always-present frame on the default path (fold the `None`-stamp into the existing auth frame behind an early return when tenancy off).
- **Revoked + tenant-exists in the auth/resolve layer** (S4), *not* the limiter — fires on read+write regardless of the limiter flag. Fail-closed: `tid` not matching a non-revoked `tenants` row → 403.
- **⟶ coverage audit (S4, durable deny-list):** `queen.revoked_tokens` loaded into an in-memory cache by `reconcile.rs`, admin route `POST /api/v1/tokens/:jti/revoke`, wired into the mandatory reconcile clear + optional `T_TENANT_CONFIG` UDP so containment latency = `cache_refresh_ms`, not "until restart."
- **⟶ coverage audit (S3, boot assertion):** if `QUEEN_TENANCY_ENABLED=true` AND verification is symmetric HS*, **fail-closed at startup** (turns the runbook precondition into an enforced invariant).
- `tenant_quota_for(slug)` cache — lazy-fetch shape of `lease_cache` **but with real invalidation** (add to `reconcile.rs:55` `clear()` — **mandatory, D5** — + optional UDP). Document revocation latency.
- **Layer-state note:** auth middleware has no pool (`auth.rs:474`); run a **second `from_fn_with_state(AppState, tenant_resolve)`** layer inside auth rather than widening `Authenticator`.
- **Exit:** bound user's token carries correct `tid`; forged inbound `tid` ignored; `revoked=true` 403s on read+write within one reconcile interval; deny-listed jti blocked; tenancy-on + HS256 → boot refuses.

#### PR-T2b — tenant/user CRUD · **M** · *parallel off T2a*
- Admin-only External routes: `GET/POST/PATCH /api/v1/tenants`, `/tenants/:slug/revoke`, `PATCH /api/v1/users/:id/tenant`. (The "how does a row get into `tenants`" answer the design lacked — G2.)

#### PR-T3 — cross-tenant isolation + queue↔tenant binding · **L (heavy)** · *critical path*
Enforced whenever `tid` present — **not gated on the limiter** (isolation is safety).
- **queue→tenant cache** (immutable-once-owned, like `partition_queue`); first-miss = one DB read (budgeted); wired into `reconcile.rs:55` + optional UDP so re-parenting/offboarding works (B5).
- **Write reject BEFORE `fusion.submit`** (M1 isolation half): resolve each batch queue's owner via cache; reject cross-tenant items 403 **at `data.rs` pre-`:246`** — never the post-commit split at `:249-259` (at `:248` the write is already durable = leak-then-403).
- **First-creator-owns** (S2 write): second tenant on an existing name → 403, never the silent `ON CONFLICT (name) DO NOTHING` merge (`042:96,109`).
- **⟶ coverage audit (S2 read, MAJOR — the surface is bigger than "a check"):** the ownership filter (`queue.tenant_id == tid || NULL`) must cover the **complete authenticated read surface**, not just message reads: `pop, ack, messages, dlq, traces` **and** `/api/v1/resources`, `analytics`, `status`, `migration` read handlers, and consumer-group list/subscription/seek (`main.rs:378-408`). For discovery/list endpoints, **default-deny/empty rather than 403** so a tenant can't even confirm another tenant's queue names exist. **Enumerate every site as a checklist; add a test asserting cross-tenant empty/403 at *each* route** so a missed site fails CI (⟶ effort audit: ~8-10 handler sites, each a chance to leak).
- **Consumer-group ownership** (S5): first-creator-owns + ownership check on group ops; per-tenant group *namespaces* OUT (stated).
- **BC:** all checks `if let Some(tid)` over `None` when tenancy off → skipped; cache never populated.
- **Biggest risk:** the pre-submit lookup on the scrutinized push path — mitigated by immutable-once-owned cache (miss only first push/queue) + `Some(tid)` gating.

#### PR-T5 — observability + semantic honesty · **M** · *lands with T4a*
- **Per-tenant Prometheus families** alongside per-queue (`metrics.rs:387`): `queen_tenant_req_throttled_total`, `msgs_charged_total`, `tokens_remaining`, `partition_count`. **Cannot enable a limiter you can't watch** (G1) — enable prerequisite. **⟶ enable-order audit:** fold the throttle/tokens-remaining counter into **T4a** so the limiter is never observability-blind and T4a's own saturation exit test has a counter to read.
- **Semantic docs (D1/D4):** per-instance, effective **1×Q..N×Q** under ClientIP pinning — **not** "global deterministic N×"; buckets reset on restart (crash-loop = bypass) → soft fairness guard. Surface replica count.
- **Cardinality guard** (risk): bound per-tenant families (cap tenant count / aggregate) so scrape doesn't blow up.
- **Optional G3 escape hatch:** a `queen.tenant_usage_metrics` flush (analog of `queue_lag_metrics`) only if you want a durable trail.

### ══ v1.1 — the rate limiter ══

#### PR-T4a — limiter substrate (req/s) · **L** · *default-OFF*
- **NOT one global `Mutex<HashMap>`** (D2 — the "lease_cache shape" analogy is false; a bucket mutates every request). Use an **N-way sharded** `Mutex<HashMap<tid, TokenBucket>>` by `hash(tid)%shards`, or a CAS `AtomicU64`-packed bucket; `Instant`-refill; **burst-cap independent of elapsed** (a scheduler stall must not hand back a full burst). No new dep (`arc-swap` optional).
- **Long-poll exemption** (D3): parked pops park 30s and book once at completion (`data.rs:499-583`) — entry-metering counts open connections and 429s a normal 60-worker pool against 50/s. Exempt; consume-side quota rides the msgs meter on the delivered count.
- **Failure-billing** (M5): charge req/s **after authz**, exempt 5xx (don't amplify outages), cross-tenant-403, **and 4xx body-validation failures** (⟶ coverage audit — charge only requests that reach the handler AND pass validation).
- **Conditional layer** (B3): `.layer()` only when `QUEEN_RATELIMIT_ENABLED=true`.
- Includes the throttle/tokens-remaining metric (moved up from T5).

#### PR-T4b — msgs-charge correctness · **L**
- **Admission gate BEFORE `fusion.submit`** (M1 rate half): per-`(queue→tenant)` counts pre-submit; overrun → whole push 429.
- **Hard `QUEEN_MAX_BATCH_ITEMS`** (M2): oversized batch rejected up front (else one client-sized batch commits fully; only the next request pays).
- **Dedup-aware charge** (M3): charge items where `dup_of.is_none() && status != dedup-sentinel` (`data.rs:276-281`), not `parsed.items.len()` — idempotent retries must not double-charge.
- **Definitive-success charge** (M4): exclude `error`/`buffered` items; **the file-buffer drain replay is the single charge site** — and **⟶ coverage audit: the spool record MUST persist the resolved `tid`** (else buffered items evade charge — a quota-evasion path — or misattribute). `handle_transaction` charges from the SP committed count, skipping `txn_fail_body` early-returns (`data.rs:1938`).
- **Explicit billable set** (M6): `{push, pop-on-delivery, transaction, streams-write, configure}`; do NOT reuse `route_access_level`'s `ReadWrite` catch-all; unknown routes non-billable.

#### PR-T6 — partition cap · **L** *(⟶ effort audit: L not M — 5 SP sites)*
- Enforce `max_partitions` at the **four** creation sites (`042:110`, `042:275`, `044:769`, `046:251`) + the **fifth** site, the **retention-GC decrement in `048`**.
- **Denormalized `partition_count` in v1** (D6) — bounded counter read, not `COUNT(*)` over 10-20k rows. **Per-tenant `pg_advisory_xact_lock`** guards only the compare-and-increment (short section), **not** held to COMMIT (that's the O(n²) fan-out cliff).
- **⟶ both audits: decide the reconciliation policy in v1, don't defer:** decrement `partition_count` in the same txn as partition/queue drop + retention GC, or a periodic authoritative recount. Don't ship an undecided counter behind an enforcement gate.
- **⟶ backward-compat audit:** guard the `queen.tenants` join with `tenant_id IS NOT NULL` so the **untenanted first-contact provisioning path** (the default) never touches `queen.tenants` (empty by default). Add an untenanted first-contact push to exit criteria.

#### PR-T7 — client 403+429 resilience + saturation validation · **M** · *start day one*
- **⟶ enable-order audit (MAJOR — not just 429):** enabling *tenancy* (before the limiter) introduces NEW **403s** (cross-tenant, first-creator, revoked). The clients that hot-loop/die on 429 (Go `consumer_manager.go:197` no-delay continue; JS `HttpClient.js:178` throws→worker death) were never checked against 403. **js/go/py cores must tolerate BOTH 403 and 429** — surface/back off, not hot-loop — and this must ship **before tenancy-enable**, not only before limiter-enable.
- Ship 429 + `Retry-After` backoff in the same release; mirror the streams-runtime clients that already retry 408/429.
- **Test gates (G4):** default-off parity 117/117 + Go/Py; tid provenance; cross-tenant reject at **every** enumerated read+write+group route; 429/quota/max-batch/dedup; revoked propagation; **one limiter-ON saturation run** (the enabled path is otherwise never load-tested).

---

## 6. DDL placement (get this exactly right)

| Object | File | Why |
|---|---|---|
| `queen.users` (+`tenant_id`), copy DO-block | `schema.sql` ~L45 | applied first; DO-block guards greenfield |
| `queen.tenants` (+`partition_count`), `queen.revoked_tokens` | `schema.sql` | independent tables |
| `ALTER queen.queues ADD tenant_id` | `schema.sql` | `queues` created in `schema.sql:22` |
| **`ALTER queen.log_queues ADD tenant_id`** | **`041_log_schema.sql` after CREATE `:21-28`** | **B2 — log_queues absent at schema.sql time; ALTER there bricks cold-start** |
| Partition-cap + `partition_count` bump/decrement | `042`, `044`, `046`, **`048` (GC)** | the five in-txn sites |
| `DROP SCHEMA queen_proxy` | **manual, post-P4 soak** | preserve rollback surface |

---

## 7. Blocker → step traceability (all 25 addressed)

| ID | Addressed by | ID | Addressed by |
|---|---|---|---|
| **S1** tid forgeable | P1/P3a + T2a (server-mint) | **D1** false determinism | T5 (docs) |
| **S2** read isolation | T3 (**full read surface**, default-empty) | **D2** Mutex contention | T4a (sharded/atomic) |
| **S3** verify==mint | P3a (asym) + **T2a boot-assert** + §9 rollover | **D3** long-poll meter | T4a (exempt) |
| **S4** revoked layer | T2a (auth layer + **durable `revoked_tokens`**) | **D4** restart reset | T5 (soft guard, documented) |
| **S5** group isolation | T3 (group ownership) | **D5** revocation prop | T2a (reconcile clear + UDP) |
| **M1** post-commit charge | T3+T4b (pre-`fusion.submit`) | **D6** partition O(n²) | T6 (denorm count, short lock) |
| **M2** oversized batch | T4b (max-batch + admission) | **B1** forces 401 | P2 (listener split + tokenless policy) |
| **M3** dedup double-charge | T4b (`dup_of` aware) | **B2** DDL order | T1 (log_queues in 041) |
| **M4** rolled-back billed | T4b (success-only + **tid in spool**) | **B3** always-on frame | T4a **+ T2a** (both conditional) |
| **M5** failed-req billed | T4a (post-authz, exempt 5xx/4xx/403) | **B4** clients | T7 (**403+429**, same release) |
| **M6** implicit routes | T4b (explicit set) | **B5** immutable binding | T3 (cache invalidation) |
| **G1** observability | T5/T4a (per-tenant metrics) | **G2** onboarding | P1 + T2b |
| **G3** audit trail | SCOPED OUT (+ optional flush) | **G4** tests | T7 + per-PR exits |
| **G5** hybrid limiter | architecture (external = choke point) | | |

---

## 8. Config, dependencies, rollback

**New env — Track P:** `QUEEN_EXTERNAL_PORT` (unset=legacy), `QUEEN_INTERNAL_PORT`, `SESSION_COOKIE_NAME/TTL`, `COOKIE_DOMAIN/SECURE`, `INTERNAL_JWT_PRIVATE_KEY/_ALGORITHM/_ISSUER/_AUDIENCE/_TTL/_KID`, (P3b) `GOOGLE_*`.
**New env — Track T:** `QUEEN_TENANCY_ENABLED` (false), `QUEEN_RATELIMIT_ENABLED` (false), `JWT_TENANT_CLAIM` (tid), `QUEEN_RATELIMIT_SHARDS/_BURST`, `QUEEN_MAX_BATCH_ITEMS`.

**New dependencies:** `bcrypt` (verify migrated hashes). **⟶ effort audit:** *if* JWKS publishing is kept (P3b-b), a JWK-serialization crate is also needed — otherwise `bcrypt` is the only one. Minting/verify reuse `jsonwebtoken`; OAuth POST reuses `tokio-rustls` via `httpget` (no `reqwest`); token bucket + sharding are in-house std.

**Rollback:** P1 additive; P2 `QUEEN_EXTERNAL_PORT` unset → legacy; P3a/b external-only; P4 keep proxy scalable-up until 24h soak passes, `DROP SCHEMA` deferred; T1 all-NULL inert; T2/T3 `QUEEN_TENANCY_ENABLED=false`; T4 `QUEEN_RATELIMIT_ENABLED=false` (layer un-wired); T6 only active for capped tenants.

---

## 9. Enable-order runbook (turning tenancy + limiter ON in prod)

⟶ *enable-order audit rewrote this — it's a sequence of gates, and several are key/rollover flows, not flips.*

**Cutover gates (at P4, before the proxy is scaled down):**
- **G-a Force-auth:** `curl` a protected data route on External with no token → **401**.
- **G-b NetworkPolicy:** unauth `:6632` unreachable externally (Dataplane-V2 enforcing).

**Asymmetric key rollover (satisfies S3 — a rollover, not a flip):**
1. Deploy P3a with **dual verification** (accept both the old HS256 secret and the new asymmetric key).
2. (If publishing) publish JWKS; confirm external verifiers pick up the new key.
3. Switch **minting** to asymmetric.
4. Drain old HS256 tokens for **≥1 `INTERNAL_JWT_TTL`**.
5. Remove HS256 verification. **S3 is satisfied only now.**

**Tenancy enable (isolation, limiter still off):**
6. Deploy clients tolerating **both 403 and 429** (T7). New 403s arrive *here*, at tenancy-enable — not at limiter-enable.
7. Set `QUEEN_TENANCY_ENABLED=true` **while all queues are still `tenant_id` NULL** (every token resolves untenanted → safe).
8. Bind users (`PATCH /users/:id/tenant`), quotas NULL (unlimited).
9. **Force token refresh or wait ≥1 `INTERNAL_JWT_TTL`** so all live tokens carry the correct `tid` — **before** any tenant creates an owned queue. (Otherwise a tenant's own pre-binding tokens carry `tid=None` and T3 **locks them out of their own new queues** with 403.) Keep TTL short during rollout.
10. Only now allow tenants to create owned queues. Soak isolation ≥1 window; confirm revoked kill-switch fires within `cache_refresh_ms`; confirm no legitimate cross-tenant 403 noise.

**Limiter enable (v1.1):**
11. Confirm `queen_tenant_*` metrics scraping (G1) — don't enable blind.
12. Set per-tenant quotas as **per-instance** numbers (doc: effective ≈ Q × instances-touched).
13. Enable limiter on **one canary replica**; watch `req_throttled_total`; confirm long-poll pools not throttled, no retry storm.
14. Run the **saturation rig limiter-ON** (D2); confirm no in-process bucket-lock contention for the biggest tenant.
15. Roll to all replicas; keep the per-jti deny-list ready.

**Rollback at any step:** `QUEEN_RATELIMIT_ENABLED=false` → `QUEEN_TENANCY_ENABLED=false` → quotas NULL. All non-destructive.

---

## 10. Decisions for you before implementation

**(a) Is v1.1 a fairness guard, not a billing meter — acceptable?** Buckets reset on restart, quota is per-instance-per-connection (1×Q..N×Q), no durable audit trail (G3 out). If you need billing-grade metering, that's a different, larger design (shared counter substrate).

**(b) Cut Google OAuth (P3b) from v1?** Recommended: keep local login (P2) + your existing external-IdP-JWKS passthrough (`auth.rs` + `JWT_JWKS_URL`) in v1; add Google OAuth as a fast-follow. It's the single largest, highest-risk Node file (rotating third-party JWKS). **Load-bearing question:** do any humans log into the dashboard via Google *today* such that a gap hurts?

**(c) Drop JWKS *publishing* from v1?** Asymmetric *minting* alone fixes S3; publishing is only needed if *other* services verify broker tokens without calling it. Dropping it avoids a PEM→JWK dependency. Keep only if you have downstream verifiers.

**(d) Effort splits confirmed?** The audit split P3→P3a/P3b, T2→T2a/T2b, T4→T4a/T4b and re-sized T6 to L. That's ~13-14 PRs total; v1 (Isolation) is P1, P2, P3a, T1, T2a, T2b, T3, T5-lite ≈ 8 PRs, ~3 of them L.
