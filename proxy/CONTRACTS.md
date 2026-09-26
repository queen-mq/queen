# queen-proxy — module contracts & agent protocol

Spec of record: `../PLAN_QUEEN_PROXY_CLOUD.md` (rev 1.2). This file governs how
agents work inside this crate. **Read both before writing code.**

## Non-negotiables

1. **File ownership.** You edit ONLY the files assigned to your task. The
   orchestrator owns `app.rs`, `state.rs`, `routes.rs`, `errors.rs`,
   `config.rs`, `CONTRACTS.md`. If your module needs a new AppState field, a
   new config knob, new wiring in app.rs, or a change to the route matrix:
   put the EXACT diff you want in your final report — do not apply it.
2. **The crate must compile when you finish**: `cargo build` in `proxy/`
   green (dev profile). If you must add a dependency, it must be
   rustls/ring-compatible and cmake-free (no reqwest, no openssl, no aws-lc);
   list it prominently in your report.
3. **No commits.** Leave changes in the working tree. `git add` nothing.
4. **The proxy runs inside the broker** (the single binary,
   `QUEEN_PROXY_EMBEDDED=true`): the broker links this crate and hands it
   its replicated KV (`app::build_embedded`). There is no standalone binary
   and no database of its own.
5. **Error contract** is `errors.rs` — reuse those helpers/codes; new codes go
   in your report, not in the file.

## Interfaces (summary — signatures live in the stubs)

- `cache::ClusterCache` — `resolve_host(&str) -> Option<Arc<ClusterCtx>>`,
  `by_key_hash(&str) -> Option<(Arc<ClusterCtx>, Uuid /*key_id*/, Scopes)>`,
  `invalidate(Uuid)`, `spawn_listener()` (polls the KV invalidation feed,
  `store::data::InvalFeed`). Slug = first DNS label of Host. Lookups are
  single-flight per key and stale-while-revalidate: an expired entry inside
  the grace window is served while ONE background refresh runs, so a TTL
  costs no request its latency and no request a herd. `api_keys.last_used_at`
  is batched by `spawn_touch_flush()` (one write per interval).
- `registry::Registry` — `admit(&ClusterCtx, queue, partition) -> Admit`,
  store-free on the request path (O(1) per-queue counts; queue rows coalesced
  per (cluster, queue) and written by `spawn_persister()` as KV batches per
  tick, `drain()` at shutdown); `spawn_reconciler()` (scoped
  broker inventory sync + retained-bytes -> `limits.set_push_blocked`, writes
  only rows whose count changed).
- `limits::Limits` — `check_req`, `check_msgs(n)`, `debit_deliveries(n)`,
  `parked_slot -> ParkedGuard (RAII)`, `enforcing()`. Shadow mode: when
  `!enforcing()`, compute+log the Deny (target `limits`, field `would_block`)
  and allow. Buckets: capacity=burst, refill=sustained (rev 2.3 T4a), sharded
  `Mutex<HashMap<Uuid, Bucket>>` (16 shards), never one global lock.
- `meter::Meter` — `record(Sample)`, `spawn_flush(store, node)`. M1–M6:
  charge from response per-item statuses; `error` never, `duplicate` never;
  exempt 5xx and scope-403s. Flush -> this node's usage rows
  in the KV (`store::usage`), spool via `spool.rs` when the KV is down,
  replayed on the next start.
- `auth` — `authenticate` returns `Principal`; the `authorize` matrix is
  final. API keys: format `qk_<env>_<43 base64url chars>`, stored as sha256
  hex (`key_hash_hex`). JWT: HS256 dev via `QUEEN_PROXY_JWT_SECRET`; cloud
  mint Ed25519 (`QUEEN_PROXY_JWT_ED25519_PEM`, private key only on auth
  host); claims `{sub: user_id, iss, exp, jti, role, cluster?}`;
  `revoked_tokens` deny-list checked on verify.
- `gateway::handle` — pipeline order in the file header is load-bearing;
  ParkedGuard must live across the upstream await; buffered request bodies
  are re-sent verbatim (`Body::from(bytes)`), responses that need metering
  are buffered with a size guard (`min(cfg.max_body_bytes, 64MiB)`).
- State: the broker's replicated KV, under the reserved tenant
  `store::schema::PROXY_TENANT`; the layout (namespaces, keys, indexes) is
  `store/schema.rs`, seeded at boot by `store/seed.rs`. Every read and write
  goes through a repository in `store/` (`data`, `web`, `usage`).
- Control-plane contract (§2 rev 1.2): the writers in `store::data` /
  `store::web` (assign_plan, set_tenant_status, create_tenant,
  create_cluster, create_user, issue_api_key, revoke_api_key,
  record_operation, set_limit_override, revoke_session,
  sweep_revoked_tokens, grant_cluster_role, revoke_cluster_role,
  bootstrap_tenant, emit_outbox, …), reachable over HTTP through `cp.rs`
  (`/api/cp/*`, `QUEEN_PROXY_CP_TOKEN`) — discipline: validate, write,
  append the `operations` row and the cluster's invalidation mark in the
  SAME atomic batch. `bootstrap_tenant` is the one-call onboarding path
  (tenant + cluster + admin user + role + first api key, returning the
  plaintext key once).
- Broker-facing constants: header `x-queen-tenant` (config::TENANT_HEADER),
  default tenant UUID `00000000-0000-0000-0000-000000000001` — must match
  server/ Track B.

## Report format (your final message)

1. Files created/edited, one line each.
2. Wiring requests (exact code) for orchestrator-owned files, if any.
3. New deps, new env knobs, new KV namespaces.
4. How you verified (commands + results — honest numbers).
5. Leftovers / known gaps.
