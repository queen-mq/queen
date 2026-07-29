# queen-proxy — module contracts & agent protocol

Spec of record: `../PLAN_QUEEN_PROXY_CLOUD.md` (rev 1.2). This file governs how
agents work inside this crate. **Read both before writing code.**

## Non-negotiables

1. **File ownership.** You edit ONLY the files assigned to your task. The
   orchestrator owns `main.rs`, `state.rs`, `routes.rs`, `errors.rs`,
   `config.rs`, `CONTRACTS.md`. If your module needs a new AppState field, a
   new config knob, new wiring in main.rs, or a change to the route matrix:
   put the EXACT diff you want in your final report — do not apply it.
2. **The crate must compile when you finish**: `cargo build` in `queen_proxy/`
   green (dev profile). If you must add a dependency, it must be
   rustls/ring-compatible and cmake-free (no reqwest, no openssl, no aws-lc);
   list it prominently in your report.
3. **No commits.** Leave changes in the working tree. `git add` nothing.
4. **Ports for local testing**: reserved elsewhere: 5432, 5455, 5457, 5460,
   5464, 6632, 6682, 6690, 6702. This crate's dev cell: pxdb :5465, cell PG
   :5466, broker :6710, proxy :6711.
5. **Error contract** is `errors.rs` — reuse those helpers/codes; new codes go
   in your report, not in the file.

## Interfaces (summary — signatures live in the stubs)

- `cache::ClusterCache` — `resolve_host(&str) -> Option<Arc<ClusterCtx>>`,
  `by_key_hash(&str) -> Option<(Arc<ClusterCtx>, Uuid /*key_id*/, Scopes)>`,
  `invalidate(Uuid)`, `spawn_listener()` (pg LISTEN `queen_proxy_inval`,
  payload = cluster_id). Slug = first DNS label of Host.
- `registry::Registry` — `admit(&ClusterCtx, queue, partition) -> Admit`;
  `spawn_reconciler()` (scoped broker inventory sync + retained-bytes -> 
  `limits.set_push_blocked`).
- `limits::Limits` — `check_req`, `check_msgs(n)`, `debit_deliveries(n)`,
  `parked_slot -> ParkedGuard (RAII)`, `enforcing()`. Shadow mode: when
  `!enforcing()`, compute+log the Deny (target `limits`, field `would_block`)
  and allow. Buckets: capacity=burst, refill=sustained (rev 2.3 T4a), sharded
  `Mutex<HashMap<Uuid, Bucket>>` (16 shards), never one global lock.
- `meter::Meter` — `record(Sample)`, `spawn_flush(db)`. M1–M6: charge from
  response per-item statuses; `error` never, `duplicate` never, `buffered`
  yes; exempt 5xx and scope-403s. Flush -> `queen_proxy.usage_minutes`
  (UPSERT add), spool via `spool.rs` when pxdb down, drain on recovery.
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
- DB: all proxy tables in schema `queen_proxy`. Migrations: files in
  `migrations/NNN_name.sql`, registered in `db::migrations()` (include_str!),
  applied in order, recorded in `queen_proxy.schema_migrations`.
- Control-plane contract (§2 rev 1.2): SQL functions in `queen_proxy.*`
  (assign_plan, set_tenant_status, create_tenant, create_cluster,
  create_user, issue_api_key, revoke_api_key, record_operation;
  set_limit_override in 003; revoke_session, sweep_revoked_tokens,
  grant_cluster_role, revoke_cluster_role, bootstrap_tenant,
  rollup_usage_days, cluster_month_msgs, emit_outbox in 004;
  prune_usage_minutes in 005) — SECURITY DEFINER-style discipline: validate,
  write, append `operations` row, `pg_notify('queen_proxy_inval', cluster_id)`.
  `bootstrap_tenant` is the one-call onboarding path (tenant + cluster + admin
  user + role + first api key, returning the plaintext key once).
- Broker-facing constants: header `x-queen-tenant` (config::TENANT_HEADER),
  default tenant UUID `00000000-0000-0000-0000-000000000001` — must match
  server/ Track B.

## Report format (your final message)

1. Files created/edited, one line each.
2. Wiring requests (exact code) for orchestrator-owned files, if any.
3. New deps, new env knobs, new SQL objects.
4. How you verified (commands + results — honest numbers).
5. Leftovers / known gaps.
