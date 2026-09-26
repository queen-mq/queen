//! The proxy's composition inside the broker (the single binary,
//! PLAN_SINGLE_BINARY.md W3/W4): the pieces the broker assembles in-process.
//! Spec: PLAN_QUEEN_PROXY_CLOUD.md, PLAN_SINGLE_BINARY.md (repo root).

use std::sync::Arc;

use axum::routing::get;
use axum::Router;

use crate::state::{AppState, St};
use crate::{auth, cache, config, console, limits, meter, oauth, operator, registry, webapp};

/// The proxy's background loops: cache invalidation and key-touch flush, the
/// queue registry's reconciler and persister, the revocation sweep, the usage
/// rollup and the storage-quota pump.
pub fn start_background(st: &St) {
    // A revoked session (the nil cluster on the invalidation feed) empties
    // every node's revocation cache. Weak: the cache owns its hooks.
    {
        let weak = Arc::downgrade(st);
        st.cache.on_invalidate(move |cluster| {
            if cluster.is_nil() {
                if let Some(st) = weak.upgrade() {
                    st.keys.clear_revoked_cache();
                }
            }
        });
    }
    // Subscribe the queue registry to `queen_proxy_inval` BEFORE the listener
    // starts, so no notification can arrive before the hook is in place. The
    // channel is the cell's one "this cluster is not what you think it is"
    // signal and the registry is keyed on a cluster like the caches are: its
    // queue-name set is what `max_queues` counts, and a soft-deleted queue held
    // its plan slot until the next restart while nothing invalidated it.
    st.cache.on_invalidate(st.registry.invalidator());
    st.cache.spawn_listener();
    // Batched api_keys.last_used_at: one statement per interval, off the
    // request path.
    st.cache.spawn_touch_flush();
    st.registry.spawn_reconciler();
    // Queue rows admitted on the data path, coalesced per (cluster, queue) and
    // written once per tick -- never awaited by a push.
    st.registry.spawn_persister();
    // Deny-list GC: drops revoked_tokens rows past their own exp.
    auth::spawn_revocation_sweep(st.clone());
    // Daily usage rollup (usage_minutes -> usage_days) + monthly quota checks.
    meter::spawn_rollup(st.clone());

    // Storage-quota pump: over_storage (registry reconciler, from the broker's
    // retainedBytes) -> limits.set_push_blocked, with release when back under,
    // plus the measured totals the in-flight accounting keys on.
    //
    // This is the proxy's OWN storage refresh cadence, and it is now named and
    // configurable rather than a bare `sleep(10)`. It was worth naming: the
    // 2026-09-03 trial measurement put the total lag from "the broker recomputes"
    // to "the proxy refuses" at up to ~96s, and this tick is one of the two
    // terms in it (QUEEN_PROXY_RECONCILE_MS, default 60s, is the other). The
    // floor of 500ms keeps a misconfigured cell from spinning the loop.
    {
        let st2 = st.clone();
        tokio::spawn(async move {
            let every = std::time::Duration::from_millis(
                config::env_u64("QUEEN_PROXY_STORAGE_REFRESH_MS", 10_000).max(500),
            );
            tracing::info!(
                target: "limits",
                refresh_ms = every.as_millis() as u64,
                "storage-quota pump started"
            );
            let mut tick = tokio::time::interval(every);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            // The first tick of a tokio interval fires immediately; the old
            // `sleep`-first loop waited a full period before its first pass, and
            // starting a storage gate a period late is the wrong direction.
            let mut blocked: std::collections::HashSet<uuid::Uuid> = Default::default();
            loop {
                tick.tick().await;
                // Measured totals first: `publish_retained` resets a cluster's
                // in-flight counter when the figure changes, so publishing
                // before the verdict means the two always describe the same
                // computation.
                for (id, total) in st2.registry.retained_totals() {
                    st2.limits.publish_retained(id, total);
                }
                let now: std::collections::HashSet<uuid::Uuid> =
                    st2.registry.over_storage().into_iter().collect();
                for id in now.difference(&blocked) {
                    tracing::warn!(target: "limits", cluster = %id, kind = "storage", blocked = true, "storage quota exceeded; pushes blocked");
                    st2.limits.set_push_blocked(*id, true);
                }
                for id in blocked.difference(&now) {
                    tracing::info!(target: "limits", cluster = %id, kind = "storage", blocked = false, "storage back under quota; pushes unblocked");
                    st2.limits.set_push_blocked(*id, false);
                }
                blocked = now;
            }
        });
    }
}

/// The proxy's HTTP surface: identity (`/auth`, JWKS), the console and
/// operator APIs, the console SPA, and — as the fallback — the data-plane
/// gateway in front of the broker plus the auth-gated dashboard, all behind
/// the W7 edge (PLAN_SINGLE_BINARY.md): the web routers (OAuth, console,
/// operator) get the cookie-plane rules — web rate limit, CORS, CSRF, small
/// body cap — and the whole surface the data-plane ones — per-IP limit, head
/// limits, timeouts, security headers.
pub fn router(st: St, edge: &crate::harden::Edge) -> Router {
    let app = Router::new()
        .route("/healthz", get(healthz))
        .route("/.well-known/jwks.json", get(jwks))
        .nest("/auth", edge.web_plane(oauth::router()))
        .nest("/api/console", edge.web_plane(console::router()))
        .nest("/api/operator", edge.web_plane(operator::router()))
        .nest("/api/cp", crate::cp::router())
        // three routes, not a bare wildcard: /console/*path alone does not
        // match "/console/" (empty remainder) under axum 0.7's matchit
        .route("/console", get(console::spa))
        .route("/console/", get(console::spa))
        .route("/console/*path", get(console::spa))
        // Everything else: broker-bound paths to the data-plane pipeline, the
        // rest to the auth-gated dashboard at `/`. The fallback used to be
        // `gateway::handle` alone, which sent unknown paths upstream and let
        // the BROKER's own embedded webapp answer — unauthenticated and
        // tenant-unaware. See webapp.rs.
        .fallback(webapp::route_fallback)
        .with_state(st);
    edge.data_plane(app)
}

/// What the broker hands the proxy when it runs it in-process
/// (PLAN_SINGLE_BINARY.md W3/W4).
pub struct Embedded {
    /// The broker's replicated KV (the proxy's state, system tenant).
    pub kv: std::sync::Arc<dyn crate::store::KvBackend>,
    /// The broker router the data plane relays to: tenancy on, reachable
    /// only through this call.
    pub broker: Router,
    /// This node's stable label (its per-node usage rows): the broker passes
    /// its raft node's name.
    pub node: String,
}

/// The single binary: the proxy's state over the broker's KV, relaying
/// in-process to the broker router, its background loops started. The caller
/// serves the returned router (it replaces the broker's own router on the
/// public port). Configured from the `QUEEN_PROXY_*` environment.
pub fn build_embedded(e: Embedded) -> Result<(St, Router), String> {
    let cfg = config::Config::load();
    let store = crate::store::Store::Kv(e.kv);
    let upstream = crate::upstream::Upstream::InProcess(e.broker);
    let cache = cache::ClusterCache::new(&cfg, store.clone());
    let limits = limits::Limits::new(&cfg);
    let meter = Arc::new(meter::Meter::new(&cfg));
    // Each node writes its own usage rows (store/usage.rs); readers sum nodes.
    meter.spawn_flush(&store, &e.node);
    // The reconciler reads the queue inventory through the in-process router.
    let registry = registry::Registry::new(store.clone()).with_inventory(upstream.clone());
    let keys = auth::Keys::from_config(&cfg);
    let boot = config::jwt_boot(config::JwtMaterial {
        // The replicated KV is the persistence a console session needs.
        has_users: true,
        ed_private: cfg.jwt_ed25519_pem.as_deref().is_some_and(|s| !s.trim().is_empty()),
        ed_public: config::jwt_ed25519_pub_pem().is_some(),
        hs_secret: cfg.jwt_hs_secret.as_deref().is_some_and(|s| !s.trim().is_empty()),
        can_mint: keys.can_mint(),
        can_verify: keys.can_verify(),
    });
    if let Some(w) = &boot.warn {
        tracing::warn!(target: "auth", "{w}");
    }
    if let Some(e) = &boot.fatal {
        // In-process there is no "refuse to boot": the broker keeps serving
        // its own surfaces and the console answers what it can.
        tracing::error!(target: "auth", mode = boot.mode.as_str(), "{e}");
    }
    let st: St = Arc::new(AppState {
        cfg,
        store,
        upstream,
        cache,
        limits,
        meter,
        registry,
        keys,
    });
    start_background(&st);
    // First boot: plans, this cell, the layout version (store/seed.rs). The
    // KV answers once a leader is elected; retry until then.
    {
        let st = st.clone();
        tokio::spawn(async move {
            let mut wait = std::time::Duration::from_millis(200);
            loop {
                match seed_embedded(&st).await {
                    Ok(()) => break,
                    Err(e) => {
                        tracing::debug!(target: "proxy", error = %e, "proxy seed: KV not ready");
                        tokio::time::sleep(wait).await;
                        wait = (wait * 2).min(std::time::Duration::from_secs(5));
                    }
                }
            }
        });
    }
    // W7: the single binary is internet-facing; its whole surface goes
    // through the edge (harden.rs). A bad edge setting refuses the boot.
    let edge = crate::harden::Edge::from_env().map_err(|e| format!("edge settings: {e}"))?;
    tracing::info!(target: "proxy", "{}", edge.describe());
    let app = router(st.clone(), &edge);
    tracing::info!(
        target: "proxy",
        node = %e.node,
        enforce = st.cfg.enforce,
        shared_hosts = ?st.cfg.shared_hosts,
        "queen-proxy up (embedded in the broker, state: replicated KV)"
    );
    Ok((st, app))
}

/// First boot of a single-binary node, idempotent and safe on every node at
/// once: the layout version, the plan catalog, this cell, and — when
/// `QUEEN_PROXY_BOOTSTRAP_TENANT` is set — one tenant with its cluster, admin
/// and (with `QUEEN_PROXY_BOOTSTRAP_API_KEY`) a known full-scope API key:
///
/// | variable | meaning |
/// |---|---|
/// | `QUEEN_PROXY_BOOTSTRAP_TENANT` | tenant slug (cluster slug = the same) |
/// | `QUEEN_PROXY_BOOTSTRAP_EMAIL` | admin email (default `admin@localhost`) |
/// | `QUEEN_PROXY_BOOTSTRAP_PASSWORD` | admin password (none = OAuth/key only) |
/// | `QUEEN_PROXY_BOOTSTRAP_PLAN` | plan code (default `dev`) |
/// | `QUEEN_PROXY_BOOTSTRAP_API_KEY` | plaintext key to issue on that cluster |
pub async fn seed_embedded(st: &St) -> Result<(), String> {
    use crate::store::data;
    let kv = st.store.kv().ok_or("no KV store")?;
    crate::store::seed::seed(kv.as_ref()).await.map_err(|e| e.to_string())?;
    data::seed_default_plans(&st.store).await.map_err(|e| e.to_string())?;
    let cell = data::upsert_cell(
        &st.store,
        &data::CellSpec {
            slug: crate::store::seed::SELF_CELL.into(),
            region: "local".into(),
            base_url: "inprocess://self".into(),
            class: "shared".into(),
            capacity_slots: 0,
            cell_secret: None,
        },
    )
    .await
    .map_err(|e| e.to_string())?;
    let env = |k: &str| std::env::var(k).ok().filter(|v| !v.trim().is_empty());
    if let Some(slug) = env("QUEEN_PROXY_BOOTSTRAP_TENANT") {
        let out = data::bootstrap_tenant(
            &st.store,
            &data::Bootstrap {
                tenant_slug: slug.clone(),
                tenant_name: None,
                cluster_slug: slug.clone(),
                plan_code: env("QUEEN_PROXY_BOOTSTRAP_PLAN").unwrap_or_else(|| "dev".into()),
                cell: Some(cell),
                admin_email: env("QUEEN_PROXY_BOOTSTRAP_EMAIL").unwrap_or_else(|| "admin@localhost".into()),
                password: env("QUEEN_PROXY_BOOTSTRAP_PASSWORD"),
                key_name: Some("bootstrap".into()),
            },
        )
        .await
        .map_err(|e| e.to_string())?;
        if let (Some(key), Some(cluster)) = (
            env("QUEEN_PROXY_BOOTSTRAP_API_KEY"),
            out.get("cluster_id")
                .and_then(|v| v.as_str())
                .and_then(|v| uuid::Uuid::parse_str(v).ok()),
        ) {
            let scopes: Vec<String> = ["produce", "consume", "admin", "read"].iter().map(|s| s.to_string()).collect();
            match data::issue_api_key(&st.store, cluster, "bootstrap (env)", &auth::key_hash_hex(&key), &scopes).await {
                Ok(_) => {}
                // Already issued by an earlier boot or another node.
                Err(data::DataError::Conflict(_)) => {}
                Err(e) => return Err(e.to_string()),
            }
        }
        tracing::info!(target: "proxy", tenant = %slug, created = out.get("api_key").is_some_and(|k| !k.is_null()), "bootstrap tenant ready");
    }
    tracing::info!(target: "proxy", "proxy state seeded");
    Ok(())
}

/// Flush the proxy's in-memory usage and queue rows (the broker's shutdown
/// calls this for the embedded proxy).
pub async fn shutdown_drain(st: &St) {
    drain_usage(st).await;
}

/// Flush the metering accumulators and the registry's pending queue rows on
/// the way out. `Meter::drain` spools to disk when the KV will not take the
/// rows, so the bound here is purely about not hanging on a KV without a
/// leader — the spool, not this timeout, is what keeps the usage (recovered
/// by `Meter::spawn_flush` on the next start). A queue row that misses the
/// bound is a restart-safety floor the reconciler rewrites on its next pass.
async fn drain_usage(st: &St) {
    let budget = config::shutdown_drain_budget();
    let started = std::time::Instant::now();
    let drains = async {
        tokio::join!(st.meter.drain(), st.registry.drain());
    };
    match tokio::time::timeout(budget, drains).await {
        Ok(()) => tracing::info!(
            target: "meter",
            ms = started.elapsed().as_millis() as u64,
            "usage and queue rows drained at shutdown"
        ),
        Err(_) => tracing::warn!(
            target: "meter",
            budget_ms = budget.as_millis() as u64,
            "usage drain exceeded its shutdown budget; exiting anyway"
        ),
    }
}

/// Liveness plus the two switches that silently change what this proxy DOES.
///
/// Enforcement and tenant-header injection were previously observable only from
/// the boot log, which survives a restart: a harness grepping the log file for
/// `enforce=true` happily passes against a proxy that has since been replaced
/// by a shadow-mode one, and a limiter test then asserts nothing at all. State
/// a cell can be asked about beats state that has to be inferred.
async fn healthz(axum::extract::State(st): axum::extract::State<St>) -> axum::response::Response {
    let body = serde_json::json!({
        "status": "ok",
        "enforce": st.limits.enforcing(),
        "tenant_header": st.cfg.send_tenant_header,
        // Third switch that silently changes what this proxy DOES: on a shared
        // host the cluster comes from the credential, not from Host. A COUNT,
        // not the list — this endpoint is unauthenticated, and a harness only
        // needs to know whether the feature is on.
        "shared_hosts": st.cfg.shared_hosts.len(),
    });
    let mut resp = axum::response::IntoResponse::into_response(body.to_string());
    resp.headers_mut().insert(
        axum::http::header::CONTENT_TYPE,
        axum::http::HeaderValue::from_static("application/json"),
    );
    resp
}

async fn jwks(axum::extract::State(st): axum::extract::State<St>) -> axum::response::Response {
    let mut resp = axum::response::IntoResponse::into_response(st.keys.jwks_json());
    resp.headers_mut().insert(
        axum::http::header::CONTENT_TYPE,
        axum::http::HeaderValue::from_static("application/json"),
    );
    resp
}
