//! The data-plane pipeline. OWNER: Agent A.
//!
//! Pipeline per request (spec §4/§14 — the order is load-bearing):
//!   1. resolve ClusterCtx from Host (miss -> 421)
//!   2. cluster status gate (Suspended -> 403 suspended; PushBlocked + Produce -> 403 push_blocked)
//!   3. authenticate (auth::authenticate) + authorize (auth::authorize vs classify)
//!   4. limits: check_req; Produce -> buffer body (cap = min(plan, cfg)), count
//!      items + per-item payload caps, registry.admit each (queue,partition),
//!      check_msgs(n); Consume wait=true -> parked_slot RAII guard held across
//!      the upstream await
//!   5. forward: rebuild URI on ctx.cell_base_url, strip hop-by-hop headers,
//!      inject Authorization (ctx.cell_token), X-Queen-Tenant (cfg.send_tenant_header),
//!      X-Queen-Request-Id; long-poll timeout = min(client timeout|30s, cfg max) + margin
//!   6. meter post-response (M1–M6): push -> parse per-item statuses (exclude
//!      error, dedupe duplicate, buffered counts), pop -> delivered count +
//!      debit_deliveries, bytes in/out always
//!   7. shadow mode: when !limits.enforcing(), Deny decisions are logged +
//!      metered as would_block but the request proceeds
//!
//! Skeleton: steps 1, 2, 3, 5 + long-poll timeout — a working authenticated
//! pass-through so the smoke path exists before enforcement lands.

use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::{header, HeaderValue, Uri};
use axum::response::Response;

use crate::errors;
use crate::routes::{classify, is_wait_pop, poll_timeout_ms, RouteClass};
use crate::state::{ClusterStatus, St};

const HOP_BY_HOP: &[&str] = &[
    "connection",
    "proxy-connection",
    "keep-alive",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
];

pub async fn handle(State(st): State<St>, req: Request) -> Response {
    let host = req
        .headers()
        .get(header::HOST)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    let Some(ctx) = st.cache.resolve_host(&host).await else {
        return errors::err_421("no cluster for this host");
    };

    match ctx.status {
        ClusterStatus::Suspended | ClusterStatus::Deleting => {
            return errors::err_403(errors::CODE_SUSPENDED, "cluster suspended");
        }
        _ => {}
    }

    let class = classify(req.method(), req.uri().path());
    if class == RouteClass::Blocked {
        return errors::err_404(errors::CODE_ROUTE_BLOCKED, "not available");
    }
    if let RouteClass::Gated(f) = class {
        let on = match f {
            crate::routes::Feature::Streams => ctx.features.streams,
            crate::routes::Feature::Traces => ctx.features.traces,
        };
        if !on {
            return errors::err_403(errors::CODE_FEATURE_GATED, "not in your plan");
        }
    }
    if ctx.status == ClusterStatus::PushBlocked && class == RouteClass::Produce {
        return errors::err_403(errors::CODE_PUSH_BLOCKED, "pushes blocked (storage quota or billing)");
    }

    let principal = match crate::auth::authenticate(&st, req.headers(), ctx.cluster_id).await {
        Ok(p) => p,
        Err(resp) => return resp,
    };
    if let Err(resp) = crate::auth::authorize(&principal, class) {
        return resp;
    }

    // Agent A: steps 4 (limits/registry/body accounting) and 6 (metering) here.

    // ----- forward -----
    let query = req.uri().query().map(|s| s.to_string());
    let wait_pop = is_wait_pop(req.uri().path(), query.as_deref());
    let timeout_ms = if wait_pop {
        let want = poll_timeout_ms(query.as_deref()).unwrap_or(30_000);
        want.min(st.cfg.longpoll_max_ms) + st.cfg.longpoll_margin_ms
    } else {
        st.cfg.upstream_request_timeout_ms
    };

    let path_q = req
        .uri()
        .path_and_query()
        .map(|pq| pq.as_str().to_string())
        .unwrap_or_else(|| "/".to_string());
    let target: Uri = match format!("{}{}", ctx.cell_base_url.trim_end_matches('/'), path_q).parse()
    {
        Ok(u) => u,
        Err(_) => return errors::err_502("bad upstream uri"),
    };

    let (mut parts, body) = req.into_parts();
    for h in HOP_BY_HOP {
        parts.headers.remove(*h);
    }
    parts.headers.remove(header::HOST);
    parts.headers.remove(header::AUTHORIZATION);
    if let Some(tok) = &ctx.cell_token {
        if let Ok(v) = HeaderValue::from_str(&format!("Bearer {tok}")) {
            parts.headers.insert(header::AUTHORIZATION, v);
        }
    }
    if st.cfg.send_tenant_header {
        if let Ok(v) = HeaderValue::from_str(&ctx.broker_tenant.to_string()) {
            parts.headers.insert(crate::config::TENANT_HEADER, v);
        }
    }
    let rid = crate::obs::request_id();
    if let Ok(v) = HeaderValue::from_str(&rid) {
        parts.headers.insert(crate::config::REQUEST_ID_HEADER, v);
    }
    parts.uri = target;

    let upstream_req = Request::from_parts(parts, body);
    let fut = st.upstream.request(upstream_req);
    let resp = match tokio::time::timeout(std::time::Duration::from_millis(timeout_ms), fut).await
    {
        Err(_) => return errors::err_504("upstream timeout"),
        Ok(Err(e)) => {
            tracing::warn!(cluster = %ctx.slug, rid, error = %e, "upstream error");
            return errors::err_502("upstream unreachable");
        }
        Ok(Ok(r)) => r,
    };

    let (mut rparts, rbody) = resp.into_parts();
    for h in HOP_BY_HOP {
        rparts.headers.remove(*h);
    }
    if let Ok(v) = HeaderValue::from_str(&rid) {
        rparts.headers.insert(crate::config::REQUEST_ID_HEADER, v);
    }
    Response::from_parts(rparts, Body::new(rbody))
}
