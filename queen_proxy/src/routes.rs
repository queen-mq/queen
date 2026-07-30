//! Route classification — the executable form of PLAN_QUEEN_PROXY_CLOUD.md §14.
//! Every broker route falls into exactly one class; anything unknown under /api,
//! /streams or /internal is Blocked by default (fail closed), everything else
//! (the SPA fallback) is Read.
//!
//! This file is the enforcement spec. Owned by the orchestrator — agents must
//! not edit it; report desired changes instead.

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Feature {
    Streams,
    Traces,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RouteClass {
    /// push, transaction — counts messages, may implicitly create queues/partitions
    Produce,
    /// pop/ack/lease — parked gauge applies on wait=true pops
    Consume,
    /// configure, deletes, seeks, subscription changes
    QueueAdmin,
    /// listings, status, analytics, dlq/messages reads
    Read,
    /// plan-gated feature surfaces
    Gated(Feature),
    /// Cell-wide surfaces a live OPERATOR principal may open (F3). Not
    /// tenant-scopable, so they are unreachable for every tenant credential —
    /// a non-operator gets the same 404 a `Blocked` route gives, and on a cell
    /// with `QUEEN_PROXY_OPERATOR_ENABLED` off the 404 is returned before
    /// authentication runs at all.
    Operator,
    /// operator-only broker surfaces — never exposed to tenants
    Blocked,
}

/// The EXACT set of otherwise-blocked broker surfaces a live operator may
/// open. Everything else `classify` blocks stays blocked for EVERY principal:
/// `/api/v1/migration/*`, `/internal/*`, `/api/v1/stats/refresh`, the
/// discovery `GET /api/v1/pop`, bare `/metrics`, the broker's own `/status`
/// and the rest of `/api/v1/system/*` are not dashboard data.
fn is_operator_route(p: &str) -> bool {
    matches!(
        p,
        "/api/v1/status"
            | "/api/v1/status/buffers"
            | "/api/v1/analytics/system-metrics"
            | "/api/v1/analytics/worker-metrics"
            | "/api/v1/analytics/postgres-stats"
            // The two maintenance kill switches. GET reads the flag, POST flips
            // it; both halves of both switches are the same operator page.
            //
            // `/maintenance/pop` was blocked here while `/maintenance` was not,
            // which left the console able to SEE pop maintenance — the push
            // endpoint reports `popMaintenanceMode` too, so the banner lit —
            // and unable to turn it off. Same blast radius as the push switch
            // (cell-wide, every tenant), same gate in front of it: a live
            // operator principal AND `QUEEN_PROXY_OPERATOR_ENABLED` on the cell.
            // Splitting them protected nothing and stranded an operator inside
            // a state they could watch but not leave.
            //
            // `/system/shared-state` is still NOT here and stays blocked.
            | "/api/v1/system/maintenance"
            | "/api/v1/system/maintenance/pop"
            | "/metrics/prometheus"
    )
}

/// Classify a broker-bound request. `path` is the URL path only (no query).
pub fn classify(method: &axum::http::Method, path: &str) -> RouteClass {
    use axum::http::Method;
    let m = method;
    let p = path;

    // --- operator-eligible, checked FIRST: the prefix blocks below would
    //     otherwise swallow /api/v1/system/maintenance and /metrics/prometheus.
    //     Reaching one of these still requires a live operator principal (see
    //     RouteClass::Operator); classification alone opens nothing.
    if is_operator_route(p) {
        return RouteClass::Operator;
    }

    // --- operator-only, always blocked at the proxy (all cells) ---
    if p.starts_with("/api/v1/migration")
        || p.starts_with("/api/v1/system")
        || p.starts_with("/internal")
        || p == "/api/v1/stats/refresh"
        || p == "/metrics"
        || p == "/status"
    {
        return RouteClass::Blocked;
    }
    // discovery pop (namespace/task, no queue) — removed from the product surface
    if p == "/api/v1/pop" || p == "/api/v1/pop/" {
        return RouteClass::Blocked;
    }
    // System aggregates are not tenant-scopable by nature (host CPU, PG
    // internals, worker lifetime counters, cell maintenance) — they are the
    // `Operator` set above, reachable by nobody else. Everything queue-shaped
    // (namespaces/tasks/overview, queue-lag/ops/parked, retention,
    // status/analytics) IS tenant-scoped broker-side since Track B2 and falls
    // through to Read below.

    // --- data plane ---
    if p == "/api/v1/push" {
        return RouteClass::Produce;
    }
    if p == "/api/v1/transaction" {
        // mixed push+ack; counted as produce, ack ops ownership-checked broker-side
        return RouteClass::Produce;
    }
    if p.starts_with("/api/v1/pop/queue/") {
        return RouteClass::Consume;
    }
    if p == "/api/v1/ack" || p == "/api/v1/ack/batch" {
        return RouteClass::Consume;
    }
    if p.starts_with("/api/v1/lease/") {
        return RouteClass::Consume;
    }

    // --- queue admin ---
    if p == "/api/v1/configure" {
        return RouteClass::QueueAdmin;
    }
    if p.starts_with("/api/v1/resources/queues/") && *m == Method::DELETE {
        return RouteClass::QueueAdmin;
    }
    if p.starts_with("/api/v1/messages/") && *m == Method::DELETE {
        return RouteClass::QueueAdmin;
    }
    if p.starts_with("/api/v1/consumer-groups") && (*m == Method::DELETE || *m == Method::POST) {
        return RouteClass::QueueAdmin;
    }

    // --- gated features ---
    if p.starts_with("/streams/") {
        return RouteClass::Gated(Feature::Streams);
    }
    if p == "/api/v1/traces" && *m == Method::POST {
        return RouteClass::Gated(Feature::Traces);
    }

    // --- reads ---
    if p.starts_with("/api/v1/resources")
        || p.starts_with("/api/v1/status")
        || p.starts_with("/api/v1/analytics")
        || p.starts_with("/api/v1/consumer-groups")
        || p == "/api/v1/dlq"
        || p.starts_with("/api/v1/messages")
        || p.starts_with("/api/v1/traces")
    {
        return RouteClass::Read;
    }

    // Unknown API-shaped path: fail closed. Anything else is the SPA/static
    // fallback, which is a read surface.
    if p.starts_with("/api/") {
        return RouteClass::Blocked;
    }
    RouteClass::Read
}

/// Does this request hold a parked-consumer slot? (long-poll pop)
pub fn is_wait_pop(path: &str, query: Option<&str>) -> bool {
    path.starts_with("/api/v1/pop/queue/")
        && query
            .map(|q| {
                q.split('&')
                    .any(|kv| matches!(kv.split_once('='), Some(("wait", "true"))))
            })
            .unwrap_or(false)
}

/// Client-requested long-poll timeout in ms, if present.
pub fn poll_timeout_ms(query: Option<&str>) -> Option<u64> {
    let q = query?;
    for kv in q.split('&') {
        if let Some(("timeout", v)) = kv.split_once('=') {
            return v.parse().ok();
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::Method;

    #[test]
    fn classes() {
        assert_eq!(classify(&Method::POST, "/api/v1/push"), RouteClass::Produce);
        assert_eq!(classify(&Method::GET, "/api/v1/pop"), RouteClass::Blocked);
        assert_eq!(
            classify(&Method::GET, "/api/v1/pop/queue/orders"),
            RouteClass::Consume
        );
        assert_eq!(
            classify(&Method::DELETE, "/api/v1/resources/queues/orders"),
            RouteClass::QueueAdmin
        );
        assert_eq!(
            classify(&Method::GET, "/api/v1/resources/queues"),
            RouteClass::Read
        );
        assert_eq!(classify(&Method::POST, "/api/v1/unknown"), RouteClass::Blocked);
        // system aggregates are operator-class: closed to every tenant
        // credential, openable only by a live operator principal
        assert_eq!(classify(&Method::GET, "/api/v1/status"), RouteClass::Operator);
        assert_eq!(
            classify(&Method::GET, "/api/v1/analytics/system-metrics"),
            RouteClass::Operator
        );
        // tenant-scoped aggregates (Track B2) are open reads
        assert_eq!(classify(&Method::GET, "/api/v1/analytics/queue-lag"), RouteClass::Read);
        assert_eq!(
            classify(&Method::GET, "/api/v1/resources/namespaces"),
            RouteClass::Read
        );
        // and the scoped listings stay open
        assert_eq!(classify(&Method::GET, "/api/v1/status/queues"), RouteClass::Read);
        assert_eq!(
            classify(&Method::GET, "/api/v1/status/queues/orders"),
            RouteClass::Read
        );
        assert_eq!(
            classify(&Method::GET, "/streams/v1/queries"),
            RouteClass::Gated(Feature::Streams)
        );
        assert_eq!(classify(&Method::GET, "/"), RouteClass::Read);
    }

    /// The operator subset is a CLOSED list. Anything not on it that used to
    /// be blocked must still be blocked — the whole point of the per-cell flag
    /// is that turning it on widens the surface by exactly these eight paths.
    #[test]
    fn operator_subset_is_exactly_the_agreed_eight() {
        for p in [
            "/api/v1/status",
            "/api/v1/status/buffers",
            "/api/v1/analytics/system-metrics",
            "/api/v1/analytics/worker-metrics",
            "/api/v1/analytics/postgres-stats",
            "/api/v1/system/maintenance",
            "/api/v1/system/maintenance/pop",
            "/metrics/prometheus",
        ] {
            assert_eq!(classify(&Method::GET, p), RouteClass::Operator, "{p}");
        }
        // Both switches' write halves belong to the same operator page.
        for p in ["/api/v1/system/maintenance", "/api/v1/system/maintenance/pop"] {
            assert_eq!(classify(&Method::POST, p), RouteClass::Operator, "POST {p}");
        }
    }

    /// The pop switch is reachable, but only on the same terms as the push one:
    /// an operator on a cell with the flag on. Nothing here says a TENANT may
    /// touch it — that is `auth::authorize`'s job, and `RouteClass::Operator`
    /// is what makes it ask.
    #[test]
    fn pop_maintenance_is_operator_not_open() {
        for m in [Method::GET, Method::POST] {
            assert_eq!(
                classify(&m, "/api/v1/system/maintenance/pop"),
                RouteClass::Operator,
                "{m} pop maintenance"
            );
            assert_ne!(classify(&m, "/api/v1/system/maintenance/pop"), RouteClass::Read);
            assert_ne!(classify(&m, "/api/v1/system/maintenance/pop"), RouteClass::QueueAdmin);
        }
        // Its neighbour did not come along for the ride.
        assert_eq!(
            classify(&Method::GET, "/api/v1/system/shared-state"),
            RouteClass::Blocked
        );
    }

    #[test]
    fn hard_blocked_routes_are_not_operator_openable() {
        for p in [
            "/api/v1/migration/start",
            "/api/v1/migration/status",
            "/internal/api/notify",
            "/internal/api/shared-state/stats",
            "/api/v1/stats/refresh",
            "/api/v1/pop",
            "/api/v1/pop/",
            "/metrics",
            "/status",
            // neighbour of the two allowed /system paths
            "/api/v1/system/shared-state",
        ] {
            assert_eq!(classify(&Method::GET, p), RouteClass::Blocked, "{p}");
        }
    }

    #[test]
    fn wait_pop() {
        assert!(is_wait_pop("/api/v1/pop/queue/q", Some("wait=true&timeout=5000")));
        assert!(!is_wait_pop("/api/v1/pop/queue/q", Some("timeout=5000")));
        assert!(!is_wait_pop("/api/v1/push", Some("wait=true")));
        assert_eq!(poll_timeout_ms(Some("wait=true&timeout=5000")), Some(5000));
    }
}
