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
    /// operator-only broker surfaces — never exposed to tenants
    Blocked,
}

/// Classify a broker-bound request. `path` is the URL path only (no query).
pub fn classify(method: &axum::http::Method, path: &str) -> RouteClass {
    use axum::http::Method;
    let m = method;
    let p = path;

    // --- operator-only, always blocked at the proxy (all cells) ---
    if p.starts_with("/api/v1/migration")
        || p.starts_with("/api/v1/system")
        || p.starts_with("/internal")
        || p == "/api/v1/stats/refresh"
        || p == "/metrics"
        || p == "/metrics/prometheus"
        || p == "/status"
    {
        return RouteClass::Blocked;
    }
    // discovery pop (namespace/task, no queue) — removed from the product surface
    if p == "/api/v1/pop" || p == "/api/v1/pop/" {
        return RouteClass::Blocked;
    }
    // Aggregate endpoints the broker does NOT tenant-scope yet (Track B leftover:
    // stats/syscollect pipeline and queue_lag/parked tables carry no tenant_id).
    // Fail closed until they do; the scoped listings (resources/queues,
    // status/queues*, consumer-groups, dlq, messages) stay open below.
    if p == "/api/v1/status"
        || p == "/api/v1/status/analytics"
        || p == "/api/v1/status/buffers"
        || p.starts_with("/api/v1/analytics")
        || p == "/api/v1/resources/namespaces"
        || p == "/api/v1/resources/tasks"
        || p == "/api/v1/resources/overview"
    {
        return RouteClass::Blocked;
    }

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
        assert_eq!(classify(&Method::GET, "/metrics/prometheus"), RouteClass::Blocked);
        assert_eq!(classify(&Method::POST, "/api/v1/unknown"), RouteClass::Blocked);
        // unscoped aggregates fail closed until the broker scopes them
        assert_eq!(classify(&Method::GET, "/api/v1/status"), RouteClass::Blocked);
        assert_eq!(classify(&Method::GET, "/api/v1/analytics/queue-lag"), RouteClass::Blocked);
        assert_eq!(
            classify(&Method::GET, "/api/v1/resources/namespaces"),
            RouteClass::Blocked
        );
        // while the scoped listings stay open
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

    #[test]
    fn wait_pop() {
        assert!(is_wait_pop("/api/v1/pop/queue/q", Some("wait=true&timeout=5000")));
        assert!(!is_wait_pop("/api/v1/pop/queue/q", Some("timeout=5000")));
        assert!(!is_wait_pop("/api/v1/push", Some("wait=true")));
        assert_eq!(poll_timeout_ms(Some("wait=true&timeout=5000")), Some(5000));
    }
}
