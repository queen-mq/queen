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
    /// PLAN_KV_TIMERS.md §9.8 P1. `plans.features` is deliberately open JSONB,
    /// so gating these is a data change, not a migration — and until a plan
    /// says otherwise the key is missing, which `parse_features` reads as
    /// false. A cell that has never heard of KV therefore denies it.
    Kv,
    Timers,
    /// EPHEMERAL_QUEUES.md §5.1. Same posture as `Kv`/`Timers` and for the
    /// same reason: the plan key is `ephemeral`, a missing key is false
    /// (`parse_features`), so a cell whose plan has never heard of the RAM
    /// class denies it. The broker's own grant row is the second lock — the
    /// family is deliberately double-gated (§8).
    Ephemeral,
}

/// Which half of PLAN_KV_TIMERS.md §9.5 a gated request sits on. The plan
/// gate (`Feature`) says whether the tenant may touch the family at all; this
/// says what happens to the request when the tenant is over a quota, and the
/// two decisions are genuinely independent.
///
/// The trap this exists to avoid (§9.6): `POST /api/v1/timers` carries
/// `cancel` in the SAME array as `schedule`. Classifying the family as a
/// Produce variant — which is what makes the storage/monthly blocks apply
/// automatically — would 403 the cancels too, while nothing stops an
/// already-scheduled fire on its own. The tenant would keep producing
/// messages it has lost the ability to stop, until the horizon or an operator.
/// The block would produce the opposite of its purpose.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GatedOp {
    /// GET. Read-level authorization, never quota-blocked.
    Read,
    /// Grows stored state (PUT). Write-level authorization, blocked exactly
    /// like `Produce` — this is the half a storage quota is FOR.
    Grow,
    /// Write-level authorization and never quota-blocked. Two populations:
    /// the DELETE/cancel paths, which are how a full tenant gets back under
    /// its quota and how a firing timer gets stopped (§9.5, §9.6); and the
    /// pre-existing gated surfaces (streams, traces), which have never been
    /// on the storage gate and must not silently join it here.
    Open,
    /// One array, both halves: `POST /api/v1/kv` and `POST /api/v1/timers`.
    /// Blocked only when the body really contains a growing op, and then the
    /// WHOLE batch is refused with a named reason rather than half of it
    /// silently dropped (§9.6).
    Mixed,
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
    /// plan-gated feature surfaces. The `GatedOp` half is the quota decision
    /// (§9.5/§9.6), NOT a second feature flag.
    Gated(Feature, GatedOp),
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
    // Streams and traces predate the GatedOp split and are `Open`: they have
    // never been on the storage gate, and giving them one here would be a
    // silent behaviour change smuggled in with an unrelated feature.
    if p.starts_with("/streams/") {
        return RouteClass::Gated(Feature::Streams, GatedOp::Open);
    }
    if p == "/api/v1/traces" && *m == Method::POST {
        return RouteClass::Gated(Feature::Traces, GatedOp::Open);
    }

    // --- kv (PLAN_KV_TIMERS.md §8.1 routes, §9.5 quota rule) ---
    // The batch endpoint is the COMPLETE surface and the only one that accepts
    // `getPrefix` and `incr`. Any other method on it is a shape the broker does
    // not register, so it stays fail-closed here rather than travelling to a
    // 405. Same reasoning for the `*key` routes: exactly three methods exist.
    if p == "/api/v1/kv" || p == "/api/v1/kv/" {
        return if *m == Method::POST {
            RouteClass::Gated(Feature::Kv, GatedOp::Mixed)
        } else {
            // Also the enforcement of §5.5: there is no prefix-in-a-query-string
            // surface, so a GET here is not a thing that can be made to work.
            RouteClass::Blocked
        };
    }
    if p.starts_with("/api/v1/kv/") {
        // `*key` is a catch-all, so everything under here is one key route.
        return match *m {
            Method::GET => RouteClass::Gated(Feature::Kv, GatedOp::Read),
            Method::PUT => RouteClass::Gated(Feature::Kv, GatedOp::Grow),
            // A DELETE is how a tenant at its row cap gets back under it.
            Method::DELETE => RouteClass::Gated(Feature::Kv, GatedOp::Open),
            _ => RouteClass::Blocked,
        };
    }

    // --- timers (§8.1, §9.6) ---
    if p == "/api/v1/timers" || p == "/api/v1/timers/" {
        return if *m == Method::POST {
            RouteClass::Gated(Feature::Timers, GatedOp::Mixed)
        } else {
            RouteClass::Blocked
        };
    }
    if p.starts_with("/api/v1/timers/") {
        return match *m {
            // peek (`/:queue/*timerKey`) and list (`/:queue`)
            Method::GET => RouteClass::Gated(Feature::Timers, GatedOp::Read),
            // THE cancel. Its own route and its own class precisely so that no
            // quota, storage block or billing hold can ever reach it (§9.6).
            Method::DELETE => RouteClass::Gated(Feature::Timers, GatedOp::Open),
            _ => RouteClass::Blocked,
        };
    }

    // --- ephemeral (EPHEMERAL_QUEUES.md §3.1 wire, §5.1 classification) ---
    // The RAM class: one family, one feature flag, method-exact on every path.
    // The broker registers exactly one method per path here, so any other
    // shape fails closed rather than travelling to a 405 — the same rule the
    // kv/timers blocks above state.
    //
    // `push` is the ONLY `Grow` half, and it is deliberate over-blocking:
    // `Grow` also inherits the retained-storage push block, which bounds PG
    // storage rather than the broker RAM this family spends (§5.1). Safe
    // direction, and the refinement (a `GrowVolatile` that sees the message
    // quota but not the storage one) is a later call, not a silent default.
    // Everything else is `Open`: `reset`/`delete` are how a tenant at its cap
    // gets its RAM back, and an ack a quota could refuse would strand its own
    // messages in a lease until expiry. The two status reads are `Read` and
    // are never quota-blocked.
    if p.starts_with("/api/v1/ephemeral/") {
        return match *m {
            Method::POST => match p {
                "/api/v1/ephemeral/push" => RouteClass::Gated(Feature::Ephemeral, GatedOp::Grow),
                "/api/v1/ephemeral/ack"
                | "/api/v1/ephemeral/configure"
                | "/api/v1/ephemeral/reset" => RouteClass::Gated(Feature::Ephemeral, GatedOp::Open),
                _ => RouteClass::Blocked,
            },
            Method::GET => match p {
                "/api/v1/ephemeral/pop" => RouteClass::Gated(Feature::Ephemeral, GatedOp::Open),
                "/api/v1/ephemeral/queues" => RouteClass::Gated(Feature::Ephemeral, GatedOp::Read),
                // `/queues/:queue/depth` — one queue segment between two fixed
                // ones. `:queue` is not a catch-all, so the tail is checked
                // rather than assumed: `/queues/orders` alone is not a route.
                _ if p.starts_with("/api/v1/ephemeral/queues/") && p.ends_with("/depth") => {
                    RouteClass::Gated(Feature::Ephemeral, GatedOp::Read)
                }
                _ => RouteClass::Blocked,
            },
            // `DELETE /queue/:queue`. Prefix-matched like its durable sibling
            // `/api/v1/resources/queues/`, and the `queues` listing above is a
            // different path, not a longer one under this prefix.
            Method::DELETE if p.starts_with("/api/v1/ephemeral/queue/") => {
                RouteClass::Gated(Feature::Ephemeral, GatedOp::Open)
            }
            _ => RouteClass::Blocked,
        };
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
///
/// Both pop families park the same way and cost the same thing: a connection
/// held open at the cell for the client's timeout. The ephemeral one parks on
/// an in-RAM gate rather than a DB re-query (EPHEMERAL_QUEUES.md §3.4), which
/// makes it cheaper for the broker and changes nothing here — the proxy is
/// counting held sockets, and a forwarded remote pop is held open at its owner
/// for the full timeout (§3.6, Q5). Its query spells `wait`/`timeout` exactly
/// as the durable wire does, so `poll_timeout_ms` needs no ephemeral arm.
pub fn is_wait_pop(path: &str, query: Option<&str>) -> bool {
    (path.starts_with("/api/v1/pop/queue/") || path == "/api/v1/ephemeral/pop")
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
            RouteClass::Gated(Feature::Streams, GatedOp::Open)
        );
        assert_eq!(classify(&Method::GET, "/"), RouteClass::Read);
    }

    // ---- PLAN_KV_TIMERS.md §9.8 P1: the kv + timers gate ----

    #[test]
    fn kv_routes_are_gated_and_split_by_what_they_do() {
        use GatedOp::*;
        assert_eq!(
            classify(&Method::POST, "/api/v1/kv"),
            RouteClass::Gated(Feature::Kv, Mixed)
        );
        assert_eq!(
            classify(&Method::GET, "/api/v1/kv/orders/9f1/items"),
            RouteClass::Gated(Feature::Kv, Read),
            "the key is a catch-all: slashes inside it are still one key route"
        );
        assert_eq!(
            classify(&Method::PUT, "/api/v1/kv/orders/9f1"),
            RouteClass::Gated(Feature::Kv, Grow)
        );
        assert_eq!(
            classify(&Method::DELETE, "/api/v1/kv/orders/9f1"),
            RouteClass::Gated(Feature::Kv, Open),
            "a delete is how a tenant at its cap gets back under it (§9.5)"
        );
    }

    /// §5.5: `getPrefix` lives only inside the POST batch. There is no
    /// prefix-in-a-query-string surface, so a GET on the batch path is not a
    /// shape that may ever be made to work — it fails closed at the proxy.
    #[test]
    fn kv_batch_path_is_post_only() {
        for m in [Method::GET, Method::PUT, Method::DELETE, Method::PATCH] {
            assert_eq!(classify(&m, "/api/v1/kv"), RouteClass::Blocked, "{m} /api/v1/kv");
        }
        assert_eq!(classify(&Method::PATCH, "/api/v1/kv/ns/k"), RouteClass::Blocked);
    }

    /// §9.6, the whole point of the split. The cancel route must never come
    /// back as anything a quota gate can refuse.
    #[test]
    fn timer_cancel_is_never_on_the_blockable_half() {
        assert_eq!(
            classify(&Method::DELETE, "/api/v1/timers/orders/campaign-42"),
            RouteClass::Gated(Feature::Timers, GatedOp::Open)
        );
        // Peek and list are reads.
        assert_eq!(
            classify(&Method::GET, "/api/v1/timers/orders/campaign-42"),
            RouteClass::Gated(Feature::Timers, GatedOp::Read)
        );
        assert_eq!(
            classify(&Method::GET, "/api/v1/timers/orders"),
            RouteClass::Gated(Feature::Timers, GatedOp::Read)
        );
        // The batch carries both halves, so its class says "look at the body".
        assert_eq!(
            classify(&Method::POST, "/api/v1/timers"),
            RouteClass::Gated(Feature::Timers, GatedOp::Mixed)
        );
        assert_eq!(classify(&Method::PUT, "/api/v1/timers"), RouteClass::Blocked);
    }

    /// The neighbours must not be dragged into the new families by a prefix
    /// that is one character too short.
    #[test]
    fn kv_and_timer_prefixes_do_not_swallow_neighbours() {
        assert_eq!(classify(&Method::POST, "/api/v1/kvstore"), RouteClass::Blocked);
        assert_eq!(classify(&Method::POST, "/api/v1/timersets"), RouteClass::Blocked);
        assert_eq!(classify(&Method::POST, "/api/v1/ephemerals"), RouteClass::Blocked);
        // and the families themselves never fall through to the open Read set
        assert_ne!(classify(&Method::GET, "/api/v1/kv/ns/k"), RouteClass::Read);
        assert_ne!(classify(&Method::GET, "/api/v1/timers/q"), RouteClass::Read);
        assert_ne!(classify(&Method::GET, "/api/v1/ephemeral/queues"), RouteClass::Read);
    }

    // ---- EPHEMERAL_QUEUES.md §5.1: the RAM family ----

    /// The §5.1 table, verbatim. Every row of it, and nothing else.
    #[test]
    fn ephemeral_routes_are_the_plan_table() {
        use GatedOp::*;
        for (m, p, op) in [
            (Method::POST, "/api/v1/ephemeral/push", Grow),
            (Method::GET, "/api/v1/ephemeral/pop", Open),
            (Method::POST, "/api/v1/ephemeral/ack", Open),
            (Method::POST, "/api/v1/ephemeral/configure", Open),
            (Method::POST, "/api/v1/ephemeral/reset", Open),
            (Method::DELETE, "/api/v1/ephemeral/queue/orders", Open),
            (Method::GET, "/api/v1/ephemeral/queues", Read),
            (Method::GET, "/api/v1/ephemeral/queues/orders/depth", Read),
        ] {
            assert_eq!(classify(&m, p), RouteClass::Gated(Feature::Ephemeral, op), "{m} {p}");
        }
    }

    /// `push` is the only verb a quota may ever refuse. A `reset` or a queue
    /// `DELETE` on the blockable half would lock a tenant out of the only way
    /// to free the RAM it is over on — the §9.5 rule, on a class where the
    /// bytes are even more finite.
    #[test]
    fn only_ephemeral_push_is_on_the_blockable_half() {
        for (m, p) in [
            (Method::GET, "/api/v1/ephemeral/pop"),
            (Method::POST, "/api/v1/ephemeral/ack"),
            (Method::POST, "/api/v1/ephemeral/configure"),
            (Method::POST, "/api/v1/ephemeral/reset"),
            (Method::DELETE, "/api/v1/ephemeral/queue/orders"),
            (Method::GET, "/api/v1/ephemeral/queues"),
            (Method::GET, "/api/v1/ephemeral/queues/orders/depth"),
        ] {
            assert_ne!(
                classify(&m, p),
                RouteClass::Gated(Feature::Ephemeral, GatedOp::Grow),
                "{m} {p}"
            );
            // and no ephemeral route is Produce: it would inherit the durable
            // storage blocks wholesale, which have no referent in RAM.
            assert_ne!(classify(&m, p), RouteClass::Produce, "{m} {p}");
        }
        assert_ne!(classify(&Method::POST, "/api/v1/ephemeral/push"), RouteClass::Produce);
    }

    /// One method per path at the broker, so every other shape fails closed
    /// here instead of travelling to a 405 — and an unknown verb in the family
    /// is a 404, not a forwarded guess.
    #[test]
    fn ephemeral_family_is_method_exact_and_fails_closed() {
        for (m, p) in [
            (Method::GET, "/api/v1/ephemeral/push"),
            (Method::PUT, "/api/v1/ephemeral/push"),
            (Method::POST, "/api/v1/ephemeral/pop"),
            (Method::DELETE, "/api/v1/ephemeral/pop"),
            (Method::PUT, "/api/v1/ephemeral/ack"),
            (Method::GET, "/api/v1/ephemeral/reset"),
            (Method::POST, "/api/v1/ephemeral/queues"),
            (Method::DELETE, "/api/v1/ephemeral/queues"),
            (Method::GET, "/api/v1/ephemeral/queue/orders"),
            (Method::POST, "/api/v1/ephemeral/queue/orders"),
            // unknown members of the family, and the family root itself
            (Method::POST, "/api/v1/ephemeral/bogus"),
            (Method::GET, "/api/v1/ephemeral/bogus"),
            (Method::DELETE, "/api/v1/ephemeral/bogus"),
            (Method::GET, "/api/v1/ephemeral/"),
            (Method::GET, "/api/v1/ephemeral"),
            // the depth read needs its exact tail: a bare queue, or anything
            // past `depth`, is not a route the broker registers
            (Method::GET, "/api/v1/ephemeral/queues/orders"),
            (Method::GET, "/api/v1/ephemeral/queues/orders/depth/extra"),
        ] {
            assert_eq!(classify(&m, p), RouteClass::Blocked, "{m} {p}");
        }
    }

    /// Regression guard for the §9.6 trap in its original form: if anyone ever
    /// "simplifies" these to Produce, every cancel starts answering 403 while
    /// the fire keeps going. Fail here, loudly, instead of in production.
    #[test]
    fn no_kv_or_timer_route_is_classified_as_produce() {
        for (m, p) in [
            (Method::POST, "/api/v1/kv"),
            (Method::PUT, "/api/v1/kv/ns/k"),
            (Method::DELETE, "/api/v1/kv/ns/k"),
            (Method::GET, "/api/v1/kv/ns/k"),
            (Method::POST, "/api/v1/timers"),
            (Method::DELETE, "/api/v1/timers/q/k"),
            (Method::GET, "/api/v1/timers/q/k"),
        ] {
            assert_ne!(classify(&m, p), RouteClass::Produce, "{m} {p}");
        }
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

    /// The RAM pop parks exactly like the durable one, so it must take the
    /// long-poll upstream timeout and a `limits.parked_slot` — both of which
    /// gateway.rs decides from this one predicate.
    #[test]
    fn ephemeral_wait_pop_parks_too() {
        assert!(is_wait_pop("/api/v1/ephemeral/pop", Some("queue=inbox&wait=true&timeout=20000")));
        assert!(!is_wait_pop("/api/v1/ephemeral/pop", Some("queue=inbox&batch=10")));
        assert!(!is_wait_pop("/api/v1/ephemeral/pop", None));
        // a push carrying the parameter is still not a parked consumer
        assert!(!is_wait_pop("/api/v1/ephemeral/push", Some("wait=true")));
        // same query vocabulary as the durable wire, so one parser serves both
        assert_eq!(poll_timeout_ms(Some("queue=inbox&wait=true&timeout=20000")), Some(20_000));
    }
}
