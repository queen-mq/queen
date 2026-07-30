//! Which cluster a request acts on, and whether its caller may.
//!
//! The proxy's original answer was "the first DNS label of the Host header",
//! which works when every cluster has its own subdomain. The internal webapp
//! is served from ONE hostname for every tenant, so a browser has no way to
//! say which cluster the page is looking at. `x-queen-act-cluster` is that way
//! (config::ACT_CLUSTER_HEADER), and this module is the whole of its policy:
//!
//!   * an API KEY never honours it. A key is a data-plane credential bound to
//!     the cluster it was issued on; letting a header retarget it would widen
//!     every leaked key from one cluster to all of them. Refused (403), not
//!     silently ignored — a producer that believes it is writing to cluster B
//!     while the proxy routes it to A is the worse failure.
//!   * a NORMAL user may act on a cluster they hold a `cluster_roles` row on,
//!     with that row's role. Anything else is 403.
//!   * a live OPERATOR may act on any cluster that exists, as Admin.
//!
//! Resolution is split in two on purpose, so gateway.rs keeps its documented
//! pipeline order (resolve cluster -> status gate -> classify -> authenticate):
//! `resolve_ctx` picks the cluster WITHOUT authenticating anybody, and
//! `authenticate_for` then decides whether the caller may act on it. The
//! separation is safe because `resolve_ctx` reveals nothing — an unknown
//! cluster and an unauthorised one get the same 403 (see `ActDecision`).

use std::sync::Arc;

use axum::http::HeaderMap;
use axum::response::Response;

use crate::auth::{self, Credential};
use crate::config::ACT_CLUSTER_HEADER;
use crate::errors;
use crate::state::{ClusterCtx, Principal, Role, St};

/// The act-as-cluster reference a request asked for, if any: a cluster slug or
/// a cluster uuid, trimmed, empty treated as absent.
pub fn requested(headers: &HeaderMap) -> Option<String> {
    headers
        .get(ACT_CLUSTER_HEADER)
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
}

/// Resolve the cluster this request acts on. No authentication happens here:
/// the caller's right to act on it is settled by `authenticate_for` below.
///
/// Without the header this is exactly the old behaviour (Host -> slug -> 421
/// on a miss). With it, an unresolvable reference is a 403, never a 404 or a
/// 421: the answer must not differ between "no such cluster" and "not yours",
/// or the header becomes a way to enumerate every tenant's cluster slugs.
pub async fn resolve_ctx(st: &St, headers: &HeaderMap) -> Result<Arc<ClusterCtx>, Response> {
    match requested(headers) {
        None => {
            let host = headers
                .get(axum::http::header::HOST)
                .and_then(|v| v.to_str().ok())
                .unwrap_or("");
            st.cache
                .resolve_host(host)
                .await
                .ok_or_else(|| errors::err_421("no cluster for this host"))
        }
        Some(reference) => st.cache.resolve_ref(&reference).await.ok_or_else(|| {
            tracing::debug!(target: "auth", cluster = %reference, "act-as-cluster: no such cluster");
            response_for(ActDecision::RefuseCluster)
        }),
    }
}

/// Authenticate the caller against the cluster `resolve_ctx` picked, applying
/// the act-as rules when the header was present. Returns the same `Principal`
/// shape `auth::authenticate` does, so gateway.rs's authorize step is unchanged.
pub async fn authenticate_for(
    st: &St,
    headers: &HeaderMap,
    ctx: &ClusterCtx,
) -> Result<Principal, Response> {
    let Some(reference) = requested(headers) else {
        // No header: the cluster came from Host, and that is the case
        // auth::authenticate has always covered (api keys included).
        return auth::authenticate(st, headers, ctx.cluster_id).await;
    };

    // dev-insecure authenticates nobody — it hands out a full-scope api-key
    // principal regardless of what was presented. There is no session to
    // check act-as against, so refuse rather than let the bypass mode grant
    // cross-cluster access it never verified.
    if st.cfg.dev_insecure {
        return Err(errors::err_403(
            errors::CODE_FORBIDDEN,
            "act-as-cluster requires a human session (this proxy runs with dev-insecure auth)",
        ));
    }

    let cred = auth::read_credential(&st.cfg.cookie_name, headers);
    let kind = match &cred {
        Credential::ApiKey(_) => CredKind::ApiKey,
        Credential::Session(_) => CredKind::Session,
        Credential::None => CredKind::None,
    };
    let token = match cred {
        Credential::Session(t) => t,
        // The two kinds decided by the credential alone, answered by the same
        // `decide_act` the session path below goes through — the matrix its
        // tests pin is the one that runs.
        _ => return Err(response_for(decide_act(kind, false, true, None))),
    };
    let session = auth::verify_session(st, &token).await?;

    // A cluster-scoped token asking to act elsewhere is a contradiction; the
    // claim wins. (Session cookies are minted unscoped, so this is inert
    // today — it exists so a future scoped token cannot be retargeted.)
    if matches!(session.claims.cluster, Some(c) if c != ctx.cluster_id) {
        return Err(errors::err_403(errors::CODE_FORBIDDEN, "token not valid for this cluster"));
    }

    // An operator needs no membership row, so no membership query is made for
    // one — `decide_act` ignores it in that arm anyway.
    let membership = if session.operator {
        None
    } else {
        st.keys.cluster_role(&st.db, session.claims.user_id, ctx.cluster_id).await
    };
    // `cluster_exists` is true by construction: `resolve_ctx` already returned
    // this exact refusal for a reference that resolved to nothing, which is
    // what makes the two indistinguishable to the caller.
    match decide_act(CredKind::Session, session.operator, true, membership) {
        ActDecision::Act(role) => {
            tracing::debug!(
                target: "auth", user = %session.claims.user_id, cluster = %ctx.slug,
                role = ?role, operator = session.operator, "act-as-cluster honoured"
            );
            Ok(Principal::User { user_id: session.claims.user_id, role, operator: session.operator })
        }
        ActDecision::RefuseCluster => {
            // A signature-valid session whose user row is gone (user deleted,
            // or a dev pxdb reset) is a dead session, not a permission
            // problem: 401 sends the SPA back to login instead of parking it
            // on a 403 it can never resolve.
            if !st.keys.user_exists(&st.db, session.claims.user_id).await {
                return Err(errors::err_401("session no longer valid"));
            }
            tracing::debug!(
                target: "auth", user = %session.claims.user_id, cluster = %reference,
                "act-as-cluster: no role on the requested cluster"
            );
            Err(response_for(ActDecision::RefuseCluster))
        }
        other => Err(response_for(other)),
    }
}

/// The single mapping from decision to wire response. Both refusal paths go
/// through it, so "no such cluster" and "no role on it" are byte-identical.
fn response_for(d: ActDecision) -> Response {
    match d {
        // Never constructed by a caller — `Act` is the success arm.
        ActDecision::Act(_) => errors::err_403(errors::CODE_FORBIDDEN, "not permitted"),
        ActDecision::Unauthenticated => errors::err_401("missing bearer credential"),
        ActDecision::RefuseApiKey => errors::err_403(
            errors::CODE_FORBIDDEN,
            "api keys are bound to their own cluster; x-queen-act-cluster is not honoured for them",
        ),
        ActDecision::RefuseCluster => {
            errors::err_403(errors::CODE_FORBIDDEN, "no such cluster, or no role on it")
        }
    }
}

// ---------------------------------------------------------------------------
// the decision, as data
// ---------------------------------------------------------------------------

/// The credential kind, as far as act-as cares.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CredKind {
    ApiKey,
    Session,
    None,
}

/// What honouring `x-queen-act-cluster` resolves to, given facts the caller
/// has already looked up. Pure, so the matrix the whole single-hostname webapp
/// rests on is pinned by tests rather than by reading `authenticate_for`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActDecision {
    /// Act on the requested cluster with this role.
    Act(Role),
    /// 401 — nothing was presented.
    Unauthenticated,
    /// 403 — a data-plane key stays on its own cluster.
    RefuseApiKey,
    /// 403 — no such cluster, or the caller holds no role on it. ONE answer
    /// for both, so the header cannot enumerate other tenants' clusters.
    RefuseCluster,
}

/// `operator` is the capability already resolved LIVE (flag AND row).
/// `membership` is the caller's `cluster_roles` row on the requested cluster.
pub fn decide_act(
    cred: CredKind,
    operator: bool,
    cluster_exists: bool,
    membership: Option<Role>,
) -> ActDecision {
    match cred {
        CredKind::None => ActDecision::Unauthenticated,
        CredKind::ApiKey => ActDecision::RefuseApiKey,
        CredKind::Session => {
            if !cluster_exists {
                // Checked before the operator arm: "any cluster" means any
                // cluster that EXISTS.
                return ActDecision::RefuseCluster;
            }
            if operator {
                return ActDecision::Act(Role::Admin);
            }
            match membership {
                Some(role) => ActDecision::Act(role),
                None => ActDecision::RefuseCluster,
            }
        }
    }
}

// ---------------------------------------------------------------------------
// operator access logging (sampled)
// ---------------------------------------------------------------------------

/// One operator-access line per window. A dashboard polls these routes every
/// few seconds; the interesting fact is "an operator was reading cell-wide
/// data around now", not each poll, so the line carries how many it stands for.
const OPERATOR_LOG_INTERVAL: std::time::Duration = std::time::Duration::from_secs(60);

/// Process-wide: one proxy fronts one cell, and the fact being reported is
/// about the cell.
static OPERATOR_LOG: std::sync::Mutex<LogGate> = std::sync::Mutex::new(LogGate::new());

/// `Some(suppressed_since_the_last_line)` when a line is due. Pure state
/// machine (the clock is the caller's) so both halves are testable.
struct LogGate {
    next: Option<std::time::Instant>,
    suppressed: u64,
}

impl LogGate {
    const fn new() -> LogGate {
        LogGate { next: None, suppressed: 0 }
    }

    fn tick(&mut self, now: std::time::Instant, every: std::time::Duration) -> Option<u64> {
        match self.next {
            Some(at) if now < at => {
                self.suppressed += 1;
                None
            }
            _ => {
                let suppressed = std::mem::take(&mut self.suppressed);
                self.next = Some(now + every);
                Some(suppressed)
            }
        }
    }
}

/// Record that an operator opened a route no tenant credential can reach.
/// Non-blocking `try_lock`, like the other sampled gates in this crate: a
/// concurrent responder skips its line instead of waiting on the request path.
pub fn note_operator_access(user_id: uuid::Uuid, cluster: &str, path: &str) {
    let Ok(mut gate) = OPERATOR_LOG.try_lock() else { return };
    let Some(suppressed) = gate.tick(std::time::Instant::now(), OPERATOR_LOG_INTERVAL) else {
        return;
    };
    drop(gate);
    tracing::info!(
        target: "auth", user = %user_id, cluster, path, suppressed,
        "operator opened a cell-wide route (QUEEN_PROXY_OPERATOR_ENABLED is on)"
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    fn hdrs(v: Option<&str>) -> HeaderMap {
        let mut h = HeaderMap::new();
        if let Some(v) = v {
            h.insert(ACT_CLUSTER_HEADER, HeaderValue::from_str(v).unwrap());
        }
        h
    }

    #[test]
    fn requested_trims_and_treats_empty_as_absent() {
        assert_eq!(requested(&hdrs(None)), None);
        assert_eq!(requested(&hdrs(Some("  acme  "))).as_deref(), Some("acme"));
        assert_eq!(requested(&hdrs(Some("   "))), None);
        assert_eq!(requested(&hdrs(Some(""))), None);
    }

    // ---- the authorisation matrix ------------------------------------------

    #[test]
    fn api_keys_never_act_as_another_cluster() {
        // Not even for a cluster that exists and whose owner is the same
        // tenant: the key is the wrong KIND of credential for this header.
        assert_eq!(
            decide_act(CredKind::ApiKey, false, true, Some(Role::Admin)),
            ActDecision::RefuseApiKey
        );
        // and an api key is never an operator, whatever a caller claims
        assert_eq!(
            decide_act(CredKind::ApiKey, true, true, Some(Role::Admin)),
            ActDecision::RefuseApiKey
        );
    }

    #[test]
    fn a_normal_user_acts_only_where_they_hold_a_role() {
        for role in [Role::Admin, Role::Producer, Role::Consumer, Role::Viewer] {
            assert_eq!(
                decide_act(CredKind::Session, false, true, Some(role)),
                ActDecision::Act(role),
                "the membership row's role is the effective one"
            );
        }
        assert_eq!(
            decide_act(CredKind::Session, false, true, None),
            ActDecision::RefuseCluster,
            "a cluster they are not a member of"
        );
    }

    #[test]
    fn an_operator_acts_on_any_existing_cluster_as_admin() {
        assert_eq!(
            decide_act(CredKind::Session, true, true, None),
            ActDecision::Act(Role::Admin),
            "no membership row needed"
        );
        // A membership row does not DEMOTE an operator — super-admin is not
        // per-cluster. This is the rule /auth/me reports, so the SPA never
        // renders a viewer nav for someone the data plane treats as admin.
        assert_eq!(
            decide_act(CredKind::Session, true, true, Some(Role::Viewer)),
            ActDecision::Act(Role::Admin)
        );
    }

    #[test]
    fn a_cluster_that_does_not_exist_is_refused_for_everyone() {
        assert_eq!(
            decide_act(CredKind::Session, true, false, None),
            ActDecision::RefuseCluster,
            "'any cluster' means any cluster that exists"
        );
        assert_eq!(
            decide_act(CredKind::Session, false, false, None),
            ActDecision::RefuseCluster
        );
    }

    #[test]
    fn unknown_cluster_and_no_role_are_indistinguishable() {
        // The property that keeps the header from enumerating slugs: same
        // decision, therefore same status and same body.
        assert_eq!(
            decide_act(CredKind::Session, false, false, None),
            decide_act(CredKind::Session, false, true, None)
        );
    }

    #[test]
    fn no_credential_is_a_401_not_a_403() {
        assert_eq!(decide_act(CredKind::None, false, true, Some(Role::Admin)), ActDecision::Unauthenticated);
        assert_eq!(decide_act(CredKind::None, true, true, None), ActDecision::Unauthenticated);
    }

    // ---- sampled operator log ----------------------------------------------

    #[test]
    fn operator_log_emits_once_per_window_with_the_suppressed_count() {
        let mut g = LogGate::new();
        let t0 = std::time::Instant::now();
        let every = std::time::Duration::from_secs(60);
        assert_eq!(g.tick(t0, every), Some(0), "first access always logs");
        assert_eq!(g.tick(t0 + std::time::Duration::from_secs(1), every), None);
        assert_eq!(g.tick(t0 + std::time::Duration::from_secs(59), every), None);
        // window elapsed: log again, reporting the two polls it stood in for
        assert_eq!(g.tick(t0 + std::time::Duration::from_secs(60), every), Some(2));
        assert_eq!(g.tick(t0 + std::time::Duration::from_secs(61), every), None);
        assert_eq!(g.tick(t0 + std::time::Duration::from_secs(121), every), Some(1));
    }
}
