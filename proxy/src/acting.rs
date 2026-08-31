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
//! `resolve_route` picks the cluster WITHOUT authenticating anybody, and
//! `authenticate_for` then decides whether the caller may act on it. The
//! separation is safe because `resolve_route` reveals nothing — an unknown
//! cluster and an unauthorised one get the same 403 (see `ActDecision`).
//!
//! ## Shared hosts (decision z)
//!
//! There is a third shape, and it is why `resolve_route` returns an enum rather
//! than a `ClusterCtx`. A host listed in `QUEEN_PROXY_SHARED_HOSTS` fronts MANY
//! clusters — one URL for several teams on a self-hosted cell, one URL for
//! every trial tenant in the cloud. On such a host the Host label names
//! nothing, so **the cluster is resolved from the credential**:
//!
//!   * an **API key** names its own cluster. `api_keys.key_hash` is UNIQUE and
//!     already joins straight through to the cluster, so this is the existing
//!     lookup used as the authority instead of as a cross-check. The key always
//!     WAS the authority; the Host label was only routing.
//!   * a **human session** names one with `x-queen-act-cluster` (or carries a
//!     `cluster` claim), and is then checked against `cluster_roles` by exactly
//!     the same `decide_act` matrix as on a per-cluster hostname.
//!
//! Two properties are load-bearing and pinned by tests below:
//!
//!   * a missing or invalid credential on a shared host is **401, never 421**.
//!     421 means "you reached the wrong host" — on a shared host you did not,
//!     and an SDK that retries a 421 elsewhere would be chasing a DNS name that
//!     does not exist. This holds for the LISTENER, not merely for the exact
//!     string: a shared host normally sits behind a wildcard record, so every
//!     other `*.domain` arrives on the same socket, and a 401-for-real-slug /
//!     421-for-nonexistent split there is an unauthenticated way to enumerate
//!     every tenant's cluster slug. `Route::UnknownHost` + `gateway`'s
//!     credential-first ordering is what closes that; see
//!     `Config::has_shared_hosts`.
//!   * the shared check happens FIRST, before the act-as header and before
//!     `cache::resolve_host`. So `QUEEN_PROXY_DEFAULT_CLUSTER` (which lives
//!     inside `resolve_host`) can never absorb a shared host, and an
//!     unauthenticated caller can never name a cluster and read its status off
//!     the pre-auth gates.
//!
//! Nothing downstream changes: `resolve_from_credential` returns the very same
//! `Arc<ClusterCtx>` the Host path returns, built by the same `ctx_from_row`,
//! so the per-cluster token buckets, the storage quota, the `x-queen-tenant`
//! injection and the meter attribution all follow the cluster the credential
//! named without knowing how it was named.

use std::sync::Arc;

use axum::http::HeaderMap;
use axum::response::Response;
use uuid::Uuid;

use crate::auth::{self, Credential, Session};
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

/// Where the cluster for this request comes from.
pub enum Route {
    /// Settled before any credential is read: the Host label, or an
    /// `x-queen-act-cluster` header on a host that is not shared. Finish with
    /// `authenticate_for`.
    Fixed(Arc<ClusterCtx>),
    /// A SHARED host: nothing is known until the credential is read. Finish
    /// with `resolve_from_credential`, which returns the cluster AND the
    /// principal together — on this host they are one question.
    FromCredential,
    /// A host that names no cluster, on a listener that HAS shared hosts. The
    /// 421 is real and is still the answer — but only for a caller that proved
    /// a live credential; see `unknown_host_refusal`. Never produced on a
    /// listener with no shared hosts, where the 421 is returned inline exactly
    /// as it always was.
    UnknownHost,
}

/// The Host header, or `""` when it is absent or not valid UTF-8. `""` is never
/// a shared host and never a slug, so both paths below treat it as a miss.
fn host_of(headers: &HeaderMap) -> &str {
    headers
        .get(axum::http::header::HOST)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
}

/// Resolve the cluster this request acts on. No authentication happens here:
/// the caller's right to act on it is settled by `authenticate_for` below, or
/// — on a shared host — by `resolve_from_credential`, which does both at once.
///
/// Without the header, and on a host that is not shared, this is exactly the
/// old behaviour (Host -> slug -> 421 on a miss). With it, an unresolvable
/// reference is a 403, never a 404 or a 421: the answer must not differ between
/// "no such cluster" and "not yours", or the header becomes a way to enumerate
/// every tenant's cluster slugs.
pub async fn resolve_route(st: &St, headers: &HeaderMap) -> Result<Route, Response> {
    route(st, headers, ActPolicy::AnyHost).await
}

/// Where `x-queen-act-cluster` is honoured.
#[derive(Clone, Copy, PartialEq, Eq)]
enum ActPolicy {
    /// The DATA PLANE's long-standing behaviour: the header retargets a human
    /// session on any host. Unchanged.
    AnyHost,
    /// The CONSOLE: the header names a cluster only where the Host cannot —
    /// i.e. on a shared host. See `resolve_route_console`.
    SharedHostOnly,
}

/// `resolve_route` for `/api/console/*`.
///
/// Same resolution, one deliberate narrowing: the act-as header is honoured
/// only on a SHARED host. The console had always ignored it, and widening it to
/// every host would silently widen what an existing account can reach —
/// `decide_act` grants a live OPERATOR `Role::Admin` on any cluster that
/// exists, so on a cell with `QUEEN_PROXY_OPERATOR_ENABLED=true` one header
/// would take an operator's console reach from the Host's cluster to every
/// cluster on the cell, including minting API keys and granting members. That
/// is a privilege change, and it is not one this feature was asked for.
///
/// On a shared host the header is the ONLY way a session can name its cluster,
/// so there it is honoured — that is decision z, and it is what the whole
/// single-URL console rests on. (The console SPA — proxy/console — matches this
/// policy from the other side: it reads `/auth/me` at boot and attaches the
/// header only when `acting_cluster` is null, i.e. exactly on a shared host, so
/// on a per-cluster hostname it sends nothing the data plane's any-host
/// honouring could act on either.)
pub async fn resolve_route_console(st: &St, headers: &HeaderMap) -> Result<Route, Response> {
    route(st, headers, ActPolicy::SharedHostOnly).await
}

async fn route(st: &St, headers: &HeaderMap, act_policy: ActPolicy) -> Result<Route, Response> {
    let host = host_of(headers);
    // FIRST, deliberately, and before `requested`. On a shared host the
    // credential is the only authority: an unauthenticated caller must not be
    // able to name a cluster with the header and read its status off the
    // pre-auth gates, and `QUEEN_PROXY_DEFAULT_CLUSTER` — which lives inside
    // `resolve_host` below — must never be able to absorb the host.
    if st.cfg.is_shared_host(host) {
        return Ok(Route::FromCredential);
    }
    // Not a shared host: the console ignores the act-as header here, exactly as
    // it did before shared hosts existed.
    let asked = match act_policy {
        ActPolicy::AnyHost => requested(headers),
        ActPolicy::SharedHostOnly => None,
    };
    match asked {
        None => match st.cache.resolve_host(host).await {
            Some(ctx) => Ok(Route::Fixed(ctx)),
            // The Host names no cluster. On a listener that fronts many tenants
            // on a shared name, saying so before a credential is read turns
            // 421-vs-401 into an unauthenticated "does this slug exist" oracle
            // (see `Config::has_shared_hosts`), so the answer is deferred.
            // dev-insecure verifies nothing, so there is no credential that
            // could ever earn the 421 there — it stays inline.
            None if st.cfg.has_shared_hosts() && !st.cfg.dev_insecure => Ok(Route::UnknownHost),
            None => Err(errors::err_421("no cluster for this host")),
        },
        Some(reference) => st.cache.resolve_ref(&reference).await.map(Route::Fixed).ok_or_else(|| {
            tracing::debug!(target: "auth", cluster = %reference, "act-as-cluster: no such cluster");
            response_for(ActDecision::RefuseCluster)
        }),
    }
}

/// The answer for `Route::UnknownHost`: a 421, but only to a caller who proved
/// a LIVE credential; otherwise that caller's own 401.
///
/// This is decision z's property applied to the whole listener rather than to
/// one exact string. The lookups are deliberately the same ones
/// `resolve_from_credential` makes — the same sha256, the same `by_key_hash`,
/// the same `verify_session` — so nothing new is computed on secret material
/// and the timing of the two paths does not diverge.
///
/// A valid credential still gets the honest 421: an SDK pointed at a DNS name
/// this cell does not serve must be told so, and it is the only caller that
/// could be. What it must NOT be is a way for an anonymous caller to tell
/// `acme.queenmq.cloud` (401 — exists) from `nosuch.queenmq.cloud` (421).
pub async fn unknown_host_refusal(st: &St, headers: &HeaderMap) -> Response {
    let secure = crate::oauth::cookie_is_secure(st, headers);
    let live = match auth::read_credential(&st.cfg.cookie_name, secure, headers) {
        Credential::None => {
            return shared_refusal(SharedRoute::Unauthenticated);
        }
        Credential::ApiKey(key) => {
            let hash = auth::key_hash_hex(&key);
            match st.cache.by_key_hash(&hash).await {
                Some(_) => true,
                // Byte-identical to the shared host's answer for the same
                // credential, which is what makes the two indistinguishable.
                None => return errors::err_401("unknown or revoked api key"),
            }
        }
        Credential::Session(token) => match auth::verify_session(st, &token).await {
            Ok(_) => true,
            // verify_session's own 401 ("session no longer valid" / expired),
            // unchanged: a dead session is a dead session on every host.
            Err(resp) => return resp,
        },
    };
    debug_assert!(live);
    errors::err_421("no cluster for this host")
}

/// Best-effort "which cluster would this request act on", for `/auth/me`'s
/// `acting_cluster` label. Never an error: on a shared host a session that has
/// not picked a cluster yet is exactly the case the SPA's selector exists for,
/// so it reports `null` and lets the user choose.
pub async fn peek_ctx(st: &St, headers: &HeaderMap) -> Option<Arc<ClusterCtx>> {
    match resolve_route(st, headers).await.ok()? {
        Route::Fixed(ctx) => Some(ctx),
        Route::FromCredential => {
            resolve_from_credential(st, headers).await.ok().map(|(ctx, _)| ctx)
        }
        // The Host names nothing. `acting_cluster: null`, the same answer a
        // shared host gives a session that has not picked one yet.
        Route::UnknownHost => None,
    }
}

/// The shared-host path: read the credential, and let it name the cluster.
/// Returns the cluster AND the principal, because on this host the two are one
/// question — there is no cluster until the credential says which one.
///
/// Timing/lookup shape is deliberately IDENTICAL to the per-cluster host path:
/// the same sha256 of the presented key, the same single UNIQUE-index lookup
/// through `ClusterCache::by_key_hash` (same 30 s positive / 5 s negative TTL,
/// same single-flight, same batched `last_used_at` touch). The only thing that
/// changes is what is done with the answer — the row's cluster is used instead
/// of being compared against one Host already chose. No new comparison is made
/// on secret material.
pub async fn resolve_from_credential(
    st: &St,
    headers: &HeaderMap,
) -> Result<(Arc<ClusterCtx>, Principal), Response> {
    // dev-insecure authenticates nobody — it hands out a full-scope api-key
    // principal regardless of what was presented. There is no verified
    // credential to read a cluster out of, so a shared host cannot work at all
    // in that mode. Refused loudly rather than silently routed somewhere.
    if st.cfg.dev_insecure {
        return Err(errors::err_403(
            errors::CODE_FORBIDDEN,
            "a shared host resolves its cluster from the credential, and this proxy runs with dev-insecure auth (nothing is verified)",
        ));
    }

    let act = requested(headers);
    let secure = crate::oauth::cookie_is_secure(st, headers);
    match auth::read_credential(&st.cfg.cookie_name, secure, headers) {
        // Answered through the same `decide_shared` the arms below go through,
        // so the matrix its tests pin is the one that runs.
        Credential::None => Err(shared_refusal(decide_shared(CredKind::None, act.as_deref(), None))),

        Credential::ApiKey(key) => match decide_shared(CredKind::ApiKey, act.as_deref(), None) {
            SharedRoute::ByKey => {
                let hash = auth::key_hash_hex(&key);
                match st.cache.by_key_hash(&hash).await {
                    Some((ctx, key_id, scopes)) => {
                        tracing::debug!(
                            target: "auth", cluster = %ctx.slug, key = %key_id,
                            "shared host: cluster resolved from the api key"
                        );
                        Ok((ctx, Principal::ApiKey { key_id, scopes }))
                    }
                    // 401, never 421 and never 403 (decision z). There is no
                    // "wrong host" to report and no cluster to be forbidden
                    // from: the key was the only thing that could have named
                    // one, and it named nothing.
                    None => Err(errors::err_401("unknown or revoked api key")),
                }
            }
            other => Err(shared_refusal(other)),
        },

        Credential::Session(token) => {
            let session = auth::verify_session(st, &token).await?;
            // The `cluster` claim is only readable once the token is verified,
            // which is why the session arm consults `decide_shared` here rather
            // than up front with the other two.
            match decide_shared(CredKind::Session, act.as_deref(), session.claims.cluster) {
                SharedRoute::BySession(reference) => {
                    let Some(ctx) = st.cache.resolve_ref(&reference).await else {
                        tracing::debug!(
                            target: "auth", cluster = %reference,
                            "shared host: no such cluster"
                        );
                        return Err(response_for(ActDecision::RefuseCluster));
                    };
                    let principal = session_principal(st, &session, &ctx).await?;
                    Ok((ctx, principal))
                }
                other => Err(shared_refusal(other)),
            }
        }
    }
}

/// Authenticate the caller against the cluster `resolve_route` picked, applying
/// the act-as rules when the header was present. Returns the same `Principal`
/// shape `auth::authenticate` does, so gateway.rs's authorize step is unchanged.
///
/// Only ever called with `Route::Fixed`: on a shared host the cluster and the
/// principal are decided together, by `resolve_from_credential`.
pub async fn authenticate_for(
    st: &St,
    headers: &HeaderMap,
    ctx: &ClusterCtx,
) -> Result<Principal, Response> {
    let Some(_reference) = requested(headers) else {
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

    let secure = crate::oauth::cookie_is_secure(st, headers);
    let cred = auth::read_credential(&st.cfg.cookie_name, secure, headers);
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
    session_principal(st, &session, ctx).await
}

/// Turn a VERIFIED session into a principal on one already-resolved cluster.
/// Shared by the act-as-cluster path and the shared-host path, so a session is
/// never accepted by one and rejected by the other, and `decide_act` stays the
/// single matrix both are pinned to.
async fn session_principal(
    st: &St,
    session: &Session,
    ctx: &ClusterCtx,
) -> Result<Principal, Response> {
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
    // `cluster_exists` is true by construction: the caller already returned
    // this exact refusal for a reference that resolved to nothing, which is
    // what makes the two indistinguishable to the caller.
    match decide_act(CredKind::Session, session.operator, true, membership) {
        ActDecision::Act(role) => {
            tracing::debug!(
                target: "auth", user = %session.claims.user_id, cluster = %ctx.slug,
                role = ?role, operator = session.operator, "session acting on cluster"
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
                target: "auth", user = %session.claims.user_id, cluster = %ctx.slug,
                "no role on the requested cluster"
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

/// What a credential on a SHARED host says about which cluster to act on,
/// before any DB work. Pure, so the routing rule the single-URL cell rests on
/// is pinned by tests rather than by reading `resolve_from_credential`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SharedRoute {
    /// Look the key up by hash; the `api_keys` row's cluster is authoritative.
    ByKey,
    /// Resolve this reference (a cluster slug or a cluster uuid) and then run
    /// the ordinary `decide_act` matrix against it.
    BySession(String),
    /// 401 — nothing was presented. NEVER a 421: there is no wrong host here.
    Unauthenticated,
    /// 403 — a key may not be retargeted by `x-queen-act-cluster`, here as
    /// anywhere else. A shared host does not weaken that: the key names its own
    /// cluster, and a header claiming another is a contradiction, not a hint.
    RefuseApiKey,
    /// 403 — a session that named no cluster at all on a host that fronts many.
    /// Not a 401 (the session is perfectly valid) and not a 421 (the host is
    /// the right one); what is missing is the choice, which is exactly what
    /// `/auth/me`'s cluster list exists to let the SPA make.
    NoClusterNamed,
}

/// `act` is the `x-queen-act-cluster` value, `claim_cluster` the verified
/// token's optional `cluster` claim (always `None` for an API key, which has no
/// claims). Both are only ever HINTS at which cluster: whether the caller may
/// act on it is still `decide_act`, and whether the key is live is still the
/// hash lookup.
pub fn decide_shared(
    cred: CredKind,
    act: Option<&str>,
    claim_cluster: Option<Uuid>,
) -> SharedRoute {
    match cred {
        CredKind::None => SharedRoute::Unauthenticated,
        CredKind::ApiKey => match act {
            Some(_) => SharedRoute::RefuseApiKey,
            None => SharedRoute::ByKey,
        },
        CredKind::Session => match (act, claim_cluster) {
            // The header wins over the claim, and the two are reconciled AFTER
            // the cluster resolves — `session_principal` refuses a scoped token
            // pointed at another cluster, exactly as on a per-cluster hostname.
            (Some(reference), _) => SharedRoute::BySession(reference.to_string()),
            (None, Some(cluster)) => SharedRoute::BySession(cluster.to_string()),
            (None, None) => SharedRoute::NoClusterNamed,
        },
    }
}

/// The single mapping from a shared-host refusal to the wire, mirroring
/// `response_for` for the act-as decisions.
fn shared_refusal(r: SharedRoute) -> Response {
    match r {
        SharedRoute::Unauthenticated => errors::err_401("missing bearer credential"),
        SharedRoute::RefuseApiKey => response_for(ActDecision::RefuseApiKey),
        SharedRoute::NoClusterNamed => errors::err_403(
            errors::CODE_FORBIDDEN,
            "this host serves several clusters; name one with x-queen-act-cluster",
        ),
        // Success arms — never routed here; kept total rather than panicking.
        SharedRoute::ByKey | SharedRoute::BySession(_) => {
            errors::err_403(errors::CODE_FORBIDDEN, "not permitted")
        }
    }
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

    // ---- routing: which of the three shapes a Host lands in ----------------

    /// A whole `St` with no pxdb behind it. Every cluster lookup therefore
    /// MISSES, which is exactly the condition these tests are about: what the
    /// proxy answers for a Host that names no cluster, with and without shared
    /// hosts configured.
    fn st_with(shared: &[&str], default_cluster: Option<&str>) -> St {
        let mut cfg = crate::config::test_config(shared);
        cfg.default_cluster = default_cluster.map(str::to_string);
        let cache = crate::cache::ClusterCache::new(&cfg, None);
        let limits = crate::limits::Limits::new(&cfg);
        let meter = std::sync::Arc::new(crate::meter::Meter::new(&cfg));
        let registry = crate::registry::Registry::new(None);
        let keys = crate::auth::Keys::from_config(&cfg);
        let mut connector = hyper_util::client::legacy::connect::HttpConnector::new();
        connector.set_nodelay(true);
        let upstream =
            hyper_util::client::legacy::Client::builder(hyper_util::rt::TokioExecutor::new())
                .build::<_, axum::body::Body>(connector);
        std::sync::Arc::new(crate::state::AppState {
            cfg,
            db: None,
            upstream,
            cache,
            limits,
            meter,
            registry,
            keys,
        })
    }

    fn host_hdrs(host: &str) -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert(axum::http::header::HOST, HeaderValue::from_str(host).unwrap());
        h
    }

    #[tokio::test]
    async fn without_shared_hosts_an_unknown_host_is_a_421_exactly_as_before() {
        // The listener nobody opted in: byte-for-byte the old behaviour, no
        // credential read, no deferral.
        let st = st_with(&[], None);
        match resolve_route(&st, &host_hdrs("nosuch.example.test")).await {
            Ok(_) => panic!("an unknown host must not resolve"),
            Err(resp) => assert_eq!(resp.status(), axum::http::StatusCode::MISDIRECTED_REQUEST),
        }
    }

    #[tokio::test]
    async fn with_shared_hosts_an_unknown_host_defers_instead_of_answering_421() {
        // The oracle this closes: on a listener that fronts many tenants behind
        // a wildcard record, 401-for-a-real-slug vs 421-for-a-nonexistent-one
        // lets an ANONYMOUS caller enumerate every tenant's cluster slug. So
        // the 421 waits for a credential.
        let st = st_with(&["shared.test"], None);
        assert!(
            matches!(
                resolve_route(&st, &host_hdrs("nosuch.example.test")).await,
                Ok(Route::UnknownHost)
            ),
            "an unknown host on a shielded listener must defer, not answer"
        );
        // And with nothing presented, the answer is the shared host's own 401 —
        // the same status, the same code, the same body.
        let resp = unknown_host_refusal(&st, &host_hdrs("nosuch.example.test")).await;
        assert_eq!(resp.status(), axum::http::StatusCode::UNAUTHORIZED);
        // An unknown key is the same 401 (no pxdb here, so every hash misses),
        // which is the half that makes presenting garbage no better than
        // presenting nothing.
        let mut h = host_hdrs("nosuch.example.test");
        h.insert(
            axum::http::header::AUTHORIZATION,
            HeaderValue::from_static("Bearer qk_live_nope"),
        );
        assert_eq!(
            unknown_host_refusal(&st, &h).await.status(),
            axum::http::StatusCode::UNAUTHORIZED
        );
    }

    #[tokio::test]
    async fn the_shared_host_itself_is_still_resolved_from_the_credential() {
        let st = st_with(&["shared.test"], Some("dev"));
        assert!(matches!(
            resolve_route(&st, &host_hdrs("shared.test")).await,
            Ok(Route::FromCredential)
        ));
        // Decision z's other half: a fully-qualified Host is the SAME host, and
        // must not fall through to `cache::resolve_host`, where the default
        // cluster lives.
        assert!(matches!(
            resolve_route(&st, &host_hdrs("shared.test.")).await,
            Ok(Route::FromCredential)
        ));
        assert!(matches!(
            resolve_route(&st, &host_hdrs("SHARED.test.:443")).await,
            Ok(Route::FromCredential)
        ));
    }

    #[tokio::test]
    async fn the_console_ignores_the_act_header_off_a_shared_host() {
        // The data plane has always honoured it; the console never did, and
        // widening it silently would widen an operator's reach from one cluster
        // to every cluster on the cell (`decide_act` -> Act(Admin) with no
        // membership row). With no pxdb the reference resolves to nothing, so
        // the two paths are told apart by WHICH refusal they produce: the
        // act-as path answers 403, the Host path 421.
        let st = st_with(&[], None);
        let mut h = host_hdrs("dev.example.test");
        h.insert(ACT_CLUSTER_HEADER, HeaderValue::from_static("other"));

        let data = resolve_route(&st, &h).await.err().expect("no cluster exists here");
        assert_eq!(data.status(), axum::http::StatusCode::FORBIDDEN, "the data plane honours it");

        let console = resolve_route_console(&st, &h).await.err().expect("no cluster exists here");
        assert_eq!(
            console.status(),
            axum::http::StatusCode::MISDIRECTED_REQUEST,
            "the console must have resolved by Host, ignoring the act-as header"
        );
    }

    #[tokio::test]
    async fn the_console_still_honours_the_act_header_on_a_shared_host() {
        // Where the Host names nothing, the header is the only way a session
        // can name its cluster — that is the whole single-URL console.
        let st = st_with(&["shared.test"], None);
        let mut h = host_hdrs("shared.test");
        h.insert(ACT_CLUSTER_HEADER, HeaderValue::from_static("other"));
        assert!(matches!(
            resolve_route_console(&st, &h).await,
            Ok(Route::FromCredential)
        ));
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

    // ---- the shared-host routing matrix (decision z) -----------------------

    const CL: &str = "11111111-1111-4111-8111-111111111111";

    fn cl() -> Uuid {
        Uuid::parse_str(CL).unwrap()
    }

    #[test]
    fn an_api_key_names_its_own_cluster_on_a_shared_host() {
        // The whole feature: no Host label, no header, just the key — and the
        // key's own row is what resolves the cluster.
        assert_eq!(decide_shared(CredKind::ApiKey, None, None), SharedRoute::ByKey);
    }

    #[test]
    fn a_missing_credential_on_a_shared_host_is_401_never_421() {
        // Decision z, the property SDKs depend on: 421 means "wrong host", and
        // on a shared host the host was right. Both with and without a header,
        // since a header is not a credential.
        assert_eq!(decide_shared(CredKind::None, None, None), SharedRoute::Unauthenticated);
        assert_eq!(decide_shared(CredKind::None, Some("acme"), None), SharedRoute::Unauthenticated);
        assert_eq!(
            shared_refusal(decide_shared(CredKind::None, None, None)).status(),
            axum::http::StatusCode::UNAUTHORIZED
        );
    }

    #[test]
    fn an_invalid_key_is_indistinguishable_from_a_missing_one_in_status() {
        // `ByKey` is the decision; the lookup that follows answers 401 for a
        // hash that matches no live row (resolve_from_credential). Pinned here
        // as the contract both halves must keep: 401, and never 421.
        assert_eq!(decide_shared(CredKind::ApiKey, None, None), SharedRoute::ByKey);
        assert_eq!(errors::err_401("unknown or revoked api key").status(), axum::http::StatusCode::UNAUTHORIZED);
    }

    #[test]
    fn a_key_is_never_retargeted_by_the_header_on_a_shared_host_either() {
        // The rule that keeps a leaked key worth one cluster and not all of
        // them holds on a shared host too — a shared host widens routing, not
        // authority.
        assert_eq!(
            decide_shared(CredKind::ApiKey, Some("other"), None),
            SharedRoute::RefuseApiKey
        );
        assert_eq!(
            shared_refusal(SharedRoute::RefuseApiKey).status(),
            axum::http::StatusCode::FORBIDDEN
        );
        // and the api key arm ignores a claim it can never have
        assert_eq!(
            decide_shared(CredKind::ApiKey, Some("other"), Some(cl())),
            SharedRoute::RefuseApiKey
        );
    }

    #[test]
    fn a_session_names_its_cluster_with_the_act_header() {
        assert_eq!(
            decide_shared(CredKind::Session, Some("acme"), None),
            SharedRoute::BySession("acme".to_string())
        );
    }

    #[test]
    fn a_scoped_session_token_names_its_own_cluster_without_a_header() {
        assert_eq!(
            decide_shared(CredKind::Session, None, Some(cl())),
            SharedRoute::BySession(CL.to_string())
        );
    }

    #[test]
    fn the_header_wins_over_the_claim_and_the_mismatch_is_caught_later() {
        // Resolution takes the header; `session_principal` then refuses a
        // scoped token pointed at a different cluster. Two steps on purpose:
        // the refusal must be identical to the one the per-cluster hostname
        // gives, and that one is expressed against a RESOLVED cluster.
        assert_eq!(
            decide_shared(CredKind::Session, Some("other"), Some(cl())),
            SharedRoute::BySession("other".to_string())
        );
    }

    #[test]
    fn a_session_that_names_no_cluster_is_403_not_401_and_not_421() {
        // The session is valid; what is missing is the choice. 401 would bounce
        // a logged-in user to the login page in a loop, and 421 would be a lie.
        assert_eq!(decide_shared(CredKind::Session, None, None), SharedRoute::NoClusterNamed);
        assert_eq!(
            shared_refusal(SharedRoute::NoClusterNamed).status(),
            axum::http::StatusCode::FORBIDDEN
        );
    }

    #[test]
    fn no_shared_refusal_is_ever_a_421() {
        // The one status the shared path must never produce, over the whole
        // refusal set — 421 is the answer for "this Host names no cluster",
        // and on a shared host that question is not being asked.
        for r in [
            SharedRoute::Unauthenticated,
            SharedRoute::RefuseApiKey,
            SharedRoute::NoClusterNamed,
            SharedRoute::ByKey,
            SharedRoute::BySession("acme".to_string()),
        ] {
            assert_ne!(
                shared_refusal(r.clone()).status(),
                axum::http::StatusCode::MISDIRECTED_REQUEST,
                "{r:?} must not answer 421 on a shared host"
            );
        }
    }

    #[test]
    fn the_shared_session_arm_reuses_the_act_matrix_unchanged() {
        // `decide_shared` only picks WHICH cluster; whether the caller may act
        // on it is still `decide_act`, so membership and operator rules cannot
        // drift between a shared host and a per-cluster one.
        assert_eq!(decide_act(CredKind::Session, false, true, Some(Role::Viewer)), ActDecision::Act(Role::Viewer));
        assert_eq!(decide_act(CredKind::Session, false, true, None), ActDecision::RefuseCluster);
        assert_eq!(decide_act(CredKind::Session, true, true, None), ActDecision::Act(Role::Admin));
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
