//! Tenants spread over several Raft groups in one process
//! (`QUEEN_RAFT_GROUPS`, default 1).
//!
//! Each group is a whole Queen state machine — its own log, store, batcher,
//! leader and data directory — and a tenant lives in exactly one group. The
//! Queen contract makes that sound with no cross-group protocol: a command
//! (a transaction included) carries ONE tenant, so it only ever touches its
//! own group; tenants never share a queue, a log or a leader election, and
//! each group scales (and fails over) on its own.
//!
//! Placement is a pure function every node computes the same way: an explicit
//! `QUEEN_TENANT_GROUPS=tenant=group,...` entry, else a stable hash of the
//! tenant name. A tenant's group never changes while it has data (there is no
//! migration yet), so the override is for NEW tenants: pin a big tenant to its
//! own group before it writes.
//!
//! On a cluster, group `g`'s Raft RPCs listen on the node's raft port + `g`,
//! and group `g` prefers the member at position `g mod n` as its leader, so
//! with as many groups as nodes every node leads one group and follows the
//! others: the followers are no longer idle.
//!
//! [`GroupRouter`] implements [`Rsm`] by dispatching every call on its
//! tenant, so the HTTP layer is unchanged.
//!
//! # A standby with several groups
//!
//! The cluster link ([`crate::rsm::link`]) is group by group: a standby's
//! group `g` replays the source's group `g`, read on the source's Raft port
//! plus `g`, and each group has its own position in its own source log. That
//! holds a tenant where this router looks for it only while both clusters
//! place tenants the same way, so every link call carries the group's
//! [`placement`] and the source refuses another's.
//!
//! Nothing ties the groups' positions together, and nothing has to: no
//! command touches two groups, so every tenant's state is a prefix of one
//! log. A promoted standby holds each group up to the point that group had
//! reached, which is not one instant of the source.
//!
//! The link's two routes speak for the whole node — its status lists every
//! group, a promotion promotes every group ([`GroupRouter::link_promote`]) —
//! where every other route is one tenant's and so one group's.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;

use super::*;
use crate::rsm::link::wire::Placement;

/// `QUEEN_RAFT_GROUPS`: how many Raft groups this node runs (default 1, at
/// most 64). Every node of a cluster must use the same value.
pub fn groups_from_env() -> usize {
    std::env::var("QUEEN_RAFT_GROUPS")
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .unwrap_or(1)
        .clamp(1, 64)
}

/// `QUEEN_TENANT_GROUPS`: `tenant=group,...` placements that override the hash.
pub fn overrides_from_env(groups: usize) -> Result<HashMap<String, usize>, String> {
    let raw = std::env::var("QUEEN_TENANT_GROUPS").unwrap_or_default();
    let mut out = HashMap::new();
    for part in raw.split(',').map(str::trim).filter(|p| !p.is_empty()) {
        let (tenant, g) = part
            .rsplit_once('=')
            .ok_or_else(|| format!("QUEEN_TENANT_GROUPS: `{part}` is not `tenant=group`"))?;
        let g: usize = g
            .trim()
            .parse()
            .map_err(|_| format!("QUEEN_TENANT_GROUPS: `{g}` is not a group number"))?;
        if g >= groups {
            return Err(format!(
                "QUEEN_TENANT_GROUPS: group {g} for `{tenant}`, but QUEEN_RAFT_GROUPS={groups}"
            ));
        }
        out.insert(tenant.trim().to_string(), g);
    }
    Ok(out)
}

/// The group `tenant` lives in: the override, else a stable hash.
pub fn group_of(tenant: &str, groups: usize, overrides: &HashMap<String, usize>) -> usize {
    if let Some(g) = overrides.get(tenant) {
        return *g;
    }
    if groups <= 1 {
        return 0;
    }
    (xxhash_rust::xxh3::xxh3_64(tenant.as_bytes()) % groups as u64) as usize
}

/// Group `group`'s [`Placement`] in a cluster of `groups`: what a link call
/// of that group states.
///
/// The pins are a digest of the `overrides` that put a tenant somewhere else
/// than its name does. A pin that names the group the hash gives changes
/// nothing and is left out, so two clusters that write their pins differently
/// and place every tenant alike still link.
pub fn placement(group: usize, groups: usize, overrides: &HashMap<String, usize>) -> Placement {
    let none = HashMap::new();
    let mut moved: Vec<(&str, usize)> = overrides
        .iter()
        .filter(|(tenant, g)| group_of(tenant, groups, &none) != **g)
        .map(|(tenant, g)| (tenant.as_str(), *g))
        .collect();
    moved.sort_unstable();
    let pins = if moved.is_empty() {
        String::new()
    } else {
        // `tenant=group`, a line each: the group is digits, so the last `=`
        // of a line ends the tenant's name whatever the name holds.
        let mut text = String::new();
        for (tenant, g) in moved {
            text.push_str(&format!("{tenant}={g}\n"));
        }
        format!("{:016x}", xxhash_rust::xxh3::xxh3_64(text.as_bytes()))
    };
    Placement {
        group,
        groups,
        pins,
    }
}

/// Several groups behind one [`Rsm`].
pub struct GroupRouter {
    groups: Vec<Arc<dyn Rsm>>,
    overrides: HashMap<String, usize>,
}

/// Group `g`'s part of an answer about the cluster link: what the group
/// answered, with its number — and its status when that is not 200, so a
/// group that could not answer reads as one beside those that did.
fn link_part(g: usize, out: Result<ApiOut, RsmError>) -> (u16, serde_json::Value) {
    let (status, body) = match out {
        Ok(out) => (out.status, serde_json::from_str(&out.body).ok()),
        Err(e) => (
            503,
            Some(serde_json::json!({"code": e.code(), "error": e.to_string()})),
        ),
    };
    let mut part = match body {
        Some(serde_json::Value::Object(fields)) => serde_json::Value::Object(fields),
        _ => serde_json::json!({}),
    };
    part["group"] = serde_json::json!(g);
    if status != 200 {
        part["status"] = serde_json::json!(status);
    }
    (status, part)
}

/// What the groups' parts say together, in the shape one group's answer has:
/// a `role`, and every group's own part under `groups`. The role is the one
/// every group has; `mixed` when they differ or one did not say — a standby
/// promoted in part, or made of groups that were not all standbys.
fn link_of(parts: Vec<serde_json::Value>) -> serde_json::Value {
    let mut roles = parts.iter().map(|p| p["role"].as_str());
    let role = match roles.next().flatten() {
        Some(role) if roles.all(|r| r == Some(role)) => role.to_string(),
        _ => "mixed".to_string(),
    };
    let mut out = serde_json::Map::new();
    out.insert("role".into(), role.into());
    out.insert("groups".into(), serde_json::Value::Array(parts));
    serde_json::Value::Object(out)
}

impl GroupRouter {
    pub fn new(groups: Vec<Arc<dyn Rsm>>, overrides: HashMap<String, usize>) -> GroupRouter {
        assert!(!groups.is_empty(), "a group router needs a group");
        GroupRouter { groups, overrides }
    }

    fn pick(&self, tenant: &str) -> &Arc<dyn Rsm> {
        &self.groups[group_of(tenant, self.groups.len(), &self.overrides)]
    }

    /// One of the link's two routes, asked of every group at once. 200 when
    /// every group answered 200, else the first other status; the body lists
    /// every group's answer either way.
    async fn link_route(&self, ctx: ReqCtx, req: ApiReq) -> Result<ApiOut, RsmError> {
        let answers = futures_util::future::join_all(
            self.groups.iter().map(|g| g.api(ctx.clone(), req.clone())),
        )
        .await;
        let mut status = 200u16;
        let mut parts = Vec::with_capacity(answers.len());
        for (g, out) in answers.into_iter().enumerate() {
            let (s, part) = link_part(g, out);
            if s != 200 && status == 200 {
                status = s;
            }
            parts.push(part);
        }
        Ok(ApiOut::json(status, link_of(parts).to_string()))
    }

    /// `POST /api/v1/system/link/promote` on a node of several groups: every
    /// group is promoted, each by its own leader, at the point its own replay
    /// had reached.
    ///
    /// The groups are asked together and each answers for itself, so one that
    /// cannot be promoted now (no leader, a promotion already in flight)
    /// keeps none of the others waiting: the answer names it, the standby is
    /// `mixed` meanwhile — the promoted groups serve their tenants, the
    /// others still refuse theirs as a standby does — and the call is
    /// repeated. Promoting a promoted group again changes nothing.
    async fn link_promote(&self, ctx: ReqCtx, req: ApiReq) -> Result<ApiOut, RsmError> {
        let out = self.link_route(ctx, req).await?;
        if out.status != 200 {
            tracing::error!(
                target: "rsm",
                status = out.status,
                answer = %out.body,
                "rsm link: not every raft group of this standby was promoted: repeat the call",
            );
        }
        Ok(out)
    }
}

/// Add `group="g"` to every sample line of a Prometheus exposition, so the
/// groups' identical metric names stay distinct series.
fn label_group(text: &str, g: usize, keep_meta: bool) -> String {
    let mut out = String::with_capacity(text.len() + text.len() / 8);
    for line in text.lines() {
        if line.starts_with('#') {
            if keep_meta {
                out.push_str(line);
                out.push('\n');
            }
            continue;
        }
        if line.trim().is_empty() {
            continue;
        }
        // name{labels} value  |  name value
        let name_end = line.find(['{', ' ']).unwrap_or(line.len());
        let (name, rest) = line.split_at(name_end);
        out.push_str(name);
        if let Some(inner) = rest.strip_prefix('{') {
            out.push_str(&format!("{{group=\"{g}\","));
            out.push_str(inner);
        } else {
            out.push_str(&format!("{{group=\"{g}\"}}"));
            out.push_str(rest);
        }
        out.push('\n');
    }
    out
}

#[async_trait]
impl Rsm for GroupRouter {
    fn bootstrap(&self) -> RsmBootstrap {
        self.groups[0].bootstrap()
    }

    fn route(&self) -> Route {
        // Several groups need every node to serve its own clients
        // (`QUEEN_RAFT_CLIENT_OFFLOAD`): the route is per group, and the HTTP
        // layer asks before it knows the tenant.
        Route::Local
    }

    async fn push(&self, ctx: ReqCtx, req: PushReq) -> Result<PushOut, RsmError> {
        self.pick(&ctx.tenant).push(ctx, req).await
    }
    async fn push_records(
        &self,
        ctx: ReqCtx,
        items: Vec<RecordPush>,
    ) -> Result<Vec<RecordPushed>, RsmError> {
        self.pick(&ctx.tenant).push_records(ctx, items).await
    }
    async fn fetch_records(
        &self,
        ctx: ReqCtx,
        entries: Vec<RecordFetch>,
        max_wait_ms: u64,
        min_bytes: usize,
    ) -> Result<Vec<RecordsFetched>, RsmError> {
        self.pick(&ctx.tenant)
            .fetch_records(ctx, entries, max_wait_ms, min_bytes)
            .await
    }
    async fn fetch_log(
        &self,
        ctx: ReqCtx,
        entries: Vec<RecordFetch>,
        max_wait_ms: u64,
        min_bytes: usize,
    ) -> Result<Vec<RecordsFetched>, RsmError> {
        self.pick(&ctx.tenant)
            .fetch_log(ctx, entries, max_wait_ms, min_bytes)
            .await
    }
    async fn partitions_changed(
        &self,
        ctx: ReqCtx,
        asks: Vec<ChangedAsk>,
    ) -> Result<PartitionsChanged, RsmError> {
        self.pick(&ctx.tenant).partitions_changed(ctx, asks).await
    }
    async fn pop_wildcard(&self, ctx: ReqCtx, req: PopReq) -> Result<PopOut, RsmError> {
        self.pick(&ctx.tenant).pop_wildcard(ctx, req).await
    }
    async fn pop_pinned(&self, ctx: ReqCtx, req: PopPinnedReq) -> Result<PopOut, RsmError> {
        self.pick(&ctx.tenant).pop_pinned(ctx, req).await
    }
    async fn pop_discover(&self, ctx: ReqCtx, req: PopDiscoverReq) -> Result<PopOut, RsmError> {
        self.pick(&ctx.tenant).pop_discover(ctx, req).await
    }
    async fn ack(&self, ctx: ReqCtx, req: AckReq) -> Result<AckOut, RsmError> {
        self.pick(&ctx.tenant).ack(ctx, req).await
    }
    async fn renew(&self, ctx: ReqCtx, req: RenewReq) -> Result<RenewOut, RsmError> {
        self.pick(&ctx.tenant).renew(ctx, req).await
    }
    async fn transaction(&self, ctx: ReqCtx, req: TxnReq) -> Result<TxnOut, RsmError> {
        self.pick(&ctx.tenant).transaction(ctx, req).await
    }
    async fn dlq_head(&self, ctx: ReqCtx, req: DlqHeadReq) -> Result<DlqHeadOut, RsmError> {
        self.pick(&ctx.tenant).dlq_head(ctx, req).await
    }
    async fn has_pending(&self, ctx: ReqCtx, req: PendingReq) -> Result<bool, RsmError> {
        self.pick(&ctx.tenant).has_pending(ctx, req).await
    }
    async fn depth(&self, ctx: ReqCtx, req: DepthReq) -> Result<DepthOut, RsmError> {
        self.pick(&ctx.tenant).depth(ctx, req).await
    }
    async fn kv(&self, ctx: ReqCtx, req: KvReq) -> Result<KvOut, KvFailure> {
        self.pick(&ctx.tenant).kv(ctx, req).await
    }
    async fn kv_list(&self, ctx: ReqCtx, req: KvListReq) -> Result<String, KvFailure> {
        self.pick(&ctx.tenant).kv_list(ctx, req).await
    }
    async fn kv_namespaces(&self, ctx: ReqCtx) -> Result<String, KvFailure> {
        self.pick(&ctx.tenant).kv_namespaces(ctx).await
    }
    async fn timers_apply(&self, ctx: ReqCtx, req: TimersReq) -> Result<TimersOut, RsmError> {
        self.pick(&ctx.tenant).timers_apply(ctx, req).await
    }
    async fn timer_peek(&self, ctx: ReqCtx, req: TimerPeekReq) -> Result<TimerReadOut, RsmError> {
        self.pick(&ctx.tenant).timer_peek(ctx, req).await
    }
    async fn timers_list(&self, ctx: ReqCtx, req: TimersListReq) -> Result<TimerReadOut, RsmError> {
        self.pick(&ctx.tenant).timers_list(ctx, req).await
    }
    async fn timers_count(
        &self,
        ctx: ReqCtx,
        req: TimersCountReq,
    ) -> Result<TimerReadOut, RsmError> {
        self.pick(&ctx.tenant).timers_count(ctx, req).await
    }
    async fn api(&self, ctx: ReqCtx, req: ApiReq) -> Result<ApiOut, RsmError> {
        // The cluster link is the node's, not a tenant's: every group has its
        // own part in it.
        match (req.method.as_str(), req.path.as_str()) {
            ("GET", crate::rsm::link::STATUS_ROUTE) => return self.link_route(ctx, req).await,
            ("POST", crate::rsm::link::PROMOTE_ROUTE) => return self.link_promote(ctx, req).await,
            _ => {}
        }
        self.pick(&ctx.tenant).api(ctx, req).await
    }

    /// Every group's membership: each group is a Raft cluster of its own.
    async fn raft_membership(&self, ctx: ReqCtx) -> Result<ApiOut, RsmError> {
        let mut groups = Vec::with_capacity(self.groups.len());
        for (g, r) in self.groups.iter().enumerate() {
            let out = r.raft_membership(ctx.clone()).await?;
            groups.push(serde_json::json!({
                "group": g,
                "status": out.status,
                "body": serde_json::from_str::<serde_json::Value>(&out.body)
                    .unwrap_or(serde_json::Value::String(out.body)),
            }));
        }
        Ok(ApiOut::json(
            200,
            serde_json::json!({"engine": "raft", "groups": groups}).to_string(),
        ))
    }

    /// A membership change is a change of every group: each is a Raft
    /// cluster of its own over the same nodes. Made group by group, in order,
    /// each on its own leader; group `g` reaches a node on its Raft port plus
    /// `g` (`cluster::group_raft_addr`). The answer lists every group's; the
    /// changes are idempotent, so a caller repeats the whole call after a
    /// group that failed.
    async fn raft_change_membership(
        &self,
        ctx: ReqCtx,
        change: crate::rsm::replicator::MembershipChange,
    ) -> Result<ApiOut, RsmError> {
        use crate::rsm::replicator::MembershipChange;
        let mut groups = Vec::with_capacity(self.groups.len());
        let mut status = 200u16;
        for (g, r) in self.groups.iter().enumerate() {
            let this = match &change {
                MembershipChange::AddLearner { node, raft, http } => {
                    let raft = crate::rsm::replicator::raft::cluster::group_raft_addr(raft, g)
                        .map_err(|e| RsmError::Rejected {
                            code: "bad_request".into(),
                            message: e,
                        })?;
                    MembershipChange::AddLearner {
                        node: *node,
                        raft,
                        http: http.clone(),
                    }
                }
                other => other.clone(),
            };
            let out = match r.raft_change_membership(ctx.clone(), this).await {
                Ok(out) => out,
                Err(e) => ApiOut::json(
                    503,
                    serde_json::json!({"ok": false, "code": e.code(), "error": e.to_string()})
                        .to_string(),
                ),
            };
            if !(200..300).contains(&out.status) && status == 200 {
                status = out.status;
            }
            groups.push(serde_json::json!({
                "group": g,
                "status": out.status,
                "body": serde_json::from_str::<serde_json::Value>(&out.body)
                    .unwrap_or(serde_json::Value::String(out.body)),
            }));
            if status != 200 {
                // Later groups are not changed: the call is repeated whole.
                break;
            }
        }
        Ok(ApiOut::json(
            status,
            serde_json::json!({"engine": "raft", "ok": status == 200, "groups": groups})
                .to_string(),
        ))
    }

    /// Ready only when every group is: a tenant on a group without a leader
    /// cannot be served.
    fn leads(&self, tenant: &str) -> bool {
        self.pick(tenant).leads(tenant)
    }

    fn leader_node(&self, tenant: &str) -> Option<u64> {
        self.pick(tenant).leader_node(tenant)
    }

    fn health(&self) -> RaftHealth {
        let mut all: Vec<RaftHealth> = self.groups.iter().map(|g| g.health()).collect();
        let applies: Vec<serde_json::Value> = all
            .iter()
            .enumerate()
            .filter_map(|(g, h)| {
                h.apply
                    .clone()
                    .map(|a| serde_json::json!({"group": g, "apply": a}))
            })
            .collect();
        // The link, when any group has a part in one: every group's, in the
        // shape the link's own route answers with. A group with none is an
        // ordinary cluster that follows nobody and that nobody reads.
        let link = all.iter().any(|h| h.link.is_some()).then(|| {
            link_of(
                all.iter()
                    .enumerate()
                    .map(|(g, h)| {
                        let mut part = match &h.link {
                            Some(serde_json::Value::Object(fields)) => {
                                serde_json::Value::Object(fields.clone())
                            }
                            _ => serde_json::json!({"role": "primary"}),
                        };
                        part["group"] = serde_json::json!(g);
                        part
                    })
                    .collect(),
            )
        });
        let mut h = all.remove(0);
        h.link = link;
        for o in all {
            h.leader_known &= o.leader_known;
            h.storage_ready &= o.storage_ready;
            h.lag_ms = h.lag_ms.max(o.lag_ms);
            h.quorum_ack_ms = match (h.quorum_ack_ms, o.quorum_ack_ms) {
                (Some(a), Some(b)) => Some(a.max(b)),
                _ => None,
            };
            // Each group has its own cluster version: the lowest is what every
            // group writes.
            h.cluster_version = match (h.cluster_version, o.cluster_version) {
                (Some(a), Some(b)) => Some(a.min(b)),
                (a, b) => a.or(b),
            };
            if o.role == "leader" && h.role != "leader" {
                h.role = format!("{}+leader", h.role);
            }
        }
        h.apply = (!applies.is_empty()).then_some(serde_json::Value::Array(applies));
        h
    }

    fn dlq_depth_by_queue(&self) -> Vec<(String, String, i64)> {
        // A tenant lives in exactly one group, so the groups' lists never
        // name the same queue twice.
        self.groups
            .iter()
            .flat_map(|g| g.dlq_depth_by_queue())
            .collect()
    }

    fn prometheus(&self) -> String {
        let mut out = String::new();
        for (g, r) in self.groups.iter().enumerate() {
            out.push_str(&label_group(&r.prometheus(), g, g == 0));
        }
        out
    }

    fn notifier(&self) -> Option<&Arc<Notifier>> {
        self.groups[0].notifier()
    }

    fn cluster_id(&self) -> Option<String> {
        self.groups[0].cluster_id()
    }

    async fn hand_off_leadership(&self, wait: std::time::Duration) {
        futures_util::future::join_all(self.groups.iter().map(|g| g.hand_off_leadership(wait)))
            .await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn placement_is_stable_and_overridable() {
        let none = HashMap::new();
        assert_eq!(group_of("anything", 1, &none), 0);
        let a = group_of("acme", 3, &none);
        assert_eq!(a, group_of("acme", 3, &none), "stable");
        assert!(a < 3);
        let mut o = HashMap::new();
        o.insert("acme".to_string(), (a + 1) % 3);
        assert_eq!(group_of("acme", 3, &o), (a + 1) % 3);
        // Spread: 300 tenants over 3 groups, none empty.
        let mut n = [0usize; 3];
        for i in 0..300 {
            n[group_of(&format!("t{i}"), 3, &none)] += 1;
        }
        assert!(n.iter().all(|c| *c > 50), "{n:?}");
    }

    #[test]
    fn overrides_parse_and_refuse_out_of_range() {
        std::env::remove_var("QUEEN_TENANT_GROUPS");
        assert!(overrides_from_env(3).unwrap().is_empty());
    }

    #[test]
    fn a_groups_placement_names_only_the_pins_that_move_a_tenant() {
        let none = HashMap::new();
        assert_eq!(placement(0, 1, &none), Placement::single());
        let p = placement(2, 3, &none);
        assert_eq!((p.group, p.groups, p.pins.as_str()), (2, 3, ""));

        // A tenant pinned where its name puts it anyway is not a pin.
        let home = group_of("acme", 3, &none);
        let mut kept = HashMap::new();
        kept.insert("acme".to_string(), home);
        assert_eq!(placement(0, 3, &kept), placement(0, 3, &none));

        // One pinned elsewhere is, the same from every group of the cluster
        // and whatever else is written beside it.
        let mut moved = HashMap::new();
        moved.insert("acme".to_string(), (home + 1) % 3);
        let pins = placement(0, 3, &moved).pins;
        assert_eq!(pins.len(), 16, "{pins}");
        assert_eq!(placement(1, 3, &moved).pins, pins);
        let mut beside = moved.clone();
        beside.insert("umbrella".to_string(), group_of("umbrella", 3, &none));
        assert_eq!(placement(0, 3, &beside).pins, pins);

        // Another group for it, or another tenant, is another placement.
        let mut other = HashMap::new();
        other.insert("acme".to_string(), (home + 2) % 3);
        assert_ne!(placement(0, 3, &other).pins, pins);
        let mut two = moved.clone();
        two.insert(
            "umbrella".to_string(),
            (group_of("umbrella", 3, &none) + 1) % 3,
        );
        assert_ne!(placement(0, 3, &two).pins, pins);
    }

    #[test]
    fn the_groups_parts_of_a_link_answer_read_as_one() {
        use serde_json::json;
        let ok = |body: serde_json::Value| Ok(ApiOut::json(200, body.to_string()));

        // Every group a standby: the node is one, and each part keeps what
        // its group said, numbered.
        let (s0, p0) = link_part(0, ok(json!({"role": "standby", "leader": true})));
        let (s1, p1) = link_part(1, ok(json!({"role": "standby", "leader": false})));
        assert_eq!((s0, s1), (200, 200));
        assert_eq!(p1, json!({"group": 1, "role": "standby", "leader": false}));
        let all = link_of(vec![p0.clone(), p1]);
        assert_eq!(all["role"], "standby");
        assert_eq!(all["groups"].as_array().map(Vec::len), Some(2));
        assert_eq!(all["groups"][0], p0);

        // One promoted and one not: neither a standby nor promoted.
        let (_, promoted) = link_part(0, ok(json!({"role": "promoted"})));
        let (_, standby) = link_part(1, ok(json!({"role": "standby"})));
        assert_eq!(link_of(vec![promoted.clone(), standby])["role"], "mixed");

        // A group that could not answer says so in its part, and the whole
        // is not what the others are.
        let (status, part) = link_part(1, Err(RsmError::Retry { leader_hint: None }));
        assert_eq!(status, 503);
        assert_eq!((&part["group"], &part["status"]), (&json!(1), &json!(503)));
        assert_eq!(part["code"], "retry");
        assert_eq!(link_of(vec![promoted, part])["role"], "mixed");
        let refused = ApiOut::json(409, json!({"code": "not_standby"}).to_string());
        let (status, part) = link_part(0, Ok(refused));
        assert_eq!((status, &part["status"]), (409, &json!(409)));
        assert_eq!(part["code"], "not_standby");
    }

    #[test]
    fn prometheus_lines_get_a_group_label() {
        let t = "# HELP x a\n# TYPE x counter\nx 1\ny{a=\"b\"} 2\n";
        assert_eq!(
            label_group(t, 2, true),
            "# HELP x a\n# TYPE x counter\nx{group=\"2\"} 1\ny{group=\"2\",a=\"b\"} 2\n"
        );
        assert_eq!(
            label_group(t, 1, false),
            "x{group=\"1\"} 1\ny{group=\"1\",a=\"b\"} 2\n"
        );
    }
}
