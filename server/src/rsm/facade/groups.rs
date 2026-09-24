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

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;

use super::*;

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

/// Several groups behind one [`Rsm`].
pub struct GroupRouter {
    groups: Vec<Arc<dyn Rsm>>,
    overrides: HashMap<String, usize>,
}

impl GroupRouter {
    pub fn new(groups: Vec<Arc<dyn Rsm>>, overrides: HashMap<String, usize>) -> GroupRouter {
        assert!(!groups.is_empty(), "a group router needs a group");
        GroupRouter { groups, overrides }
    }

    fn pick(&self, tenant: &str) -> &Arc<dyn Rsm> {
        &self.groups[group_of(tenant, self.groups.len(), &self.overrides)]
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
        let name_end = line
            .find(|c: char| c == '{' || c == ' ')
            .unwrap_or(line.len());
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
    async fn timers_list(
        &self,
        ctx: ReqCtx,
        req: TimersListReq,
    ) -> Result<TimerReadOut, RsmError> {
        self.pick(&ctx.tenant).timers_list(ctx, req).await
    }
    async fn timers_count(
        &self,
        ctx: ReqCtx,
        req: TimersCountReq,
    ) -> Result<TimerReadOut, RsmError> {
        self.pick(&ctx.tenant).timers_count(ctx, req).await
    }
    async fn kafka_append(
        &self,
        ctx: ReqCtx,
        parts: Vec<KafkaAppendReq>,
    ) -> Result<Vec<Result<u64, RsmError>>, RsmError> {
        self.pick(&ctx.tenant).kafka_append(ctx, parts).await
    }
    async fn kafka_read(
        &self,
        ctx: ReqCtx,
        asks: Vec<KafkaReadReq>,
        max_wait_ms: u64,
        min_bytes: usize,
    ) -> Result<Vec<KafkaReadOut>, RsmError> {
        self.pick(&ctx.tenant)
            .kafka_read(ctx, asks, max_wait_ms, min_bytes)
            .await
    }
    async fn api(&self, ctx: ReqCtx, req: ApiReq) -> Result<ApiOut, RsmError> {
        self.pick(&ctx.tenant).api(ctx, req).await
    }

    /// Ready only when every group is: a tenant on a group without a leader
    /// cannot be served.
    fn health(&self) -> RaftHealth {
        let mut all: Vec<RaftHealth> = self.groups.iter().map(|g| g.health()).collect();
        let mut h = all.remove(0);
        for o in all {
            h.leader_known &= o.leader_known;
            h.storage_ready &= o.storage_ready;
            h.lag_ms = h.lag_ms.max(o.lag_ms);
            if o.role == "leader" && h.role != "leader" {
                h.role = format!("{}+leader", h.role);
            }
        }
        h
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
    fn prometheus_lines_get_a_group_label() {
        let t = "# HELP x a\n# TYPE x counter\nx 1\ny{a=\"b\"} 2\n";
        assert_eq!(
            label_group(t, 2, true),
            "# HELP x a\n# TYPE x counter\nx{group=\"2\"} 1\ny{group=\"2\",a=\"b\"} 2\n"
        );
        assert_eq!(label_group(t, 1, false), "x{group=\"1\"} 1\ny{group=\"1\",a=\"b\"} 2\n");
    }
}
