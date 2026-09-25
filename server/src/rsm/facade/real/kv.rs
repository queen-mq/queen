//! The KV receiver of the real facade (PLAN_RAFT.md WP-2.2): 024's surface
//! (`kv_apply_v1`, `kv_list_v1`, `kv_namespaces_v1`) on the RSM.
//!
//! The receiver's half of the split `rsm/planner/kv.rs` describes:
//!
//! 1. **Pass 1** — [`parse_ops`](kvp::parse_ops): every shape and budget
//!    refusal of 024, before anything is submitted. A refused call costs no
//!    planning and writes nothing.
//! 2. **Writes** — a call with at least one write is ONE [`Command::Kv`],
//!    planned serially and answered once its entry is committed AND applied on
//!    this node (I4). A call with none never enters the planner.
//! 3. **Reads** — of a call that writes: rendered by apply right after the
//!    call's own last effect, before any later command's
//!    ([`crate::rsm::kv_reads`]), so they see the call's own writes, as 024's
//!    phase ordering does, and nothing planned after it (reading the applied
//!    state once the entry had applied let two calls each read the other's
//!    write: Jepsen W4, G1c). Of a read-only call, or of a call answered from
//!    committed state (a request-id hit): evaluated here, off this node's
//!    applied state. Their bytes never enter the log (D7). One read budget
//!    per call, spent in 024's apply order.
//!
//! The store reads run on the blocking pool inside one read transaction (pin
//! 2, I15), under the request's deadline.

use crate::rsm::batcher::{Command, Reply};
use crate::rsm::entry::{KvOpOutcome, Outcome};
use crate::rsm::planner::kv::{self as kvp, KvCommand, KvInvalid, KvOp};
use crate::rsm::store::{Reads, Store, TypedReads};

use super::super::{KvFailure, KvListReq, KvOut, KvReq, ReqCtx, RsmError};
use super::{reply_error, wall_micros, RaftFacade};

fn invalid(e: KvInvalid) -> KvFailure {
    KvFailure::Invalid {
        status: e.status,
        reason: e.reason.to_string(),
        detail: e.detail,
    }
}

impl RaftFacade {
    /// `POST /api/v1/kv` and the path routes (024 `kv_apply_v1`, HTTP surface).
    pub(super) async fn kv_impl(&self, ctx: ReqCtx, req: KvReq) -> Result<KvOut, KvFailure> {
        let max_key = self.store.max_key_len();
        let ops = kvp::parse_ops(&req.ops, &ctx.tenant, false, max_key).map_err(invalid)?;
        if ops.is_empty() {
            return Ok(KvOut {
                results: Vec::new(),
            });
        }

        let pre: Vec<KvOpOutcome> = if ops.iter().any(KvOp::is_write) {
            let registered = ops
                .iter()
                .any(|op| !op.is_write())
                .then(|| crate::rsm::kv_reads::global().register(ctx.request_id, &ctx.tenant, &ops))
                .flatten();
            let cmd = Command::Kv(KvCommand {
                request_id: ctx.request_id,
                tenant: ctx.tenant.clone(),
                ops: ops.clone(),
            });
            let reply = self.submit(&ctx, cmd).await.map_err(KvFailure::Rsm)?;
            let (o, at) = match reply {
                Reply::Done {
                    outcome: Outcome::Kv(o),
                    at,
                } => (o, at),
                Reply::Done { outcome, .. } => {
                    return Err(KvFailure::Rsm(RsmError::Internal(format!(
                        "kv got a non-kv outcome: {outcome:?}"
                    ))))
                }
                // The planner's own client refusals speak 024's vocabulary: a
                // size (§5.1's 413) or a shape.
                Reply::Refused(r) if !r.retryable => {
                    let status = if r.code.contains("too_large") {
                        413
                    } else {
                        400
                    };
                    return Err(KvFailure::Invalid {
                        status,
                        reason: r.code,
                        detail: r.message,
                    });
                }
                other => return Err(KvFailure::Rsm(reply_error(other))),
            };
            if let Some(f) = &o.failed {
                return Err(KvFailure::Precondition {
                    detail: kvp::precondition_detail(&ops, f),
                });
            }
            // §6.4: index-aligned or a broken contract, never a short answer.
            if o.results.len() != ops.len() {
                return Err(KvFailure::Rsm(RsmError::Internal(
                    "kv_result_misaligned".into(),
                )));
            }
            // The entry applied on this node before the reply: its answer was
            // rendered at the call's position.
            if let (Some(reg), Some(_)) = (&registered, at) {
                if let Some(answer) = reg.take() {
                    return answer.map(|results| KvOut { results }).map_err(|e| {
                        KvFailure::Rsm(RsmError::Internal(format!("kv read: {e}")))
                    });
                }
            }
            o.results
        } else {
            // A call with a write reads after its own entry applied here, which
            // covers everything committed before it. A read-only call has no
            // entry: wait for the cluster's read index instead, or it can miss
            // a write another node already answered.
            self.linearizable(&ctx).await.map_err(KvFailure::Rsm)?;
            vec![KvOpOutcome::Deferred; ops.len()]
        };

        let results = self
            .kv_read(&ctx, move |r, tenant, now| {
                kvp::render_call(r, tenant, &ops, &pre, now)
            })
            .await?;
        Ok(KvOut { results })
    }

    /// `POST /api/v1/resources/kv/list` (`kv_list_v1`).
    pub(super) async fn kv_list_impl(
        &self,
        ctx: ReqCtx,
        req: KvListReq,
    ) -> Result<String, KvFailure> {
        // The namespace charset, and nothing else: the prefix is free text and
        // a prefix no key can carry is a page with no rows, not an error.
        kvp::check_namespace(Some(&req.namespace)).map_err(invalid)?;
        // CLAMPED, never refused (§5.5), here and only here.
        let limit = req
            .limit
            .map(|l| l.clamp(1, kvp::PREFIX_CAP as i64) as usize)
            .unwrap_or(kvp::PREFIX_DEFAULT);
        let after = req.after.filter(|a| !a.is_empty());
        self.linearizable(&ctx).await.map_err(KvFailure::Rsm)?;
        self.kv_read(&ctx, move |r, tenant, now| {
            let page = kvp::page_of(
                r,
                tenant,
                &req.namespace,
                &req.prefix,
                after.as_deref(),
                limit,
                req.keys_only,
                req.include_expired,
                now,
                kvp::MAX_READ_BYTES,
                kvp::ts_list,
            )?;
            Ok(serde_json::json!({
                "rows": page.rows,
                "truncated": page.truncated,
                "nextAfter": page.next_after,
                "bytes": page.bytes,
            })
            .to_string())
        })
        .await
    }

    /// `GET /api/v1/resources/kv/namespaces` (`kv_namespaces_v1`).
    pub(super) async fn kv_namespaces_impl(&self, ctx: ReqCtx) -> Result<String, KvFailure> {
        self.linearizable(&ctx).await.map_err(KvFailure::Rsm)?;
        self.kv_read(&ctx, |r, tenant, _now| {
            Ok(kvp::namespaces_of(r, tenant)?.to_string())
        })
        .await
    }

    /// Run `f` over ONE read transaction on the blocking pool (pin 2, I15),
    /// under the request's deadline, at one instant for the whole call (§5.7):
    /// the wall clock, never behind the RSM's own clock (D5), so a row the
    /// planner already judged expired is never read back alive.
    async fn kv_read<T: Send + 'static>(
        &self,
        ctx: &ReqCtx,
        f: impl FnOnce(&dyn Reads, &str, i64) -> crate::rsm::store::Result<T> + Send + 'static,
    ) -> Result<T, KvFailure> {
        let store = self.store.clone();
        let tenant = ctx.tenant.clone();
        let wall = wall_micros();
        let task = tokio::task::spawn_blocking(move || {
            store.read(|r| {
                let now = wall.max(r.last_now_us()?);
                f(r, &tenant, now)
            })
        });
        match tokio::time::timeout(ctx.deadline.remaining(), task).await {
            Ok(Ok(Ok(v))) => Ok(v),
            Ok(Ok(Err(e))) if e.retryable() => {
                Err(KvFailure::Rsm(RsmError::Retry { leader_hint: None }))
            }
            Ok(Ok(Err(e))) => Err(KvFailure::Rsm(RsmError::Internal(format!("kv read: {e}")))),
            Ok(Err(join)) => Err(KvFailure::Rsm(RsmError::Internal(format!(
                "kv read task: {join}"
            )))),
            Err(_) => Err(KvFailure::Rsm(RsmError::Timeout)),
        }
    }

    /// Test-only: the raw row count of the two KV keyspaces, expired rows
    /// included — what the sweep is judged by.
    #[cfg(test)]
    pub(crate) fn kv_rows_physical(&self) -> (u64, u64) {
        use crate::rsm::store::Keyspace;
        self.store
            .read(|r| Ok((r.count(Keyspace::Kv)?, r.count(Keyspace::KvExpiry)?)))
            .expect("count kv rows")
    }
}
