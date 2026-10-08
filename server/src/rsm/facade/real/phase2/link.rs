//! The cluster link's routes ([`crate::rsm::link`]): what this node knows of
//! the link, and a standby's promotion.

use serde_json::{json, Value};

use super::{read_error, ApiOut, RaftFacade, ReqCtx, RsmError};
use crate::rsm::batcher::{Command, Reply, NOT_STANDBY_CODE};
use crate::rsm::link::driver::Status;
use crate::rsm::link::{self, Position, Role};
use crate::rsm::replicator::Replicator;
use crate::rsm::store::Store;

fn position_json(p: Position) -> Value {
    json!({"index": p.index, "term": p.term, "nowUs": p.now_us})
}

/// The follower's part of the status, at `now_us`.
fn follower_json(s: &Status, now_us: i64) -> Value {
    json!({
        "state": s.state.name(),
        "source": s.source,
        "scanned": s.scanned,
        "sourceApplied": s.source_applied,
        "lagEntries": s.lag_entries(),
        "lagMs": s.lag_us(now_us) / 1000,
        "entries": s.entries,
        "lastAnswerMs": (s.last_answer_us > 0).then(|| (now_us - s.last_answer_us).max(0) / 1000),
        "error": s.error,
    })
}

impl RaftFacade {
    /// What this node knows of the cluster link: the cluster's role and
    /// position from its own applied state, its follower's state (which
    /// follows only on the node that leads a standby), and the standbys
    /// reading THIS node's log.
    fn link_view(&self) -> Result<Option<Value>, crate::rsm::store::StoreError> {
        let (role, position) = self
            .store
            .read(|r| Ok((link::read_role(r)?, link::read_position(r)?)))?;
        let source = self.repl.link_source();
        let readers = source.as_ref().map(|s| s.readers()).unwrap_or_default();
        // An ordinary cluster that follows nobody and that nobody reads: no
        // link to speak of.
        if matches!(role, Role::Primary) && self.link_follower.is_none() && readers.is_empty() {
            return Ok(None);
        }
        let now_us = super::super::wall_micros();
        let mut out = json!({ "role": role.name() });
        match &role {
            Role::Primary => {}
            Role::Standby(doc) => {
                out["id"] = json!(doc.id);
                // What the source's `readers` call this standby.
                out["name"] = json!(link::reader_name(
                    self.link_source.as_ref().and_then(|c| c.name.as_deref()),
                    &doc.id,
                ));
                out["source"] = json!(doc.source);
                out["sinceUs"] = json!(doc.wall_us);
                out["position"] = position_json(position);
            }
            Role::Promoted(doc) => {
                out["id"] = json!(doc.id);
                out["source"] = json!(doc.source);
                out["promotedAtUs"] = json!(doc.wall_us);
                // The last source entry applied before the promotion: what
                // the source committed after it is not in this cluster.
                out["position"] = match doc.position {
                    Some(p) => json!({"index": p.index, "term": p.term, "nowUs": p.now_us}),
                    None => position_json(position),
                };
            }
        }
        // A promoted cluster's follower has nothing left to do.
        if let (Some(f), false) = (&self.link_follower, matches!(role, Role::Promoted(_))) {
            out["follower"] = follower_json(&f.status(), now_us);
        }
        if let Some(cfg) = &self.link_source {
            out["configuredSource"] = json!(cfg.sources);
        }
        if !readers.is_empty() {
            out["readers"] = readers
                .iter()
                .map(|r| json!({"name": r.name, "after": r.after, "idleMs": r.idle.as_millis() as u64}))
                .collect();
            // `false`: this node's data volume is too full to keep its log
            // for them (`QUEEN_LINK_HOLD_DISK_PCT`).
            out["holding"] = json!(source.as_ref().is_none_or(|s| s.holding()));
        }
        Ok(Some(out))
    }

    /// `/health`'s `link` block ([`crate::rsm::facade::RaftHealth::link`]).
    pub(in crate::rsm::facade::real) fn link_health(&self) -> Option<Value> {
        self.link_view().ok().flatten()
    }

    /// The link's part of `/metrics/prometheus`. Nothing on a cluster that
    /// follows nobody and that nobody reads.
    ///
    /// A standby's follower works on its leader only: the other nodes of a
    /// standby report `idle` and no lag, so a query takes the cluster's
    /// maximum. The lag is what the source last ANSWERED; a source that
    /// stopped answering shows in `queen_link_last_answer_seconds`.
    pub(in crate::rsm::facade::real) fn link_prometheus(&self) -> String {
        use crate::metrics::escape_label;
        use crate::rsm::link::driver::State;

        let Ok((role, position)) = self
            .store
            .read(|r| Ok((link::read_role(r)?, link::read_position(r)?)))
        else {
            return String::new();
        };
        let source = self.repl.link_source();
        let readers = source.as_ref().map(|s| s.readers()).unwrap_or_default();
        if matches!(role, Role::Primary) && self.link_follower.is_none() && readers.is_empty() {
            return String::new();
        }
        let mut out = String::new();
        out.push_str("# HELP queen_link_role This cluster's part in a cluster link: 1 on the role it has\n# TYPE queen_link_role gauge\n");
        for name in ["primary", "standby", "promoted"] {
            out.push_str(&format!(
                "queen_link_role{{role=\"{name}\"}} {}\n",
                u8::from(role.name() == name)
            ));
        }
        if !matches!(role, Role::Primary) {
            out.push_str("# HELP queen_link_position_index The index, in the source's log, of the last source entry this cluster applied as a standby\n# TYPE queen_link_position_index gauge\n");
            out.push_str(&format!("queen_link_position_index {}\n", position.index));
        }
        if let (Some(f), false) = (&self.link_follower, matches!(role, Role::Promoted(_))) {
            let s = f.status();
            let now_us = super::super::wall_micros();
            out.push_str("# HELP queen_link_follower_state What this node's follower of the source is doing: 1 on its state (it follows on the standby's leader only)\n# TYPE queen_link_follower_state gauge\n");
            for state in [State::Idle, State::Following, State::Waiting, State::Halted] {
                out.push_str(&format!(
                    "queen_link_follower_state{{state=\"{}\"}} {}\n",
                    state.name(),
                    u8::from(s.state == state)
                ));
            }
            out.push_str("# HELP queen_link_lag_entries Entries of the source's log this standby had still to read when the source last answered\n# TYPE queen_link_lag_entries gauge\n");
            out.push_str(&format!("queen_link_lag_entries {}\n", s.lag_entries()));
            out.push_str("# HELP queen_link_lag_seconds The age of the last source entry this standby applied, while the source is known to hold more; 0 when it holds everything the source had\n# TYPE queen_link_lag_seconds gauge\n");
            out.push_str(&format!(
                "queen_link_lag_seconds {:.3}\n",
                s.lag_us(now_us) as f64 / 1e6
            ));
            if s.last_answer_us > 0 {
                out.push_str("# HELP queen_link_last_answer_seconds Seconds since the source last answered this node's follower\n# TYPE queen_link_last_answer_seconds gauge\n");
                out.push_str(&format!(
                    "queen_link_last_answer_seconds {:.3}\n",
                    (now_us - s.last_answer_us).max(0) as f64 / 1e6
                ));
            }
            out.push_str("# HELP queen_link_entries_total Source entries this node replayed since it started\n# TYPE queen_link_entries_total counter\n");
            out.push_str(&format!("queen_link_entries_total {}\n", s.entries));
        }
        if let (Some(source), false) = (&source, readers.is_empty()) {
            let applied = source.applied();
            out.push_str("# HELP queen_link_readers Standbys this node keeps its log for\n# TYPE queen_link_readers gauge\n");
            out.push_str(&format!("queen_link_readers {}\n", readers.len()));
            out.push_str("# HELP queen_link_reader_behind_entries Entries of this node's log a standby had still to apply when it last said where it was\n# TYPE queen_link_reader_behind_entries gauge\n");
            for r in &readers {
                out.push_str(&format!(
                    "queen_link_reader_behind_entries{{reader=\"{}\"}} {}\n",
                    escape_label(&r.name),
                    applied.saturating_sub(r.after)
                ));
            }
            out.push_str("# HELP queen_link_reader_idle_seconds Seconds since a standby last said where it was\n# TYPE queen_link_reader_idle_seconds gauge\n");
            for r in &readers {
                out.push_str(&format!(
                    "queen_link_reader_idle_seconds{{reader=\"{}\"}} {:.3}\n",
                    escape_label(&r.name),
                    r.idle.as_secs_f64()
                ));
            }
            out.push_str("# HELP queen_link_holding Whether this node keeps its log for its standbys: 0 while its data volume is too full (QUEEN_LINK_HOLD_DISK_PCT)\n# TYPE queen_link_holding gauge\n");
            out.push_str(&format!("queen_link_holding {}\n", u8::from(source.holding())));
        }
        out
    }

    /// `GET /api/v1/system/link`.
    pub(super) async fn api_link_status(&self) -> Result<ApiOut, RsmError> {
        let view = self.link_view().map_err(read_error)?;
        let leads = self.repl.role().is_leader();
        let body = match view {
            Some(mut v) => {
                v["leader"] = json!(leads);
                v
            }
            None => json!({"role": "primary", "leader": leads}),
        };
        Ok(ApiOut::json(200, body.to_string()))
    }

    /// `POST /api/v1/system/link/promote`: make this standby an ordinary
    /// cluster. The request travels as a command, so any node takes it and
    /// the leader does it; it answers once the leader takes its clients'
    /// commands and THIS node has applied the promotion. Promoting a promoted
    /// cluster again changes nothing.
    ///
    /// Nothing here tells the source: a promotion while the source still
    /// takes writes makes two clusters that both serve. Stop the source
    /// first.
    pub(super) async fn api_link_promote(&self, ctx: ReqCtx) -> Result<ApiOut, RsmError> {
        let cmd = Command::Effects(link::promote_command(ctx.request_id));
        match self.submit(&ctx, cmd).await? {
            Reply::Done { .. } => {
                tracing::warn!(target: "rsm", "rsm link: this cluster was promoted by request");
                // The leader's driver answered once it plans as an ordinary
                // leader and its engine serves, so whoever asked can write to
                // this cluster next, through any node. What they are told is
                // the status AFTER the promotion as THIS node holds it: a
                // follower that forwarded the request has applied the entry
                // by now, and this read makes sure.
                loop {
                    let role = self.store.read(|r| link::read_role(r)).map_err(read_error)?;
                    if !role.is_standby() || ctx.deadline.remaining().is_zero() {
                        break;
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                }
                self.api_link_status().await
            }
            Reply::Refused(r) if r.code == NOT_STANDBY_CODE => Ok(ApiOut::json(
                409,
                json!({"code": r.code, "error": r.message}).to_string(),
            )),
            Reply::Refused(r) if r.retryable => Err(RsmError::Retry { leader_hint: None }),
            Reply::Refused(r) => Err(RsmError::Rejected {
                code: r.code,
                message: r.message,
            }),
            Reply::Retry { hint } => Err(RsmError::Retry {
                leader_hint: hint.map(|h| h.to_string()),
            }),
        }
    }
}
