//! The console's view of Kafka groups: the rows the broker's dashboard reads
//! (server/src/rsm/facade/real/phase2/reads/kafka_groups.rs), written by the
//! coordinator on the raft leader in each group's own tenant.
//!
//! ```text
//!   namespace  queen-kafka
//!   qk:groups:<esc group>  {"pt":"consumer","ts":<ms>}                 once per group and process
//!   qk:live:<esc group>    {"v":1,"members":N,"topics":{t:n}|null,
//!                           "committing":bool,"freshMs":<3 x interval>, ...}
//! ```
//!
//! A row is fresh for `freshMs` after the broker wrote it; a group whose rows
//! stop (its coordinator moved, it died) reads as gone after that.

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;
use std::time::Duration;

use serde_json::json;

use crate::core::{KvOp, Queen};
use crate::groups::PresenceRow;
use crate::topics::{now_ms, NS};

/// How often a group's row is written when nothing changes.
pub const EVERY: Duration = Duration::from_secs(15);
const OPS_PER_CALL: usize = 100;

/// A group id as a key: every byte outside `[A-Za-z0-9._-]` percent-encoded.
pub fn escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'.' | b'_' | b'-' => out.push(b as char),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// Groups whose index row this process has written.
#[derive(Default)]
pub struct Indexed(Mutex<HashSet<String>>);

/// Write the rows, grouped by the token they are written with.
pub async fn write(queen: &Queen, indexed: &Indexed, node: i32, rows: Vec<PresenceRow>) {
    let mut by_bearer: HashMap<Option<String>, Vec<KvOp>> = HashMap::new();
    let at = now_ms();
    for r in rows {
        let key = escape(&r.group);
        if key.len() > 480 {
            continue;
        }
        let ops = by_bearer.entry(r.bearer.clone()).or_default();
        let first = indexed
            .0
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(r.group.clone());
        if first {
            ops.push(KvOp::put(
                NS,
                &format!("qk:groups:{key}"),
                json!({"pt": if r.protocol_type.is_empty() { "" } else { "consumer" }, "ts": at}),
            ));
        }
        ops.push(KvOp::put(
            NS,
            &format!("qk:live:{key}"),
            json!({
                "v": 1,
                "members": r.members,
                "state": r.state,
                "generation": r.generation,
                "protocolType": r.protocol_type,
                "topics": r.topics,
                "committing": r.committing,
                "freshMs": (EVERY * 3).as_millis() as u64,
                "source": "coordinator",
                "at": at,
                "node": node,
            }),
        ));
    }
    for (bearer, ops) in by_bearer {
        for chunk in ops.chunks(OPS_PER_CALL) {
            if let Err(e) = queen.kv(bearer.as_deref(), chunk).await {
                tracing::debug!(target: "kafka", error = %e, "cannot write group presence");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_group_id_is_escaped_as_the_broker_reads_it() {
        assert_eq!(escape("team a:billing"), "team%20a%3Abilling");
        assert_eq!(escape("é"), "%C3%A9");
        assert_eq!(escape("orders.v1_x-2"), "orders.v1_x-2");
    }
}
