//! `POST /api/v1/consumer-groups/positions`: where a consumer group reads,
//! partition by partition — the read half of the transaction's `positions`
//! rider (`planner::positions`).
//!
//! ```text
//! {"consumerGroup": "billing", "entries": [{"queue": "orders", "partition": "3"}, …]}
//!   → {"consumerGroup": "billing",
//!      "entries": [{"queue": "orders", "partition": "3", "offset": 42, "metadata": ""}, …]}
//! ```
//!
//! `offset` is the next offset the group reads (the cursor's `committed + 1`)
//! and `null` where the group has no position — no cursor row, no such
//! partition, no such queue. One answer per asked entry, in order. Without
//! `entries`, the answer is every position the group holds, on every queue it
//! is registered on.
//!
//! LINEARIZABLE: a read barrier first, so a position set and acknowledged
//! anywhere in the cluster is what this answers on any node — a stale read
//! here is a consumer that rewinds. The group travels in the body, not in the
//! path: a group name is any string.

use serde_json::{json, Value};

use super::{read_error, reject, ApiOut, RaftFacade, ReqCtx, RsmError};
use crate::rsm::store::{Store, TypedReads};

/// The most entries one read may name. The caller pages past it.
const MAX_ENTRIES: usize = 65_536;

impl RaftFacade {
    pub(super) async fn api_group_positions(
        &self,
        ctx: ReqCtx,
        body: &[u8],
    ) -> Result<ApiOut, RsmError> {
        let v: Value = serde_json::from_slice(body).map_err(|e| super::rejected("bad_body", e))?;
        let group = v
            .get("consumerGroup")
            .and_then(Value::as_str)
            .filter(|g| !g.is_empty())
            .ok_or_else(|| reject("bad_request", "consumerGroup is required"))?
            .to_string();
        let asked: Option<Vec<(String, String)>> = match v.get("entries") {
            None | Some(Value::Null) => None,
            Some(Value::Array(a)) => {
                if a.len() > MAX_ENTRIES {
                    return Err(reject(
                        "bad_request",
                        format!("{} entries, the ceiling is {MAX_ENTRIES}", a.len()),
                    ));
                }
                let mut out = Vec::with_capacity(a.len());
                for e in a {
                    let queue = e
                        .get("queue")
                        .and_then(Value::as_str)
                        .filter(|q| !q.is_empty())
                        .ok_or_else(|| reject("bad_request", "every entry names a queue"))?;
                    let partition = e
                        .get("partition")
                        .and_then(Value::as_str)
                        .filter(|p| !p.is_empty())
                        .unwrap_or("Default");
                    out.push((queue.to_string(), partition.to_string()));
                }
                Some(out)
            }
            Some(_) => return Err(reject("bad_request", "entries is an array")),
        };
        self.linearizable(&ctx).await?;
        let store = self.store.clone();
        let tenant = ctx.tenant.clone();
        let group_r = group.clone();
        let entries = tokio::task::spawn_blocking(move || {
            store.read(|r| {
                let mut out: Vec<Value> = Vec::new();
                let position = |pid: u64| -> crate::rsm::store::Result<Option<(i64, String)>> {
                    Ok(r.cursor(pid, &group_r)?
                        .map(|c| (c.committed.saturating_add(1), c.metadata)))
                };
                match &asked {
                    Some(asked) => {
                        for (queue, partition) in asked {
                            let found = match r.pid_of(&tenant, queue, partition)? {
                                Some(pid) => position(pid)?,
                                None => None,
                            };
                            out.push(entry(queue, partition, found));
                        }
                    }
                    None => {
                        let mut queues: Vec<String> = Vec::new();
                        r.scan_queues(&tenant, usize::MAX, &mut |q, _| {
                            queues.push(q.to_string());
                            true
                        })?;
                        for queue in queues {
                            if r.group(&tenant, &queue, &group_r)?.is_none() {
                                continue;
                            }
                            let mut pids: Vec<u64> = Vec::new();
                            r.scan_queue_partitions(
                                &tenant,
                                &queue,
                                None,
                                usize::MAX,
                                &mut |pid| {
                                    pids.push(pid);
                                    true
                                },
                            )?;
                            for pid in pids {
                                let Some(found) = position(pid)? else {
                                    continue;
                                };
                                let Some(part) = r.partition(pid)? else {
                                    continue;
                                };
                                out.push(entry(&queue, &part.partition, Some(found)));
                            }
                        }
                    }
                }
                Ok(out)
            })
        })
        .await
        .map_err(|e| RsmError::Internal(format!("positions read task: {e}")))?
        .map_err(read_error)?;
        Ok(ApiOut::json(
            200,
            json!({"consumerGroup": group, "entries": entries}).to_string(),
        ))
    }
}

fn entry(queue: &str, partition: &str, found: Option<(i64, String)>) -> Value {
    match found {
        Some((offset, metadata)) => json!({
            "queue": queue,
            "partition": partition,
            "offset": offset,
            "metadata": metadata,
        }),
        None => json!({"queue": queue, "partition": partition, "offset": Value::Null}),
    }
}

/// Percent-decode one path segment (`%2F` → `/`). The consumer-group routes
/// name the group and the queue IN the path, and every SDK encodes them there
/// (`encodeURIComponent`, `quote`, `rawurlencode`): the segment is the
/// encoded form, and a name is only what it decodes to. A `%` not followed by
/// two hex digits is kept as it stands.
pub(super) fn pct_decode(segment: &str) -> String {
    if !segment.contains('%') {
        return segment.to_string();
    }
    let b = segment.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%' && i + 2 < b.len() {
            let hex = std::str::from_utf8(&b[i + 1..i + 3])
                .ok()
                .and_then(|h| u8::from_str_radix(h, 16).ok());
            if let Some(v) = hex {
                out.push(v);
                i += 3;
                continue;
            }
        }
        out.push(b[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

#[cfg(test)]
mod tests {
    use super::pct_decode;

    #[test]
    fn a_segment_decodes_to_the_name_it_encodes() {
        assert_eq!(pct_decode("orders-consumer"), "orders-consumer");
        assert_eq!(pct_decode("a%2Fb"), "a/b");
        assert_eq!(pct_decode("a%20b%3Ac"), "a b:c");
        assert_eq!(pct_decode("100%"), "100%", "a trailing % is kept");
        assert_eq!(pct_decode("%zz%41"), "%zzA", "a bad escape is kept");
        assert_eq!(pct_decode("%C3%A9t%C3%A9"), "été");
    }
}
