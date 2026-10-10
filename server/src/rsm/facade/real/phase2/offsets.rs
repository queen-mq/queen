//! `POST /api/v1/fetch/offsets` — where a partition's log reaches a point in
//! time: for each entry, the first offset appended at or after a timestamp.
//!
//! `POST /api/v1/fetch` reads from an offset, and a reader that knows WHEN it
//! wants to start rather than WHERE had no way to turn the one into the other:
//! a consumer started "from 10:00", a replay of the last hour, Kafka's
//! `offsetsForTimes` (KIP-79) through the Kafka facade, which until this route
//! answered every timestamp "no offset" and so sent Flink's timestamp start
//! mode to the END of the log without a word. It is the fetch route's twin and
//! takes its rules: read-only, served from this node's own store with no
//! barrier (a follower answers from what it has applied, exactly as its
//! fetch does), at most 1024 entries, a partition named the way a fetch names
//! one, and `UNKNOWN_TOPIC_OR_PARTITION` for a queue that is not there.
//!
//! ```text
//!   {"entries": [{"queue": "orders", "partition": "0",
//!                 "timestamp": 1790000000000 | "2026-09-21T10:00:00Z"}]}
//! → {"entries": [{"queue": "orders", "partition": "0", "offset": 42,
//!                 "ts": "2026-09-21T10:00:00.004211Z",
//!                 "highWatermark": 97, "logStartOffset": 0}]}
//! ```
//!
//! `offset` and `ts` are `null` when nothing in the log was appended that late
//! — the reader then starts at `highWatermark`, which is what "the first offset
//! at or after a time that has not come yet" means. A timestamp older than the
//! log answers its first retained offset.
//!
//! ## What "appended at" means
//!
//! The broker's clock, never the payload's: every append carries the
//! `created_at_us` its leader stamped when it planned it, and those stamps
//! rise with the offsets of a partition — the planner's clock is floored at
//! the newest stamp it has seen, so a new leader does not run it backwards.
//! A producer's own event time, if its payload has one, is not something the
//! broker reads. The Kafka facade says what that means for a Kafka client
//! (`protocols/queen-kafka/src/handlers/list_offsets.rs`).
//!
//! ## How it is found
//!
//! Every append leaves one `txns` row keyed by its base offset and carrying
//! its stamp ([`crate::rsm::dedup`]), and they cover the whole retained log
//! (`txns_start <= log_start`). So the answer is a binary search over offsets
//! in which each probe is ONE seek to the first row at or past it: about
//! log2(width) seeks, no walk of the log and no payload read
//! ([`first_appended_at_or_after`]). A broker that keeps no txns rows
//! (`QUEEN_RAFT_DEDUP_INDEX=segment`) walks the partition's records instead,
//! as a consumer group's timestamp seek does.

use serde::Deserialize;
use serde_json::{json, Value};

use super::reads::{fetch_partition_name, walk_payloads, Part};
use super::{read_error, ApiOut, RaftFacade, ReqCtx, RsmError};
use crate::rsm::store::{Store, TypedReads};

/// The fetch route's own ceiling, for the same reason: one request is one
/// store transaction.
const MAX_ENTRIES: usize = 1024;

#[derive(Deserialize)]
struct Body {
    #[serde(default)]
    entries: Vec<Entry>,
}

#[derive(Deserialize)]
struct Entry {
    queue: String,
    #[serde(default)]
    partition: Option<Value>,
    timestamp: Value,
}

/// One entry's answer before it is rendered.
#[derive(Default)]
struct Found {
    /// `(offset, created_at_us)` of the first append at or after the time.
    at: Option<(u64, i64)>,
    high: u64,
    log_start: u64,
    error: Option<&'static str>,
}

/// An entry's timestamp in epoch MILLISECONDS: a number of them, or an
/// ISO-8601 time as every other Queen route takes one. Negative is not a time.
fn timestamp_ms(v: &Value) -> Option<i64> {
    match v {
        Value::Number(n) => n.as_i64(),
        Value::String(s) => crate::util::parse_iso_ms(s),
        _ => None,
    }
    .filter(|ms| *ms >= 0)
}

use crate::rsm::dedup::first_appended_at_or_after;

impl RaftFacade {
    pub(super) async fn api_fetch_offsets(
        &self,
        ctx: ReqCtx,
        body: &[u8],
    ) -> Result<ApiOut, RsmError> {
        let request: Body =
            serde_json::from_slice(body).map_err(|e| super::rejected("bad_body", e))?;
        if request.entries.len() > MAX_ENTRIES {
            return Ok(ApiOut::json(
                400,
                json!({"error":"too many entries"}).to_string(),
            ));
        }
        let mut asks: Vec<(String, String, i64)> = Vec::with_capacity(request.entries.len());
        for e in request.entries {
            let Some(ms) = timestamp_ms(&e.timestamp) else {
                return Ok(ApiOut::json(
                    400,
                    json!({"error":"timestamp must be a non-negative number of epoch \
                                    milliseconds or an ISO-8601 time"})
                    .to_string(),
                ));
            };
            asks.push((e.queue, fetch_partition_name(e.partition.as_ref()), ms));
        }

        let store = self.store.clone();
        let tenant = ctx.tenant.clone();
        let qlog = self.qlog_reader.clone();
        let reader = self.reader.clone();
        let indexed = crate::rsm::dedup::txns_carry_len();
        let lookups = asks.clone();
        let found = tokio::task::spawn_blocking(move || -> Result<Vec<Found>, RsmError> {
            // The partitions and, where the txns rows answer it, the offset,
            // from ONE store transaction; the walk of a broker without them
            // after it, from the queue log, the way a fetch reads.
            let mut walks: Vec<(usize, Part, i64)> = Vec::new();
            let list_sealed = qlog.is_none();
            let mut found = store
                .read(|r| {
                    let mut out = Vec::with_capacity(lookups.len());
                    for (i, (queue, partition, ms)) in lookups.iter().enumerate() {
                        let part = match r.pid_of(&tenant, queue, partition)? {
                            Some(pid) => r.partition(pid)?.map(|row| (pid, row)),
                            None => None,
                        };
                        let Some((pid, row)) = part else {
                            // Never written is an empty log; no queue at all is
                            // the one refusal, spelled as the fetch spells it.
                            let missing = r.queue(&tenant, queue)?.is_none();
                            out.push(Found {
                                error: missing.then_some("UNKNOWN_TOPIC_OR_PARTITION"),
                                ..Found::default()
                            });
                            continue;
                        };
                        let high = (row.last_offset + 1).max(0) as u64;
                        let log_start = row.log_start;
                        let at_us = ms.saturating_mul(1_000);
                        let at = if log_start >= high {
                            None
                        } else if indexed {
                            // The retained messages that lost their row
                            // (catalogue version 6) are searched in the queue
                            // log's index, the rest in the rows.
                            let rows_at = row.rows_start.max(row.txns_start);
                            let below = rows_at.min(high);
                            let mut at = None;
                            if log_start < below {
                                if let Some(q) = qlog.as_ref() {
                                    let qid = crate::rsm::qlog::set::QLogReader::queue_id_of(
                                        &tenant, queue,
                                    );
                                    let first = q
                                        .record_before(qid, pid, below, at_us)
                                        .map_or(log_start, |rec| rec.end.max(log_start));
                                    if first < below {
                                        at = q
                                            .record_before(qid, pid, first + 1, i64::MAX)
                                            .map(|rec| (first, rec.created_at_us));
                                    }
                                }
                            }
                            let rows_from = log_start.max(rows_at);
                            if at.is_none() && rows_from < high {
                                at = first_appended_at_or_after(r, pid, rows_from, high, at_us)?;
                            }
                            at
                        } else {
                            let mut sealed = Vec::new();
                            if list_sealed {
                                r.scan_partition_files(pid, usize::MAX, &mut |f| {
                                    sealed.push(f);
                                    true
                                })?;
                            }
                            walks.push((i, Part { pid, row, sealed }, at_us));
                            None
                        };
                        out.push(Found {
                            at,
                            high,
                            log_start,
                            error: None,
                        });
                    }
                    Ok(out)
                })
                .map_err(read_error)?;
            for (i, part, at_us) in walks {
                let (queue, from, high) =
                    (part.row.queue.clone(), found[i].log_start, found[i].high);
                let mut at = None;
                walk_payloads(
                    &reader,
                    qlog.as_ref(),
                    &tenant,
                    &queue,
                    &part,
                    from,
                    high,
                    usize::MAX,
                    false,
                    |offset, created_at_us, _, _, _| {
                        if created_at_us >= at_us {
                            at = Some((offset, created_at_us));
                            return false;
                        }
                        true
                    },
                )?;
                found[i].at = at;
            }
            Ok(found)
        })
        .await
        .map_err(|e| RsmError::Internal(format!("fetch offsets read task: {e}")))??;

        let entries: Vec<Value> = asks
            .into_iter()
            .zip(found)
            .map(|((queue, partition, _), f)| {
                let mut entry = json!({
                    "queue": queue,
                    "partition": partition,
                    "offset": f.at.map(|(offset, _)| offset),
                    "ts": f.at.map(|(_, us)| crate::rsm::planner::timers::iso_us(us)),
                    "highWatermark": f.high,
                    "logStartOffset": f.log_start,
                });
                if let Some(error) = f.error {
                    entry["error"] = Value::from(error);
                }
                entry
            })
            .collect();
        Ok(ApiOut::json(200, json!({ "entries": entries }).to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rsm::store::{HeedStore, StoreOpts, Writes};

    struct Dir(std::path::PathBuf);

    impl Drop for Dir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    /// A store holding one append per `(base, end, created_us)` of pid 1.
    fn store(tag: &str, appends: &[(u64, u64, i64)]) -> (Dir, HeedStore) {
        let d = std::env::temp_dir().join(format!(
            "queen-rsm-fetch-offsets-{tag}-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        let store = HeedStore::open(
            &d.join("store"),
            &StoreOpts {
                map_bytes: Some(64 << 20),
                ..Default::default()
            },
        )
        .unwrap();
        {
            let mut w = store.write().unwrap();
            for (base, end, created) in appends {
                let accepted: Vec<([u8; 16], u64)> = (*base..=*end)
                    .map(|off| {
                        let mut h = [0u8; 16];
                        h[..8].copy_from_slice(&off.to_be_bytes());
                        (h, off)
                    })
                    .collect();
                crate::rsm::dedup::record_txns(&mut w, 1, *base, *end, &accepted, *created)
                    .unwrap();
            }
            w.commit().unwrap();
        }
        (Dir(d), store)
    }

    fn find(s: &HeedStore, from: u64, to: u64, at_us: i64) -> Option<(u64, i64)> {
        s.read(|r| first_appended_at_or_after(r, 1, from, to, at_us))
            .unwrap()
    }

    /// Three appends of several records each, stamped 100, 200 and 300. Every
    /// time lands on the FIRST offset of the first append at or after it, and
    /// a time past the newest finds nothing.
    #[test]
    fn a_time_finds_the_first_append_at_or_after_it() {
        let (_d, s) = store("basic", &[(0, 4, 100), (5, 9, 200), (10, 19, 300)]);
        for (at, want) in [
            (0, Some((0, 100))),
            (100, Some((0, 100))),
            (101, Some((5, 200))),
            (200, Some((5, 200))),
            (250, Some((10, 300))),
            (300, Some((10, 300))),
            (301, None),
            (i64::MAX, None),
        ] {
            assert_eq!(find(&s, 0, 20, at), want, "at {at}");
        }
    }

    /// Retention moved the log start past the first append: the answer is never
    /// below it, and the rows of the hash window that outlive the payload are
    /// not mistaken for records.
    #[test]
    fn the_answer_is_never_below_the_log_start() {
        let (_d, s) = store("start", &[(0, 4, 100), (5, 9, 200), (10, 19, 300)]);
        assert_eq!(find(&s, 5, 20, 0), Some((5, 200)));
        assert_eq!(find(&s, 10, 20, 150), Some((10, 300)));
        // ...nor at or past the high watermark the caller read.
        assert_eq!(find(&s, 0, 10, 250), None);
    }

    /// Many one-record appends, the shape of a slow producer: the search costs
    /// seeks, not a walk, and still lands on every boundary exactly.
    #[test]
    fn every_boundary_of_a_long_log_is_found() {
        let appends: Vec<(u64, u64, i64)> = (0..1_000u64)
            .map(|i| (i, i, 1_000 + 10 * i as i64))
            .collect();
        let (_d, s) = store("long", &appends);
        for i in [0u64, 1, 2, 499, 500, 998, 999] {
            let at = 1_000 + 10 * i as i64;
            assert_eq!(find(&s, 0, 1_000, at), Some((i, at)), "exact {i}");
            assert_eq!(
                find(&s, 0, 1_000, at - 5),
                Some((i, at)),
                "between {} and {i}",
                i.saturating_sub(1)
            );
        }
        assert_eq!(find(&s, 0, 1_000, 1_000 + 10 * 1_000), None);
    }

    /// An empty range and a partition with no rows answer nothing, and never
    /// an offset the log does not hold.
    #[test]
    fn an_empty_log_answers_nothing() {
        let (_d, s) = store("empty", &[]);
        assert_eq!(find(&s, 0, 0, 0), None);
        assert_eq!(find(&s, 0, 10, 0), None);
    }

    #[test]
    fn a_timestamp_is_epoch_millis_or_an_iso_time_and_never_negative() {
        assert_eq!(
            timestamp_ms(&json!(1_790_000_000_000i64)),
            Some(1_790_000_000_000)
        );
        assert_eq!(
            timestamp_ms(&json!("1970-01-01T00:00:01.500Z")),
            Some(1_500)
        );
        assert_eq!(timestamp_ms(&json!(-1)), None);
        assert_eq!(timestamp_ms(&json!("yesterday")), None);
        assert_eq!(timestamp_ms(&json!(1.5)), None);
        assert_eq!(timestamp_ms(&Value::Null), None);
    }
}
