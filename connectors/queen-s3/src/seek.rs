//! seek — the backwards probe-seek that recovers a position from a timestamp
//! (plan §4.5(2)).
//!
//! `seek(p, T)` answers **the first offset of partition `p` whose record has
//! `ts >= T`** — the position a window starting at `T` must read from. It is
//! used for `start=latest` (every partition's position at `T_0`), for a
//! partition first seen long after `T_0`, and whenever no checkpoint entry
//! exists (plan §4.5).
//!
//! # Why a search is sound at all
//!
//! Segment `created_at` is stamped after the allocator's row lock, so it is
//! **monotone per partition in commit order** (003_log_push.sql:219-223, plan
//! F5). `ts >= T` is therefore a monotone predicate over the offset axis —
//! false, false, …, true, true — and the answer is its single flip point. That
//! is the one fact the whole module rests on; without it a partition would have
//! to be scanned.
//!
//! # Sans I/O
//!
//! Like [`crate::window`], this type performs no I/O and reads no clock. It
//! emits one [`FetchRequestEntry`] at a time through [`Seek::next_probe`] and
//! consumes the answer through [`Seek::on_result`]; the driver owns the HTTP.
//! Every timestamp it compares came from the broker.
//!
//! # The search
//!
//! 1. **Gallop backwards** from `last_offset` in exponentially growing steps —
//!    1, 2, 4, 8, … records back — until a record with `ts < T` is seen (the
//!    lower bracket) or `log_start` is reached. Backwards, because the caller is
//!    almost always looking for a `T` near the head of the log: `start=latest`
//!    asks for "now", a partition that just started writing asks for a `T` inside
//!    its newest segment. A forward binary search from `log_start` would cost the
//!    full `log2(n)` every time; the gallop costs `log2(distance)`.
//! 2. **Bisect** the bracket the gallop produced.
//!
//! Each probe reads whatever the broker hands back — a fetch is segment-granular,
//! so a probe usually returns many records — and *every* record in the answer
//! tightens the bracket, which is why a probe that straddles `T` finishes the
//! search on the spot.
//!
//! Both phases are logarithmic, so a partition of `n` retained records costs at
//! most about `2·log2(n) + 2` probes; [`Seek::probe_budget`] is that bound plus
//! slack, and running out of it is a [`SeekStep::Failed`] rather than a loop.

use std::sync::Arc;

use crate::types::{FetchError, FetchRequestEntry, FetchedEntry, Micros};

/// What a probe's answer did to the search.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SeekStep {
    /// The bracket narrowed; call [`Seek::next_probe`] again.
    Continue,
    /// The answer: the first offset of the partition with `ts >= t`, which is
    /// `last_offset + 1` when every retained record is older than `t` and
    /// `log_start` when every retained record is at or after it.
    Found(i64),
    /// The search cannot be completed. The driver's options are to retry with a
    /// fresh [`Seek`] (a transient per-entry error) or to fall back to
    /// `log_start` (plan §4.5(3)), which is always correct and only costs a
    /// re-read the `ts < T_{k-1}` filter discards.
    Failed(String),
}

/// A probe's `maxBytes`, in compressed segment bytes.
///
/// Deliberately tiny: the broker delivers the first segment of a call whatever
/// the budget says (032_log_fetch.sql, the "one over-large segment" exemption),
/// so a probe always gets its record, and a small ceiling keeps a seek over a
/// partition of fat segments from moving megabytes. Scaled by
/// `probe_records` in [`Seek::new`].
const PROBE_BYTES_PER_RECORD: i64 = 4096;

/// The floor and ceiling of the computed probe `maxBytes`.
const PROBE_BYTES_MIN: i64 = 4096;
const PROBE_BYTES_MAX: i64 = 1024 * 1024;

/// One backwards probe-seek over one partition. Single use: construct, drive to
/// [`SeekStep::Found`], drop.
///
/// # Invariant
///
/// `log_start - 1 <= lo < hi <= last_offset + 1`, where `lo` is the largest
/// offset known to hold `ts < t` and `hi` the smallest known to hold `ts >= t`.
/// The sentinels — `lo = log_start - 1`, `hi = last_offset + 1` — are exactly the
/// two "nothing known" ends, and they are also the two legitimate answers, which
/// is why the search needs no special cases for them. It finishes when
/// `lo + 1 == hi`.
pub struct Seek {
    queue: String,
    partition: Arc<str>,
    t: Micros,
    lo: i64,
    hi: i64,
    log_start: i64,
    last_offset: i64,
    /// `1 << gallop` is the next backwards step, in records.
    gallop: u32,
    probe_bytes: i64,
    probes: u32,
    budget: u32,
    outcome: Option<SeekStep>,
}

impl Seek {
    /// Start a search for the first offset of `partition` with `ts >= t`.
    ///
    /// `last_offset` and `log_start` are the partition's bounds as the caller
    /// last saw them (from discovery or from a previous fetch); both are
    /// re-learned from every probe, so a stale pair only costs a probe.
    /// `probe_records` sizes each probe's `maxBytes` — it is a hint, not a
    /// promise: the broker answers in whole segments.
    ///
    /// The queue name is here because a [`FetchRequestEntry`] carries one; the
    /// plan's signature for this constructor omits it.
    pub fn new(
        queue: String,
        partition: Arc<str>,
        t: Micros,
        last_offset: i64,
        log_start: i64,
        probe_records: usize,
    ) -> Seek {
        let n = (last_offset + 1 - log_start).max(0);
        let mut s = Seek {
            queue,
            partition,
            t,
            lo: log_start - 1,
            hi: last_offset + 1,
            log_start,
            last_offset,
            gallop: 0,
            probe_bytes: ((probe_records.max(1) as i64).saturating_mul(PROBE_BYTES_PER_RECORD))
                .clamp(PROBE_BYTES_MIN, PROBE_BYTES_MAX),
            probes: 0,
            // Gallop: at most ceil(log2 n) + 1 probes. Bisection: at most
            // ceil(log2 n). Plus slack for the two ends and for a probe the
            // broker answered empty because an earlier entry of the call spent
            // the budget (032_log_fetch.sql: "a later entry can come back empty").
            budget: 2 * (64 - n.max(1).leading_zeros()) + 8,
            outcome: None,
        };
        if s.t == Micros::MIN {
            // Every record is at or after -infinity; no probe can say otherwise.
            s.outcome = Some(SeekStep::Found(s.log_start));
        } else {
            s.settle();
        }
        s
    }

    /// The next probe to run, or `None` once the search has an answer.
    pub fn next_probe(&self) -> Option<FetchRequestEntry> {
        if self.outcome.is_some() {
            return None;
        }
        Some(FetchRequestEntry {
            queue: self.queue.clone(),
            partition: self.partition.clone(),
            offset: self.probe_offset(),
            max_bytes: Some(self.probe_bytes),
        })
    }

    /// Feed back the entry that answered [`Seek::next_probe`].
    pub fn on_result(&mut self, entry: &FetchedEntry) -> SeekStep {
        if let Some(done) = &self.outcome {
            return done.clone();
        }
        if entry.partition != self.partition {
            return self.fail(format!(
                "seek: answer for partition {:?} while probing {:?}",
                entry.partition, self.partition
            ));
        }
        self.probes += 1;
        if self.probes > self.budget {
            return self.fail(format!(
                "seek: {} probes over partition {:?} without an answer",
                self.probes, self.partition
            ));
        }

        let probe = self.probe_offset();
        match &entry.error {
            Some(FetchError::UnknownTopicOrPartition) => {
                return self.fail("seek: UNKNOWN_TOPIC_OR_PARTITION".to_string());
            }
            Some(FetchError::Other(e)) => {
                return self.fail(format!("seek: fetch error {e}"));
            }
            Some(FetchError::OffsetOutOfRange) => {
                // Two ways to be out of range (032_log_fetch.sql:25-35), and
                // they mean opposite things.
                if entry.log_start_offset > probe {
                    // Retention overtook the probe. Everything below the new
                    // logStart is gone, so the answer can only be at or above
                    // it — and since ts is monotone, if the true answer WAS
                    // below it then every surviving record is at or after `t`
                    // and logStart is the answer.
                    self.raise_log_start(entry.log_start_offset);
                } else {
                    // The probe sits above the head: our `last_offset` was
                    // stale (a bounds read from a different snapshot, or a lane
                    // that never had the offsets we assumed).
                    self.lower_head(entry.high_watermark);
                }
            }
            None => {
                self.raise_log_start(entry.log_start_offset);
                self.lower_head(entry.high_watermark);
                if entry.records.is_empty() {
                    // Valid and empty: either the probe is exactly the high
                    // watermark (handled by lower_head above) or an earlier
                    // entry of the same call spent the byte budget. Re-probe;
                    // the budget bounds the retries.
                    return self.settle();
                }
                // Every record in the answer is evidence. The first one at or
                // after `t` is an upper bracket, the last one below it a lower
                // one — a probe that straddles `t` therefore ends the search.
                for rec in &entry.records {
                    if rec.ts < self.t {
                        if rec.offset > self.lo {
                            self.lo = rec.offset;
                        }
                    } else {
                        if rec.offset < self.hi {
                            self.hi = rec.offset;
                        }
                        break;
                    }
                }
                if self.lo == self.log_start - 1 {
                    // Still no lower bracket: keep galloping, twice as far.
                    self.gallop += 1;
                }
            }
        }
        self.settle()
    }

    /// The current status: [`SeekStep::Continue`] while probing.
    pub fn step(&self) -> SeekStep {
        self.outcome.clone().unwrap_or(SeekStep::Continue)
    }

    /// Probes issued so far.
    pub fn probes(&self) -> u32 {
        self.probes
    }

    /// The hard ceiling on probes for this partition's size, about
    /// `2·log2(retained records) + 8`.
    pub fn probe_budget(&self) -> u32 {
        self.budget
    }

    /// The partition being searched.
    pub fn partition(&self) -> &Arc<str> {
        &self.partition
    }

    // -- internals ----------------------------------------------------------

    /// The offset the next probe reads from: the gallop's step while the search
    /// has no lower bracket, the bisection's midpoint once it has one.
    fn probe_offset(&self) -> i64 {
        if self.lo == self.log_start - 1 {
            let step = 1i64 << self.gallop.min(62);
            let p = (self.last_offset + 1 - step).max(self.log_start);
            // The gallop doubles, so `p` always falls strictly below the last
            // successful probe; the clamp is belt and braces against a bounds
            // update that moved `hi` under us.
            p.min(self.hi - 1).max(self.log_start)
        } else {
            self.lo + (self.hi - self.lo) / 2
        }
    }

    /// Retention moved: nothing below `ls` can be read any more.
    fn raise_log_start(&mut self, ls: i64) {
        if ls <= self.log_start {
            return;
        }
        self.log_start = ls;
        if self.hi < ls {
            // The flip point itself was deleted; the first surviving record is
            // at `ls` and, by monotonicity, is at or after `t`.
            self.hi = ls;
        }
        if self.lo < ls - 1 {
            self.lo = ls - 1;
        }
    }

    /// The head is lower than we thought: `high` is the next offset the log will
    /// assign, so `high - 1` is the last readable record.
    fn lower_head(&mut self, high: i64) {
        if high - 1 < self.last_offset {
            self.last_offset = high - 1;
        }
        if self.hi > high {
            self.hi = high;
        }
        if self.lo >= self.hi {
            self.lo = self.hi - 1;
        }
    }

    /// Finish if the bracket has closed, otherwise report `Continue`.
    fn settle(&mut self) -> SeekStep {
        if self.hi <= self.log_start {
            self.outcome = Some(SeekStep::Found(self.log_start));
        } else if self.lo + 1 >= self.hi {
            self.outcome = Some(SeekStep::Found(self.hi));
        }
        self.step()
    }

    fn fail(&mut self, why: String) -> SeekStep {
        self.outcome = Some(SeekStep::Failed(why));
        self.step()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_partition_answers_without_a_probe() {
        // Never written: last_offset -1, log_start 0 (032_log_fetch.sql:170-176).
        let s = Seek::new("q".into(), "p".into(), Micros(1_000), -1, 0, 1);
        assert_eq!(s.step(), SeekStep::Found(0));
        assert!(s.next_probe().is_none());
        assert_eq!(s.probes(), 0);
    }

    #[test]
    fn everything_retained_away_answers_log_start() {
        // log_start == high: retention took every live segment.
        let s = Seek::new("q".into(), "p".into(), Micros(1_000), 99, 100, 1);
        assert_eq!(s.step(), SeekStep::Found(100));
    }

    #[test]
    fn min_needs_no_probe() {
        let s = Seek::new("q".into(), "p".into(), Micros::MIN, 5_000, 12, 1);
        assert_eq!(s.step(), SeekStep::Found(12));
        assert!(s.next_probe().is_none());
    }

    #[test]
    fn probe_carries_the_partition_and_a_small_ceiling() {
        let s = Seek::new("orders".into(), "cust-1".into(), Micros(5), 100, 0, 4);
        let p = s.next_probe().unwrap();
        assert_eq!(p.queue, "orders");
        assert_eq!(&*p.partition, "cust-1");
        assert_eq!(p.offset, 100, "the first probe reads the head");
        assert_eq!(p.max_bytes, Some(4 * PROBE_BYTES_PER_RECORD));
    }
}
