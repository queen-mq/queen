//! Reads by offset and partition discovery, as queue methods:
//!
//! ```no_run
//! # async fn demo(queen: queen_mq::Queen) -> queen_mq::Result<()> {
//! let read = queen.queue("orders").fetch(["customer-42", "customer-7"]).await?;
//! for record in &read["customer-42"].records {
//!     println!("{} {}", record.offset, record.payload);
//! }
//!
//! let changed = queen.queue("orders").changed_since("2026-10-20T14:30:00Z").await?;
//! let names: Vec<&str> = changed.partitions.iter().map(|p| p.name.as_str()).collect();
//! # Ok(()) }
//! ```
//!
//! `fetch` is `POST /api/v1/fetch` and `changed_since` is
//! `POST /api/v1/partitions/changed`. Neither takes a lease, moves a cursor or
//! needs an ack: two readers asking the same question get the same answer, and
//! no consumer group notices them. Each is served from the answering node's own
//! applied state.
//!
//! Both do the loops a caller of the raw routes has to write:
//! - `fetch` asks for at most 1000 partitions per call, and asks again from one
//!   past the last record until each partition reaches the high watermark its
//!   first answer reported. A partition whose start was deleted by retention is
//!   read from its oldest kept record (an offset below `logStartOffset` answers
//!   `OFFSET_OUT_OF_RANGE` and no records, so a loop that does not restart there
//!   silently reads nothing).
//! - `changed_since` follows the page cursor (`next`) until the broker says the
//!   pass is over, and answers the smallest `safeTime` of its pages.

use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::Serialize;

use queen_protocol::{
    ChangedEntry, ChangedPartition, ChangedRequest, ChangedResponse, FetchEntry, FetchRecord,
    FetchResponse, ERR_OFFSET_OUT_OF_RANGE, ERR_UNKNOWN_TOPIC_OR_PARTITION,
};

use crate::error::{Error, Result};
use crate::http::Opts;
use crate::queue::QueueBuilder;

/// The broker refuses more than 1024 entries in one call; 1000 leaves room.
const ENTRIES_PER_CALL: usize = 1000;

/// Bytes of payload one fetch call may carry by default: each entry's budget
/// and the ceiling of the whole call. 8 MiB is the most the broker reads for
/// one entry.
pub const DEFAULT_FETCH_MAX_BYTES: i64 = 8 * 1024 * 1024;

/// Partitions per discovery page: the broker's default and its ceiling.
const CHANGED_PAGE: i64 = 1000;

/// Options for [`QueueBuilder::fetch_with`].
#[derive(Debug, Clone, Default)]
pub struct FetchOptions {
    offsets: HashMap<String, i64>,
    max_bytes: Option<i64>,
}

impl FetchOptions {
    pub fn new() -> Self {
        Self::default()
    }

    /// Start `partition` at `offset`, e.g. the `next_offset` of an earlier
    /// read. A partition not named starts at 0, the oldest record still kept.
    pub fn offset(mut self, partition: impl Into<String>, offset: i64) -> Self {
        self.offsets.insert(partition.into(), offset);
        self
    }

    /// [`Self::offset`] for each pair.
    pub fn offsets<I, S>(mut self, offsets: I) -> Self
    where
        I: IntoIterator<Item = (S, i64)>,
        S: Into<String>,
    {
        for (partition, offset) in offsets {
            self.offsets.insert(partition.into(), offset);
        }
        self
    }

    /// Cap the payload bytes of one call. Default [`DEFAULT_FETCH_MAX_BYTES`].
    pub fn max_bytes(mut self, bytes: i64) -> Self {
        self.max_bytes = Some(bytes);
        self
    }
}

/// What [`QueueBuilder::fetch`] read of one partition.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct FetchedPartition {
    pub partition: String,
    /// In offset order. `ts` is when the push that wrote the record was stored.
    pub records: Vec<FetchRecord>,
    /// The next offset the partition will assign.
    pub high_watermark: i64,
    /// The oldest offset retention kept.
    pub log_start_offset: i64,
    /// Where to start next time to read only what is new. Above
    /// `high_watermark` it says the partition was deleted and written again.
    pub next_offset: i64,
}

/// What [`QueueBuilder::changed_since`] answers.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ChangedPartitions {
    /// In the order they were created. `last_offset` is the offset of a
    /// partition's last record (-1 when none).
    pub partitions: Vec<ChangedPartition>,
    /// The broker's time up to which every write is counted in this answer:
    /// pass it as the next `since` to see only what changed after this call.
    /// It comes from the broker's clock; never compare it with the local one.
    pub safe_time: String,
}

/// The `since` of [`QueueBuilder::changed_since`]: an ISO-8601 string, a
/// [`SystemTime`], or [`Since::All`] for every partition of the queue. `None`
/// is `All` too, so an `Option` from an earlier answer can be passed as it is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Since {
    All,
    At(String),
}

impl From<&str> for Since {
    fn from(s: &str) -> Self {
        Since::At(s.to_string())
    }
}

impl From<String> for Since {
    fn from(s: String) -> Self {
        Since::At(s)
    }
}

impl From<&String> for Since {
    fn from(s: &String) -> Self {
        Since::At(s.clone())
    }
}

impl From<SystemTime> for Since {
    fn from(t: SystemTime) -> Self {
        let us = match t.duration_since(UNIX_EPOCH) {
            Ok(d) => d.as_micros() as i64,
            Err(e) => -(e.duration().as_micros() as i64),
        };
        Since::At(iso_from_us(us))
    }
}

impl<T: Into<Since>> From<Option<T>> for Since {
    fn from(o: Option<T>) -> Self {
        o.map_or(Since::All, Into::into)
    }
}

/// Microseconds since the epoch in the spelling the broker writes its own
/// times in (`2026-10-20T14:30:00.000000Z`).
fn iso_from_us(us: i64) -> String {
    const US_PER_DAY: i64 = 86_400_000_000;
    let days = us.div_euclid(US_PER_DAY);
    let rem = us.rem_euclid(US_PER_DAY);
    // Howard Hinnant's `civil_from_days`: days since 1970-01-01 to a date in
    // the proleptic Gregorian calendar.
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    let secs = rem / 1_000_000;
    format!(
        "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}.{:06}Z",
        secs / 3600,
        (secs / 60) % 60,
        secs % 60,
        rem % 1_000_000
    )
}

#[derive(Serialize)]
struct FetchBody<'a> {
    entries: &'a [FetchEntry],
    /// The ceiling of the whole call (broker 2.3.0 and later).
    #[serde(rename = "maxBytes")]
    max_bytes: i64,
}

/// Every call of one read goes to the same node: a follower answers from what
/// it has applied, so two nodes can disagree about where a partition ends.
fn read_opts(queue: &str) -> Opts {
    Opts::affinity(Some(format!("{queue}:*:__READ__")))
}

struct PartState {
    read: FetchedPartition,
    /// The high watermark of the first answer: the read ends there.
    end: Option<i64>,
}

impl QueueBuilder {
    /// Every record of these partitions, read by offset from the oldest record
    /// still kept up to where each partition ended when it was first read. No
    /// lease, no cursor, nothing to ack.
    ///
    /// The map has one [`FetchedPartition`] per partition named. A partition
    /// nobody has written, or of a queue that does not exist, has no records.
    pub async fn fetch<I, S>(&self, partitions: I) -> Result<HashMap<String, FetchedPartition>>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.fetch_with(partitions, FetchOptions::default()).await
    }

    /// [`Self::fetch`] with start offsets and a byte cap per call. A partition
    /// read from an offset past its end has no records.
    pub async fn fetch_with<I, S>(
        &self,
        partitions: I,
        options: FetchOptions,
    ) -> Result<HashMap<String, FetchedPartition>>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let queue = self
            .queue
            .as_deref()
            .ok_or_else(|| Error::Invalid("a queue name is required for fetch".into()))?;
        let max_bytes = options.max_bytes.unwrap_or(DEFAULT_FETCH_MAX_BYTES);
        if max_bytes < 1 {
            return Err(Error::Invalid(format!(
                "fetch: max_bytes must be positive, got {max_bytes}"
            )));
        }

        let mut names: Vec<String> = Vec::new();
        let mut state: HashMap<String, PartState> = HashMap::new();
        for p in partitions {
            let name: String = p.into();
            if name.is_empty() {
                return Err(Error::Invalid(
                    "fetch: a partition name must be a non-empty string".into(),
                ));
            }
            if state.contains_key(&name) {
                continue;
            }
            let start = options.offsets.get(&name).copied().unwrap_or(0);
            if start < 0 {
                return Err(Error::Invalid(format!(
                    "fetch: the offset of '{name}' must be non-negative, got {start}"
                )));
            }
            state.insert(
                name.clone(),
                PartState {
                    read: FetchedPartition {
                        partition: name.clone(),
                        next_offset: start,
                        ..FetchedPartition::default()
                    },
                    end: None,
                },
            );
            names.push(name);
        }

        let opts = read_opts(queue);
        let mut pending = names.clone();
        while !pending.is_empty() {
            let mut again: Vec<String> = Vec::new();
            for chunk in pending.chunks(ENTRIES_PER_CALL) {
                let entries: Vec<FetchEntry> = chunk
                    .iter()
                    .map(|name| {
                        FetchEntry::new(queue, state[name].read.next_offset)
                            .partition(name.clone())
                            .max_bytes(max_bytes)
                    })
                    .collect();
                let body = FetchBody {
                    entries: &entries,
                    max_bytes,
                };
                let answer: FetchResponse = self
                    .inner
                    .http
                    .post_json("/api/v1/fetch", &body, &opts)
                    .await?
                    .ok_or_else(|| Error::Decode(format!("fetch {queue}: empty answer")))?;
                if answer.entries.len() != chunk.len() {
                    return Err(Error::Decode(format!(
                        "fetch {queue}: the broker answered {} entries for {} asked",
                        answer.entries.len(),
                        chunk.len()
                    )));
                }

                let mut delivered = 0usize;
                // Entries that came back empty while short of their end: the
                // call's byte ceiling was spent by the entries before them.
                let mut starved: Vec<String> = Vec::new();
                for (name, entry) in chunk.iter().zip(answer.entries) {
                    let s = state.get_mut(name).expect("asked partition");
                    s.read.high_watermark = entry.high_watermark;
                    s.read.log_start_offset = entry.log_start_offset;
                    if let Some(error) = entry.error.as_deref() {
                        if error == ERR_OFFSET_OUT_OF_RANGE
                            && s.read.next_offset < s.read.log_start_offset
                        {
                            // Retention deleted what was asked for: go on from
                            // the oldest kept.
                            s.read.next_offset = s.read.log_start_offset;
                            let end = *s.end.get_or_insert(s.read.high_watermark);
                            if s.read.next_offset < end {
                                again.push(name.clone());
                            }
                            continue;
                        }
                        if error == ERR_OFFSET_OUT_OF_RANGE
                            || error == ERR_UNKNOWN_TOPIC_OR_PARTITION
                        {
                            continue;
                        }
                        return Err(Error::Decode(format!(
                            "fetch {queue}/{name}: the broker answered {error}"
                        )));
                    }
                    let end = *s.end.get_or_insert(s.read.high_watermark);
                    if let Some(last) = entry.records.last() {
                        delivered += entry.records.len();
                        s.read.next_offset = last.offset + 1;
                        s.read.records.extend(entry.records);
                        if s.read.next_offset < end {
                            again.push(name.clone());
                        }
                    } else if s.read.next_offset < end {
                        starved.push(name.clone());
                    }
                }
                // An empty entry is asked again only when its call read
                // something: a call that read nothing at all would answer the
                // same next time.
                if delivered > 0 {
                    again.extend(starved);
                }
            }
            pending = again;
        }

        Ok(state.into_iter().map(|(name, s)| (name, s.read)).collect())
    }

    /// The partitions of the queue written at or after `since`, in the order
    /// they were created, and the `safe_time` of the answer. `since` is an
    /// ISO-8601 string, a [`SystemTime`] or [`Since::All`]. A queue that does
    /// not exist has none.
    ///
    /// Pass `safe_time` as the next `since` to see only what changed after this
    /// call.
    pub async fn changed_since(&self, since: impl Into<Since>) -> Result<ChangedPartitions> {
        let queue = self
            .queue
            .as_deref()
            .ok_or_else(|| Error::Invalid("a queue name is required for changed_since".into()))?;
        let since = since.into();
        if since == Since::At(String::new()) {
            return Err(Error::Invalid(
                "changed_since: since must be a time; use Since::All for every partition".into(),
            ));
        }
        let opts = read_opts(queue);
        let mut out = ChangedPartitions::default();
        let mut after: Option<String> = None;
        loop {
            let mut entry = ChangedEntry::new(queue).limit(CHANGED_PAGE);
            if let Since::At(at) = &since {
                entry = entry.since(at.clone());
            }
            if let Some(cursor) = after.take() {
                entry = entry.after(cursor);
            }
            let answer: ChangedResponse = self
                .inner
                .http
                .post_json(
                    "/api/v1/partitions/changed",
                    &ChangedRequest::new(vec![entry]),
                    &opts,
                )
                .await?
                .ok_or_else(|| Error::Decode(format!("changed_since {queue}: empty answer")))?;
            // Pages served by different nodes: the smallest is the one that
            // holds for all of them. The broker spells every one the same way,
            // so the strings compare in time order.
            if out.safe_time.is_empty() || answer.safe_time < out.safe_time {
                out.safe_time = answer.safe_time;
            }
            let Some(result) = answer.entries.into_iter().next() else {
                return Err(Error::Decode(format!(
                    "changed_since {queue}: the broker answered no entry"
                )));
            };
            if result.is_unknown_queue() {
                return Ok(out);
            }
            if let Some(error) = result.error {
                return Err(Error::Decode(format!(
                    "changed_since {queue}: the broker answered {error}"
                )));
            }
            out.partitions.extend(result.partitions);
            match result.next {
                Some(next) if !next.is_empty() => after = Some(next),
                _ => return Ok(out),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_system_time_is_spelled_as_the_broker_spells_its_times() {
        let t = UNIX_EPOCH + std::time::Duration::from_micros(1_792_506_600_123_456);
        assert_eq!(
            Since::from(t),
            Since::At("2026-10-20T14:30:00.123456Z".into())
        );
        assert_eq!(
            Since::from(UNIX_EPOCH),
            Since::At("1970-01-01T00:00:00.000000Z".into())
        );
        assert_eq!(Since::from(None::<&str>), Since::All);
        assert_eq!(Since::from(Some("x")), Since::At("x".into()));
    }
}
