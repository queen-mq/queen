//! Topics: the queues of the caller's tenant, each with a Kafka width.
//!
//! Queen declares no partition count, so a topic's width is
//! `max(declared or QUEEN_KAFKA_DEFAULT_PARTITIONS, partitions that exist)`.
//! A declared width is a KV row, written before the queue on CreateTopics so a
//! node that lists the queue has already applied its width (raft applies in
//! order). The row's key and its `partitions` field are the first facade's, so
//! the topics it created keep their width.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde_json::json;

use crate::core::{CallError, KvOp, Queen};

pub const NS: &str = "queen-kafka";
pub const WIDTH_PREFIX: &str = "qk:topiccfg:";
const TTL: Duration = Duration::from_millis(1000);
/// A snapshot younger than this is not re-read for a missing name: a client
/// asking for a topic that does not exist cannot turn every request into a
/// queue list.
const MISS_FLOOR: Duration = Duration::from_millis(100);
const PAGE: i64 = 1000;
const MAX_PAGES: usize = 100;

/// Every topic of one tenant at one moment: name → width.
#[derive(Debug)]
pub struct Snapshot {
    pub widths: HashMap<String, i32>,
    /// Topics with a width row: the ones the console shows as Kafka topics.
    pub declared: std::collections::HashSet<String>,
    pub at: Instant,
}

struct Entry {
    at: Instant,
    snap: Arc<Snapshot>,
}

pub struct Topics {
    queen: Queen,
    default_partitions: i32,
    dedup_window_seconds: i64,
    cache: Mutex<HashMap<Option<String>, Entry>>,
    refreshing: tokio::sync::Mutex<()>,
    /// Topics whose dedup window this process has checked.
    deduped: Mutex<std::collections::HashSet<String>>,
    /// Topics this process gave a width row.
    marked: Mutex<std::collections::HashSet<String>>,
}

impl Topics {
    pub fn new(queen: Queen, default_partitions: i32, dedup_window_seconds: i64) -> Topics {
        Topics {
            queen,
            default_partitions,
            dedup_window_seconds,
            cache: Mutex::new(HashMap::new()),
            refreshing: tokio::sync::Mutex::new(()),
            deduped: Mutex::new(std::collections::HashSet::new()),
            marked: Mutex::new(std::collections::HashSet::new()),
        }
    }

    /// A topic Kafka clients write to and that has no width row gets one (its
    /// width now), once per process: that row is what the console reads as
    /// "a Kafka topic". Written only where no row is: a width another node
    /// declared a moment ago (CreateTopics there) is never overwritten.
    pub async fn ensure_marked(
        &self,
        bearer: Option<&str>,
        snap: &Snapshot,
        topic: &str,
        width: i32,
    ) {
        if snap.declared.contains(topic) {
            return;
        }
        if !self
            .marked
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(topic.to_string())
        {
            return;
        }
        if let Err(e) = self.declare_if_absent(bearer, topic, width).await {
            tracing::debug!(target: "kafka", %topic, error = %e, "cannot write the topic's width row");
            self.marked
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .remove(topic);
        }
    }

    /// Before an idempotent producer's first write to `topic` in this process:
    /// a queue with no dedup window (every topic the first facade created had
    /// none) gets `QUEEN_KAFKA_DEDUP_WINDOW_SECONDS`, so a retried batch is a
    /// duplicate there as on any Queen queue. A queue that has a window keeps
    /// it. Failures are logged and retried on the next write.
    pub async fn ensure_dedup(&self, bearer: Option<&str>, topic: &str) {
        if self.dedup_window_seconds <= 0 {
            return;
        }
        if self
            .deduped
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .contains(topic)
        {
            return;
        }
        let window = match self.queen.queue_options(bearer, topic).await {
            Ok(Some(o)) => o.get("dedupWindowSeconds").and_then(|v| v.as_i64()),
            Ok(None) => None,
            Err(e) => {
                tracing::warn!(target: "kafka", %topic, error = %e, "cannot read the dedup window");
                return;
            }
        };
        if window == Some(0) {
            let options = json!({ "dedupWindowSeconds": self.dedup_window_seconds });
            match self.queen.configure(bearer, topic, options).await {
                Ok(()) => tracing::info!(
                    target: "kafka",
                    %topic,
                    window_s = self.dedup_window_seconds,
                    "dedup window set on a topic an idempotent producer writes to (it had none)"
                ),
                Err(e) => {
                    tracing::warn!(target: "kafka", %topic, error = %e, "cannot set the dedup window");
                    return;
                }
            }
        }
        self.deduped
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(topic.to_string());
    }

    pub fn default_partitions(&self) -> i32 {
        self.default_partitions
    }

    fn cached(&self, bearer: Option<&str>, fresh: bool) -> Option<Arc<Snapshot>> {
        let cache = self.cache.lock().unwrap_or_else(|p| p.into_inner());
        let e = cache.get(&bearer.map(str::to_string))?;
        (!fresh || e.at.elapsed() < TTL).then(|| Arc::clone(&e.snap))
    }

    /// The topics, at most [`TTL`] old.
    pub async fn snapshot(&self, bearer: Option<&str>) -> Result<Arc<Snapshot>, CallError> {
        if let Some(s) = self.cached(bearer, true) {
            return Ok(s);
        }
        let _one = self.refreshing.lock().await;
        if let Some(s) = self.cached(bearer, true) {
            return Ok(s);
        }
        self.load(bearer).await
    }

    /// The topics, re-read once when one of `names` is missing: a topic that
    /// another node just created is not answered unknown for the rest of a TTL.
    /// The re-read waits until the snapshot is [`MISS_FLOOR`] old (a topic
    /// created within the floor is found too), and one that another request
    /// made meanwhile serves this one.
    pub async fn covering<'a>(
        &self,
        bearer: Option<&str>,
        names: impl IntoIterator<Item = &'a str>,
    ) -> Result<Arc<Snapshot>, CallError> {
        let snap = self.snapshot(bearer).await?;
        let missing = names
            .into_iter()
            .any(|n| valid_topic_name(n) && !snap.widths.contains_key(n));
        if !missing {
            return Ok(snap);
        }
        if let Some(wait) = MISS_FLOOR.checked_sub(snap.at.elapsed()) {
            tokio::time::sleep(wait).await;
        }
        let _one = self.refreshing.lock().await;
        match self.cached(bearer, true) {
            Some(newer) if newer.at > snap.at => Ok(newer),
            _ => self.load(bearer).await,
        }
    }

    /// The topics, read now.
    pub async fn refresh(&self, bearer: Option<&str>) -> Result<Arc<Snapshot>, CallError> {
        let _one = self.refreshing.lock().await;
        self.load(bearer).await
    }

    /// Every snapshot is re-read before it serves again. Kept, not dropped:
    /// a re-read that cannot reach the width rows (an election under way)
    /// falls back to it rather than to default widths.
    pub fn invalidate(&self) {
        let mut cache = self.cache.lock().unwrap_or_else(|p| p.into_inner());
        for e in cache.values_mut() {
            if let Some(old) = Instant::now().checked_sub(TTL) {
                e.at = old;
            }
        }
    }

    async fn load(&self, bearer: Option<&str>) -> Result<Arc<Snapshot>, CallError> {
        // The queues first, then the width rows: CreateTopics writes the row
        // before it creates the queue, so a queue this list holds has its row
        // in the read that follows. Read side by side, a row written between
        // the two reads went missing and its topic took the default width.
        let queues = self.queen.queues(bearer).await?;
        let declared = match self.declared(bearer).await {
            Ok(d) => d,
            // Unreadable rows never become default widths (Metadata would
            // advertise them, a produce would write partitions past the real
            // width and mark the default as the topic's width): the last
            // snapshot is served for another TTL, else the caller answers a
            // retriable error.
            Err(e) => {
                tracing::warn!(target: "kafka", error = %e, "cannot read the topic widths");
                let mut cache = self.cache.lock().unwrap_or_else(|p| p.into_inner());
                return match cache.get_mut(&bearer.map(str::to_string)) {
                    Some(last) => {
                        last.at = Instant::now();
                        Ok(Arc::clone(&last.snap))
                    }
                    None => Err(e),
                };
            }
        };
        let mut widths = HashMap::with_capacity(queues.len());
        for q in queues {
            if !valid_topic_name(&q.name) {
                continue;
            }
            let floor = declared
                .get(&q.name)
                .copied()
                .unwrap_or(self.default_partitions);
            let width = floor
                .max(q.partitions.clamp(0, i32::MAX as i64) as i32)
                .max(1);
            widths.insert(q.name, width);
        }
        let snap = Arc::new(Snapshot {
            declared: declared.keys().cloned().collect(),
            widths,
            at: Instant::now(),
        });
        self.cache.lock().unwrap_or_else(|p| p.into_inner()).insert(
            bearer.map(str::to_string),
            Entry {
                at: Instant::now(),
                snap: Arc::clone(&snap),
            },
        );
        Ok(snap)
    }

    async fn declared(&self, bearer: Option<&str>) -> Result<HashMap<String, i32>, CallError> {
        let mut out = HashMap::new();
        let mut after: Option<String> = None;
        for _ in 0..MAX_PAGES {
            let ops = [KvOp::GetPrefix {
                ns: NS.to_string(),
                prefix: WIDTH_PREFIX.to_string(),
                limit: PAGE,
                after: after.take(),
            }];
            let answer = self.queen.kv(bearer, &ops).await?.into_iter().next();
            let Some(answer) = answer else { break };
            for row in &answer.rows {
                let Some(topic) = row.key.strip_prefix(WIDTH_PREFIX) else {
                    continue;
                };
                if let Some(p) = row.value.get("partitions").and_then(|p| p.as_i64()) {
                    if p > 0 {
                        out.insert(topic.to_string(), p.min(i32::MAX as i64) as i32);
                    }
                }
            }
            match answer.next_after {
                Some(next) if !next.is_empty() => after = Some(next),
                _ => break,
            }
        }
        Ok(out)
    }

    /// Declare a topic's width, before its queue exists.
    pub async fn declare(
        &self,
        bearer: Option<&str>,
        topic: &str,
        partitions: i32,
    ) -> Result<(), CallError> {
        let ops = [KvOp::put(
            NS,
            &format!("{WIDTH_PREFIX}{topic}"),
            json!({ "partitions": partitions, "at": now_ms(), "set": {} }),
        )];
        self.queen.kv(bearer, &ops).await.map(|_| ())
    }

    /// [`Topics::declare`], only where the topic has no width row yet.
    pub async fn declare_if_absent(
        &self,
        bearer: Option<&str>,
        topic: &str,
        partitions: i32,
    ) -> Result<(), CallError> {
        let ops = [KvOp::put_at(
            NS,
            &format!("{WIDTH_PREFIX}{topic}"),
            json!({ "partitions": partitions, "at": now_ms(), "set": {} }),
            0,
        )];
        self.queen.kv(bearer, &ops).await.map(|_| ())
    }

    pub async fn forget(&self, bearer: Option<&str>, topic: &str) -> Result<(), CallError> {
        let ops = [KvOp::Delete {
            ns: NS.to_string(),
            key: format!("{WIDTH_PREFIX}{topic}"),
        }];
        self.queen.kv(bearer, &ops).await.map(|_| ())
    }
}

pub fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Kafka's rule: 1 to 249 of `[A-Za-z0-9._-]`, and not `.` or `..`.
pub fn valid_topic_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 249
        && name != "."
        && name != ".."
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn topic_names_follow_kafka() {
        assert!(valid_topic_name("orders.v1_x-2"));
        assert!(!valid_topic_name(""));
        assert!(!valid_topic_name(".."));
        assert!(!valid_topic_name("a/b"));
        assert!(!valid_topic_name(&"x".repeat(250)));
    }
}
