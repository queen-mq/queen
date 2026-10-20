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
    pub at: Instant,
}

struct Entry {
    at: Instant,
    snap: Arc<Snapshot>,
}

pub struct Topics {
    queen: Queen,
    default_partitions: i32,
    cache: Mutex<HashMap<Option<String>, Entry>>,
    refreshing: tokio::sync::Mutex<()>,
}

impl Topics {
    pub fn new(queen: Queen, default_partitions: i32) -> Topics {
        Topics {
            queen,
            default_partitions,
            cache: Mutex::new(HashMap::new()),
            refreshing: tokio::sync::Mutex::new(()),
        }
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
    pub async fn covering<'a>(
        &self,
        bearer: Option<&str>,
        names: impl IntoIterator<Item = &'a str>,
    ) -> Result<Arc<Snapshot>, CallError> {
        let snap = self.snapshot(bearer).await?;
        let missing = names
            .into_iter()
            .any(|n| valid_topic_name(n) && !snap.widths.contains_key(n));
        if !missing || snap.at.elapsed() < MISS_FLOOR {
            return Ok(snap);
        }
        self.refresh(bearer).await
    }

    /// The topics, read now.
    pub async fn refresh(&self, bearer: Option<&str>) -> Result<Arc<Snapshot>, CallError> {
        let _one = self.refreshing.lock().await;
        self.load(bearer).await
    }

    pub fn invalidate(&self) {
        self.cache.lock().unwrap_or_else(|p| p.into_inner()).clear();
    }

    async fn load(&self, bearer: Option<&str>) -> Result<Arc<Snapshot>, CallError> {
        let (queues, declared) = tokio::join!(self.queen.queues(bearer), self.declared(bearer));
        let queues = queues?;
        // A width row that cannot be read leaves the default: the topic is
        // still served, and the next refresh tries again.
        let declared = declared.unwrap_or_else(|e| {
            tracing::warn!(target: "kafka", error = %e, "cannot read the topic widths");
            HashMap::new()
        });
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
        let ops = [KvOp::Put {
            ns: NS.to_string(),
            key: format!("{WIDTH_PREFIX}{topic}"),
            value: json!({ "partitions": partitions, "at": now_ms(), "set": {} }),
            forever: true,
        }];
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
