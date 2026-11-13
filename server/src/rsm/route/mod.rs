//! Routing without copies (PLAN_ROUTING.md).
//!
//! - [`cond`]: the `where` language, shared with the tenant feed.
//! - [`link`]: reading a link frame back as the message it stands for.
//!
//! The rest is what the planner, the facade and retention share: the names of
//! a filtered group's hidden queue, the compiled conditions of a reader, and
//! the partition a message of a source lands in.
//!
//! A READING QUEUE (`config({ from, where, partitionBy })`) reads other queues
//! of its tenant: when a push to a source commits, the same entry appends a
//! link frame for every message a reader's `where` takes. A FILTERED GROUP
//! (`.group(g).where(...)`) is a hidden reading queue of its own, named
//! [`hidden_queue`], with the source's partitions and the source's settings;
//! the API keeps naming the source and the group. The source keeps every
//! message: a reader counts as one more consumer group of it for its
//! completed retention ([`crate::rsm::maintenance`]).

pub mod cond;
pub mod link;

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

use serde_json::Value;

use crate::rsm::effect::{QueueConfig, RouteCfg};

/// The separator of a hidden queue's name: a control character nobody types.
pub const HIDDEN_SEP: char = '\u{1}';

/// The hidden queue of group `group` filtering `queue`.
pub fn hidden_queue(queue: &str, group: &str) -> String {
    format!("{queue}{HIDDEN_SEP}{group}")
}

/// `(queue, group)` of a hidden queue's name; `None` for any other name.
pub fn split_hidden(name: &str) -> Option<(&str, &str)> {
    name.split_once(HIDDEN_SEP)
}

/// Whether `name` is a hidden queue's.
pub fn is_hidden(name: &str) -> bool {
    name.contains(HIDDEN_SEP)
}

/// The name a client sees for a queue: a hidden queue's source.
pub fn shown_name(name: &str) -> &str {
    split_hidden(name).map_or(name, |(q, _)| q)
}

/// The most queues reading one source, by default (`QUEEN_MAX_READERS_PER_QUEUE`):
/// each costs an evaluation of its `where` on every push.
pub fn max_readers_per_queue() -> usize {
    static V: OnceLock<usize> = OnceLock::new();
    *V.get_or_init(|| env_usize("QUEEN_MAX_READERS_PER_QUEUE", 64))
}

/// The most sources one reading queue reads, by default (`QUEEN_MAX_SOURCES_PER_QUEUE`).
pub fn max_sources_per_queue() -> usize {
    static V: OnceLock<usize> = OnceLock::new();
    *V.get_or_init(|| env_usize("QUEEN_MAX_SOURCES_PER_QUEUE", 16))
}

/// The deepest chain of readers (a reader read by a reader...). Loops are
/// refused when a reader is configured; this bounds the planner anyway.
pub const MAX_CHAIN_DEPTH: usize = 8;

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .filter(|n| *n > 0)
        .unwrap_or(default)
}

/// A reader's conditions on one source, compiled from the catalogue's list
/// once and shared (a reader is evaluated on every push to its source).
pub fn compiled(where_json: &str) -> Arc<Vec<cond::Cond>> {
    static CACHE: OnceLock<Mutex<HashMap<String, Arc<Vec<cond::Cond>>>>> = OnceLock::new();
    let cache = CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    if let Some(c) = cache.lock().expect("where cache").get(where_json) {
        return c.clone();
    }
    // The catalogue only holds lists the receiver checked; a list that does not
    // parse (a damaged row) takes nothing rather than everything.
    let conds = serde_json::from_str::<Value>(where_json)
        .ok()
        .and_then(|v| cond::parse_value(&v, &cond::Rules::FEED).ok());
    let conds = Arc::new(conds.unwrap_or_else(|| vec![never()]));
    let mut g = cache.lock().expect("where cache");
    if g.len() >= 4096 {
        g.clear();
    }
    g.insert(where_json.to_string(), conds.clone());
    conds
}

/// A condition no message meets.
fn never() -> cond::Cond {
    cond::Cond {
        field: cond::Field::Header(String::new()),
        op: cond::Op::Exists,
        value: Value::Null,
    }
}

/// The partition a message of `source_partition` lands in at a reader that
/// names its partitions by `partition_by` (`""`: the same name). `None` when
/// the field is missing, empty or not a string or a number: such a message is
/// not routed (and counted as `unrouted`).
pub fn target_partition(
    partition_by: &str,
    source_partition: &str,
    doc: Option<&Value>,
    headers: Option<&[u8]>,
) -> Option<String> {
    if partition_by.is_empty() {
        return Some(source_partition.to_string());
    }
    if let Some(key) = partition_by.strip_prefix("headers.") {
        let v = headers.and_then(|b| crate::frames::header_get(b, key))?;
        return (!v.is_empty()).then(|| v.to_string());
    }
    let path = partition_by.strip_prefix("data.")?;
    let mut cur = doc?;
    for key in path.split('.') {
        cur = match cur {
            Value::Object(m) => m.get(key)?,
            Value::Array(a) => a.get(key.parse::<usize>().ok()?)?,
            _ => return None,
        };
    }
    match cur {
        Value::String(s) if !s.is_empty() => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        _ => None,
    }
}

/// Whether `partition_by` is one a reader may name its partitions by.
pub fn partition_by_valid(partition_by: &str) -> bool {
    if partition_by.is_empty() {
        return true;
    }
    if let Some(k) = partition_by.strip_prefix("headers.") {
        return crate::frames::header_key_valid(k);
    }
    match partition_by.strip_prefix("data.") {
        Some(p) => {
            let parts: Vec<&str> = p.split('.').collect();
            !parts.iter().any(|s| s.is_empty()) && parts.len() <= cond::MAX_FIELD_DEPTH
        }
        None => false,
    }
}

/// Whether any of a reader's sources reads the payload, for one source.
pub fn reads_payload(route: &RouteCfg, source: &str) -> bool {
    route.partition_by.starts_with("data.")
        || route
            .sources
            .iter()
            .filter(|s| s.queue == source)
            .any(|s| cond::wants_payload(&compiled(&s.where_json)))
}

/// The consume settings a filtered group's hidden queue takes from its
/// source (lease, retries, DLQ, priority, delays), with its keep settings:
/// it keeps what the source keeps.
pub fn follow_source(hidden: &mut QueueConfig, source: &QueueConfig) {
    hidden.namespace = source.namespace.clone();
    hidden.task = source.task.clone();
    hidden.priority = source.priority;
    hidden.lease_time = source.lease_time;
    hidden.retry_limit = source.retry_limit;
    hidden.retry_delay = source.retry_delay;
    hidden.ttl = source.ttl;
    hidden.dead_letter_queue = source.dead_letter_queue;
    hidden.dlq_after_max_retries = source.dlq_after_max_retries;
    hidden.delayed_processing = source.delayed_processing;
    hidden.window_buffer = source.window_buffer;
    hidden.min_pop_wait_time = source.min_pop_wait_time;
    keep_like(hidden, &[source]);
}

/// A reading queue's keep settings, from its sources: it never keeps a link
/// longer than the longest of them would keep the message (a link whose
/// message a shorter source dropped reads as gone). Retention off at any
/// source keeps everything; `0` in a time setting is no limit.
pub fn keep_like(reader: &mut QueueConfig, sources: &[&QueueConfig]) {
    if sources.is_empty() {
        return;
    }
    let longest = |f: &dyn Fn(&QueueConfig) -> i32| -> i32 {
        if sources.iter().any(|s| f(s) <= 0) {
            0
        } else {
            sources.iter().map(|s| f(s)).max().unwrap_or(0)
        }
    };
    let enabled = sources.iter().all(|s| s.retention_enabled);
    reader.retention_enabled = enabled;
    reader.retention_seconds = if enabled {
        longest(&|s| s.retention_seconds)
    } else {
        0
    };
    reader.completed_retention_seconds = if enabled {
        longest(&|s| s.completed_retention_seconds)
    } else {
        0
    };
    reader.max_wait_time_seconds = longest(&|s| s.max_wait_time_seconds);
    // A link is never encrypted (it holds an address); dedup follows the
    // longest window, so a link retried within any source's window is found.
    reader.encryption_enabled = false;
    reader.dedup_window_seconds = sources
        .iter()
        .map(|s| s.dedup_window_seconds)
        .max()
        .unwrap_or(0);
    reader.max_queue_size = 0;
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn hidden_names_round_trip() {
        let h = hidden_queue("orders", "billing");
        assert!(is_hidden(&h));
        assert_eq!(split_hidden(&h), Some(("orders", "billing")));
        assert_eq!(shown_name(&h), "orders");
        assert_eq!(shown_name("orders"), "orders");
        assert!(!is_hidden("orders~billing"));
    }

    #[test]
    fn target_partitions_follow_the_field() {
        let doc = json!({"customerId": "c-42", "n": 7, "nested": {"k": "v"}, "empty": ""});
        let h = crate::frames::encode_headers([("customer", "c-9")]);
        assert_eq!(
            target_partition("", "order-1", Some(&doc), None).as_deref(),
            Some("order-1")
        );
        assert_eq!(
            target_partition("data.customerId", "o", Some(&doc), None).as_deref(),
            Some("c-42")
        );
        assert_eq!(
            target_partition("data.n", "o", Some(&doc), None).as_deref(),
            Some("7")
        );
        assert_eq!(
            target_partition("data.nested.k", "o", Some(&doc), None).as_deref(),
            Some("v")
        );
        assert_eq!(target_partition("data.empty", "o", Some(&doc), None), None);
        assert_eq!(
            target_partition("data.missing", "o", Some(&doc), None),
            None
        );
        assert_eq!(
            target_partition("headers.customer", "o", None, Some(&h)).as_deref(),
            Some("c-9")
        );
        assert_eq!(target_partition("headers.other", "o", None, Some(&h)), None);
        assert!(
            partition_by_valid("data.a.b")
                && partition_by_valid("headers.x-y")
                && partition_by_valid("")
        );
        assert!(
            !partition_by_valid("partition")
                && !partition_by_valid("data..a")
                && !partition_by_valid("headers.X")
        );
    }

    #[test]
    fn a_damaged_list_takes_nothing() {
        let c = compiled("not json");
        assert!(!cond::all_hold(&c, &cond::Subject::default()));
        let all = compiled("[]");
        assert!(cond::all_hold(&all, &cond::Subject::default()));
    }
}
