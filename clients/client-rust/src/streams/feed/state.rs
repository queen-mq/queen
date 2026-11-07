//! The state of one stateful step of a feed stream (feed/state.js): its open
//! windows, sessions, waiting messages or table rows, by state key, in
//! memory, with every change recorded so that it can be undone.
//!
//! It is not written to KV at every commit: a commit writes the position, and
//! now and then the runner writes a snapshot of every step's state
//! (snapshot.rs); a restart loads the last snapshot and reads the feed again
//! from its position. A function that fails leaves no trace (its message is
//! retried, skipped or dead-lettered against the state from before it), and
//! a commit that did not happen rolls back to the last one that did.
//!
//! Entries keep the order a JS `Map` keeps: insertion order, a key set again
//! keeps its place, a key deleted and set again goes to the end.

use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet};

use serde_json::Value;

use crate::js;

/// The messages an event-time step holds until its watermark reaches them
/// are entries of the step's state under this prefix; the engines leave them
/// alone.
pub(crate) const BUFFER_PREFIX: &str = "b\u{1f}";
/// Windows, deadlines or runs closed in one go; then the results run, then the next batch.
pub(crate) const CLOSE_BATCH: usize = 2_000;
/// Entries indexed between two yields when a loaded state is indexed again.
pub(crate) const REBUILD_BATCH: usize = 50_000;

pub(crate) fn is_buffer_key(key: &str) -> bool {
    key.starts_with(BUFFER_PREFIX)
}

// ------------------------------------------------------------ ordered map

/// A map in insertion order, as a JS `Map`.
#[derive(Debug, Clone, Default)]
pub(crate) struct OrderedMap {
    slots: Vec<Option<(String, Value)>>,
    index: HashMap<String, usize>,
}

impl OrderedMap {
    pub fn len(&self) -> usize {
        self.index.len()
    }

    pub fn get(&self, key: &str) -> Option<&Value> {
        self.index
            .get(key)
            .and_then(|&i| self.slots[i].as_ref().map(|(_, v)| v))
    }

    /// Set `key`; a key already there keeps its place. The value it had.
    pub fn insert(&mut self, key: String, value: Value) -> Option<Value> {
        if let Some(&i) = self.index.get(&key) {
            let slot = self.slots[i].as_mut().expect("indexed slot");
            return Some(std::mem::replace(&mut slot.1, value));
        }
        self.index.insert(key.clone(), self.slots.len());
        self.slots.push(Some((key, value)));
        None
    }

    pub fn remove(&mut self, key: &str) -> Option<Value> {
        let i = self.index.remove(key)?;
        let (_, v) = self.slots[i].take().expect("indexed slot");
        // Compact once the holes outnumber the entries.
        if self.slots.len() > 64 && self.slots.len() > 2 * self.index.len() {
            self.compact();
        }
        Some(v)
    }

    fn compact(&mut self) {
        let slots: Vec<Option<(String, Value)>> =
            self.slots.drain(..).filter(|s| s.is_some()).collect();
        self.index.clear();
        for (i, s) in slots.iter().enumerate() {
            if let Some((k, _)) = s {
                self.index.insert(k.clone(), i);
            }
        }
        self.slots = slots;
    }

    pub fn iter(&self) -> impl Iterator<Item = (&String, &Value)> {
        self.slots
            .iter()
            .filter_map(|s| s.as_ref().map(|(k, v)| (k, v)))
    }

    /// Entries from slot `from` on, at most `n`: `(entries, next slot)`. The
    /// slots stay put while nothing is set or deleted, so a caller can walk
    /// the map in pieces.
    pub fn slice_from(&self, from: usize, n: usize) -> (Vec<(&String, &Value)>, usize) {
        let mut out = Vec::new();
        let mut i = from;
        while i < self.slots.len() && out.len() < n {
            if let Some((k, v)) = &self.slots[i] {
                out.push((k, v));
            }
            i += 1;
        }
        (out, i)
    }

    pub fn slot_count(&self) -> usize {
        self.slots.len()
    }

    /// The entry in slot `i` (`None`: a hole left by a delete).
    pub fn entry_at(&self, i: usize) -> Option<(&String, &Value)> {
        self.slots
            .get(i)
            .and_then(|s| s.as_ref().map(|(k, v)| (k, v)))
    }

    pub fn clear(&mut self) {
        self.slots.clear();
        self.index.clear();
    }
}

/// A set in insertion order.
#[derive(Debug, Clone, Default)]
pub(crate) struct OrderedSet {
    items: Vec<String>,
    seen: HashSet<String>,
}

impl OrderedSet {
    pub fn insert(&mut self, key: &str) {
        if self.seen.insert(key.to_string()) {
            self.items.push(key.to_string());
        }
    }
    pub fn len(&self) -> usize {
        self.items.len()
    }
    pub fn clear(&mut self) {
        self.items.clear();
        self.seen.clear();
    }
    pub fn items(&self) -> &[String] {
        &self.items
    }
}

// ------------------------------------------------------------- feed state

/// What the engine of a step says its keys are, for the counts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Classify {
    /// `held` for a buffer key; `open` for a window (`w\x1f`) or a session (`s\x1f`).
    Window,
    /// `held` for a buffer key; `open` for anything else.
    Pattern,
    /// `held` for a buffer key; `open` for a run not done.
    Collect,
}

impl Classify {
    fn class(self, key: &str, value: &Value) -> Option<&'static str> {
        match self {
            Classify::Window => {
                if is_buffer_key(key) {
                    Some("held")
                } else if key.starts_with("w\u{1f}") || key.starts_with("s\u{1f}") {
                    Some("open")
                } else {
                    None
                }
            }
            Classify::Pattern => Some(if is_buffer_key(key) { "held" } else { "open" }),
            Classify::Collect => {
                if is_buffer_key(key) {
                    Some("held")
                } else if !value.is_null() && !js::truthy(value.get("done")) {
                    Some("open")
                } else {
                    None
                }
            }
        }
    }
}

struct Undo {
    key: String,
    prev: Option<Value>,
}

/// One step's state: entries, an undo log, what changed since the last
/// snapshot, and counts by class.
#[derive(Default)]
pub(crate) struct FeedState {
    map: OrderedMap,
    undo: Vec<Undo>,
    changed: bool,
    touched: OrderedSet,
    classify: Option<Classify>,
    open: i64,
    held: i64,
}

impl FeedState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn size(&self) -> usize {
        self.map.len()
    }

    /// Something changed since the last commit.
    pub fn dirty(&self) -> bool {
        !self.undo.is_empty()
    }

    /// Something changed since the last snapshot.
    pub fn changed_since_snapshot(&self) -> bool {
        self.changed
    }

    /// How many keys were set or deleted since the last snapshot.
    pub fn touched_since_snapshot(&self) -> usize {
        self.touched.len()
    }

    pub fn snapshot_taken(&mut self) {
        self.changed = false;
        self.touched.clear();
    }

    pub fn classify_with(&mut self, c: Classify) {
        self.classify = Some(c);
        self.recount();
    }

    /// How many entries of class `name` (`open`, `held`) the state holds.
    pub fn count(&self, name: &str) -> usize {
        match name {
            "open" => self.open.max(0) as usize,
            "held" => self.held.max(0) as usize,
            _ => 0,
        }
    }

    pub fn get(&self, key: &str) -> Option<&Value> {
        self.map.get(key)
    }

    pub fn entries(&self) -> impl Iterator<Item = (&String, &Value)> {
        self.map.iter()
    }

    pub fn map(&self) -> &OrderedMap {
        &self.map
    }

    /// Store `value` (stored values are never changed in place).
    pub fn set(&mut self, key: String, value: Value) {
        self.bump(&key, &value, 1);
        let prev = self.map.insert(key.clone(), value);
        if let Some(p) = &prev {
            self.bump(&key, p, -1);
        }
        self.touched.insert(&key);
        self.undo.push(Undo { key, prev });
        self.changed = true;
    }

    pub fn delete(&mut self, key: &str) {
        let Some(prev) = self.map.remove(key) else {
            return;
        };
        self.bump(key, &prev, -1);
        self.touched.insert(key);
        self.undo.push(Undo {
            key: key.to_string(),
            prev: Some(prev),
        });
        self.changed = true;
    }

    /// A point to roll back to.
    pub fn mark(&self) -> usize {
        self.undo.len()
    }

    pub fn rollback_to(&mut self, mark: usize) {
        while self.undo.len() > mark {
            let u = self.undo.pop().expect("undo entry");
            if let Some(cur) = self.map.get(&u.key).cloned() {
                self.bump(&u.key, &cur, -1);
            }
            match u.prev {
                Some(prev) => {
                    self.bump(&u.key, &prev, 1);
                    self.map.insert(u.key, prev);
                }
                None => {
                    self.map.remove(&u.key);
                }
            }
            // The key stays touched: a delta writes it as it is now, which is
            // right whether or not it went back to what the snapshot holds.
        }
    }

    /// Back to the state of the last commit.
    pub fn discard(&mut self) {
        self.rollback_to(0);
    }

    /// The changes are committed: they are the new starting point.
    pub fn committed(&mut self) {
        self.undo.clear();
    }

    /// The keys touched since the last snapshot, in the order first touched.
    pub fn touched_keys(&self) -> &[String] {
        self.touched.items()
    }

    /// Replace everything with a snapshot's entries; nothing to undo.
    pub fn load(&mut self, entries: Vec<(String, Value)>) {
        self.map.clear();
        for (k, v) in entries {
            self.map.insert(k, v);
        }
        self.undo.clear();
        self.changed = false;
        self.touched.clear();
        self.recount();
    }

    /// A load in pieces: start empty, then `load_set` / `load_delete` each piece, then `loaded()`.
    pub fn load_begin(&mut self) {
        self.map.clear();
        self.undo.clear();
        self.open = 0;
        self.held = 0;
    }

    pub fn load_set(&mut self, key: String, value: Value) {
        self.bump(&key, &value, 1);
        if let Some(prev) = self.map.insert(key.clone(), value) {
            self.bump(&key, &prev, -1);
        }
    }

    pub fn load_delete(&mut self, key: &str) {
        if let Some(prev) = self.map.remove(key) {
            self.bump(key, &prev, -1);
        }
    }

    /// The pieces are in: the state is as the snapshot holds it.
    pub fn loaded(&mut self) {
        self.undo.clear();
        self.changed = false;
        self.touched.clear();
    }

    /// Start empty (a reset): the next snapshot writes the empty state.
    pub fn wipe(&mut self) {
        self.map.clear();
        self.undo.clear();
        self.changed = true;
        self.touched.clear();
        self.open = 0;
        self.held = 0;
    }

    fn bump(&mut self, key: &str, value: &Value, d: i64) {
        let Some(c) = self.classify else { return };
        match c.class(key, value) {
            Some("open") => self.open += d,
            Some("held") => self.held += d,
            _ => {}
        }
    }

    fn recount(&mut self) {
        self.open = 0;
        self.held = 0;
        let Some(c) = self.classify else { return };
        let (mut open, mut held) = (0, 0);
        for (k, v) in self.map.iter() {
            match c.class(k, v) {
                Some("open") => open += 1,
                Some("held") => held += 1,
                _ => {}
            }
        }
        self.open = open;
        self.held = held;
    }
}

// --------------------------------------------------------------- due heap

#[derive(Debug, Clone)]
pub(crate) struct Due {
    pub due: f64,
    pub key: String,
}

impl PartialEq for Due {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}
impl Eq for Due {}
impl PartialOrd for Due {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Due {
    /// Reversed (a max-heap of these pops the EARLIEST): the earlier due,
    /// then the smaller key, so ties close in one order every time.
    fn cmp(&self, other: &Self) -> Ordering {
        let by_due = if self.due < other.due {
            Ordering::Less
        } else if self.due > other.due {
            Ordering::Greater
        } else {
            Ordering::Equal
        };
        by_due
            .then_with(|| js::str_cmp(&self.key, &other.key))
            .reverse()
    }
}

/// A min-heap of `{ due, key }`: the next window or deadline to close.
#[derive(Debug, Default)]
pub(crate) struct DueHeap {
    heap: BinaryHeap<Due>,
}

impl DueHeap {
    pub fn clear(&mut self) {
        self.heap.clear();
    }
    pub fn peek(&self) -> Option<&Due> {
        self.heap.peek()
    }
    pub fn push(&mut self, due: f64, key: String) {
        self.heap.push(Due { due, key });
    }
    pub fn pop(&mut self) -> Option<Due> {
        self.heap.pop()
    }
}

// ------------------------------------------------------------------ shards

/// FNV-1a, 32 bit, over the UTF-8 bytes of `text`: the hash the broker picks
/// a shard by (feed.rs `fnv1a32`).
pub fn fnv1a32(text: &str) -> u32 {
    let mut h: u32 = 0x811c_9dc5;
    for b in text.as_bytes() {
        h ^= u32::from(*b);
        h = h.wrapping_mul(0x0100_0193);
    }
    h
}

/// The shard of `count` that a key (a partition name, by default) belongs to.
pub fn shard_of(key: &str, count: u32) -> u32 {
    fnv1a32(key) % count.max(1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn the_same_hash_as_the_broker() {
        assert_eq!(fnv1a32(""), 0x811c9dc5);
        assert_eq!(fnv1a32("a"), 0xe40c292c);
        assert_eq!(fnv1a32("foobar"), 0xbf9cf968);
        assert_eq!(
            fnv1a32("é"),
            fnv1a32(std::str::from_utf8(&[0xc3, 0xa9]).unwrap())
        );
        assert_eq!(shard_of("x", 0), 0, "a count below one counts as one");
        assert_eq!(shard_of("x", 3), fnv1a32("x") % 3);
    }

    #[test]
    fn a_map_keeps_js_map_order() {
        let mut m = OrderedMap::default();
        m.insert("a".into(), json!(1));
        m.insert("b".into(), json!(2));
        m.insert("a".into(), json!(3));
        m.remove("b");
        m.insert("b".into(), json!(4));
        let keys: Vec<_> = m.iter().map(|(k, _)| k.clone()).collect();
        assert_eq!(keys, ["a", "b"]);
        assert_eq!(m.get("a"), Some(&json!(3)));
        for i in 0..200 {
            m.insert(format!("k{i}"), json!(i));
        }
        for i in 0..190 {
            m.remove(&format!("k{i}"));
        }
        let keys: Vec<_> = m.iter().map(|(k, _)| k.clone()).collect();
        assert_eq!(keys.len(), 12);
        assert_eq!(keys[2], "k190");
    }

    #[test]
    fn keeps_its_class_counts_through_sets_deletes_rollbacks_and_loads() {
        let mut st = FeedState::new();
        st.classify_with(Classify::Collect);
        let scan = |st: &FeedState, c: &str| {
            st.entries()
                .filter(|(k, v)| Classify::Collect.class(k, v) == Some(c))
                .count()
        };
        let mut seed: u64 = 7;
        let mut rnd = |n: u64| {
            seed = (seed * 1103515245 + 12345) % 2147483648;
            seed % n
        };
        for round in 0..300 {
            let mark = st.mark();
            for _ in 0..20 {
                let k = format!("{}{}", if rnd(2) == 1 { "b\u{1f}" } else { "c" }, rnd(30));
                if rnd(3) == 0 {
                    st.delete(&k);
                } else {
                    let done = rnd(4) == 0;
                    st.set(k, json!({ "done": done }));
                }
            }
            if rnd(2) == 1 {
                st.rollback_to(mark);
            } else {
                st.committed();
            }
            assert_eq!(st.count("open"), scan(&st, "open"), "open, round {round}");
            assert_eq!(st.count("held"), scan(&st, "held"), "held, round {round}");
        }
        let entries: Vec<(String, Value)> =
            st.entries().map(|(k, v)| (k.clone(), v.clone())).collect();
        st.load_begin();
        for (k, v) in entries.iter().cloned() {
            st.load_set(k, v);
        }
        st.load_delete(&entries[0].0);
        st.loaded();
        assert_eq!(st.count("open"), scan(&st, "open"));
        assert_eq!(st.count("held"), scan(&st, "held"));
        assert_eq!(
            st.touched_since_snapshot(),
            0,
            "a load leaves nothing to write"
        );
        st.wipe();
        assert_eq!(st.count("open"), 0);
    }

    #[test]
    fn the_heap_pops_the_earliest_then_the_smaller_key() {
        let mut h = DueHeap::default();
        h.push(5.0, "b".into());
        h.push(5.0, "a".into());
        h.push(1.0, "z".into());
        h.push(7.0, "c".into());
        let order: Vec<_> = std::iter::from_fn(|| h.pop())
            .map(|d| (d.due, d.key))
            .collect();
        assert_eq!(
            order,
            [
                (1.0, "z".into()),
                (5.0, "a".into()),
                (5.0, "b".into()),
                (7.0, "c".into())
            ]
        );
    }
}
