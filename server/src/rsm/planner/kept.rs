//! KEEP_OVERLAY (`QUEEN_RAFT_KEEP_OVERLAY`, default on): the planner's
//! [`Overlay`] kept BETWEEN planning cycles.
//!
//! The batcher used to rebuild the overlay every cycle: a fresh one over the
//! committed bases, then every in-flight entry folded again
//! ([`Overlay::ingest_entry`]) — up to the pipeline depth, every cycle, so a
//! pushed message was folded once per cycle it stayed in flight. Planning runs
//! on ONE thread (the batcher's `queen-planner`), so one overlay can live there
//! instead: each entry's effects are folded ONCE, as the planner plans them,
//! and when an entry LANDS (its index is at or below the planning read's
//! applied index) exactly its contributions come out again.
//!
//! # Exactly its contributions, field by field
//!
//! Every contribution carries the tag of the fold that wrote it, one tag per
//! entry ([`Tagged`], `OverlayAppend::tag`, [`TimerSlot`]). A landing walks the
//! landed entry's own effect list — O(its effects):
//!
//! | field | kind | on the landing of entry `T` |
//! |---|---|---|
//! | `queues`, `groups`, `pids_by_key`, `cursors`, `kv` | last writer wins | drop the key iff its tag is `T` |
//! | `parts[pid].created`, `.watermark` | last writer wins | drop iff tagged `T`; drop the part once empty |
//! | `parts[pid].appends` | one per `Append` | remove the one tagged `T` at that base |
//! | `dedup` | one occurrence per frame | remove `(offset, created_at)`: an offset is unique in its partition |
//! | `request_ids` | first writer wins | drop iff tagged `T` (a second writer never enters a kept overlay) |
//! | `timers` | patched by `TimerBackoff` | drop / re-fold from the entries still in flight / keep ([`TimerSlot`]) |
//! | `next_pid`, `next_kv_version`, `max_now_us`, `max_created_at_us` | running maxima | recomputed from the committed bases and the remaining entries' own maxima |
//!
//! The `created` flag is the non-idempotent one: a partition created in flight
//! and not dropped when its create lands would read as uncommitted for ever.
//! It goes with its tag like every other last-writer value.
//!
//! The result is, field for field and tag for tag, the overlay the old path
//! rebuilds from the same in-flight list and the same committed read. That is
//! what [`KeptOverlay::diff`] checks, and `rsm::tests::keep_overlay` asserts it
//! after every cycle of a mixed workload whose entries land out of step with
//! planning.
//!
//! # What it does NOT do
//!
//! It keeps ONE overlay and every probe stays a single lookup (a previous
//! attempt that probed one read-only map per in-flight entry made planning
//! slower). And it never decides on its own that a kept overlay is still good:
//! anything it cannot reconcile — an in-flight list that is not its own list
//! minus a landed prefix, a landed contribution that is not where the fold put
//! it, a request id in two entries — is an `Err`, and the batcher rebuilds
//! from scratch (the old path) that same cycle.

use std::collections::hash_map::Entry as MapEntry;
use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::Arc;

use super::{frame_hash, Overlay, Tagged};
use crate::rsm::effect::Effect;
use crate::rsm::entry::Entry;
use crate::rsm::fasthash::FxBuild;

type TimerKey = (String, String, String);

/// One entry whose effects are folded into a kept overlay, with what its folds
/// contributed to the overlay's running maxima.
struct KeptEntry {
    entry: Arc<Entry>,
    tag: u64,
    /// `max(pid + 1)` over its `PartitionCreate`s (0: none).
    pid_hi: u64,
    /// `max(version + 1)` over its `KvPut`s (0: none).
    kv_hi: u64,
    /// `max(created_at)` over its `PartitionCreate`s and `Append`s (0: none).
    created_hi: i64,
}

impl KeptEntry {
    fn new(entry: Arc<Entry>, tag: u64) -> KeptEntry {
        let (mut pid_hi, mut kv_hi, mut created_hi) = (0u64, 0u64, 0i64);
        for eff in &entry.effects {
            match eff {
                Effect::PartitionCreate {
                    pid, created_at_us, ..
                } => {
                    pid_hi = pid_hi.max(pid.saturating_add(1));
                    created_hi = created_hi.max(*created_at_us);
                }
                Effect::Append { created_at_us, .. } => created_hi = created_hi.max(*created_at_us),
                Effect::KvPut { version, .. } => kv_hi = kv_hi.max(version.saturating_add(1)),
                _ => {}
            }
        }
        KeptEntry {
            entry,
            tag,
            pid_hi,
            kv_hi,
            created_hi,
        }
    }
}

/// The overlay the planner thread keeps between cycles and the in-flight
/// entries folded into it, oldest first.
pub(crate) struct KeptOverlay {
    ov: Overlay,
    entries: VecDeque<KeptEntry>,
    next_tag: u64,
    /// False when the build met a request id in two in-flight entries: the
    /// overlay is right for THIS cycle (first writer wins, as always), but the
    /// landing of the first could not restore the second's, so it is not kept.
    exact: bool,
}

impl KeptOverlay {
    /// THE OLD PATH: a fresh overlay over the committed bases with every entry
    /// in flight above `store_applied` folded in index order (§7.2) — what the
    /// batcher did every cycle before KEEP_OVERLAY, what it still does with the
    /// knob off, and what every fallback does.
    pub(crate) fn rebuild(
        folded: &[(u64, Arc<Entry>)],
        store_applied: u64,
        base_pid: u64,
        base_kv: u64,
    ) -> KeptOverlay {
        let mut k = KeptOverlay {
            ov: Overlay::new(base_pid, base_kv),
            entries: VecDeque::new(),
            next_tag: 1,
            exact: true,
        };
        for (index, e) in folded {
            if *index <= store_applied {
                continue;
            }
            if e.commands
                .iter()
                .any(|c| k.ov.request_ids.contains_key(&c.request_id))
            {
                k.exact = false;
            }
            let tag = k.take_tag();
            k.ov.set_tag(tag);
            k.ov.ingest_entry(e);
            k.entries.push_back(KeptEntry::new(e.clone(), tag));
        }
        k
    }

    fn take_tag(&mut self) -> u64 {
        let t = self.next_tag;
        self.next_tag += 1;
        t
    }

    /// Whether this overlay may be kept for the next cycle.
    pub(crate) fn exact(&self) -> bool {
        self.exact
    }

    /// Bring a kept overlay to the one [`KeptOverlay::rebuild`] would build from
    /// the same arguments: take out every entry that has landed, then reset the
    /// running maxima from the committed bases and the entries still in flight.
    ///
    /// `folded` must be this overlay's own entries minus a landed prefix (by
    /// identity, in order): an entry leaves the batcher's in-flight list only
    /// once it has landed or when the whole pipeline is dropped, which bumps
    /// the epoch and never reaches here. Anything else is an `Err`, and the
    /// caller rebuilds.
    pub(crate) fn advance(
        mut self,
        folded: &[(u64, Arc<Entry>)],
        store_applied: u64,
        base_pid: u64,
        base_kv: u64,
    ) -> Result<KeptOverlay, &'static str> {
        let desired: Vec<&Arc<Entry>> = folded
            .iter()
            .filter(|(index, _)| *index > store_applied)
            .map(|(_, e)| e)
            .collect();
        let landed = self
            .entries
            .len()
            .checked_sub(desired.len())
            .ok_or("an entry in flight was never folded into the kept overlay")?;
        if !self
            .entries
            .iter()
            .skip(landed)
            .zip(desired.iter())
            .all(|(k, d)| Arc::ptr_eq(&k.entry, d))
        {
            return Err("the kept entries are not the entries in flight");
        }
        let mut refold: HashSet<TimerKey> = HashSet::new();
        for _ in 0..landed {
            let k = self.entries.pop_front().expect("counted above");
            self.ov.unfold_entry(&k.entry, k.tag, &mut refold)?;
        }
        if !refold.is_empty() {
            self.ov
                .refold_timers(&refold, self.entries.iter().map(|k| (k.tag, &*k.entry)));
        }
        if landed > 0 {
            self.ov.shrink_idle();
        }
        let ov = &mut self.ov;
        ov.next_pid = self
            .entries
            .iter()
            .map(|k| k.pid_hi)
            .fold(base_pid, u64::max);
        ov.next_kv_version = self.entries.iter().map(|k| k.kv_hi).fold(base_kv, u64::max);
        ov.cycle_pid_base = base_pid;
        ov.cycle_kv_base = base_kv;
        ov.max_now_us = self
            .entries
            .iter()
            .map(|k| k.entry.now_us)
            .fold(0, i64::max);
        ov.max_created_at_us = self.entries.iter().map(|k| k.created_hi).fold(0, i64::max);
        Ok(self)
    }

    pub(crate) fn overlay(&self) -> &Overlay {
        &self.ov
    }

    pub(crate) fn overlay_mut(&mut self) -> &mut Overlay {
        &mut self.ov
    }

    /// Start a cycle: every command it plans folds under the returned tag,
    /// which [`KeptOverlay::push_entry`] then gives the cycle's entry.
    pub(crate) fn begin_cycle(&mut self) -> u64 {
        let tag = self.take_tag();
        self.ov.set_tag(tag);
        tag
    }

    /// The cycle built `entry`, whose effects the planner has ALREADY folded
    /// under `tag`: keep it as in flight and record its request ids, exactly as
    /// [`Overlay::ingest_entry`] would when the next cycle rebuilt. An id
    /// already in the overlay (an entry in flight carrying it) is an `Err`:
    /// the old path's first-writer-wins could not be undone at landing.
    pub(crate) fn push_entry(&mut self, entry: Arc<Entry>, tag: u64) -> Result<(), &'static str> {
        for c in &entry.commands {
            match self.ov.request_ids.entry(c.request_id) {
                MapEntry::Occupied(_) => return Err("a request id is in two entries in flight"),
                MapEntry::Vacant(v) => {
                    v.insert(Tagged {
                        v: c.outcome.clone(),
                        tag,
                    });
                }
            }
        }
        self.entries.push_back(KeptEntry::new(entry, tag));
        Ok(())
    }

    /// The first difference between this overlay and `reference` — the old
    /// path's rebuild from the same arguments — or `None`. Tags are compared by
    /// the entry they name (its position in each one's in-flight list), never
    /// as numbers: the two numbered their folds independently.
    pub(crate) fn diff(&self, reference: &KeptOverlay) -> Option<String> {
        if self.entries.len() != reference.entries.len()
            || !self
                .entries
                .iter()
                .zip(reference.entries.iter())
                .all(|(a, b)| Arc::ptr_eq(&a.entry, &b.entry))
        {
            return Some(format!(
                "in-flight entries: kept {} vs rebuilt {}",
                self.entries.len(),
                reference.entries.len()
            ));
        }
        let pos_a: HashMap<u64, usize> = self
            .entries
            .iter()
            .enumerate()
            .map(|(i, k)| (k.tag, i))
            .collect();
        let pos_b: HashMap<u64, usize> = reference
            .entries
            .iter()
            .enumerate()
            .map(|(i, k)| (k.tag, i))
            .collect();
        let pa = |t: u64| pos_a.get(&t).copied();
        let pb = |t: u64| pos_b.get(&t).copied();
        let (a, b) = (&self.ov, &reference.ov);
        let scalars = [
            ("next_pid", a.next_pid as i128, b.next_pid as i128),
            (
                "next_kv_version",
                a.next_kv_version as i128,
                b.next_kv_version as i128,
            ),
            (
                "cycle_pid_base",
                a.cycle_pid_base as i128,
                b.cycle_pid_base as i128,
            ),
            (
                "cycle_kv_base",
                a.cycle_kv_base as i128,
                b.cycle_kv_base as i128,
            ),
            ("max_now_us", a.max_now_us as i128, b.max_now_us as i128),
            (
                "max_created_at_us",
                a.max_created_at_us as i128,
                b.max_created_at_us as i128,
            ),
        ];
        for (name, x, y) in scalars {
            if x != y {
                return Some(format!("{name}: kept {x} vs rebuilt {y}"));
            }
        }
        diff_tagged("queues", &a.queues, &b.queues, &pa, &pb)
            .or_else(|| diff_tagged("groups", &a.groups, &b.groups, &pa, &pb))
            .or_else(|| diff_tagged("pids_by_key", &a.pids_by_key, &b.pids_by_key, &pa, &pb))
            .or_else(|| diff_tagged("cursors", &a.cursors, &b.cursors, &pa, &pb))
            .or_else(|| diff_tagged("kv", &a.kv, &b.kv, &pa, &pb))
            .or_else(|| diff_tagged("gone", &a.gone, &b.gone, &pa, &pb))
            .or_else(|| {
                diff_tagged(
                    "dropped_queues",
                    &a.dropped_queues,
                    &b.dropped_queues,
                    &pa,
                    &pb,
                )
            })
            .or_else(|| {
                diff_tagged(
                    "purged_tenants",
                    &a.purged_tenants,
                    &b.purged_tenants,
                    &pa,
                    &pb,
                )
            })
            .or_else(|| diff_tagged("request_ids", &a.request_ids, &b.request_ids, &pa, &pb))
            .or_else(|| diff_parts(a, b, &pa, &pb))
            .or_else(|| diff_map("dedup", &a.dedup, &b.dedup, |x, y| x == y))
            .or_else(|| {
                diff_map("timers", &a.timers, &b.timers, |x, y| {
                    x.v == y.v && pa(x.last) == pb(y.last) && pa(x.base) == pb(y.base)
                })
            })
    }
}

fn diff_map<K: Eq + Hash + Debug, V: Debug>(
    name: &str,
    a: &HashMap<K, V, FxBuild>,
    b: &HashMap<K, V, FxBuild>,
    same: impl Fn(&V, &V) -> bool,
) -> Option<String> {
    for (k, va) in a {
        match b.get(k) {
            None => return Some(format!("{name}: {k:?} only in the kept overlay ({va:?})")),
            Some(vb) if !same(va, vb) => {
                return Some(format!("{name}: {k:?}: kept {va:?} vs rebuilt {vb:?}"))
            }
            Some(_) => {}
        }
    }
    b.iter()
        .find(|(k, _)| !a.contains_key(*k))
        .map(|(k, vb)| format!("{name}: {k:?} only in the rebuilt overlay ({vb:?})"))
}

fn diff_tagged<K: Eq + Hash + Debug, V: PartialEq + Debug>(
    name: &str,
    a: &HashMap<K, Tagged<V>, FxBuild>,
    b: &HashMap<K, Tagged<V>, FxBuild>,
    pa: &dyn Fn(u64) -> Option<usize>,
    pb: &dyn Fn(u64) -> Option<usize>,
) -> Option<String> {
    diff_map(name, a, b, |x, y| x.v == y.v && pa(x.tag) == pb(y.tag))
}

fn diff_parts(
    a: &Overlay,
    b: &Overlay,
    pa: &dyn Fn(u64) -> Option<usize>,
    pb: &dyn Fn(u64) -> Option<usize>,
) -> Option<String> {
    diff_map("parts", &a.parts, &b.parts, |x, y| {
        let created = match (&x.created, &y.created) {
            (None, None) => true,
            (Some(cx), Some(cy)) => cx.v == cy.v && pa(cx.tag) == pb(cy.tag),
            _ => false,
        };
        let watermark = match (&x.watermark, &y.watermark) {
            (None, None) => true,
            (Some(wx), Some(wy)) => wx.v == wy.v && pa(wx.tag) == pb(wy.tag),
            _ => false,
        };
        created
            && watermark
            && x.appends.len() == y.appends.len()
            && x.appends.iter().zip(y.appends.iter()).all(|(p, q)| {
                p.base == q.base
                    && p.end == q.end
                    && p.created_at_us == q.created_at_us
                    && p.hashes == q.hashes
                    && pa(p.tag) == pb(q.tag)
            })
    })
}

/// Drop `key` iff its value is still the one `tag` wrote. A value older than
/// `tag` means an earlier landing missed it: the kept overlay is not exact.
fn drop_if_tagged<K: Eq + Hash, V>(
    map: &mut HashMap<K, Tagged<V>, FxBuild>,
    key: &K,
    tag: u64,
) -> Result<(), &'static str> {
    match map.get(key) {
        Some(t) if t.tag == tag => {
            map.remove(key);
            Ok(())
        }
        Some(t) if t.tag < tag => Err("an overlay value outlived the entry that wrote it"),
        // Overwritten by a later entry, or already taken out by an earlier
        // effect of this same entry writing the same key.
        _ => Ok(()),
    }
}

impl Overlay {
    /// Give back the room a burst left behind. A rebuilt overlay was sized by
    /// what was in flight; a kept one never shrinks on its own, and a map far
    /// larger than its contents costs memory and every clone of the overlay (a
    /// transaction saves one to restore on refusal). O(len), and only once a
    /// map is eight times too large.
    fn shrink_idle(&mut self) {
        fn shrink<K: Eq + Hash, V>(m: &mut HashMap<K, V, FxBuild>) {
            if m.capacity() > 4096 && m.capacity() > 8 * m.len() {
                m.shrink_to(2 * m.len());
            }
        }
        shrink(&mut self.dedup);
        shrink(&mut self.parts);
        shrink(&mut self.cursors);
        shrink(&mut self.request_ids);
        shrink(&mut self.kv);
        shrink(&mut self.timers);
        shrink(&mut self.gone);
    }

    /// Take out everything entry `e` put in when it was folded under `tag`.
    /// Timer keys whose value was derived THROUGH `e` go to `refold`.
    fn unfold_entry(
        &mut self,
        e: &Entry,
        tag: u64,
        refold: &mut HashSet<TimerKey>,
    ) -> Result<(), &'static str> {
        for eff in &e.effects {
            match eff {
                Effect::QueueUpsert { tenant, queue, .. } => {
                    drop_if_tagged(&mut self.queues, &(tenant.clone(), queue.clone()), tag)?;
                }
                // What the fold swept (the names, the groups) needs nothing
                // back: committed state has swept them too once this lands.
                Effect::QueueDelete { tenant, queue } => {
                    let key = (tenant.clone(), queue.clone());
                    drop_if_tagged(&mut self.queues, &key, tag)?;
                    drop_if_tagged(&mut self.dropped_queues, &key, tag)?;
                }
                Effect::TenantPurge { tenant } => {
                    drop_if_tagged(&mut self.purged_tenants, tenant, tag)?;
                }
                Effect::GarbageAdd { pids, scope, .. }
                    if !matches!(scope, crate::rsm::effect::GarbageScope::Group { .. }) =>
                {
                    for pid in pids {
                        drop_if_tagged(&mut self.gone, pid, tag)?;
                    }
                }
                Effect::PartitionDelete { pid } => {
                    drop_if_tagged(&mut self.gone, pid, tag)?;
                }
                Effect::GroupUpsert {
                    tenant,
                    queue,
                    group,
                    ..
                }
                | Effect::GroupDelete {
                    tenant,
                    queue,
                    group,
                } => {
                    drop_if_tagged(
                        &mut self.groups,
                        &(tenant.clone(), queue.clone(), group.clone()),
                        tag,
                    )?;
                }
                Effect::PartitionCreate {
                    pid,
                    tenant,
                    queue,
                    partition,
                    ..
                } => {
                    drop_if_tagged(
                        &mut self.pids_by_key,
                        &(tenant.clone(), queue.clone(), partition.clone()),
                        tag,
                    )?;
                    if let Some(p) = self.parts.get_mut(pid) {
                        match &p.created {
                            Some(c) if c.tag == tag => p.created = None,
                            Some(c) if c.tag < tag => {
                                return Err("a created-in-flight flag outlived its create")
                            }
                            _ => {}
                        }
                        if p.is_empty() {
                            self.parts.remove(pid);
                        }
                    }
                }
                Effect::Append {
                    pid,
                    base_offset,
                    count,
                    created_at_us,
                    hashes,
                    ..
                } => {
                    let p = self
                        .parts
                        .get_mut(pid)
                        .ok_or("a landed append has no overlay partition")?;
                    let at = p
                        .appends
                        .iter()
                        .position(|a| a.tag == tag && a.base == *base_offset)
                        .ok_or("a landed append is not in the overlay")?;
                    p.appends.remove(at);
                    if p.is_empty() {
                        self.parts.remove(pid);
                    }
                    for i in 0..*count as usize {
                        let key = (*pid, frame_hash(hashes, i));
                        let occ = self
                            .dedup
                            .get_mut(&key)
                            .ok_or("a landed frame has no dedup occurrence")?;
                        let want = (base_offset + i as u64, *created_at_us);
                        let j = occ
                            .iter()
                            .position(|o| *o == want)
                            .ok_or("a landed frame's dedup occurrence is missing")?;
                        occ.remove(j);
                        if occ.is_empty() {
                            self.dedup.remove(&key);
                        }
                    }
                }
                Effect::CursorSet { pid, group, .. } | Effect::CursorDelete { pid, group } => {
                    drop_if_tagged(&mut self.cursors, &(*pid, group.clone()), tag)?;
                }
                Effect::Watermark { pid, .. } => {
                    if let Some(p) = self.parts.get_mut(pid) {
                        match &p.watermark {
                            Some(w) if w.tag == tag => p.watermark = None,
                            Some(w) if w.tag < tag => {
                                return Err("an overlay watermark outlived its entry")
                            }
                            _ => {}
                        }
                        if p.is_empty() {
                            self.parts.remove(pid);
                        }
                    }
                }
                Effect::KvPut {
                    tenant, ns, key, ..
                }
                | Effect::KvDelete { tenant, ns, key } => {
                    drop_if_tagged(
                        &mut self.kv,
                        &(tenant.clone(), ns.clone(), key.clone()),
                        tag,
                    )?;
                }
                Effect::TimerUpsert {
                    tenant, queue, key, ..
                }
                | Effect::TimerDelete { tenant, queue, key }
                | Effect::TimerBackoff {
                    tenant, queue, key, ..
                } => {
                    let k = (tenant.clone(), queue.clone(), key.clone());
                    match self.timers.get(&k) {
                        Some(s) if s.last == tag => {
                            self.timers.remove(&k);
                            refold.remove(&k);
                        }
                        Some(s) if s.last < tag => {
                            return Err("an overlay timer outlived the entry that wrote it")
                        }
                        // Written later, but derived through this entry (or an
                        // earlier one): re-fold from what is still in flight.
                        Some(s) if s.base <= tag => {
                            refold.insert(k);
                        }
                        _ => {}
                    }
                }
                // Not folded (see `fold_effect`): nothing to take out.
                _ => {}
            }
        }
        for c in &e.commands {
            match self.request_ids.get(&c.request_id) {
                Some(t) if t.tag == tag => {
                    self.request_ids.remove(&c.request_id);
                }
                Some(t) if t.tag < tag => {
                    return Err("an in-flight request id outlived its entry");
                }
                _ => {}
            }
        }
        Ok(())
    }

    /// Re-fold the timer `keys` from nothing through `remaining` (the entries
    /// still in flight, in fold order, with their tags) — what a rebuild folds
    /// for them.
    fn refold_timers<'e>(
        &mut self,
        keys: &HashSet<TimerKey>,
        remaining: impl Iterator<Item = (u64, &'e Entry)>,
    ) {
        for k in keys {
            self.timers.remove(k);
        }
        for (tag, e) in remaining {
            for eff in &e.effects {
                let (Effect::TimerUpsert {
                    tenant, queue, key, ..
                }
                | Effect::TimerDelete { tenant, queue, key }
                | Effect::TimerBackoff {
                    tenant, queue, key, ..
                }) = eff
                else {
                    continue;
                };
                if keys.contains(&(tenant.clone(), queue.clone(), key.clone())) {
                    self.fold_timer(eff, tag);
                }
            }
        }
    }
}
