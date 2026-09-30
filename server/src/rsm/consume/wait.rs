//! Long polls held on the leader, and the engine's clock work.
//!
//! A `wait` pop that finds nothing parks in its group ([`Engine::park`]). When a
//! part of the group becomes claimable (an append, a released or expired
//! lease, a hold running out) the group is queued for the engine's serve
//! thread, which re-runs the oldest waiter first; a waiter whose deadline
//! (minus the reply margin) comes is answered empty. The same thread runs the
//! timers (lease expiries, delay and window holds), the transaction
//! reservations' TTL and, when a new leader's pause ends, the commands held
//! through it. [`super::Engine::tick`] runs the same work from the facade's
//! ticker.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashSet, VecDeque};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use crate::rsm::planner::PopCommand;

use super::state::{lock, read, Gid, Group, Waiter, SHARDS};
use super::{wall_us, Engine};

/// How often a parked pop without a deadline is looked at again (its caller
/// may have left).
const NO_DEADLINE_RECHECK_US: i64 = 1_000_000;

/// The serve thread's queue and the condition it waits on.
#[derive(Default)]
pub(crate) struct Waking {
    pub q: std::sync::Mutex<WakeQ>,
    pub cv: std::sync::Condvar,
}

/// The serve thread's queue: groups to look at again.
#[derive(Default)]
pub(crate) struct WakeQ {
    pub gids: VecDeque<Gid>,
    pub queued: HashSet<Gid>,
    /// When parked waiters come due: `(the instant their pop expires,
    /// their group)`, one entry per park. A sweep visits only the groups
    /// whose entries came due (an entry whose waiter was served already
    /// finds nothing), never every group that has a waiter: at 10k queues
    /// the old whole-set sweep ran on every wake, held this queue's lock
    /// across a parked x parked scan, and stalled apply's wakes behind it
    /// (2026-09-30: the leader applied 52 entries/s, 1.3 s behind).
    pub due: BinaryHeap<Reverse<(i64, Gid)>>,
    pub stop: bool,
    /// Something besides a group needs the thread (the pause, a nudge).
    pub nudged: bool,
}

impl WakeQ {
    pub fn clear(&mut self) {
        self.gids.clear();
        self.queued.clear();
        self.due.clear();
    }
}

impl Engine {
    /// Park a long-poll pop in its group.
    pub(crate) fn park(
        &self,
        g: &Arc<Group>,
        cmd: PopCommand,
        pinned: Option<crate::rsm::effect::Pid>,
        sink: tokio::sync::oneshot::Sender<crate::rsm::batcher::Reply>,
        now: i64,
    ) {
        let seq = g.wake_seq.load(Ordering::Acquire);
        let due = self.due_at(&cmd, now);
        {
            let mut st = lock(&g.st);
            if st.dropped {
                drop(st);
                let _ = sink.send(Engine::empty_pop());
                return;
            }
            st.waiters.push_back(Waiter { cmd, pinned, sink });
            g.waiting.fetch_add(1, Ordering::AcqRel);
        }
        lock(&self.waking.q).due.push(Reverse((due, g.id)));
        // Something became claimable between the empty walk and the park.
        if g.wake_seq.load(Ordering::Acquire) != seq {
            self.wake_group(g.id);
        }
    }

    /// Queue a group for the serve thread (when it has waiters).
    pub(crate) fn wake_group(&self, gid: Gid) {
        let Some(g) = read(&self.reg).by_id.get(&gid).cloned() else {
            return;
        };
        if g.waiting.load(Ordering::Acquire) > 0 {
            let mut w = lock(&self.waking.q);
            if w.queued.insert(gid) {
                w.gids.push_back(gid);
            }
            drop(w);
            self.waking.cv.notify_one();
        }
        self.wake_facade(&g.tenant, &g.queue, &g.name);
    }

    /// Wake the serve thread for work that is not a group's.
    pub(crate) fn nudge(&self) {
        lock(&self.waking.q).nudged = true;
        self.waking.cv.notify_one();
    }

    /// Start the serve thread (once per engine).
    pub(crate) fn start_thread(&self) {
        let mut t = lock(self.thread_slot());
        if t.is_some() {
            return;
        }
        let Some(me) = self.arc() else {
            return;
        };
        let weak = Arc::downgrade(&me);
        drop(me);
        let waking = self.waking.clone();
        *t = std::thread::Builder::new()
            .name("consume-serve".into())
            .spawn(move || serve_loop(weak, waking))
            .ok();
    }

    /// Serve a woken group's waiters, oldest first, until one finds nothing.
    pub(crate) fn serve_group(&self, g: &Arc<Group>) {
        loop {
            let seq = g.wake_seq.load(Ordering::Acquire);
            let w = {
                let mut st = lock(&g.st);
                match st.waiters.pop_front() {
                    Some(w) => {
                        g.waiting.fetch_sub(1, Ordering::AcqRel);
                        w
                    }
                    None => return,
                }
            };
            let now = wall_us();
            match self.retry_waiter(g, w, now) {
                None => continue, // answered: the next one
                Some(w) => {
                    let mut st = lock(&g.st);
                    if st.dropped {
                        drop(st);
                        let _ = w.sink.send(Engine::empty_pop());
                        return;
                    }
                    st.waiters.push_front(w);
                    g.waiting.fetch_add(1, Ordering::AcqRel);
                    drop(st);
                    if g.wake_seq.load(Ordering::Acquire) == seq {
                        return;
                    }
                }
            }
        }
    }

    /// When a parked pop's group must be swept for it: the instant it
    /// expires ([`super::pop::expired`]), or, for a pop without a deadline,
    /// a recheck a second on (its caller may leave).
    fn due_at(&self, cmd: &PopCommand, now: i64) -> i64 {
        if cmd.deadline_us > 0 {
            cmd.deadline_us.saturating_sub(self.k.margin_us)
        } else {
            now.saturating_add(NO_DEADLINE_RECHECK_US)
        }
    }

    /// Answer the waiters whose deadline came (or whose caller left), in the
    /// groups whose due entries came.
    fn sweep_deadlines(&self, now: i64) {
        let mut gids: Vec<Gid> = {
            let mut w = lock(&self.waking.q);
            let mut out = Vec::new();
            while let Some(Reverse((due, gid))) = w.due.peek().copied() {
                if due > now {
                    break;
                }
                w.due.pop();
                out.push(gid);
            }
            out
        };
        if gids.is_empty() {
            return;
        }
        gids.sort_unstable();
        gids.dedup();
        let groups: Vec<Arc<Group>> = {
            let reg = read(&self.reg);
            gids.iter()
                .filter_map(|gid| reg.by_id.get(gid).cloned())
                .collect()
        };
        let mut again: Vec<Reverse<(i64, Gid)>> = Vec::new();
        for g in &groups {
            let expired: Vec<Waiter> = {
                let mut st = lock(&g.st);
                let mut keep = VecDeque::with_capacity(st.waiters.len());
                let mut out = Vec::new();
                for w in st.waiters.drain(..) {
                    if w.sink.is_closed() || super::pop::expired(&w.cmd, now, self.k.margin_us) {
                        out.push(w);
                    } else {
                        if w.cmd.deadline_us <= 0 {
                            again.push(Reverse((self.due_at(&w.cmd, now), g.id)));
                        }
                        keep.push_back(w);
                    }
                }
                st.waiters = keep;
                g.waiting.store(st.waiters.len(), Ordering::Release);
                out
            };
            for w in expired {
                let _ = w.sink.send(Engine::empty_pop());
            }
        }
        if !again.is_empty() {
            lock(&self.waking.q).due.extend(again);
        }
    }

    /// The timers due: expired leases and ended holds arm their parts.
    fn run_timers(&self, now: i64) {
        let grace = self.grace();
        let mut woken: Vec<Gid> = Vec::new();
        for si in 0..SHARDS {
            let mut sh = lock(&self.shards[si]);
            let sh = &mut *sh;
            while let Some(std::cmp::Reverse((due, gid, pid))) = sh.timers.peek().copied() {
                if due > now {
                    break;
                }
                sh.timers.pop();
                if let Some(p) = sh
                    .groups
                    .get_mut(&gid)
                    .and_then(|gs| gs.parts.get_mut(&pid))
                {
                    if p.timer_at == due {
                        p.timer_at = 0;
                    }
                }
                // A lease that ran out leaves the worker's index (its fields
                // stay: a redelivery counts the attempt from them).
                let expired_worker = sh
                    .groups
                    .get(&gid)
                    .and_then(|gs| gs.parts.get(&pid))
                    .and_then(|p| {
                        (p.cur.lease.is_some() && !p.cur.leased(now, grace))
                            .then(|| p.cur.worker().map(str::to_string))
                            .flatten()
                    });
                if let Some(w) = expired_worker {
                    sh.unindex_lease(&w, pid, gid);
                }
                if super::pop::arm(sh, gid, pid, now, grace) {
                    woken.push(gid);
                }
            }
        }
        woken.sort_unstable();
        woken.dedup();
        for gid in woken {
            self.wake_group(gid);
        }
    }

    /// The clock work (timers, deadlines, reservation TTLs, the pause).
    pub(crate) fn tick_inner(&self, now: i64) {
        self.run_timers(now);
        self.sweep_deadlines(now);
        self.expire_txns(now);
        if now >= self.serve_after_us.load(Ordering::Acquire) {
            let held = std::mem::take(&mut *lock(&self.paused));
            for h in held {
                self.run_held(h, now);
            }
        }
    }

    fn thread_slot(&self) -> &std::sync::Mutex<Option<std::thread::JoinHandle<()>>> {
        &self.thread
    }
}

/// The serve thread: woken groups, and the clock work every checkpoint
/// interval. It holds the engine only while it works: waiting, it keeps a
/// `Weak` (a facade that drops its engine drops its store with it).
fn serve_loop(weak: std::sync::Weak<Engine>, waking: Arc<Waking>) {
    let mut period = Duration::from_millis(5);
    // The clock work runs once a period, not on every wake: wakes come
    // thousands of times a second.
    let mut clock_at = i64::MIN;
    loop {
        let gids: Vec<Gid> = {
            let mut w = lock(&waking.q);
            if w.stop {
                return;
            }
            if w.gids.is_empty() && !w.nudged {
                let (g, _) = waking
                    .cv
                    .wait_timeout(w, period)
                    .unwrap_or_else(|p| p.into_inner());
                w = g;
                if w.stop {
                    return;
                }
            }
            w.nudged = false;
            let out: Vec<Gid> = w.gids.drain(..).collect();
            for gid in &out {
                w.queued.remove(gid);
            }
            out
        };
        let Some(e) = weak.upgrade() else {
            return;
        };
        period = Duration::from_millis(e.k.ckpt_ms.clamp(1, 50));
        if !e.leader.load(Ordering::Acquire) {
            continue;
        }
        // Appends apply queued while their shard was busy: armed first, so
        // the groups they wake are served in this pass.
        e.drain_appends();
        let now = wall_us();
        if now.saturating_sub(clock_at) >= period.as_micros() as i64 {
            clock_at = now;
            e.tick_inner(now);
        }
        for gid in gids {
            let g = read(&e.reg).by_id.get(&gid).cloned();
            if let Some(g) = g {
                e.serve_group(&g);
            }
        }
    }
}
