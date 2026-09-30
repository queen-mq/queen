//! Push admission: a byte budget for the storage-growing commands on their way
//! through the serial planner, shared FAIRLY between the clients this node
//! serves itself and the commands each follower forwards to it.
//!
//! Without it, an offered rate above the planner's ceiling piled request bodies
//! up in RAM until the kernel killed the broker (measured 2026-09-22/23 at
//! 240k–300k msg/s: 10 GB and rising, then an OOM kill).
//!
//! **Where.** An HTTP push or transaction takes permits at the edge, sized by
//! its `Content-Length`, BEFORE its body is read (`admit_edge` in
//! `handlers::raft`). A push that waits for room holds only its connection: its
//! bytes stay in the socket and TCP slows the sender down — what Kafka does by
//! not reading a muted channel. The facade (`push_impl`, `submit`) gates the
//! callers that do not come through that edge (in-process ones, transactions,
//! KV puts, timer schedules) and skips the requests the edge already admitted
//! ([`pre_admitted`]). Permits are held until the reply arrives. Drain work
//! (acks, pops) never takes permits.
//!
//! **Who.** On the leader the budget is the planner's, and three kinds of
//! caller compete for it: this node's own clients (the edge and the facade,
//! [`Source::Local`]) and every follower's forwarded commands
//! ([`Source::Node`], one source per follower; [`Source::Forwarded`] when the
//! forwarding path does not say which). One FIFO queue for all of them let the
//! leader's own clients crowd the followers' out: under saturation the
//! leader-attached clients got 98% of what they offered and the
//! follower-attached ones 46–52%, with 8–9 s push p50 and sheds (2026-09-30).
//! A follower's push also waited in that queue once per partition it touches
//! while a local push waited once. So each source has its own FIFO queue, and
//! room goes round them by deficit round robin in bytes: a backlogged source
//! gets its share whatever the others offer, and an idle source's share goes
//! to the rest. On a follower only its own clients take permits: the budget
//! is then just its memory guard, and the leader decides the rest.
//!
//! **Hold, then 429.** When the budget is spent a new command WAITS for room,
//! so a normal producer just slows down. Only after the hold is it refused with
//! `429` + `Retry-After`, which protects the broker from senders that never
//! slow down. Both are jittered: the hold by ±25% and `Retry-After` over 1–5 s.
//! Measured 2026-09-23 at 300k offered with a fixed 5 s hold and a fixed
//! `Retry-After: 5`: requests that queued together were refused together and
//! came back together (the SDKs retry a 429 by themselves), and every wave
//! reconnected thousands of sockets at once and stalled pops and acks for
//! 2–4 s. A forwarded command waits on the leader with its bytes already in
//! the leader's memory, and with its follower's request open behind it: it is
//! held for `QUEEN_RAFT_ADMIT_FWD_HOLD_MS` at most (and never past half of
//! what its caller has left), then refused with the explicit overload the
//! follower answers as `429` + `Retry-After` — not a timeout.
//!
//! `QUEEN_RAFT_ADMIT_MAX_MB` (default: `QUEEN_RAFT_PIPELINE` + 2 planner
//! drains of `QUEEN_RAFT_BATCH_MAX_BYTES`, and at least 128 MiB — 40 MiB left
//! the budget full while the planner had room, because a command holds its
//! bytes until its answer, a forwarded one across its hop too; 0 = off),
//! `QUEEN_RAFT_ADMIT_HOLD_MS` (default 15000, under the SDKs' 30 s request
//! timeout) and `QUEEN_RAFT_ADMIT_FWD_HOLD_MS` (default 2000).

use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use rand::Rng;
use tokio::sync::oneshot;

const DEFAULT_HOLD_MS: u64 = 15_000;
const DEFAULT_FWD_HOLD_MS: u64 = 2_000;

/// The bytes a source may take in one turn of the round robin before the next
/// source's turn: a few typical pushes, well under one planner drain.
const QUANTUM: u64 = 256 * 1024;

/// The size charged to a request that has no `Content-Length` (chunked).
pub const UNKNOWN_LEN_BYTES: usize = 64 * 1024;

/// Commands admitted after waiting for room / refused after the hold.
static WAITED: AtomicU64 = AtomicU64::new(0);
static REFUSED: AtomicU64 = AtomicU64::new(0);
/// The same for forwarded commands only (a leader's view of its followers).
static FWD_ADMITTED: AtomicU64 = AtomicU64::new(0);
static FWD_REFUSED: AtomicU64 = AtomicU64::new(0);
/// Budget, bytes currently held, and commands waiting for room, for the gauges.
static CAP_BYTES: AtomicU64 = AtomicU64::new(0);
static HELD_BYTES: AtomicU64 = AtomicU64::new(0);
static WAITING: AtomicU64 = AtomicU64::new(0);

/// The budget was spent for the whole hold: refuse, retry after this long.
#[derive(Debug, Clone, Copy)]
pub struct Overloaded {
    pub retry_after_s: u64,
}

/// Who a command comes from: the gate is fair between sources.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Source {
    /// This node's own clients: its HTTP edge, and the facade's callers.
    Local,
    /// The commands a follower forwards, by its node id.
    Node(u64),
    /// Forwarded commands whose follower is not named (the per-command
    /// `/raft/v1/submit` path).
    Forwarded,
}

pub struct AdmitGate {
    inner: Arc<Inner>,
    cap: u64,
    hold: Duration,
    fwd_hold: Duration,
}

struct Inner {
    state: Mutex<State>,
}

/// One command waiting for room.
struct Waiter {
    bytes: u64,
    tx: oneshot::Sender<Admitted>,
}

#[derive(Default)]
struct SourceQueue {
    waiters: VecDeque<Waiter>,
    /// Deficit round robin: the bytes this source may still take this turn.
    deficit: u64,
}

struct State {
    /// Room left in the budget.
    avail: u64,
    queues: HashMap<Source, SourceQueue>,
    /// The sources with waiters, in turn order; the front one has the turn.
    round: VecDeque<Source>,
    /// The front source has not yet been given its quantum for this turn.
    fresh_turn: bool,
    /// Waiters queued, including ones that gave up and are not pruned yet.
    queued: usize,
}

/// A command's admission: its bytes return to the budget when this drops.
pub struct Admitted {
    inner: Arc<Inner>,
    bytes: u64,
}

impl Drop for Admitted {
    fn drop(&mut self) {
        let bytes = std::mem::take(&mut self.bytes);
        if bytes > 0 {
            HELD_BYTES.fetch_sub(bytes, Ordering::Relaxed);
            self.inner.release(bytes);
        }
    }
}

/// Decrements the waiting gauge however the wait ends (admitted, refused, or
/// the request dropped).
struct Waiting;

impl Drop for Waiting {
    fn drop(&mut self) {
        WAITING.fetch_sub(1, Ordering::Relaxed);
    }
}

/// The process's gate, shared by the HTTP edge, every facade and the leader's
/// intake of forwarded commands, from the environment on first use; `None`
/// when `QUEEN_RAFT_ADMIT_MAX_MB=0`.
pub fn global() -> Option<&'static AdmitGate> {
    static GATE: OnceLock<Option<AdmitGate>> = OnceLock::new();
    GATE.get_or_init(AdmitGate::from_env).as_ref()
}

tokio::task_local! {
    static PRE_ADMITTED: ();
    static FORWARDED_FROM: Source;
}

/// Run `f` as a request the HTTP edge already admitted: the facade's gate lets
/// it through without taking permits a second time.
pub async fn pre_admitted_scope<F: Future>(f: F) -> F::Output {
    PRE_ADMITTED.scope((), f).await
}

/// Inside [`pre_admitted_scope`].
pub fn pre_admitted() -> bool {
    PRE_ADMITTED.try_with(|_| ()).is_ok()
}

/// Run `f` as the handling of a command `from` forwarded: the leader's intake
/// names the follower for [`forwarded_from`].
pub async fn forwarded_scope<F: Future>(from: Source, f: F) -> F::Output {
    FORWARDED_FROM.scope(from, f).await
}

/// The source [`forwarded_scope`] named; [`Source::Forwarded`] outside one.
pub fn forwarded_from() -> Source {
    FORWARDED_FROM.try_with(|s| *s).unwrap_or(Source::Forwarded)
}

/// The least default budget. A command holds its bytes until its answer, and
/// a follower's forwarded one across its hop too: at 40 MiB (the old default,
/// the pipeline plus two drains) the budget was full with ~15k commands
/// waiting in every 1M msg/s shape while the planner's own queue waited ~35 ms,
/// and 128 MiB took 861k -> 974k msg/s at 1,000 queues x 1M partitions
/// (2026-09-30).
const DEFAULT_MIN_CAP_BYTES: u64 = 128 << 20;

/// `QUEEN_RAFT_ADMIT_MAX_MB` unset: room for every pipeline slot and two
/// drains more, in bytes, and never less than [`DEFAULT_MIN_CAP_BYTES`].
fn default_cap_bytes() -> u64 {
    let pipeline = env_u64("QUEEN_RAFT_PIPELINE")
        .filter(|v| *v > 0)
        .unwrap_or(8);
    let drain = env_u64("QUEEN_RAFT_BATCH_MAX_BYTES")
        .filter(|v| *v > 0)
        .unwrap_or(crate::rsm::entry::BATCH_MAX_BYTES_DEFAULT as u64);
    (pipeline + 2)
        .saturating_mul(drain)
        .max(DEFAULT_MIN_CAP_BYTES)
}

impl AdmitGate {
    /// The gate from the environment; `None` when `QUEEN_RAFT_ADMIT_MAX_MB=0`.
    pub fn from_env() -> Option<AdmitGate> {
        let cap = match env_u64("QUEEN_RAFT_ADMIT_MAX_MB") {
            Some(0) => return None,
            Some(mb) => mb.saturating_mul(1 << 20),
            None => default_cap_bytes(),
        };
        let hold = env_u64("QUEEN_RAFT_ADMIT_HOLD_MS").unwrap_or(DEFAULT_HOLD_MS);
        let fwd_hold = env_u64("QUEEN_RAFT_ADMIT_FWD_HOLD_MS").unwrap_or(DEFAULT_FWD_HOLD_MS);
        Some(
            AdmitGate::new(cap, Duration::from_millis(hold))
                .with_forward_hold(Duration::from_millis(fwd_hold)),
        )
    }

    pub fn new(cap_bytes: u64, hold: Duration) -> AdmitGate {
        let cap = cap_bytes.max(1);
        CAP_BYTES.store(cap, Ordering::Relaxed);
        AdmitGate {
            inner: Arc::new(Inner {
                state: Mutex::new(State {
                    avail: cap,
                    queues: HashMap::new(),
                    round: VecDeque::new(),
                    fresh_turn: true,
                    queued: 0,
                }),
            }),
            cap,
            hold,
            fwd_hold: Duration::from_millis(DEFAULT_FWD_HOLD_MS),
        }
    }

    /// The most a forwarded command waits for room ([`AdmitGate::admit_forwarded`]).
    pub fn with_forward_hold(mut self, hold: Duration) -> AdmitGate {
        self.fwd_hold = hold;
        self
    }

    /// Take `bytes` of budget for this node's own client, waiting up to the
    /// (jittered) hold for room. A command larger than the whole budget takes
    /// all of it (it runs alone).
    pub async fn admit(&self, bytes: usize) -> Result<Admitted, Overloaded> {
        self.admit_from(Source::Local, bytes as u64, self.hold)
            .await
    }

    /// Take `bytes` of budget for a command a follower forwarded (`from`),
    /// which its caller needs answered within `left`: it waits for room at
    /// most the forward hold, and never past half of `left` — the refusal
    /// must reach the follower while its client still listens.
    pub async fn admit_forwarded(
        &self,
        from: Source,
        bytes: usize,
        left: Duration,
    ) -> Result<Admitted, Overloaded> {
        let r = self
            .admit_from(from, bytes as u64, self.fwd_hold.min(left / 2))
            .await;
        match &r {
            Ok(_) => FWD_ADMITTED.fetch_add(1, Ordering::Relaxed),
            Err(_) => FWD_REFUSED.fetch_add(1, Ordering::Relaxed),
        };
        r
    }

    async fn admit_from(
        &self,
        source: Source,
        bytes: u64,
        hold: Duration,
    ) -> Result<Admitted, Overloaded> {
        let n = bytes.clamp(1, self.cap);
        let (rx, grants) = {
            let mut st = self.inner.state.lock().unwrap_or_else(|p| p.into_inner());
            // Nobody waits: take it now if it fits.
            if st.queued == 0 && st.avail >= n {
                st.avail -= n;
                drop(st);
                HELD_BYTES.fetch_add(n, Ordering::Relaxed);
                return Ok(Admitted {
                    inner: self.inner.clone(),
                    bytes: n,
                });
            }
            let (tx, rx) = oneshot::channel();
            let q = st.queues.entry(source).or_default();
            let joins_round = q.waiters.is_empty();
            q.waiters.push_back(Waiter { bytes: n, tx });
            if joins_round && !st.round.contains(&source) {
                st.round.push_back(source);
            }
            st.queued += 1;
            (rx, st.dispatch())
        };
        self.inner.deliver(grants);
        WAITED.fetch_add(1, Ordering::Relaxed);
        WAITING.fetch_add(1, Ordering::Relaxed);
        let _waiting = Waiting;
        let hold = hold.mul_f64(rand::thread_rng().gen_range(0.75..1.25));
        let mut rx = rx;
        match tokio::time::timeout(hold, &mut rx).await {
            Ok(Ok(admitted)) => Ok(admitted),
            _ => {
                // Refuse, unless room came at the very last moment. Closing
                // first: a grant sent after this comes back to the gate.
                rx.close();
                if let Ok(admitted) = rx.try_recv() {
                    return Ok(admitted);
                }
                REFUSED.fetch_add(1, Ordering::Relaxed);
                self.inner.prune();
                Err(Overloaded {
                    retry_after_s: rand::thread_rng().gen_range(1..=5),
                })
            }
        }
    }

    /// Room left and commands waiting (a test's view).
    #[cfg(test)]
    fn snapshot(&self) -> (u64, usize) {
        let st = self.inner.state.lock().unwrap();
        (st.avail, st.queued)
    }
}

impl Inner {
    fn release(self: &Arc<Self>, bytes: u64) {
        let grants = {
            let mut st = self.state.lock().unwrap_or_else(|p| p.into_inner());
            st.avail += bytes;
            st.dispatch()
        };
        self.deliver(grants);
    }

    /// Hand each grant its admission, outside the lock. A waiter that gave up
    /// meanwhile returns its bytes, which go round again (a loop, not a
    /// recursion through `Drop`).
    fn deliver(self: &Arc<Self>, grants: Vec<(oneshot::Sender<Admitted>, u64)>) {
        let mut grants = grants;
        while !grants.is_empty() {
            let mut returned = 0u64;
            for (tx, bytes) in grants.drain(..) {
                HELD_BYTES.fetch_add(bytes, Ordering::Relaxed);
                let a = Admitted {
                    inner: self.clone(),
                    bytes,
                };
                if let Err(mut a) = tx.send(a) {
                    HELD_BYTES.fetch_sub(bytes, Ordering::Relaxed);
                    a.bytes = 0;
                    returned += bytes;
                }
            }
            if returned == 0 {
                break;
            }
            let mut st = self.state.lock().unwrap_or_else(|p| p.into_inner());
            st.avail += returned;
            grants = st.dispatch();
        }
    }

    /// A waiter gave up: let the waiters behind it through if it was in their
    /// way.
    fn prune(self: &Arc<Self>) {
        let grants = self
            .state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .dispatch();
        self.deliver(grants);
    }
}

impl State {
    /// Deficit round robin over the sources with waiters: the source whose
    /// turn it is gets a quantum of credit, and takes room for its waiters in
    /// order while its credit and the room last; when its credit does not
    /// cover its next waiter the turn passes, and when the room does not, the
    /// turn waits for bytes to come back (a large command is never passed
    /// over by smaller ones forever). A source with nobody waiting leaves the
    /// round, and its credit with it. Returns the grants, room already taken.
    fn dispatch(&mut self) -> Vec<(oneshot::Sender<Admitted>, u64)> {
        let mut grants = Vec::new();
        while let Some(&src) = self.round.front() {
            let Some(q) = self.queues.get_mut(&src) else {
                self.round.pop_front();
                self.fresh_turn = true;
                continue;
            };
            while q.waiters.front().is_some_and(|w| w.tx.is_closed()) {
                q.waiters.pop_front();
                self.queued -= 1;
            }
            let Some(need) = q.waiters.front().map(|w| w.bytes) else {
                self.queues.remove(&src);
                self.round.pop_front();
                self.fresh_turn = true;
                continue;
            };
            if self.fresh_turn {
                q.deficit = q.deficit.saturating_add(QUANTUM);
                self.fresh_turn = false;
            }
            if q.deficit < need {
                // Its turn is spent; it keeps its credit for the next one.
                self.round.rotate_left(1);
                self.fresh_turn = true;
                continue;
            }
            if self.avail < need {
                break;
            }
            let w = q.waiters.pop_front().expect("a waiter at the front");
            self.queued -= 1;
            q.deficit -= need;
            self.avail -= need;
            grants.push((w.tx, need));
        }
        grants
    }
}

fn env_u64(key: &str) -> Option<u64> {
    std::env::var(key).ok().and_then(|v| v.trim().parse().ok())
}

/// Prometheus lines for the admission budget.
pub fn render(out: &mut String) {
    use std::fmt::Write;
    let _ = writeln!(
        out,
        "# HELP queen_raft_admit_bytes Admission budget and bytes held by storage-growing commands in the pipeline\n# TYPE queen_raft_admit_bytes gauge"
    );
    let _ = writeln!(
        out,
        "queen_raft_admit_bytes{{kind=\"cap\"}} {}",
        CAP_BYTES.load(Ordering::Relaxed)
    );
    let _ = writeln!(
        out,
        "queen_raft_admit_bytes{{kind=\"held\"}} {}",
        HELD_BYTES.load(Ordering::Relaxed)
    );
    let _ = writeln!(
        out,
        "# HELP queen_raft_admit_waiting Commands waiting for admission room now\n# TYPE queen_raft_admit_waiting gauge"
    );
    let _ = writeln!(
        out,
        "queen_raft_admit_waiting {}",
        WAITING.load(Ordering::Relaxed)
    );
    let _ = writeln!(
        out,
        "# HELP queen_raft_admit_total Commands that waited for admission room, and that were refused with 429\n# TYPE queen_raft_admit_total counter"
    );
    let _ = writeln!(
        out,
        "queen_raft_admit_total{{outcome=\"waited\"}} {}",
        WAITED.load(Ordering::Relaxed)
    );
    let _ = writeln!(
        out,
        "queen_raft_admit_total{{outcome=\"refused\"}} {}",
        REFUSED.load(Ordering::Relaxed)
    );
    let _ = writeln!(
        out,
        "# HELP queen_raft_admit_forwarded_total Commands followers forwarded to this node that it admitted, and that it refused as overloaded\n# TYPE queen_raft_admit_forwarded_total counter"
    );
    let _ = writeln!(
        out,
        "queen_raft_admit_forwarded_total{{outcome=\"admitted\"}} {}",
        FWD_ADMITTED.load(Ordering::Relaxed)
    );
    let _ = writeln!(
        out,
        "queen_raft_admit_forwarded_total{{outcome=\"refused\"}} {}",
        FWD_REFUSED.load(Ordering::Relaxed)
    );
    crate::rsm::replicator::raft::forward::render(out);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn under_budget_is_immediate_over_budget_waits_then_proceeds() {
        let g = AdmitGate::new(1000, Duration::from_millis(500));
        let a = g.admit(600).await.expect("room");
        // 600 + 600 > 1000: waits until `a` is released.
        let g2 = std::sync::Arc::new(g);
        let g3 = g2.clone();
        let t = tokio::spawn(async move { g3.admit(600).await.is_ok() });
        tokio::time::sleep(Duration::from_millis(50)).await;
        drop(a);
        assert!(t.await.unwrap(), "admitted once room was released");
    }

    #[tokio::test]
    async fn refused_after_the_jittered_hold_with_a_jittered_retry_after() {
        let g = AdmitGate::new(1000, Duration::from_millis(100));
        let _a = g.admit(1000).await.expect("room");
        let mut after = std::collections::HashSet::new();
        for _ in 0..40 {
            let t0 = std::time::Instant::now();
            let r = g.admit(10).await;
            let o = r.err().expect("no room within the hold must refuse");
            assert!(t0.elapsed() >= Duration::from_millis(75), "it held first");
            assert!((1..=5).contains(&o.retry_after_s));
            after.insert(o.retry_after_s);
        }
        assert!(after.len() > 1, "Retry-After is spread, not one value");
    }

    #[tokio::test]
    async fn a_command_larger_than_the_budget_runs_alone() {
        let g = AdmitGate::new(1000, Duration::from_millis(100));
        let big = g.admit(5000).await.expect("clamped to the whole budget");
        assert!(g.admit(1).await.is_err(), "nothing else fits while it runs");
        drop(big);
        assert!(g.admit(1).await.is_ok());
    }

    #[tokio::test]
    async fn pre_admitted_only_inside_the_scope() {
        assert!(!pre_admitted());
        assert!(pre_admitted_scope(async { pre_admitted() }).await);
        assert!(!pre_admitted());
    }

    #[tokio::test]
    async fn the_forwarding_source_is_named_only_inside_its_scope() {
        assert_eq!(forwarded_from(), Source::Forwarded);
        let got = forwarded_scope(Source::Node(3), async { forwarded_from() }).await;
        assert_eq!(got, Source::Node(3));
    }

    /// Queue `n` waiters of `bytes` from `src`, each recording its grant
    /// order in `log` and then holding its admission until `release` fires.
    fn queue(
        g: &Arc<AdmitGate>,
        src: Source,
        n: usize,
        bytes: usize,
        log: &Arc<Mutex<Vec<Source>>>,
        release: &Arc<tokio::sync::Notify>,
    ) -> Vec<tokio::task::JoinHandle<bool>> {
        (0..n)
            .map(|_| {
                let (g, log, release) = (g.clone(), log.clone(), release.clone());
                tokio::spawn(async move {
                    let r = match src {
                        Source::Local => g.admit(bytes).await,
                        s => g.admit_forwarded(s, bytes, Duration::from_secs(60)).await,
                    };
                    match r {
                        Ok(a) => {
                            log.lock().unwrap().push(src);
                            release.notified().await;
                            drop(a);
                            true
                        }
                        Err(_) => false,
                    }
                })
            })
            .collect()
    }

    /// The leader's own clients flood the queue first, a follower's commands
    /// arrive after: once room comes back, the follower's are not served
    /// after every local one (the one FIFO queue did that), but in turn.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_follower_is_served_in_turn_not_behind_every_local_waiter() {
        let quantum = QUANTUM as usize;
        let g = Arc::new(
            AdmitGate::new(4 * QUANTUM, Duration::from_secs(30))
                .with_forward_hold(Duration::from_secs(30)),
        );
        let full = g.admit(4 * quantum).await.expect("room");
        let log = Arc::new(Mutex::new(Vec::new()));
        let release = Arc::new(tokio::sync::Notify::new());
        let mut ts = queue(&g, Source::Local, 40, quantum, &log, &release);
        tokio::time::sleep(Duration::from_millis(50)).await;
        ts.extend(queue(&g, Source::Node(2), 10, quantum, &log, &release));
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(g.snapshot().1, 50, "every one waits");
        drop(full);
        // Each release lets the next waiter in: one at a time, 20 of them.
        for _ in 0..20 {
            tokio::time::sleep(Duration::from_millis(5)).await;
            release.notify_one();
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
        let order = log.lock().unwrap().clone();
        let first20 = &order[..order.len().min(20)];
        let node = first20.iter().filter(|s| **s == Source::Node(2)).count();
        assert!(
            (8..=10).contains(&node),
            "the follower got its turns among the first 20 grants: {first20:?}"
        );
        release.notify_waiters();
        for _ in 0..200 {
            release.notify_waiters();
            tokio::time::sleep(Duration::from_millis(2)).await;
            if ts.iter().all(|t| t.is_finished()) {
                break;
            }
        }
        for t in ts {
            t.abort();
        }
    }

    /// Two followers and the local edge, all backlogged with commands of
    /// different sizes: each gets about a third of the bytes admitted.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn backlogged_sources_share_the_room_by_bytes() {
        let g = Arc::new(
            AdmitGate::new(QUANTUM, Duration::from_secs(30))
                .with_forward_hold(Duration::from_secs(30)),
        );
        let full = g.admit(QUANTUM as usize).await.expect("room");
        let bytes: Arc<Mutex<HashMap<Source, u64>>> = Arc::default();
        let mut ts = Vec::new();
        for (src, size) in [
            (Source::Local, 8 * 1024usize),
            (Source::Node(2), 32 * 1024),
            (Source::Node(3), 64 * 1024),
        ] {
            for _ in 0..200 {
                let (g, bytes) = (g.clone(), bytes.clone());
                ts.push(tokio::spawn(async move {
                    let r = match src {
                        Source::Local => g.admit(size).await,
                        s => g.admit_forwarded(s, size, Duration::from_secs(60)).await,
                    };
                    if let Ok(a) = r {
                        *bytes.lock().unwrap().entry(src).or_default() += size as u64;
                        // Hold briefly, like a command in the pipeline.
                        tokio::time::sleep(Duration::from_millis(1)).await;
                        drop(a);
                    }
                }));
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        drop(full);
        // Stop measuring while all three are still backlogged.
        let t0 = std::time::Instant::now();
        loop {
            tokio::time::sleep(Duration::from_millis(5)).await;
            let b = bytes.lock().unwrap().clone();
            let min = [Source::Local, Source::Node(2), Source::Node(3)]
                .iter()
                .map(|s| b.get(s).copied().unwrap_or(0))
                .min()
                .unwrap();
            if min >= 1 << 20 || t0.elapsed() > Duration::from_secs(10) {
                break;
            }
        }
        let b = bytes.lock().unwrap().clone();
        let got: Vec<u64> = [Source::Local, Source::Node(2), Source::Node(3)]
            .iter()
            .map(|s| b.get(s).copied().unwrap_or(0))
            .collect();
        let (lo, hi) = (*got.iter().min().unwrap(), *got.iter().max().unwrap());
        assert!(
            lo > 0 && hi <= 2 * lo + 2 * QUANTUM,
            "bytes per source: {got:?}"
        );
        for t in ts {
            t.abort();
        }
    }

    /// A forwarded command is held at most the forward hold, and at most half
    /// of what its caller has left, before the overload answer.
    #[tokio::test]
    async fn a_forwarded_command_is_refused_within_its_short_hold() {
        let g = AdmitGate::new(1000, Duration::from_secs(30))
            .with_forward_hold(Duration::from_millis(200));
        let _full = g.admit(1000).await.expect("room");
        let t0 = std::time::Instant::now();
        let r = g
            .admit_forwarded(Source::Node(2), 10, Duration::from_secs(60))
            .await;
        assert!(r.is_err());
        assert!(
            t0.elapsed() < Duration::from_millis(400),
            "{:?}",
            t0.elapsed()
        );
        let t0 = std::time::Instant::now();
        let r = g
            .admit_forwarded(Source::Node(2), 10, Duration::from_millis(100))
            .await;
        assert!(r.is_err());
        assert!(
            t0.elapsed() < Duration::from_millis(90),
            "half of what the caller has left: {:?}",
            t0.elapsed()
        );
    }

    /// Waiters that gave up leave nothing behind: the room is whole again
    /// and a newcomer passes at once.
    #[tokio::test]
    async fn waiters_that_gave_up_leave_no_room_taken() {
        let g = Arc::new(AdmitGate::new(1000, Duration::from_millis(50)));
        let full = g.admit(1000).await.expect("room");
        let mut ts = Vec::new();
        for i in 0..20 {
            let g = g.clone();
            ts.push(tokio::spawn(async move {
                g.admit_forwarded(Source::Node(i % 3), 100, Duration::from_millis(80))
                    .await
                    .is_ok()
            }));
        }
        for t in ts {
            assert!(!t.await.unwrap(), "no room within the hold");
        }
        drop(full);
        let (avail, _) = g.snapshot();
        assert_eq!(avail, 1000, "every byte came back");
        let t0 = std::time::Instant::now();
        let a = g.admit(1000).await.expect("room");
        assert!(t0.elapsed() < Duration::from_millis(20));
        drop(a);
        assert_eq!(g.snapshot(), (1000, 0));
    }
}
