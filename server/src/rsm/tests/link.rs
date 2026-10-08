//! The cluster link (`rsm/link`): a standby that replays a source's entries.
//!
//! What this file proves:
//!
//! - **mirrored entries, same state**
//!   ([`a_standby_fed_the_mirrored_entries_holds_the_sources_state`]): a
//!   second cluster that is proposed the source's entries through
//!   [`mirror_entry`] holds every replicated keyspace byte-equal to the
//!   source's, except the log positions (see [`comparable`]) and the link's
//!   own flags, and its position names the last source entry.
//! - **the whole path, through the drivers**
//!   ([`a_standby_replays_its_source_restarts_and_is_promoted`]): what a
//!   source's batcher logged — pushes with a duplicate, a cursor checkpoint,
//!   KV writes, a timer and its fire, the leader's own steps — is read back
//!   from the source's queue logs ([`LinkSource::read`]), rebuilt
//!   ([`full_entry`]) and replayed by a standby's batcher, which refuses its
//!   own clients meanwhile; the standby resumes from its position after a
//!   restart; and a promotion makes it an ordinary cluster.
//! - **a promotion's answer**
//!   ([`a_promotion_is_answered_once_the_cluster_takes_its_clients_commands`]):
//!   the driver answers a promotion only when it plans as an ordinary leader
//!   and its engine no longer refuses clients — read, with a client's push
//!   sent, at the instant the answer is given.
//! - **the driver's refusals** ([`a_standby_refuses_what_does_not_follow_it`]):
//!   an entry out of sequence, an entry that does not continue the standby's
//!   state, and a link command on a cluster that is not a standby.
//! - **across a leader change**
//!   ([`a_node_elected_again_answers_what_its_cluster_is_now`],
//!   [`a_command_that_waited_out_a_standbys_election_is_never_planned`]): a
//!   node answers what its cluster is, not what it led last — once it has
//!   applied a promotion it never says `standby`, and the retry of a write
//!   the promoted cluster took is answered as taken — and a command that
//!   reached a node while a standby elected it is refused once the node
//!   leads, and planned neither then nor at the promotion.
//! - **the follower** ([`the_follower_follows_its_source_and_says_why_when_it_cannot`]):
//!   the task of [`crate::rsm::link::driver`] against a source it reads
//!   in-process — it follows, leaves its hold on the source's log, and reports
//!   a source that does not answer, one that is behind the standby, one that
//!   purged what the standby needs and one whose log is another log, each for
//!   what it is; and it goes on when the source does.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use serde_json::json;
use tokio::sync::watch;

use crate::rsm::apply::{self, state_digest, StateDigest, SystemClock};
use crate::rsm::batcher::{
    Batcher, BatcherConfig, Command, CommandTx, LinkBoot, LinkOp, LinkSubmission, LinkTx, Reply,
    Submission, DIVERGED_CODE, NOT_STANDBY_CODE, OUT_OF_SEQUENCE_CODE,
};
use crate::rsm::consume::Engine;
use crate::rsm::effect::Effect;
use crate::rsm::entry::{decode_entry, encode_entry, Entry, Outcome, PushVerdict};
use crate::rsm::facade::real::RaftFacade;
use crate::rsm::link::mirror::{mirror_entry, standby_entry};
use crate::rsm::link::wire::{full_entry, Answer};
use crate::rsm::link::{self, Cursor, Position, Role};
use crate::rsm::planner::kv::{parse_ops, KvCommand};
use crate::rsm::planner::timers::{parse_timer_ops, TimersCommand};
use crate::rsm::planner::{EffectsCommand, PushCommand};
use crate::rsm::replicator::local::{NoWaker, OpenConfig};
use crate::rsm::replicator::log::{Fsync, LogOptions};
use crate::rsm::replicator::raft::RaftReplicator;
use crate::rsm::replicator::{
    AppliedAt, Membership, MembershipChange, NodeId, ProposeError, ReplError, ReplMetrics,
    Replicator, Role as NodeRole,
};
use crate::rsm::store::{HeedStore, Keyspace, Reads, Store, TypedReads};

use super::apply::{cfg, seg_opts, store_opts, Workload, QUEUE, TENANT};
use super::planner_harness::{cursor_row, item, qcfg, rid};

static SEQ: AtomicU64 = AtomicU64::new(0);

const SEED: u64 = 0x0_11A0_7000_0001;

fn scratch(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "queen-rsm-link-{tag}-{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("scratch dir");
    dir
}

fn repl_config(dir: &Path) -> OpenConfig {
    OpenConfig {
        node_id: 1,
        log_dir: dir.join("log"),
        log_opts: LogOptions {
            segment_bytes: 64 << 10,
            fsync: Fsync::Off,
        },
        seg_root: dir.join("seg"),
        seg_opts: seg_opts(),
        apply_cfg: apply::ApplyConfig {
            qlog: true,
            ..cfg()
        },
        apply_channel_capacity: 64,
        replay_deadline: Duration::from_secs(30),
        writer_pipeline: false,
    }
}

fn open_raft(dir: &Path) -> RaftReplicator<HeedStore> {
    let store = Arc::new(HeedStore::open(&dir.join("store"), &store_opts()).expect("store"));
    RaftReplicator::open(
        store,
        repl_config(dir),
        Arc::new(NoWaker),
        Arc::new(SystemClock),
    )
    .expect("open the openraft replicator")
}

fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(30)
}

fn request_id(n: u64) -> [u8; 16] {
    let mut id = [0xEE; 16];
    id[..8].copy_from_slice(&n.to_le_bytes());
    id
}

/// What a source and its standby must agree on, keyspace by keyspace: every
/// replicated one but `meta` (the applied index and term, and each cluster's
/// own membership notes), `groups` (`reg_index`, the index of the entry that
/// registered the group), `flags` (the standby holds the link's rows) and the
/// two request-id keyspaces (the standby's own entries are commands too, and
/// record their ids). [`group_rows`], [`flags_without_the_link`] and
/// [`request_rows`] check the rest of those.
pub(super) fn comparable(d: &StateDigest) -> Vec<(&'static str, u128, u64)> {
    d.per_keyspace
        .iter()
        .filter(|(name, _, _)| {
            !matches!(
                *name,
                "meta" | "groups" | "flags" | "request_ids" | "request_expiry"
            )
        })
        .cloned()
        .collect()
}

/// The rows of the two request-id keyspaces, without those of `own` (the
/// request ids of the entries a standby built itself). Both keys end with the
/// id.
pub(super) fn request_rows(store: &HeedStore, own: &[[u8; 16]]) -> Vec<(Vec<u8>, Vec<u8>)> {
    store
        .read(|r| {
            let mut out = Vec::new();
            for ks in [Keyspace::RequestIds, Keyspace::RequestExpiry] {
                r.scan_raw(ks, &[], &[], usize::MAX, &mut |k, v| {
                    if !own.iter().any(|id| k.ends_with(id)) {
                        out.push((k.to_vec(), v.to_vec()));
                    }
                    true
                })?;
            }
            Ok(out)
        })
        .expect("read request ids")
}

/// Every row of the counters keyspace.
pub(super) fn counter_rows(store: &HeedStore) -> Vec<(Vec<u8>, Vec<u8>)> {
    store
        .read(|r| {
            let mut out = Vec::new();
            r.scan_raw(Keyspace::Counters, &[], &[], usize::MAX, &mut |k, v| {
                out.push((k.to_vec(), v.to_vec()));
                true
            })?;
            Ok(out)
        })
        .expect("read the counters")
}

/// The workload's group rows with the log position set aside.
fn group_rows(store: &HeedStore) -> Vec<String> {
    use crate::rsm::store::TypedReads;
    store
        .read(|r| {
            let mut out = Vec::new();
            for g in ["g1", "g2"] {
                let row = r.group(TENANT, QUEUE, g)?;
                out.push(format!("{g}: {:?}", row.map(|r| (r.meta, r.reg_effect))));
            }
            Ok(out)
        })
        .expect("read group rows")
}

/// Every flag row that is not the link's.
pub(super) fn flags_without_the_link(store: &HeedStore) -> Vec<(Vec<u8>, Vec<u8>)> {
    // A flag's key starts with its name.
    let link_prefix = link::FLAG_PREFIX.as_bytes();
    store
        .read(|r| {
            let mut out = Vec::new();
            r.scan_raw(Keyspace::Flags, &[], &[], usize::MAX, &mut |k, v| {
                if !k.starts_with(link_prefix) {
                    out.push((k.to_vec(), v.to_vec()));
                }
                true
            })?;
            Ok(out)
        })
        .expect("read flags")
}

struct Closed {
    digest: StateDigest,
    groups: Vec<String>,
    flags: Vec<(Vec<u8>, Vec<u8>)>,
    requests: Vec<(Vec<u8>, Vec<u8>)>,
    role: Role,
    position: Position,
}

/// Shut a node down and read what the comparisons need. `own`: the request
/// ids of the entries the node built itself (a standby's), see
/// [`request_rows`].
fn close(repl: RaftReplicator<HeedStore>, own: &[[u8; 16]]) -> Closed {
    let (_stats, store) = repl.shutdown().expect("shutdown");
    let groups = group_rows(&store);
    let flags = flags_without_the_link(&store);
    let requests = request_rows(&store, own);
    let (role, position) = store
        .read(|r| Ok((link::read_role(r)?, link::read_position(r)?)))
        .expect("read the link rows");
    let store = Arc::try_unwrap(store).unwrap_or_else(|_| panic!("the store is still shared"));
    let digest = store
        .read(|r| Ok(state_digest(r).expect("digest")))
        .expect("read");
    store.close();
    Closed {
        digest,
        groups,
        flags,
        requests,
        role,
        position,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_standby_fed_the_mirrored_entries_holds_the_sources_state() {
    const N: u64 = 400;
    let mut w = Workload::new(SEED);
    let entries: Vec<Bytes> = (0..N)
        .map(|_| Bytes::from(encode_entry(&w.next().entry).expect("encode")))
        .collect();

    // The source: an ordinary cluster. Where each entry landed is what a
    // standby is told.
    let dir_a = scratch("mirror-source");
    let source = open_raft(&dir_a);
    let mut landed: Vec<(AppliedAt, Bytes)> = Vec::with_capacity(entries.len());
    for e in entries {
        let at = source.propose(e.clone(), deadline()).await.expect("propose");
        landed.push((at, e));
    }
    let last = landed.last().expect("entries").0;

    // The standby: its first entry makes it one, then every source entry goes
    // through `mirror_entry`, each admitted by the cursor first.
    let dir_b = scratch("mirror-standby");
    let standby = open_raft(&dir_b);
    let mut cursor = standby
        .store_for_test()
        .read(|r| Cursor::read(r))
        .expect("read the cursor");
    assert_eq!(cursor.position, Position::START);
    let first = standby_entry(&cursor, request_id(1), "test://source", Position::START, 0)
        .expect("the standby entry");
    standby
        .propose(Bytes::from(encode_entry(&first).expect("encode")), deadline())
        .await
        .expect("become a standby");

    let mut prev = 0;
    for (at, bytes) in &landed {
        let src = decode_entry(bytes).expect("decode");
        cursor
            .admit(prev, at.index, &src)
            .unwrap_or_else(|why| panic!("source entry {} was not admitted: {why}", at.index));
        let mirrored = mirror_entry(&src, at.index, at.term).expect("mirror");
        standby
            .propose(Bytes::from(encode_entry(&mirrored).expect("encode")), deadline())
            .await
            .expect("propose the mirrored entry");
        cursor.advance(at.index, at.term, &src);
        prev = at.index;
    }
    // The cursor the driver kept is the one committed state holds.
    let committed = standby
        .store_for_test()
        .read(|r| Cursor::read(r))
        .expect("read the cursor");
    assert_eq!(cursor, committed, "the kept cursor drifted from the state");

    let a = close(source, &[]);
    let b = close(standby, &[request_id(1)]);

    assert_eq!(
        comparable(&b.digest),
        comparable(&a.digest),
        "the standby built different replicated state from the source's entries"
    );
    assert_eq!(b.groups, a.groups, "the group rows differ beyond their log position");
    assert_eq!(b.flags, a.flags, "the flags differ beyond the link's own");
    assert_eq!(
        b.requests.len(),
        a.requests.len(),
        "the standby records the source's request ids and its own entry's, no other"
    );
    assert!(b.requests == a.requests, "the request-id rows differ");
    assert_eq!(a.role, Role::Primary, "the source knows nothing of the link");
    assert!(b.role.is_standby(), "the standby's role row says so");
    assert_eq!(
        (b.position.index, b.position.term),
        (last.index, last.term),
        "the standby's position is the last source entry"
    );

    let _ = std::fs::remove_dir_all(&dir_a);
    let _ = std::fs::remove_dir_all(&dir_b);
}

// ---------------------------------------------------------------------------
// Through the drivers
// ---------------------------------------------------------------------------

/// The batcher's knobs: the leader's own steps run often, so a source logs
/// them and a standby is seen not to.
fn driver_cfg() -> BatcherConfig {
    BatcherConfig {
        pipeline: 4,
        request_expire_every_ms: 40,
        timer_tick_ms: 20,
        cluster_version_every_ms: 50,
        ..BatcherConfig::default()
    }
}

/// A cluster of one node: its store and replicator, and its driver while one
/// runs.
struct Cluster {
    dir: PathBuf,
    store: Arc<HeedStore>,
    repl: Arc<RaftReplicator<HeedStore>>,
    driver: Option<Driver>,
}

struct Driver {
    tx: CommandTx,
    link: LinkTx,
    handle: tokio::task::JoinHandle<()>,
}

impl Cluster {
    fn open(dir: PathBuf) -> Cluster {
        let repl = Arc::new(open_raft(&dir));
        Cluster {
            store: repl.store_for_test(),
            repl,
            driver: None,
            dir,
        }
    }

    /// Start a driver; `boot` is what makes the cluster a standby.
    fn start(&mut self, boot: Option<LinkBoot>) {
        self.start_with(driver_cfg(), boot);
    }

    fn start_with(&mut self, cfg: BatcherConfig, boot: Option<LinkBoot>) {
        self.start_driver(cfg, boot, None);
    }

    /// Start a driver that has a consumption engine, as a facade's does: the
    /// engine is what a leader's intake asks whether its cluster is a standby.
    fn start_with_engine(&mut self, boot: Option<LinkBoot>) -> Arc<Engine> {
        let engine = Engine::new(self.store.clone());
        self.start_driver(driver_cfg(), boot, Some(engine.clone()));
        engine
    }

    fn start_driver(
        &mut self,
        cfg: BatcherConfig,
        boot: Option<LinkBoot>,
        engine: Option<Arc<Engine>>,
    ) {
        let (link, link_rx) = tokio::sync::mpsc::channel(64);
        let mut batcher =
            Batcher::new(self.store.clone(), self.repl.clone(), cfg).with_link(link_rx, boot);
        if let Some(engine) = engine {
            batcher = batcher.with_engine(engine);
        }
        let (tx, handle) = batcher.spawn();
        self.driver = Some(Driver { tx, link, handle });
    }

    /// Stop the driver: nothing more is logged until the next one.
    async fn stop(&mut self) {
        if let Some(d) = self.driver.take() {
            drop(d.tx);
            drop(d.link);
            d.handle.await.expect("driver join");
        }
    }

    fn driver(&self) -> &Driver {
        self.driver.as_ref().expect("a running driver")
    }

    async fn command(&self, command: Command) -> Reply {
        let (sub, rx) = Submission::new(command);
        self.driver().tx.send(sub).await.expect("send command");
        rx.await.expect("await reply")
    }

    async fn link(&self, op: LinkOp) -> Reply {
        let (sub, rx) = LinkSubmission::new(op);
        self.driver().link.send(sub).await.expect("send link op");
        rx.await.expect("await link reply")
    }

    /// Stop the node and open it again from its directory.
    async fn restart(self) -> Cluster {
        let dir = self.dir.clone();
        self.shut().await;
        Cluster::open(dir)
    }

    /// Stop the node for good (its directory stays).
    async fn shut(mut self) {
        self.stop().await;
        let Cluster { store, repl, .. } = self;
        drop(store);
        let end = Instant::now() + Duration::from_secs(10);
        let mut repl = repl;
        let repl = loop {
            match Arc::try_unwrap(repl) {
                Ok(r) => break r,
                Err(still) => {
                    assert!(Instant::now() < end, "the replicator is still shared");
                    repl = still;
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            }
        };
        let (_stats, store) = tokio::task::block_in_place(|| repl.shutdown()).expect("shutdown");
        Arc::try_unwrap(store)
            .unwrap_or_else(|_| panic!("the store is still shared"))
            .close();
    }
}

fn push(id: u64, queue: &str, partition: &str, txns: &[&str]) -> Command {
    Command::Push(PushCommand {
        request_id: rid(id),
        tenant: TENANT.to_string(),
        queue: queue.to_string(),
        partition: partition.to_string(),
        items: txns.iter().map(|t| item(t)).collect(),
        create_cfg: qcfg(),
    })
}

/// A consumption-engine checkpoint: `group` has acked each `(pid, committed)`.
fn checkpoint(id: u64, group: &str, cursors: &[(u64, i64)]) -> Command {
    Command::Effects(EffectsCommand {
        request_id: rid(id),
        tenant: TENANT.to_string(),
        effects: cursors
            .iter()
            .map(|(pid, committed)| Effect::CursorSet {
                pid: *pid,
                group: group.to_string(),
                row: cursor_row(*committed),
            })
            .collect(),
    })
}

fn kv_put(id: u64, key: &str, value: i64) -> Command {
    let op = json!({"op":"put","ns":"n","key":key,"value":{"v":value},"forever":true});
    Command::Kv(KvCommand {
        request_id: rid(id),
        tenant: TENANT.to_string(),
        ops: parse_ops(
            &[op],
            TENANT,
            false,
            511,
            crate::rsm::planner::kv::MAX_VALUE_BYTES_DEFAULT,
        )
        .expect("kv op"),
    })
}

/// A timer due at once: the leader's fire step pushes its message.
fn timer_now(id: u64, key: &str) -> Command {
    let op = json!({
        "op":"schedule","queue":"qt","timerKey":key,
        "delayMs": 0, "txn": format!("timer-{key}"), "payload": "e30=",
    });
    Command::Timers(TimersCommand {
        request_id: rid(id),
        tenant: TENANT.to_string(),
        ops: parse_timer_ops(&[op], Some("svc")).expect("timer op"),
    })
}

/// The `(pid, offset)` of a push's first item, which must have been created.
fn push_created(reply: &Reply) -> (u64, u64) {
    match reply {
        Reply::Done {
            outcome: Outcome::Push(p),
            ..
        } => match &p.items[0] {
            PushVerdict::Created { pid, offset, .. } => (*pid, *offset),
            other => panic!("expected Created, got {other:?}"),
        },
        other => panic!("expected a push outcome, got {other:?}"),
    }
}

fn done(reply: Reply, what: &str) {
    assert!(
        matches!(reply, Reply::Done { .. }),
        "{what}: expected Done, got {reply:?}"
    );
}

fn refused(reply: Reply, code: &str, retryable: bool, what: &str) {
    match reply {
        Reply::Refused(r) => {
            assert_eq!(r.code, code, "{what}: {}", r.message);
            assert_eq!(r.retryable, retryable, "{what}: {}", r.message);
        }
        other => panic!("{what}: expected a refusal `{code}`, got {other:?}"),
    }
}

/// The application entries `src` has applied after `after`, as a standby
/// receives them: read from the queue logs and rebuilt.
fn source_entries(src: &Cluster, after: Position) -> Vec<(u64, u64, Arc<Entry>)> {
    match src
        .repl
        .link_source()
        .read(after.index, after.term, 1 << 20)
        .expect("read the source")
    {
        Answer::Entries { entries, .. } => entries
            .iter()
            .map(|e| {
                (
                    e.index,
                    e.term,
                    Arc::new(full_entry(&e.stored).expect("rebuild the entry")),
                )
            })
            .collect(),
        other => panic!("the source did not serve its entries: {other:?}"),
    }
}

/// Replay on `dst` everything `src` has applied past `dst`'s position, a
/// batch in the pipeline at a time. Returns how many entries went over.
async fn replicate(src: &Cluster, dst: &Cluster) -> usize {
    let mut n = 0;
    loop {
        let at = dst
            .store
            .read(|r| link::read_position(r))
            .expect("read the position");
        let entries = source_entries(src, at);
        if entries.is_empty() {
            return n;
        }
        let mut prev = at.index;
        let mut replies = Vec::with_capacity(entries.len());
        for (index, term, entry) in entries {
            let (sub, rx) = LinkSubmission::new(LinkOp::Mirror {
                prev,
                index,
                term,
                entry,
            });
            dst.driver().link.send(sub).await.expect("send link op");
            replies.push((index, rx));
            prev = index;
        }
        for (index, rx) in replies {
            done(
                rx.await.expect("await link reply"),
                &format!("source entry {index}"),
            );
            n += 1;
        }
        // An entry is answered once it is committed; what the next round and
        // the caller read is the store, a moment later.
        let end = Instant::now() + Duration::from_secs(10);
        while dst
            .store
            .read(|r| link::read_position(r))
            .expect("read the position")
            .index
            < prev
        {
            assert!(Instant::now() < end, "source entry {prev} never applied");
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    }
}

/// The standby `b` holds what its source `a` holds. Both are at rest.
fn assert_same(a: &Cluster, b: &Cluster, when: &str) {
    let digest = |c: &Cluster| {
        c.store
            .read(|r| Ok(state_digest(r).expect("digest")))
            .expect("read")
    };
    // An applied entry's counters reach the store at apply's next commit, a
    // few milliseconds after its rows: wait for that, not for ever.
    let end = Instant::now() + Duration::from_secs(5);
    while comparable(&digest(b)) != comparable(&digest(a)) && Instant::now() < end {
        std::thread::sleep(Duration::from_millis(5));
    }
    if comparable(&digest(b)) != comparable(&digest(a)) {
        // Say which counters, before the digests: the keyspace every effect
        // touches, and so the first to show an entry applied another way.
        let (ca, cb) = (counter_rows(&a.store), counter_rows(&b.store));
        let only = |x: &[(Vec<u8>, Vec<u8>)], y: &[(Vec<u8>, Vec<u8>)]| -> Vec<String> {
            x.iter()
                .filter(|row| !y.contains(row))
                .map(|(k, v)| format!("{k:?}={v:?}"))
                .collect()
        };
        eprintln!(
            "{when}: counters only on the source: {:#?}\ncounters only on the standby: {:#?}",
            only(&ca, &cb),
            only(&cb, &ca)
        );
    }
    assert_eq!(
        comparable(&digest(b)),
        comparable(&digest(a)),
        "{when}: the standby's replicated state differs from its source's"
    );
    assert_eq!(
        flags_without_the_link(&b.store),
        flags_without_the_link(&a.store),
        "{when}: the flags differ beyond the link's own"
    );
    // The source's request ids, row for row, and at most the two rows of the
    // standby's own entry (until an expiry step of the source retires them).
    let (ra, rb) = (request_rows(&a.store, &[]), request_rows(&b.store, &[]));
    assert!(
        ra.iter().all(|row| rb.contains(row)),
        "{when}: a request-id row of the source is not on the standby"
    );
    assert!(
        rb.len() <= ra.len() + 2,
        "{when}: the standby holds request ids of its own ({} against {})",
        rb.len(),
        ra.len()
    );
    // What apply gates the source's next entry on.
    let meta = |c: &Cluster| {
        c.store
            .read(|r| {
                Ok((
                    r.last_now_us()?,
                    r.next_pid()?,
                    r.kv_version_next()?,
                    r.cluster_version()?,
                ))
            })
            .expect("read meta")
    };
    assert_eq!(
        meta(b),
        meta(a),
        "{when}: the stamp, the bases or the cluster version differ"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_standby_replays_its_source_restarts_and_is_promoted() {
    let boot = || Some(LinkBoot::empty("test://source"));

    // The source: an ordinary cluster, its batcher planning client commands
    // and its own steps.
    let mut a = Cluster::open(scratch("drivers-source"));
    a.start(None);
    let (pid0, _) = push_created(&a.command(push(1, "q", "p0", &["a", "b", "c"])).await);
    push_created(&a.command(push(2, "q", "p1", &["d"])).await);
    // "a" again: a duplicate, answered from the dedup window.
    done(a.command(push(3, "q", "p0", &["a", "e"])).await, "a push");
    done(
        a.command(checkpoint(4, "g", &[(pid0, 1)])).await,
        "a checkpoint",
    );
    done(a.command(kv_put(5, "k1", 1)).await, "a KV put");
    done(a.command(kv_put(6, "k2", 2)).await, "a KV put");
    done(a.command(timer_now(7, "t1")).await, "a timer");
    // The leader's own steps log too: the timer's fire, a request-id expiry.
    tokio::time::sleep(Duration::from_millis(150)).await;
    a.stop().await;

    // The standby: an empty cluster told to be one.
    let mut b = Cluster::open(scratch("drivers-standby"));
    b.start(boot());
    refused(
        b.command(push(100, "q", "p9", &["z"])).await,
        link::STANDBY_CODE,
        true,
        "a client write on a standby",
    );
    let n1 = replicate(&a, &b).await;
    assert!(n1 >= 7, "the source's commands and steps went over ({n1})");
    assert_same(&a, &b, "after the first replay");
    assert!(
        b.store
            .read(|r| link::read_role(r))
            .expect("role")
            .is_standby(),
        "the standby entry wrote the role row"
    );

    // Left alone, a standby logs nothing: none of its leader's steps runs.
    let idle = b.repl.applied_index();
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        b.repl.applied_index(),
        idle,
        "an idle standby wrote entries of its own"
    );

    // More on the source while the standby restarts.
    a.start(None);
    push_created(&a.command(push(8, "q", "p2", &["f", "g"])).await);
    done(a.command(kv_put(9, "k1", 3)).await, "a KV put");
    tokio::time::sleep(Duration::from_millis(100)).await;
    a.stop().await;
    let mut b = b.restart().await;
    b.start(boot());
    refused(
        b.command(push(101, "q", "p9", &["z"])).await,
        link::STANDBY_CODE,
        true,
        "a client write on a restarted standby",
    );
    let n2 = replicate(&a, &b).await;
    assert!(n2 >= 2, "the standby resumed from its position ({n2})");
    assert_same(&a, &b, "after the restart");
    let last = b
        .store
        .read(|r| link::read_position(r))
        .expect("position");
    assert!(last.index > 0 && last.now_us > 0);

    // Promotion: an ordinary cluster from that entry on.
    done(b.link(LinkOp::Promote).await, "the promotion");
    let next_offset = a
        .store
        .read(|r| r.partition(pid0))
        .expect("read the partition")
        .expect("the source's partition")
        .last_offset
        + 1;
    let (pid, offset) = push_created(&b.command(push(200, "q", "p0", &["after"])).await);
    assert_eq!(
        (pid, offset as i64),
        (pid0, next_offset),
        "the promoted cluster continues the source's partition"
    );
    match b.store.read(|r| link::read_role(r)).expect("role") {
        Role::Promoted(doc) => assert_eq!(
            doc.position.map(|p| p.index),
            Some(last.index),
            "the role row names the last source entry applied"
        ),
        other => panic!("expected a promoted role, got {other:?}"),
    }
    let entry = source_entries(&a, Position::START).remove(0);
    refused(
        b.link(LinkOp::Mirror {
            prev: last.index,
            index: last.index + 1,
            term: entry.1,
            entry: entry.2,
        })
        .await,
        NOT_STANDBY_CODE,
        false,
        "a source entry on a promoted cluster",
    );
    done(b.link(LinkOp::Promote).await, "a second promotion");

    let (da, db) = (a.dir.clone(), b.dir.clone());
    a.shut().await;
    b.shut().await;
    let _ = std::fs::remove_dir_all(da);
    let _ = std::fs::remove_dir_all(db);
}

/// The waker of a promotion's answer. A oneshot's send wakes its receiver
/// before it returns, so this runs on the driver's own task at the instant the
/// answer is given and before the driver does anything else: what it reads
/// and sends is read and sent then, with no timing involved.
struct AtTheAnswer {
    /// Taken at the answer: the engine a leader's intake asks, and a client's
    /// push with the driver's channel to put it in.
    armed: std::sync::Mutex<Option<(Arc<Engine>, CommandTx, Submission)>>,
    /// What held then: whether the engine still called its cluster a standby,
    /// and the role this node's own state said.
    seen: std::sync::Mutex<Option<(bool, Option<Role>)>>,
    /// Whether the driver's channel took the push.
    sent: AtomicBool,
    answered: tokio::sync::Notify,
}

impl std::task::Wake for AtTheAnswer {
    fn wake(self: Arc<Self>) {
        if let Some((engine, tx, push)) = self.armed.lock().expect("armed").take() {
            let role = engine.store.read(|r| link::read_role(r)).ok();
            *self.seen.lock().expect("seen") = Some((engine.is_standby(), role));
            self.sent.store(tx.try_send(push).is_ok(), Ordering::SeqCst);
        }
        self.answered.notify_one();
    }
}

/// A promotion's answer says the cluster takes writes: whoever asked sends
/// one next. The driver therefore answers once it plans as an ordinary leader
/// and its engine serves — a step after the promotion's entry applies here.
/// Answered at that apply, the cluster that had just said it was promoted
/// refused the next command as a standby (a pop on the promoted cluster of
/// `link_cluster`, under load, 2026-10-08).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_promotion_is_answered_once_the_cluster_takes_its_clients_commands() {
    use std::future::Future;
    use std::pin::Pin;
    use std::task::{Context, Poll, Waker};

    // A standby whose driver has an engine, as a facade's does: the intake
    // refuses every client command while the engine says standby.
    let mut b = Cluster::open(scratch("answer-standby"));
    let engine = b.start_with_engine(Some(LinkBoot::empty("test://source")));
    refused(
        b.command(push(1, "q", "p0", &["before"])).await,
        link::STANDBY_CODE,
        true,
        "a client write on a standby",
    );
    assert!(
        engine.is_standby(),
        "the driver told the engine its cluster is a standby"
    );

    // The promotion, with the probe as its answer's waker from before it is
    // asked: the answer cannot be given without the probe running.
    let (write, written) = Submission::new(push(2, "q", "p0", &["after"]));
    let probe = Arc::new(AtTheAnswer {
        armed: std::sync::Mutex::new(Some((engine.clone(), b.driver().tx.clone(), write))),
        seen: std::sync::Mutex::new(None),
        sent: AtomicBool::new(false),
        answered: tokio::sync::Notify::new(),
    });
    let waker = Waker::from(probe.clone());
    let (sub, answer) = LinkSubmission::new(LinkOp::Promote);
    // Outside the runtime's poll budget, which wakes a waker too: nothing but
    // the answer wakes the probe.
    let mut answer = tokio::task::unconstrained(answer);
    assert!(
        Pin::new(&mut answer)
            .poll(&mut Context::from_waker(&waker))
            .is_pending(),
        "nothing was asked yet"
    );
    b.driver().link.send(sub).await.expect("send link op");
    tokio::time::timeout(Duration::from_secs(30), probe.answered.notified())
        .await
        .expect("the promotion was never answered");
    let Poll::Ready(reply) = Pin::new(&mut answer).poll(&mut Context::from_waker(&waker)) else {
        panic!("woken without an answer");
    };
    done(reply.expect("await link reply"), "the promotion");

    assert!(
        probe.sent.load(Ordering::SeqCst),
        "the driver's channel took the push"
    );
    let written = tokio::time::timeout(Duration::from_secs(30), written)
        .await
        .expect("the push was never answered")
        .expect("await reply");
    let (standby, role) = probe
        .seen
        .lock()
        .expect("seen")
        .take()
        .expect("the probe ran");
    let planned = matches!(
        written,
        Reply::Done {
            outcome: Outcome::Push(_),
            ..
        }
    );
    assert!(
        !standby && planned,
        "the promotion was answered before its cluster took a client's command: at the answer \
         the engine said standby = {standby}, and a push sent then was answered {written:?}"
    );
    // And after its entry applied here: what the node says of itself next is
    // the promoted cluster's.
    assert!(
        matches!(role, Some(Role::Promoted(_))),
        "the promotion was answered while this node's state said {role:?}"
    );

    // The engine goes before the node: its serve thread may hold the store.
    drop((waker, probe));
    let gone = Arc::downgrade(&engine);
    drop(engine);
    b.stop().await;
    let end = Instant::now() + Duration::from_secs(10);
    while gone.strong_count() > 0 {
        assert!(Instant::now() < end, "the engine is still held");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let dir = b.dir.clone();
    b.shut().await;
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_standby_refuses_what_does_not_follow_it() {
    // A source that logs its clients' commands and nothing of its own, so its
    // first two entries are the two pushes.
    let quiet = || BatcherConfig {
        pipeline: 4,
        request_expire_every_ms: 3_600_000,
        ..BatcherConfig::default()
    };
    let mut a = Cluster::open(scratch("refusals-source"));
    a.start_with(quiet(), None);
    push_created(&a.command(push(1, "q", "p0", &["a"])).await);
    push_created(&a.command(push(2, "q", "p1", &["b"])).await);
    a.stop().await;
    let entries = source_entries(&a, Position::START);
    assert_eq!(entries.len(), 2, "the two pushes");
    let (first, second) = (entries[0].clone(), entries[1].clone());

    let mut b = Cluster::open(scratch("refusals-standby"));
    b.start(Some(LinkBoot::empty("test://source")));
    let mirror = |prev: u64, e: &(u64, u64, Arc<Entry>)| LinkOp::Mirror {
        prev,
        index: e.0,
        term: e.1,
        entry: e.2.clone(),
    };

    // Not the entry after the standby's position.
    refused(
        b.link(mirror(first.0, &second)).await,
        OUT_OF_SEQUENCE_CODE,
        true,
        "an entry sent ahead of its turn",
    );
    // In sequence by its numbers, but planned on a state the standby does
    // not hold: the first entry created the queue and its partition.
    refused(
        b.link(mirror(0, &second)).await,
        DIVERGED_CODE,
        false,
        "an entry that does not continue the standby's state",
    );
    // Neither refusal moved anything.
    done(b.link(mirror(0, &first)).await, "the first entry");
    done(b.link(mirror(first.0, &second)).await, "the second entry");
    // A duplicate is out of sequence, never applied twice.
    refused(
        b.link(mirror(first.0, &second)).await,
        OUT_OF_SEQUENCE_CODE,
        true,
        "an entry sent twice",
    );
    assert_same(&a, &b, "after the refusals");

    // Its own log is no source: the entry that made this cluster a standby
    // comes back only from this cluster itself (a source configured to be
    // the standby's own address), and replaying it would never end.
    let own = source_entries(&b, Position::START).remove(0);
    refused(
        b.link(LinkOp::Mirror {
            prev: second.0,
            index: second.0 + 1,
            term: own.1,
            entry: own.2.clone(),
        })
        .await,
        DIVERGED_CODE,
        false,
        "the standby's own standby entry",
    );
    assert_same(&a, &b, "after its own entry was refused");

    // An ordinary cluster takes no link command.
    a.start_with(quiet(), None);
    refused(
        a.link(mirror(0, &first)).await,
        NOT_STANDBY_CODE,
        false,
        "a source entry on a cluster that is not a standby",
    );
    refused(
        a.link(LinkOp::Promote).await,
        NOT_STANDBY_CODE,
        false,
        "a promotion of a cluster that was never a standby",
    );

    let (da, db) = (a.dir.clone(), b.dir.clone());
    a.shut().await;
    b.shut().await;
    let _ = std::fs::remove_dir_all(da);
    let _ = std::fs::remove_dir_all(db);
}

// ---------------------------------------------------------------------------
// Leader changes
// ---------------------------------------------------------------------------

/// The replicator of a one-node cluster, with the ROLE the test says: what one
/// node of a larger cluster lives through when the leadership moves. The
/// driver, and whoever asks this node its role, are told what the test set.
/// The log, its commits and the apply are the real replicator's, which leads
/// its own cluster throughout: what is logged on it while the driver is told
/// another node leads ([`Cluster::another_leader`]) is what that leader
/// logged, and it reaches this node's state as it reaches a follower's.
struct Told {
    node: Arc<RaftReplicator<HeedStore>>,
    role: watch::Sender<NodeRole>,
}

impl Told {
    fn new(node: Arc<RaftReplicator<HeedStore>>, role: NodeRole) -> Arc<Told> {
        Arc::new(Told {
            node,
            role: watch::channel(role).0,
        })
    }

    fn tell(&self, role: NodeRole) {
        self.role.send_replace(role);
    }
}

#[async_trait]
impl Replicator for Told {
    async fn propose(&self, entry: Bytes, deadline: Instant) -> Result<AppliedAt, ProposeError> {
        self.node.propose(entry, deadline).await
    }

    fn wants_bytes(&self) -> bool {
        self.node.wants_bytes()
    }

    async fn propose_entry(
        &self,
        entry: Bytes,
        planned: Arc<Entry>,
        deadline: Instant,
    ) -> Result<AppliedAt, ProposeError> {
        self.node.propose_entry(entry, planned, deadline).await
    }

    fn role(&self) -> NodeRole {
        *self.role.borrow()
    }

    fn watch_role(&self) -> watch::Receiver<NodeRole> {
        self.role.subscribe()
    }

    fn applied_notify(&self) -> Option<Arc<tokio::sync::Notify>> {
        self.node.applied_notify()
    }

    fn committed_watch(&self) -> Option<watch::Receiver<(u64, u64)>> {
        self.node.committed_watch()
    }

    async fn read_barrier(&self, deadline: Instant) -> Result<u64, ProposeError> {
        self.node.read_barrier(deadline).await
    }

    fn applied_index(&self) -> u64 {
        self.node.applied_index()
    }

    fn applied_term_at(&self, index: u64) -> Option<u64> {
        self.node.applied_term_at(index)
    }

    async fn transfer_leadership(
        &self,
        to: Option<NodeId>,
        deadline: Instant,
    ) -> Result<(), ReplError> {
        self.node.transfer_leadership(to, deadline).await
    }

    async fn membership(&self) -> Membership {
        self.node.membership().await
    }

    async fn change_membership(
        &self,
        change: MembershipChange,
        deadline: Instant,
    ) -> Result<(), ReplError> {
        self.node.change_membership(change, deadline).await
    }

    fn metrics(&self) -> ReplMetrics {
        self.node.metrics()
    }

    fn kinds_floor(&self) -> Option<u32> {
        self.node.kinds_floor()
    }
}

/// A driver that logs its clients' commands and nothing of its own: no step
/// of the leader's comes due within a test, so a log holds what the test put
/// in it.
fn quiet_cfg() -> BatcherConfig {
    BatcherConfig {
        pipeline: 4,
        request_expire_every_ms: 3_600_000,
        ..BatcherConfig::default()
    }
}

impl Cluster {
    /// Start a driver, with its engine, on a replicator that says the role
    /// the test tells it ([`Told`]): `role` to begin with.
    fn start_told(&mut self, boot: Option<LinkBoot>, role: NodeRole) -> (Arc<Engine>, Arc<Told>) {
        let told = Told::new(self.repl.clone(), role);
        let engine = Engine::new(self.store.clone());
        let (link, link_rx) = tokio::sync::mpsc::channel(64);
        let (tx, handle) = Batcher::new(self.store.clone(), told.clone(), quiet_cfg())
            .with_link(link_rx, boot)
            .with_engine(engine.clone())
            .spawn();
        self.driver = Some(Driver { tx, link, handle });
        (engine, told)
    }

    /// The role this node's own replicator says once it leads: what
    /// [`Told::tell`] is given to make the driver lead again.
    async fn leading(&self) -> NodeRole {
        let end = Instant::now() + Duration::from_secs(30);
        loop {
            let role = self.repl.role();
            if role.is_leader() {
                return role;
            }
            assert!(Instant::now() < end, "the node never led: {role:?}");
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    }

    /// Wait until the driver has taken a role in which node `leader` leads:
    /// it then answers a command with that node as the hint, and plans none.
    async fn until_the_driver_follows(&self, leader: NodeId) {
        let end = Instant::now() + Duration::from_secs(30);
        loop {
            match self.command(push(9_000, "q", "probe", &["x"])).await {
                Reply::Retry { hint: Some(l) } if l == leader => return,
                other => assert!(
                    Instant::now() < end,
                    "the driver never followed node {leader}: {other:?}"
                ),
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }

    /// Wait until the driver has taken every command sent before this one. It
    /// takes them in the order they were sent, and this one, which would
    /// write a row of the link's, it answers at once whatever its role.
    async fn until_the_driver_took_what_was_sent(&self) {
        let barrier = Command::Effects(EffectsCommand {
            request_id: rid(9_001),
            tenant: TENANT.to_string(),
            effects: vec![Position::START.effect()],
        });
        refused(
            self.command(barrier).await,
            "link_rows",
            false,
            "a command that would write the link's rows",
        );
    }

    /// Wait until this node's state says `what` of its place in a link.
    async fn until_the_role_is(&self, what: &str) -> Role {
        let end = Instant::now() + Duration::from_secs(30);
        loop {
            let role = self.store.read(|r| link::read_role(r)).expect("role");
            if role.name() == what {
                return role;
            }
            assert!(
                Instant::now() < end,
                "the role never became {what}: {role:?}"
            );
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    }

    /// The driver of another node, which leads this cluster while this node's
    /// driver is told to follow it. It runs on this node's replicator: what
    /// that leader logs is applied here as a follower applies it.
    fn another_leader(&self) -> Driver {
        let (link, link_rx) = tokio::sync::mpsc::channel(64);
        let (tx, handle) = Batcher::new(self.store.clone(), self.repl.clone(), quiet_cfg())
            .with_link(link_rx, None)
            .spawn();
        Driver { tx, link, handle }
    }

    /// Stop a cluster started with [`Cluster::start_told`] and remove it. The
    /// engine goes before the node: its serve thread may hold the store.
    async fn end(mut self, engine: Arc<Engine>, told: Arc<Told>) {
        let gone = Arc::downgrade(&engine);
        drop((engine, told));
        self.stop().await;
        let end = Instant::now() + Duration::from_secs(10);
        while gone.strong_count() > 0 {
            assert!(Instant::now() < end, "the engine is still held");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let dir = self.dir.clone();
        self.shut().await;
        let _ = std::fs::remove_dir_all(dir);
    }
}

impl Driver {
    async fn command(&self, command: Command) -> Reply {
        let (sub, rx) = Submission::new(command);
        self.tx.send(sub).await.expect("send command");
        rx.await.expect("await reply")
    }

    async fn link(&self, op: LinkOp) -> Reply {
        let (sub, rx) = LinkSubmission::new(op);
        self.link.send(sub).await.expect("send link op");
        rx.await.expect("await link reply")
    }

    async fn stop(self) {
        drop((self.tx, self.link));
        self.handle.await.expect("driver join");
    }
}

type Call = std::pin::Pin<Box<dyn std::future::Future<Output = Reply>>>;

/// `command` through the leader's intake of `b`'s node, as its own client's
/// arrives, or one a follower forwards: answered there, or handed to the
/// driver.
fn through_the_intake(engine: &Arc<Engine>, b: &Cluster, command: Command) -> Call {
    let (engine, tx) = (engine.clone(), b.driver().tx.clone());
    Box::pin(async move {
        RaftFacade::submit_here_for_test(&engine, &tx, command, deadline())
            .await
            .unwrap_or_else(|e| panic!("the intake did not answer: {e}"))
    })
}

/// One poll of `call`: `Some` when that answered it. The intake decides in
/// its first poll, and a command it does not answer itself is in the driver's
/// channel when the poll returns.
async fn first_poll(call: &mut Call) -> Option<Reply> {
    use std::task::Poll;

    std::future::poll_fn(|cx| match call.as_mut().poll(cx) {
        Poll::Ready(reply) => Poll::Ready(Some(reply)),
        Poll::Pending => Poll::Ready(None),
    })
    .await
}

/// `call`'s answer, which is owed now: nothing more has to happen for it.
async fn owed(call: &mut Call, what: &str) -> Reply {
    tokio::time::timeout(Duration::from_secs(30), call)
        .await
        .unwrap_or_else(|_| panic!("{what}: not answered within 30 s"))
}

/// What `command` is answered when it reaches `b`'s node while the node is
/// being elected, and the node then leads. Elected, a node says `Candidate`
/// until an entry of its term has applied (I13), and the followers already
/// send it their clients' commands. Unless the intake answers the command
/// itself, the driver holds it before the node leads.
async fn sent_to_a_node_being_elected(
    engine: &Arc<Engine>,
    b: &Cluster,
    told: &Told,
    leading: NodeRole,
    command: Command,
    what: &str,
) -> Reply {
    told.tell(NodeRole::Candidate);
    let mut call = through_the_intake(engine, b, command);
    let at_once = first_poll(&mut call).await;
    if at_once.is_none() {
        b.until_the_driver_took_what_was_sent().await;
    }
    told.tell(leading);
    match at_once {
        Some(reply) => reply,
        None => owed(&mut call, what).await,
    }
}

/// What a node answers a client follows what its cluster IS, across every
/// leader change, and never what the node led last.
///
/// The leader's intake refuses a client's command while the engine says its
/// cluster is a standby, and the driver told the engine so only when it began
/// to lead: a node that had led the standby went on saying it once it no
/// longer led. After the cluster had been promoted under another leader, it
/// answered `standby` to every command a node forwarded to it, until it led
/// again and its driver had read the role: a promoted cluster, and among
/// those commands the retry of a write that cluster had taken, whose first
/// answer a leader change had lost (Jepsen `fo-repro-primaries-3`,
/// 2026-10-08: two pushes answered `503 standby` 32 s after the promotion,
/// both in the log).
///
/// A node is sent commands before its driver leads: elected, it says
/// `Candidate` until an entry of its term has applied (I13), and its
/// followers forward to it from the moment raft names it. That window is held
/// open here, not raced ([`sent_to_a_node_being_elected`]).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_elected_again_answers_what_its_cluster_is_now() {
    let mut b = Cluster::open(scratch("elected-again"));
    let leading = b.leading().await;
    let (engine, told) = b.start_told(Some(LinkBoot::empty("test://source")), leading);
    refused(
        through_the_intake(&engine, &b, push(1, "q", "p0", &["first"])).await,
        link::STANDBY_CODE,
        true,
        "a client write on the standby's leader",
    );
    b.until_the_role_is("standby").await;

    // Another node takes the leadership, and this one is elected again while
    // the cluster is still a standby: a command that reaches it meanwhile is
    // answered what a standby answers, at the latest when it leads.
    told.tell(NodeRole::Follower { leader: Some(2) });
    b.until_the_driver_follows(2).await;
    refused(
        sent_to_a_node_being_elected(
            &engine,
            &b,
            &told,
            leading,
            push(2, "q", "p0", &["second"]),
            "a command sent to a node elected to lead a standby",
        )
        .await,
        link::STANDBY_CODE,
        true,
        "a command sent to a node elected to lead a standby",
    );

    // Another node again. Under it the cluster is promoted, and takes a
    // client's write.
    told.tell(NodeRole::Follower { leader: Some(2) });
    b.until_the_driver_follows(2).await;
    let other = b.another_leader();
    done(other.link(LinkOp::Promote).await, "the promotion");
    let taken = push_created(&other.command(push(3, "q", "p0", &["third"])).await);
    other.stop().await;

    // This node has applied the promotion, and nothing it answers from here
    // says standby, whoever leads and whatever it led before. Still a
    // follower, it sends a command on to the leader. Elected, it is sent the
    // retry of the write the cluster took, whose answer the leader change
    // lost: the write is answered as taken, once.
    let forwarded = through_the_intake(&engine, &b, push(4, "q", "p0", &["fourth"])).await;
    let retried = sent_to_a_node_being_elected(
        &engine,
        &b,
        &told,
        leading,
        push(3, "q", "p0", &["third"]),
        "the retry of a write a promoted cluster took",
    )
    .await;
    assert!(
        matches!(&retried, Reply::Done { .. }) && push_created(&retried) == taken,
        "a promoted cluster answered the retry of a write it had taken: {retried:?}"
    );
    assert!(
        matches!(forwarded, Reply::Retry { hint: Some(2) }),
        "a follower of a promoted cluster answered a command forwarded to it: {forwarded:?}"
    );
    // The retry planned nothing: the next push follows the write.
    let next = push_created(&through_the_intake(&engine, &b, push(5, "q", "p0", &["fifth"])).await);
    assert_eq!(
        next,
        (taken.0, taken.1 + 1),
        "the retry of a taken write was planned again"
    );

    b.end(engine, told).await;
}

/// A standby plans nothing of its clients', whenever their commands reached
/// its driver.
///
/// A command that arrives while no leader is known is queued: the node may
/// win the election, and plans it then. A node that wins a STANDBY's election
/// plans the link's entries and nothing else, so what waited is answered as
/// every client command is on a standby. It used to stay in the queue,
/// unanswered, until the next link cycle launched the cycle after it early,
/// as an ordinary leader does, and that cycle planned it: behind a source
/// entry, an entry of the standby's own in a log that replays its source's
/// (Jepsen `fo-crash-lz-kill`, 2026-10-08); behind the promotion, a command
/// planned long after its caller had given up.
///
/// Here the command comes through the leader's intake of a node that has not
/// led the standby since it started (its engine says nothing of a standby),
/// while the node is elected and its driver does not lead yet. The driver
/// holds the command before the node leads: told the role first, it would
/// refuse the command as it arrived, with or without the fix.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_command_that_waited_out_a_standbys_election_is_never_planned() {
    // A source whose first two entries are two pushes.
    let mut a = Cluster::open(scratch("election-source"));
    a.start_with(quiet_cfg(), None);
    push_created(&a.command(push(1, "q", "p0", &["a"])).await);
    push_created(&a.command(push(2, "q", "p1", &["b"])).await);
    a.stop().await;
    let entries = source_entries(&a, Position::START);
    assert_eq!(entries.len(), 2, "the two pushes");
    let mirror = |prev: u64, e: &(u64, u64, Arc<Entry>)| LinkOp::Mirror {
        prev,
        index: e.0,
        term: e.1,
        entry: e.2.clone(),
    };

    // Its standby, one entry behind.
    let boot = || Some(LinkBoot::empty("test://source"));
    let mut b = Cluster::open(scratch("election-standby"));
    b.start_with(quiet_cfg(), boot());
    done(b.link(mirror(0, &entries[0])).await, "the first entry");
    b.stop().await;

    // An election of the standby: this node's driver does not lead, and a
    // client's command reaches it.
    let leading = b.leading().await;
    let (engine, told) = b.start_told(boot(), NodeRole::Candidate);
    let mut waited = through_the_intake(&engine, &b, push(100, "q", "p9", &["mine"]));
    assert!(
        first_poll(&mut waited).await.is_none(),
        "an election keeps a command for its outcome"
    );
    b.until_the_driver_took_what_was_sent().await;

    // The node wins, and what it leads is a standby.
    told.tell(leading);
    refused(
        owed(
            &mut waited,
            "a command that waited out a standby's election",
        )
        .await,
        link::STANDBY_CODE,
        true,
        "a command that waited out a standby's election",
    );

    // The standby goes on replaying, and holds its source's state: no entry
    // of its own but the one that made it a standby.
    done(
        b.link(mirror(entries[0].0, &entries[1])).await,
        "the second entry",
    );
    assert_same(&a, &b, "after a command waited out the election");

    // Nor is the command planned once the cluster is promoted: a push sent
    // then is the first the cluster plans.
    done(b.link(LinkOp::Promote).await, "the promotion");
    push_created(&b.command(push(101, "q", "p0", &["after"])).await);
    assert_eq!(
        b.store.read(|r| r.pid_of(TENANT, "q", "p9")).expect("read"),
        None,
        "the promoted cluster planned a command its caller was refused as a standby's"
    );

    let da = a.dir.clone();
    a.shut().await;
    let _ = std::fs::remove_dir_all(da);
    b.end(engine, told).await;
}

// ---------------------------------------------------------------------------
// The follower
// ---------------------------------------------------------------------------

/// What the scripted source answers its next reads.
#[derive(Clone, Debug)]
enum Script {
    /// What the source really holds.
    Real,
    /// No answer at all.
    Unreachable,
    /// A node that has applied nothing.
    Behind,
    /// The standby's position is below the purge point.
    Purged,
    /// Another entry than the standby's at its position.
    Mismatch,
}

async fn status_is(
    follower: &crate::rsm::link::driver::Follower,
    what: &str,
    want: impl Fn(&crate::rsm::link::driver::Status) -> bool,
) -> crate::rsm::link::driver::Status {
    let end = Instant::now() + Duration::from_secs(20);
    loop {
        let s = follower.status();
        if want(&s) {
            return s;
        }
        assert!(Instant::now() < end, "{what}: the follower is at {s:?}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_follower_follows_its_source_and_says_why_when_it_cannot() {
    use crate::rsm::link::driver::{self, Fetch, LinkConfig, State};
    use crate::rsm::link::wire::Answer;

    let quiet = || BatcherConfig {
        pipeline: 4,
        request_expire_every_ms: 3_600_000,
        ..BatcherConfig::default()
    };
    let mut a = Cluster::open(scratch("follower-source"));
    a.start_with(quiet(), None);
    push_created(&a.command(push(1, "q", "p0", &["a", "b"])).await);
    push_created(&a.command(push(2, "q", "p1", &["c"])).await);

    let mut b = Cluster::open(scratch("follower-standby"));
    b.start(Some(LinkBoot::empty("test://source")));

    // The source, read in this process: what it serves a standby over HTTP,
    // or what the script says instead.
    let script = Arc::new(std::sync::Mutex::new(Script::Real));
    // Every call: the node asked, and whether it was only told the position.
    let asked: Arc<std::sync::Mutex<Vec<(String, bool)>>> = Arc::default();
    let fetch: Fetch = {
        let (script, asked, source) = (script.clone(), asked.clone(), a.repl.link_source());
        Arc::new(move |addr: String, req: crate::rsm::link::wire::Request| {
            let script = script.lock().expect("script").clone();
            asked.lock().expect("asked").push((addr, req.hold_only));
            let source = source.clone();
            Box::pin(async move {
                // The two addresses are one node here: what the other node is
                // told must not move the hold the reads leave on this one.
                if req.hold_only {
                    return Ok(Answer::Entries {
                        entries: Vec::new(),
                        upto: req.after,
                        applied: 0,
                        purged: 0,
                    });
                }
                match script {
                    Script::Real => {
                        let bytes = source.serve(req).await.map_err(|e| e.to_string())?;
                        Answer::decode(&bytes).map_err(|e| e.to_string())
                    }
                    Script::Unreachable => Err("connection refused".to_string()),
                    Script::Behind => Ok(Answer::Entries {
                        entries: Vec::new(),
                        upto: 0,
                        applied: 0,
                        purged: 0,
                    }),
                    Script::Purged => Ok(Answer::Purged {
                        purged: req.after + 100,
                        applied: req.after + 200,
                    }),
                    Script::Mismatch => Ok(Answer::Mismatch {
                        index: req.after,
                        source_term: Some(req.after_term + 5),
                        applied: req.after + 200,
                        purged: 0,
                    }),
                }
            })
        })
    };
    let follower = driver::spawn(
        LinkConfig {
            sources: vec!["one:7400".into(), "two:7400".into()],
            token: None,
            name: Some("the-standby".into()),
        },
        Arc::downgrade(&b.store),
        b.repl.watch_role(),
        b.driver().link.clone(),
        fetch,
    );
    let set = |s: Script| *script.lock().expect("script") = s;
    let following =
        |s: &driver::Status| s.state == State::Following && s.lag_entries() == 0 && s.entries >= 2;

    // It follows, and the source knows who reads it and how far it got.
    let s = status_is(&follower, "the first entries", following).await;
    assert_eq!(s.error, None);
    assert_same(&a, &b, "after the follower caught up");
    let readers = a.repl.link_source().readers();
    assert_eq!(readers.len(), 1, "{readers:?}");
    assert_eq!(readers[0].name, "the-standby");
    assert_eq!(readers[0].after, s.position.index, "its hold is at its position");
    // One node is read; the other is only told where the standby is, so it
    // keeps the entries after that too.
    status_is(&follower, "the other node is told the position", |_| {
        let asked = asked.lock().expect("asked");
        asked.contains(&("one:7400".to_string(), false))
            && asked.contains(&("two:7400".to_string(), true))
    })
    .await;
    {
        let asked = asked.lock().expect("asked");
        assert!(
            !asked.contains(&("two:7400".to_string(), false)),
            "a node that answers is not left for another: {asked:?}"
        );
    }

    // And goes on following: what the source logs next arrives by itself.
    push_created(&a.command(push(3, "q", "p2", &["d"])).await);
    status_is(&follower, "a later entry", |s| following(s) && s.entries >= 3).await;
    assert_same(&a, &b, "after a later entry");

    // A source that does not answer: it waits, and tries the other node.
    asked.lock().expect("asked").clear();
    set(Script::Unreachable);
    let s = status_is(&follower, "an unreachable source", |s| {
        s.state == State::Waiting
    })
    .await;
    assert!(
        s.error.as_deref().is_some_and(|e| e.contains("refused")),
        "{s:?}"
    );
    status_is(&follower, "both nodes are tried", |_| {
        let asked = asked.lock().expect("asked");
        let read = |node: &str| asked.iter().any(|(a, hold_only)| a == node && !hold_only);
        read("one:7400") && read("two:7400")
    })
    .await;

    // Nodes that are behind the standby: once every one of them has said so.
    set(Script::Behind);
    let s = status_is(&follower, "a source behind the standby", |s| {
        s.error.as_deref().is_some_and(|e| e.contains("as far as"))
    })
    .await;
    assert_eq!(s.state, State::Waiting, "{s:?}");

    // What does not pass: the standby needs a new seed, and says which way.
    set(Script::Purged);
    let s = status_is(&follower, "a purged source", |s| {
        s.state == State::Halted && s.error.as_deref().is_some_and(|e| e.contains("purged"))
    })
    .await;
    assert!(s.error.unwrap().contains("needs a new seed"));
    set(Script::Mismatch);
    status_is(&follower, "another log", |s| {
        s.state == State::Halted
            && s.error
                .as_deref()
                .is_some_and(|e| e.contains("followed another log"))
    })
    .await;
    // Nothing was applied through any of it.
    assert_same(&a, &b, "after the refusals of the source");

    // The source is itself again: so is the follower, with no restart.
    set(Script::Real);
    push_created(&a.command(push(4, "q", "p3", &["e"])).await);
    status_is(&follower, "after the source came back", |s| {
        following(s) && s.entries >= 4
    })
    .await;
    assert_same(&a, &b, "after the source came back");

    // Stopped, it lets go of the store and the channel.
    follower.stop();
    drop(follower);
    let (da, db) = (a.dir.clone(), b.dir.clone());
    a.shut().await;
    b.shut().await;
    let _ = std::fs::remove_dir_all(da);
    let _ = std::fs::remove_dir_all(db);
}
