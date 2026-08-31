//! The fence: the compare-and-set that makes an offset commit safe when two
//! facades disagree about who the coordinator is.
//!
//! Routing ([`super::rendezvous`]) makes the coordinator unique WHEN THE VIEWS
//! AGREE. This makes the WRITE safe when they do not, which is the window
//! routing cannot close: a node that is stale about the live set still believes
//! it is the owner, and the measured symptom of that belief was a group that
//! committed 50 and read back 16, silently.
//!
//! ```text
//!   key    qk:fence:<escaped group>       (crate::offsets::fence_key)
//!   value  {"node":2,"incarnation":"3f2a…","generation":7,"ts":…}
//!   expiry forever
//! ```
//!
//! ## Why it is `forever` and why it is in the CLIENT's tenant
//!
//! A TTL would make a slow group's fence expire between commits: the `expect`
//! would come back `absent` and a perfectly legitimate commit would abort. It
//! inherits the same stated debt as the offsets it guards
//! ([`crate::offsets`]) and the same one-line prefix delete whenever
//! DeleteGroups arrives.
//!
//! It lives in the same `queen.kv` tenant as those offsets because `p_tenant`
//! is ONE argument for the whole call (024_kv.sql §6.1 point 6) — a fence in
//! another tenant could not ride in the same transaction, and riding in the
//! same transaction is the entire mechanism. So it is written with the CLIENT's
//! token, on the commit path, while the node registry is written with the
//! facade's own ([`super::registry`]). Nothing here retains a token: what is
//! kept per group is a `version` integer, which is not a credential. The
//! constraint is `coordinator/mod.rs`'s and it is stated there — *"a map that
//! outlives every connection must not be where a fleet's bearer tokens are
//! kept."*
//!
//! ## `required: true` is the whole mechanism
//!
//! Without it, a lost precondition is a VERDICT that "rolls back nothing"
//! (024_kv.sql §6.1 point 5) and the offset writes beside it would apply
//! anyway — which is the bug, not the fix. With it, the stored procedure raises
//! `check_violation` and the transaction aborts, so a zombie coordinator writes
//! exactly zero offsets (`:1498-1502`). It costs no extra round trip and one
//! operation of the call's budget.
//!
//! ## One bounded retry, never a CAS loop
//!
//! 024_kv.sql:1467-1471 is explicit: *"The version handed to a loser is
//! ADVISORY, never a fencing token to reuse blindly."* And `:585-587` notes
//! there is no server-side backoff, so an unbounded CAS loop on a contended key
//! never converges. The rule below therefore retries AT MOST ONCE and then
//! hands the backoff to the client, which has one.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use tokio::sync::Mutex as AsyncMutex;

use serde_json::json;

use crate::identity::TenantKey;
use crate::offsets::{self, Committed};
use crate::queen::{self, KvOp, QueenApi};

use super::{Cluster, ClusterState, Owner};

/// One line per window when a commit is fenced off. A fleet whose members all
/// commit against a stale coordinator produces one of these per member per
/// interval, which is exactly the shape that floods a log.
static FENCED: crate::obs::Sampler = crate::obs::Sampler::new(60_000);

/// The fence operation of one commit attempt.
#[derive(Debug, Clone, PartialEq)]
pub struct FenceOp {
    pub key: String,
    pub value: serde_json::Value,
    /// The version this facade last saw for this group, or 0 for "must not
    /// exist" — which is what a takeover, and a group's first commit, send.
    pub expect: i64,
}

impl FenceOp {
    /// The same fence, expecting a different version — what a commit wide
    /// enough to be chunked needs, because writing the fence advances its
    /// version and every chunk is its own transaction.
    pub fn at(&self, expect: i64) -> FenceOp {
        FenceOp {
            key: self.key.clone(),
            value: self.value.clone(),
            expect,
        }
    }

    /// The operation as it goes on the wire: conditional, `required`, forever.
    pub fn kv_op(&self) -> KvOp {
        KvOp::fence(
            offsets::NAMESPACE,
            &self.key,
            self.value.clone(),
            self.expect,
        )
    }
}

/// What a fenced commit did.
#[derive(Debug)]
pub enum Verdict {
    /// The writes were attempted; one result per input pair.
    Stored(Vec<queen::Result<()>>),
    /// Another node holds this group's fence and this node is not its
    /// rendezvous owner. Nothing was written. Answer NOT_COORDINATOR, which is
    /// a redirect: the client re-runs FindCoordinator and commits where it
    /// should have.
    NotCoordinator,
    /// The fence could not be settled — the one retry lost as well. Nothing was
    /// written. Answer COORDINATOR_NOT_AVAILABLE, which the client backs off
    /// on; by the time it asks again the views have converged.
    Unavailable,
}

/// Write one commit's offsets, fenced when this facade is clustered.
///
/// In single mode this is exactly the old call — no fence operation, no extra
/// round trip, the same body on the wire — because there is no second writer to
/// fence against.
pub async fn commit(
    api: &dyn QueenApi,
    cluster: &Cluster,
    tenant: &TenantKey,
    group: &str,
    generation: i32,
    pairs: &[(String, Committed)],
    token: Option<&str>,
) -> Verdict {
    let Some(state) = cluster.state() else {
        return Verdict::Stored(offsets::store(api, pairs, token, None).await.results);
    };
    let Some(key) = offsets::fence_key(group) else {
        // Unreachable: a fence key is shorter than any offset key of the same
        // group, so a group that gets here has at least one storable key and
        // therefore a storable fence. Refused rather than written unfenced —
        // an unfenced write in cluster mode is the defect this module exists
        // for.
        return Verdict::Unavailable;
    };

    // Serialises the members of one group in this process, so the retry below
    // is rare rather than routine. It is an OPTIMISATION: correctness is the
    // retry rule, not the lock. Held only across the KV call.
    let cell = state.fences.cell(tenant, group);
    let mut held = cell.lock().await;
    let mut expect = held.unwrap_or(0);

    // Two attempts at most. The second one is either the same-process race
    // resolving or the takeover completing; a third would be a CAS loop.
    for attempt in 0..2 {
        let op = FenceOp {
            key: key.clone(),
            value: value_of(state, generation),
            expect,
        };
        let stored = offsets::store(api, pairs, token, Some(&op)).await;
        let Some(lost) = stored.lost else {
            if let Some(version) = stored.fence_version {
                *held = Some(version);
            }
            return Verdict::Stored(stored.results);
        };

        // Who holds it now, and is that us? Two credentials of one tenant are
        // two `TenantKey`s (identity.rs's fallback) but ONE `queen.kv` row, so
        // two members of one group committing at once inside this process lose
        // to each other — which is a race, not a fencing event.
        let winner = Winner::of(&lost.value);
        let ours = winner.as_ref().is_some_and(|w| w.is(state));
        // ...and if the fence names somebody else while WE are the rendezvous
        // owner, the retry below IS the takeover. `absent` (version 0) lands
        // here too, and the retry is then a `putIfAbsent` that wins.
        let owner = matches!(cluster.owner_of_group(group), Owner::Us);

        if attempt == 0 && (ours || owner) {
            expect = lost.version;
            continue;
        }

        // A real fence, or a retry that lost as well. Either way this facade's
        // remembered version is worthless.
        *held = None;
        if let Some(suppressed) = FENCED.tick_now() {
            tracing::warn!(
                target: "kafka",
                group,
                attempt,
                reason = %lost.reason,
                winner = winner.as_ref().map_or(-1, |w| w.node),
                winner_incarnation = winner.as_ref().map_or("", |w| w.incarnation.as_str()),
                me = state.me.id,
                suppressed,
                "an offset commit was fenced off; nothing was written"
            );
        }
        return if ours || owner {
            Verdict::Unavailable
        } else {
            Verdict::NotCoordinator
        };
    }
    // Unreachable: every arm of the loop returns.
    Verdict::Unavailable
}

/// The value this node writes into a group's fence.
fn value_of(state: &ClusterState, generation: i32) -> serde_json::Value {
    json!({
        "node": state.me.id,
        "incarnation": state.me.incarnation,
        "generation": generation,
        "ts": std::time::UNIX_EPOCH
            .elapsed()
            .map(|d| d.as_millis() as i64)
            .unwrap_or_default(),
    })
}

/// Who a lost fence says holds it.
#[derive(Debug)]
struct Winner {
    node: i32,
    incarnation: String,
}

impl Winner {
    /// `None` for an absent fence and for a value nothing here wrote — both of
    /// which mean "not us", which is the only question asked of it.
    fn of(value: &serde_json::Value) -> Option<Winner> {
        Some(Winner {
            node: value.get("node")?.as_i64()? as i32,
            incarnation: value.get("incarnation")?.as_str()?.to_string(),
        })
    }

    /// Is the holder THIS process? The incarnation is what makes that
    /// different from "a process that had my node id before" — equality only,
    /// never ordering.
    fn is(&self, state: &ClusterState) -> bool {
        self.node == state.me.id && self.incarnation == state.me.incarnation
    }
}

/// The per-group fence versions this process remembers.
///
/// A version is not a credential and not durable state: losing one costs
/// exactly one extra retry on that group's next commit, which is why the map
/// may be capped and evicted without a correctness argument.
pub struct Fences {
    cells: Mutex<HashMap<(TenantKey, String), Held>>,
    /// Ticks once per lookup. The ordering `cells` is evicted in.
    clock: AtomicU64,
}

struct Held {
    cell: Arc<AsyncMutex<Option<i64>>>,
    used: u64,
}

/// How many groups' fences are remembered at once. The same bound, for the same
/// reason, as the coordinator's own `MAX_GROUPS`: a group id is a string a
/// client chooses, so a map keyed by one is a map a peer can size.
const MAX_FENCE_CELLS: usize = 10_000;

impl Default for Fences {
    fn default() -> Fences {
        Fences::new()
    }
}

impl Fences {
    pub fn new() -> Fences {
        Fences {
            cells: Mutex::new(HashMap::new()),
            clock: AtomicU64::new(0),
        }
    }

    /// The cell for one (tenant, group), made on first use.
    ///
    /// The lock here is a `std::sync::Mutex` and nothing is awaited while it is
    /// held: the `tokio::Mutex` inside is CLONED out and this one released
    /// before the caller waits on it.
    fn cell(&self, tenant: &TenantKey, group: &str) -> Arc<AsyncMutex<Option<i64>>> {
        let now = self.clock.fetch_add(1, Ordering::Relaxed);
        let mut cells = self
            .cells
            .lock()
            .expect("the fence map lock is never held across a panic");
        let key = (tenant.clone(), group.to_string());
        if let Some(held) = cells.get_mut(&key) {
            held.used = now;
            return Arc::clone(&held.cell);
        }
        while cells.len() >= MAX_FENCE_CELLS {
            let Some(coldest) = cells
                .iter()
                .min_by_key(|(_, held)| held.used)
                .map(|(k, _)| k.clone())
            else {
                break;
            };
            cells.remove(&coldest);
        }
        let cell = Arc::new(AsyncMutex::new(None));
        cells.insert(
            key,
            Held {
                cell: Arc::clone(&cell),
                used: now,
            },
        );
        cell
    }

    /// How many cells are held. For the tests and for a log line, not for
    /// control flow.
    pub fn len(&self) -> usize {
        self.cells
            .lock()
            .expect("the fence map lock is never held across a panic")
            .len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::testing;
    use crate::queen::testing::FakeQueen;

    const THREE: [(i32, &str, u16); 3] = [
        (1, "kafka-1.example.com", 9092),
        (2, "kafka-2.example.com", 9092),
        (3, "kafka-3.example.com", 9092),
    ];

    /// The pinned rendezvous table says node 2 owns `orders-consumer`.
    const OWNED: &str = "orders-consumer";

    fn cluster(me: i32) -> Cluster {
        Cluster::Enabled(testing::state(&THREE, me))
    }

    fn pairs(group: &str, offsets: &[(i32, i64)]) -> Vec<(String, Committed)> {
        offsets
            .iter()
            .map(|(partition, offset)| {
                (
                    offsets::key(group, "orders", *partition).unwrap(),
                    Committed {
                        offset: *offset,
                        metadata: String::new(),
                        ts: 1_787_824_800_123,
                    },
                )
            })
            .collect()
    }

    async fn commit_as(
        api: &FakeQueen,
        cluster: &Cluster,
        group: &str,
        offsets: &[(i32, i64)],
    ) -> Verdict {
        commit(
            api,
            cluster,
            &TenantKey::anonymous(),
            group,
            7,
            &pairs(group, offsets),
            None,
        )
        .await
    }

    fn stored_offset(api: &FakeQueen, group: &str, partition: i32) -> Option<i64> {
        api.kv_get(
            offsets::NAMESPACE,
            &offsets::key(group, "orders", partition).unwrap(),
        )
        .and_then(|v| v["offset"].as_i64())
    }

    /// Single mode sends no fence at all: one operation per partition, and the
    /// body is the one this facade has always written.
    #[tokio::test]
    async fn single_mode_writes_exactly_what_it_always_did() {
        let api = FakeQueen::with(&[]);
        let verdict = commit_as(&api, &Cluster::Single, "g", &[(0, 41)]).await;
        assert!(matches!(verdict, Verdict::Stored(r) if r.len() == 1 && r[0].is_ok()));
        let ops = api.kv_ops();
        assert_eq!(ops.len(), 1, "single mode sent a fence: {ops:?}");
        assert_eq!(
            ops[0],
            KvOp::put(
                offsets::NAMESPACE,
                "qk:group:g:orders:0",
                serde_json::json!({"offset": 41, "metadata": "", "ts": 1_787_824_800_123i64}),
            )
        );
        assert!(api.kv_get(offsets::NAMESPACE, "qk:fence:g").is_none());
    }

    /// The fence op is index 0, conditional, `required`, forever — and the
    /// version it comes back with is what the next commit expects, so a second
    /// commit from the same node is one call and no retry.
    #[tokio::test]
    async fn the_fence_is_the_first_operation_and_its_version_is_kept() {
        let api = FakeQueen::with(&[]);
        let cluster = cluster(2);
        commit_as(&api, &cluster, OWNED, &[(0, 41)]).await;

        let calls = api.kv_calls.lock().unwrap().clone();
        assert_eq!(calls.len(), 1, "the first commit was not one call");
        match &calls[0][0] {
            KvOp::Put {
                key,
                expect,
                required,
                forever,
                ttl_seconds,
                ..
            } => {
                assert_eq!(key, "qk:fence:orders-consumer");
                assert_eq!(*expect, Some(0), "the first commit is a putIfAbsent");
                assert!(*required, "the fence must abort the transaction it loses");
                assert!(*forever, "a fence that expires aborts a legitimate commit");
                assert_eq!(*ttl_seconds, None);
            }
            other => panic!("operation 0 is not the fence: {other:?}"),
        }

        // The second commit expects the version the first one produced.
        let version = api.kv_version_of(offsets::NAMESPACE, "qk:fence:orders-consumer");
        api.kv_calls.lock().unwrap().clear();
        commit_as(&api, &cluster, OWNED, &[(0, 42)]).await;
        let calls = api.kv_calls.lock().unwrap().clone();
        assert_eq!(calls.len(), 1, "the second commit retried");
        match &calls[0][0] {
            KvOp::Put { expect, .. } => assert_eq!(*expect, Some(version)),
            other => panic!("operation 0 is not the fence: {other:?}"),
        }
        assert_eq!(stored_offset(&api, OWNED, 0), Some(42));
    }

    /// THE defect: a zombie coordinator's commit writes NOTHING, and is told so
    /// in the one code that makes its client go and find the real one.
    #[tokio::test]
    async fn a_fenced_commit_writes_no_offset_at_all() {
        let api = FakeQueen::with(&[]);
        // Node 2 owns the group and commits 50.
        let owner = cluster(2);
        commit_as(&api, &owner, OWNED, &[(0, 50), (1, 50)]).await;
        assert_eq!(stored_offset(&api, OWNED, 0), Some(50));

        // Node 1 is stale: it still believes it is the coordinator, but it is
        // not the rendezvous owner. It commits 16.
        let zombie = cluster(1);
        let verdict = commit_as(&api, &zombie, OWNED, &[(0, 16), (1, 16)]).await;
        assert!(
            matches!(&verdict, Verdict::NotCoordinator),
            "{verdict:?} instead of the redirect"
        );
        assert_eq!(
            stored_offset(&api, OWNED, 0),
            Some(50),
            "the rewind that this whole module exists to stop"
        );
        assert_eq!(stored_offset(&api, OWNED, 1), Some(50));
    }

    /// TAKEOVER: the new owner has no version, loses to its predecessor's
    /// fence, and its ONE retry is the takeover itself.
    #[tokio::test]
    async fn the_first_commit_after_a_takeover_is_the_takeover() {
        let api = FakeQueen::with(&[]);
        // Node 1 held the fence while it was (or believed it was) the owner.
        api.kv_seed(
            offsets::NAMESPACE,
            "qk:fence:orders-consumer",
            serde_json::json!({"node": 1, "incarnation": "gone", "generation": 3, "ts": 1}),
        );

        // Node 2 IS the rendezvous owner and has never committed.
        let verdict = commit_as(&api, &cluster(2), OWNED, &[(0, 7)]).await;
        assert!(
            matches!(&verdict, Verdict::Stored(r) if r.iter().all(|x| x.is_ok())),
            "{verdict:?}"
        );
        assert_eq!(stored_offset(&api, OWNED, 0), Some(7));
        assert_eq!(
            api.kv_calls.lock().unwrap().len(),
            2,
            "the retry is one, and it is the takeover"
        );
        let fence = api
            .kv_get(offsets::NAMESPACE, "qk:fence:orders-consumer")
            .unwrap();
        assert_eq!(fence["node"], 2);
    }

    /// A concurrent writer inside THIS process is not a fencing event: the same
    /// node and the same incarnation, so the version is adopted and the commit
    /// goes through on the retry.
    #[tokio::test]
    async fn losing_to_ourselves_is_a_race_and_not_a_fence() {
        let api = FakeQueen::with(&[]);
        let cluster = cluster(2);
        let state = cluster.state().unwrap();
        // What a second connection of this process would have left behind: our
        // own node and our own incarnation, at a version this call's cell does
        // not know.
        api.kv_seed(
            offsets::NAMESPACE,
            "qk:fence:orders-consumer",
            serde_json::json!({
                "node": state.me.id,
                "incarnation": state.me.incarnation,
                "generation": 6,
                "ts": 1,
            }),
        );
        let verdict = commit_as(&api, &cluster, OWNED, &[(0, 9)]).await;
        assert!(
            matches!(&verdict, Verdict::Stored(r) if r.iter().all(|x| x.is_ok())),
            "{verdict:?}"
        );
        assert_eq!(stored_offset(&api, OWNED, 0), Some(9));
    }

    /// A node whose view has gone stale cannot claim the group even if it holds
    /// the version: `owner_of_group` answers Unknown, so a lost fence is not
    /// retried into a takeover.
    #[tokio::test(start_paused = true)]
    async fn a_stale_node_does_not_take_a_group_over() {
        let api = FakeQueen::with(&[]);
        api.kv_seed(
            offsets::NAMESPACE,
            "qk:fence:orders-consumer",
            serde_json::json!({"node": 3, "incarnation": "elsewhere", "generation": 1, "ts": 1}),
        );
        let cluster = cluster(2);
        tokio::time::advance(cluster.state().unwrap().ttl + std::time::Duration::from_secs(1))
            .await;
        let verdict = commit_as(&api, &cluster, OWNED, &[(0, 3)]).await;
        assert!(matches!(&verdict, Verdict::NotCoordinator), "{verdict:?}");
        assert_eq!(stored_offset(&api, OWNED, 0), None);
    }

    /// A wide commit is chunked one operation SHORTER when it is fenced, and
    /// every chunk carries its own fence — because a chunk is a transaction and
    /// `required` aborts the transaction it is in.
    #[tokio::test]
    async fn a_fenced_commit_is_chunked_one_shorter() {
        let api = FakeQueen::with(&[]);
        let wide: Vec<(i32, i64)> = (0..600).map(|p| (p, i64::from(p))).collect();
        commit_as(&api, &cluster(2), OWNED, &wide).await;

        let calls = api.kv_calls.lock().unwrap().clone();
        // 600 offsets at 255 per call is three calls, each with a fence.
        assert_eq!(calls.len(), 3);
        for call in &calls {
            assert!(call.len() <= queen::MAX_KV_OPS_PER_CALL);
            match &call[0] {
                KvOp::Put { key, required, .. } => {
                    assert_eq!(key, "qk:fence:orders-consumer");
                    assert!(*required);
                }
                other => panic!("a chunk went out without a fence: {other:?}"),
            }
        }
        assert_eq!(stored_offset(&api, OWNED, 599), Some(599));
    }

    /// The version map is bounded, and a group whose cell was evicted still
    /// commits — it costs the one retry the rule allows.
    #[tokio::test]
    async fn the_version_map_is_bounded_and_an_evicted_group_still_commits() {
        let api = FakeQueen::with(&[]);
        let cluster = cluster(2);
        let state = cluster.state().unwrap();
        let tenant = TenantKey::anonymous();
        for i in 0..MAX_FENCE_CELLS + 1 {
            state.fences.cell(&tenant, &format!("group-{i}"));
        }
        assert_eq!(state.fences.len(), MAX_FENCE_CELLS);

        // `group-0` is the coldest and is gone. It commits anyway.
        api.kv_seed(
            offsets::NAMESPACE,
            "qk:fence:group-0",
            serde_json::json!({"node": 2, "incarnation": "an-older-process", "generation": 1, "ts": 1}),
        );
        // ...and it is a group node 2 owns, so the retry is the takeover.
        let owned_by_two = (0..64)
            .map(|i| format!("group-{i}"))
            .find(|g| matches!(cluster.owner_of_group(g), Owner::Us))
            .expect("no generated group lands on node 2");
        api.kv_seed(
            offsets::NAMESPACE,
            &offsets::fence_key(&owned_by_two).unwrap(),
            serde_json::json!({"node": 1, "incarnation": "older", "generation": 1, "ts": 1}),
        );
        let verdict = commit_as(&api, &cluster, &owned_by_two, &[(0, 5)]).await;
        assert!(
            matches!(&verdict, Verdict::Stored(r) if r.iter().all(|x| x.is_ok())),
            "{verdict:?}"
        );
    }

    /// A store that fails for an ordinary reason is not a fencing verdict: the
    /// per-partition errors come back and the client retries the commit rather
    /// than being told to go and find another coordinator.
    #[tokio::test]
    async fn a_transport_failure_is_not_a_fence() {
        let api = FakeQueen::with(&[]);
        api.fail_kv(queen::Error::Transport("connection refused".into()));
        let verdict = commit_as(&api, &cluster(2), OWNED, &[(0, 1)]).await;
        match verdict {
            Verdict::Stored(results) => assert!(results[0].is_err()),
            other => panic!("{other:?}"),
        }
    }
}
