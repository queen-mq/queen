# Findings while rehearsing recovery (2026-10-01, image local/queen:rec2 = HEAD 22773001 + test knob)

Every procedure in `helm_v2/RECOVERY.md` works around these. None is fixed yet.

| # | What | Severity | Where to fix |
|---|---|---|---|
| F11 | Two PVCs+pods deleted at once (k8s): cluster cannot commit, every pod Ready | high | follows from F1 + F8 |
| F1 | A node whose log went backwards (wiped / restored old copy, not removed first) is never repaired; its clients hang | high | refuse a fresh node that is still a voter, or openraft `allow_log_reversion` |
| F8 | A node without quorum (or cut off) keeps answering `/health` 200 healthy, leader:true | high | `health()`: leader known only with recent leader contact |
| F3 | Damage in an old sealed queue-log file: not found at boot or by scrub; pops through the node 500, auto-ack loses batches; no ERROR log | high | ERROR + /health 503 on a qlog checksum failure; scrub sealed qlog files |
| F6 | `qlog/SHARDS` deleted: node reads the wrong layout, serves nothing of the old data | medium (needs a deleted file) | refuse when shared logs exist without the file |
| F4 | A deleted sealed queue-log file is not noticed; reads miss messages | medium (needs a deleted file) | check each partition's committed range is locatable at open |
| F2 | `/health` lag 0 for a node that applied nothing while far behind | medium | `catch_up_lag`: no applied entry while > slack behind = lagging |
| F10 | Wrong `QUEEN_ENCRYPTION_KEY`: consumers get the ciphertext envelope as data, silently | medium | decrypt failure = error / ERROR log + metric |
| F5 | Bit flip in `data.mdb` can segfault at boot (exit 139) with no log line | low (still refuses) | let the SIGBUS watch also catch SIGSEGV |
| F7 | `raft/state.json` missing: refusal text blames "the local replicator" | low | name the missing file |
| F9 | The disk gate guards a node only from its own clients; a smaller disk fills from replication | design note | keep the 3 PVCs the same size |

Damage matrix (`sc04-corrupt.sh`, one follower, data verified through that node when it booted):

| Damage | Outcome |
|---|---|
| bit flip in the active queue-log file | refused at boot: `rsm qlog corrupt … corruption inside acknowledged data, not a torn tail` |
| bit flip in a sealed queue-log file the boot reads | refused at boot: `damaged record at byte … of a sealed file` |
| bit flip in an OLD sealed queue-log file | **boots**; pops 500 `record checksum` (F3) |
| active queue-log file truncated | refused: `acknowledged records are missing (a truncated log)` |
| sealed queue-log file deleted | **boots**; 666 acked messages missing through it (F4) |
| `store/data.mdb` bit flip | refused, exit 139, no message (F5) |
| `store/data.mdb` truncated | refused: `FATAL store: bus error (SIGBUS) reading …` |
| `store/data.mdb` deleted, log never purged | boots, rebuilds the store from the log, data correct |
| `store/data.mdb` deleted, log purged | refused: `Cannot re-apply logs: need logs from index 0, but purged up to …` |
| `raft/state.json` garbage / deleted | refused (`does not parse` / misleading F7) |
| `raft/membership.json` garbage | refused: `does not parse` |
| `snapshot.pending` garbage | refused: `snapshot.pending: expected value` |
| `qlog/SHARDS` deleted | **boots**; every message missing through it (F6) |
| `raft/committed.json`, `local.db`, `dash.db`, `seg/*.seg` garbage | tolerated, data correct |

## F1 — a node whose log went BACKWARDS (wiped, or rolled back to an old copy) is never repaired: replication to it stalls (silent)
- Also sc06-stale-copy.sh: a follower restored from its own 40-s-old copy stays at applied 598 while the leader
  holds matchIndex 1537; requests through it time out; /health 200 (gap < 1000 entries of slack).
- Repro: sc03c-empty-voter.sh. Wipe a follower's volume, start it again (k8s: PVC deleted, pod recreated).
- The fresh node probes peers, joins (does not initialize), learns the leader and term, but its applied
  index stays 0 forever. Leader's view: matchIndex = the pre-wipe value (747), never moves.
- Cause: openraft `allow_log_reversion` is off (default). The leader keeps `matching=747`, the follower
  answers conflict below it, `update_conflicting` only sets searching_end (debug_assert in debug builds),
  replication never backs off. openraft progress/entry/update.rs.
- Impact: clients routed to that node hang/time out (load test: 192 writes in 60 s instead of 2,400).
- Remedy that works (tested): DELETE member -> wipe again -> start -> add learner -> promote.
- Fix options: refuse to start a fresh node that is still a VOTER in the cluster's membership
  (clear FATAL: "remove node N first"); or set `allow_log_reversion` (openraft keeps the quorum value).
- STILL OPEN (2026-10-08, sc03c again on a 2.0.3 build): the node never catches up. What changed is
  what it says: with the F2 fix below its /health is 503 once the cluster is more than 1000 entries
  ahead (applied 0, lag from the epoch), where it was 200 for good.

## F2 — /health says healthy for a node that applied nothing
- server/src/rsm/facade/real.rs catch_up_lag(): `if last <= 0 { return 0 }` — a node with no applied
  entry (store last_now_us 0) reports lag 0 although it is 1000+ entries behind the leader's commit.
- Impact: k8s readiness passes for the stuck node of F1, the Service sends it clients.
- Fix: nothing applied while > slack behind -> report a lag (e.g. now - read time, or u64::MAX).
- FIXED 2026-10-08. Two causes, not one. (1) `if last <= 0 { return 0 }`: gone, the age is counted
  from the epoch. (2) That line was never reached for the node of F1: it asks the leader its commit
  index through a membership it does not have, gets no answer, and "no reading" also meant lag 0.
  The catch-up lag now falls back to the commit point of the view the leader puts on its appends
  (`ClusterMembers::commit_seen`: what a majority of the voters hold), which does reach that node.
  Rehearsal: 3,262 entries, follower wiped and restarted under its id -> 503 settling from its
  first answer, role learner, applied 0, lag 1.79e12; on 2.0.3 the same node said healthy.
  Within the slack (1000 entries, QUEEN_RAFT_READY_LAG_ENTRIES) such a node still says healthy.

## F3 — damage in an OLD sealed queue-log file is not detected; the node stays healthy and answers 500
- Repro: probe-sealed.sh (flip 16 bytes in a sealed qlog file whose entries are below the store's
  durable point and that openraft does not read at boot).
- Boot trusts the sealed file's .qidx and never re-reads the .qlog; the store scrub covers LMDB only.
- Pops through that node for the affected partition: HTTP 500
  `read pop payload (qlog) at pid 1 off 79: record checksum 0x.. != 0x..` (no file, no node named);
  NO ERROR line in the node's log; /health 200.
- auto-ack consumers reading through it lose the claimed batches (cursor already advanced): 1000 missing.
- The same data read through the other nodes is complete.
- Remedy that works: replace the node (wipe + rejoin).
- Fix options: ERROR log naming the file + /health 503 (or fatal exit) on a qlog checksum failure in a
  read; background scrub of sealed qlog files like the store scrub.

## F4 — a DELETED sealed queue-log file is not noticed; reads through the node miss messages
- sc04 case qlog-delete-sealed: rm qlog/q3/r00000001.qlog (its .qidx left) on a stopped follower.
- Node boots, healthy; auto-ack verify through it: 666 acked messages missing (other nodes complete).
- Nothing checks that the files a log needs are all there (check_tails covers the newest record only).
- Runbook rule: never delete files inside a data directory to free space.
- Fix option: at open, each partition's committed range must be locatable (or log continuity of file ids
  above the retention floor) -> refuse like the other corruption checks.

## F5 — a bit flip in store/data.mdb can crash the boot with SIGSEGV (exit 139) and NO log line
- sc04 case store-flip: 16 bytes flipped mid data.mdb -> exit 139 on every restart, no FATAL line.
- The SIGBUS watch (store/sigbus.rs) prints a FATAL + exit 1 for a truncated file; a damaged
  branch page that LMDB follows segfaults instead, silently.
- Runbook: CrashLoopBackOff with exit 139 at boot and no FATAL = treat as a damaged store: replace.
- Fix option: let MapWatch also catch SIGSEGV on watched threads (same notice, exit 1).

## F6 — a deleted qlog/SHARDS file makes the node read the wrong layout; ALL reads through it miss data
- sc04 case shards-delete: node boots (only WARN "QUEEN_QLOG_SHARDS differs ... the directory's wins"),
  auto-ack verify through it: 11,100 of 11,100 acked messages missing.
- qlog/set.rs load_shards(): no file + existing q* logs => 0 shards (one log per queue, the pre-62041ef3
  layout), so the shared logs q0..q5 holding the data are never consulted.
- Fix option: no SHARDS file but shared-layout logs present => refuse to start.

## F7 — raft/state.json deleted => misleading refusal
- "…/raft was written by the local replicator: the openraft replicator does not adopt an existing
  directory (start it on an empty one)". The directory was written by openraft; state.json is missing.
- Fix option: say "raft/state.json is missing (vote and purge point)" and point at the replace procedure.

## Tolerated (no action needed, verified data intact through the node):
raft/committed.json garbage, local.db garbage, dash.db garbage, seg/*.seg garbage.
## Refused at boot with a clear FATAL (remedy = replace the node):
qlog active file bit flip or truncation, sealed qlog damage that the boot reads, data.mdb truncated
(SIGBUS FATAL), state.json garbage, membership.json garbage, snapshot.pending garbage.

## F8 — a node that lost its quorum keeps answering /health 200 "healthy", leader:true, indefinitely
- Kill 2 of 3 nodes; poll the survivor: 45 s later still {"status":"healthy","role":"follower","leader":true,
  term unchanged}. Requests through it hang until their deadline (curl -m 5 => no answer).
- Cause: health() leader_known = metrics.current_leader.is_some(); openraft keeps the last leader id while
  this follower's pre-votes fail (term never moves).
- Docs say the opposite (ha.mdx "every node answers 503 until a majority is back", operations.mdx).
- Impact: readiness stays green, an alert on /health never fires for the survivor; clients hang.
- Fix option: leader_known only if the leader's last append/heartbeat reached this node within ~2x the
  election timeout max (the members view carries its age), else 503 settling.
- FIXED 2026-10-08: /health is ready only while the leader the node follows (itself, when it leads)
  heard from a majority of the voters within QUEEN_RAFT_READY_QUORUM_MS (5000; 0 = not checked).
  The figure is in the body (`raft.quorumAckMs`): the silence of the voter that completes the
  majority in the leader's members view, plus the view's age on a follower
  (`ClusterMembers::quorum_ack_age`). It rides the appends, so no load on the client path holds
  it up. Rehearsals (3 containers, kill -9): survivor a follower -> 503 at +5.0 s; survivor the
  leader -> 503 at +5.2 s; back to 200 2-3 s after one node returns; a plain leader failover
  (3.7 s) -> both remaining nodes 200 throughout. On 2.0.3 the survivor said healthy for good.
  Test: rsm::tests::raft_cluster::a_node_without_its_quorum_stops_reporting_ready (fails with the
  window at 0).
- AGAIN 2026-10-10 on the 2.2.0 code, where the fix ships (health-quorum.py: three processes on
  loopback, kill -9, three runs): the leader left alone answers 503 from +5.00 and +5.03 s
  (quorumAckMs 5021; the first run's polling was blocked by its own probe), with a write through
  it at +3 s still hanging (000 after 5 s); 200 again 1.1, 1.8 and 2.0 s after one node is
  started; a leader failover took 3.8, 3.5 and 3.5 s, and the two survivors answered 200 through
  it, apart from one 503 each at the instant of the election in the first run (no leader known).
  A follower stopped, wiped and started under its id answers 503 from its first answer (learner,
  applied 0, lag from the epoch). That run was a DEBUG build, on which the node then applied
  everything within 3.6 s: no evidence about F1, which was reproduced on release images (sc03c
  was not run again).

## F9 (design note, not a bug) — the disk gate protects a node only from its OWN clients
- S5: node 3 on a smaller disk. At 85% its gate closes (pushes sent to it: 507), but writes taken by the
  other nodes keep replicating to it: 92% -> 98% -> 100% -> "raft log write failed: No space left on
  device" -> node 3 stops (role stopped, /health 503, process stays up; k8s keeps the pod NotReady, no restart).
- The cluster keeps serving on the other two; growing the volume + restarting node 3 recovers it (tested).
- Implication: keep all voters' volumes the SAME size; alert on disk % per pod, not on 507s.

## F10 — a wrong QUEEN_ENCRYPTION_KEY is silent: consumers get the ciphertext envelope as `data`
- S22: queue with encryptionEnabled, key replaced on every node (rolling). Pop answers 200 with
  data = {"encrypted":"…","iv":"…","authTag":"…"}; no error, no WARN/ERROR in the log.
- encryption.rs decrypt_payload_bytes -> None on a failed tag check; the pop render then sends the raw bytes.
- Nothing on disk is damaged: the original key brings the plaintext back (tested).
- Fix option: a decrypt failure is an error the client sees (or at least an ERROR log + metric).

## F11 (Kubernetes) — deleting two PVCs+pods at once leaves a cluster that cannot commit but reports Ready
- k8s-K2b-lost2.sh on the real chart (k3s): delete PVCs + pods of 2 of 3 voters (the leader's included).
- The SIGTERM hand-off cascaded onto pods that were themselves terminating: T1-N1 -> 3 (T2) -> 2 (T3).
- The StatefulSet recreated both pods EMPTY under the old ids; the new leader kept stale progress
  (node 3 matchIndex 464, node 1 no ack for 210 s, no error logged after the first Connect failures):
  nothing commits (KV put and pops hang on every pod), yet every pod answers /health 200 and is Ready.
- Recovery that works (tested): treat as majority lost — OnDelete + FORCE_RECOVER on the survivor,
  rebuild the two others one at a time (k8s-K2c-ondelete.sh): 73 s, 0 acked writes lost.
- Runbook rules: never delete more than one PVC at a time; remove the member BEFORE deleting its PVC.

## K8s procedure notes (validated on k3s with the real chart)
- `kubectl set env sts/...` while pods are not Ready: the RollingUpdate waits forever for Ready; a pod you
  delete meanwhile comes back from the OLD revision (still carrying the old env). => use
  updateStrategy OnDelete for env-based repairs, delete exactly the pods that must take the change.
- Scaling up from 0 with fresh PVCs for lost pods: a fresh pod can take the survivor's node (local-path) /
  zone (zonal PD + DoNotSchedule zone spread) => survivor Pending ("didn't match pod anti-affinity rules",
  "volume node affinity conflict"). OnDelete + restarting the survivor in place avoids the race.
