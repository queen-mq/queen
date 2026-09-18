# PLAN_RAFT.md — Queen without Postgres: one replicated log

Status: G0 RATIFIED 2026-09-18 (phase 0 spikes done, evidence in RAFT_STATUS.md
and test/raft/spikes/*/MEMO.md); phase 1 is next. Written 2026-09-17 from the pgless
investigation (PLAN_NATIVE_LOG.md, NATIVE_LOG_STATUS.md) and three read-only
surveys: the openraft source, every piece of Postgres state and every
background loop in the broker, and everything outside the storage core that
assumes stateless brokers. Every decision below is a PROPOSED DEFAULT until
Alice ratifies it at gate G0; a ratified decision is marked RATIFIED with a
date. The working record (what is done, findings, numbers) lives in
`RAFT_STATUS.md`, created by WP-0.1, never in this file.

Line numbers cited here are hints. Hints into `server/src/native/` refer to
`6e96e228` (pgless, phase 4); every other hint refers to `fc71b65b` (master,
1.6.0), the base of the raft branch. Function and file names are the stable
references; re-locate lines before relying on them.

Revision 2 (2026-09-17): applied an adversarial review of revision 1 (23
findings: in-flight entries on timeout, recovery against engines that reopen
past a durable point, snapshot sealing and install, dedup hashes outliving
segments, KV version assignment, delete semantics, clock-judged reads, empty
commands, route classes, seeding, wire compatibility, stale references).

---

## 0. Read this first

### 0.1 What this is

Replace Postgres inside the broker with one replicated state machine (RSM).
Every broker pod is a voter in a single Raft group (3 voters by default, 1 in
embedded or single-node mode). One leader turns client commands into log
entries of EFFECTS; every node applies committed entries, in order, to its
own full copy of the state: payload segment files plus an embedded ordered
key-value store. Followers forward writes to the leader, serve reads from
their applied copy, wake parked long-polls when they apply pushes, and serve
pop payloads from their own files. The feature set is exactly the postgres
class's, whose SQL procedures (`server/sql/procedures/*.sql`) are the
specification. Postgres stays in the binary as the test oracle until GA; the
proxy keeps its own database. No sharding, no per-queue storage classes, no
cross-cell transactions.

Why: the Postgres class spends up to four synchronous commits per delivered
message, its maintenance scales with stored segments and partitions (vacuum,
xmin, stats refresh, retention cliffs), and Postgres is 70–80% of the CPU in
sparse, high-cardinality load. The goal is a system whose cost follows the
write rate, not the stored volume, while every feature stays exact. The
sharded native class of pgless showed that a home-grown distributed design is
where the bugs are (20 of its 28 confirmed defects), so this plan keeps one
log, one leader and a borrowed consensus protocol.

### 0.2 How to use this document (agents)

1. Read §0–§4 fully. They hold the rules every work package must keep.
2. Read the work package (WP) you are assigned in §15 and every section it
   references. The appendices are the spec index: the SQL functions you port,
   the loops you replace, the hazards found in pgless.
3. Before writing code for a semantic WP, read the SQL functions it ports
   (§8, Appendix A) and the pgless module it reuses (`git show 6e96e228:<path>`).
4. Record progress, evidence and findings in `RAFT_STATUS.md` (§15.0 gives
   its shape). Change this plan only when a decision changes, and log the
   change with its reason in `RAFT_STATUS.md`.
5. A WP is done only when its exit criteria are met AND the evidence (the
   commands run, a summary of their output, the numbers) is in
   `RAFT_STATUS.md`.

### 0.3 Rules of engagement

- Never push. Never commit to `master` or `pgless`. Work on branch `raft`
  in the checkout `/Users/alice/Work/queen` (created 2026-09-17 from
  `fc71b65b`; the pgless hardening tree is parked as commit `2bbd10d1` on
  `pgless`). Ignore the other worktrees under `.claude/worktrees/`.
- One commit per WP (or per coherent sub-step of a large WP). Write the
  message to a file and commit with `git commit -F <file>` (zsh expands
  backticks in `-m`). End the message with the attribution line your session
  prescribes.
- Subagents and workflow agents run on Opus.
- WPs marked ⚠ touch durability, recovery, consensus or identity. Before
  their commit they get an adversarial review (§13.8). Record the result.
- Harnesses, scripts, fixtures and checkers live in the repo under
  `test/raft/`, never only in `/private/tmp` (pgless lost its harness that way).
- Numbers to quote come from the Linux VM (§13.6). macOS serializes
  `F_FULLFSYNC` and Postgres in Docker does not really flush: laptop numbers
  are for smoke only.
- Every I/O call, RPC and store call has a deadline. No blocking call on a
  tokio worker thread. No `std::sync::Mutex` held across an `.await`.
- Refuse, never guess: if the state needed to decide is unavailable, answer a
  retryable error.
- Stop and ask Alice at every gate (G0–G5) and before: changing a RATIFIED
  decision, anything on the VM beyond `/root` and its local Postgres, deleting
  data that is not yours, anything touching stage or prod.
- Known traps:
  - The repo's `cargo fmt` is red. Never format the whole crate; format only
    the files you touched (`rustfmt --edition 2021 <files>`). In zsh,
    `cargo fmt -- $VAR` does not split the variable and formats everything.
  - `pkill -f target/release/queen` kills your own shell (its command line
    contains the string). Use `pkill -x queen` or the PID you started.
  - Never kill processes by port (it once killed the Docker daemon).
  - macOS has no `timeout`.
  - `goload` (benchmark-queen/2026-07-29-vm-campaign/goload) builds with
    `GOWORK=off go build`; cross-compile for the VM with
    `GOOS=linux GOARCH=amd64`.
  - A push answers 201 with a per-item status: 201 alone proves nothing.
  - Docker/OrbStack can die mid-session: `orb start`, then restart containers.
  - `QUEEN_NATIVE_*` knobs belong to pgless and mean nothing here.

### 0.4 Glossary

| term | meaning |
|---|---|
| RSM | the replicated state machine: effects applied to the store and segment files |
| voter / learner | Raft member that votes / receives the log without voting |
| leader, follower | Raft roles; only the leader plans and proposes |
| command | a client operation turned into a typed request by the receiving broker |
| entry | one Raft log entry: a batch of commands' effects plus a header |
| effect | a deterministic state change (§5.2); apply executes effects |
| planner | leader-side code that turns commands into effects and outcomes (§7) |
| apply | the only code that mutates committed state (I1) |
| overlay | the planner's view of effects planned in the current cycle, discarded after it |
| committed state | the state produced by applying committed entries |
| durable point | a store commit plus file fsyncs that make an applied index survive a crash (§11.4) |
| snapshot / manifest | a durable point plus the list of immutable files that reproduce it (§11.6) |
| receiver | the broker where a client request lands |
| forwarding | receiver → leader transfer of a command (§9.2) |
| hold | receiver keeps a request while no leader is known (D13) |
| request id | 16-byte id minted once by the receiver per command, used to dedupe retries (D6) |
| outcome | the compact answer data recorded for a command (§5.4) |
| cluster version | replicated gate for new effect kinds (D20) |
| bucket | `xxh3(tenant ␟ queue ␟ partition) % 256`, the unit that groups segment files |
| position | `(bucket, file id, offset, len)` of a segment inside a node's files; node-local (D8) |

---

## 1. Goals, non-goals, constraints

Goals:

| id | goal | acceptance |
|---|---|---|
| G-1 | Exact feature parity with the postgres class | every client suite, facade lane and semantics test identical on raft and postgres topologies (§13) |
| G-2 | No Postgres in raft mode (broker and embedded) | `raft1`/`raft3` test topologies run without a Postgres container |
| G-3 | Cost follows the write rate | flatness test (§13.6): latency, CPU and RSS on a store holding hundreds of millions of messages match an empty store |
| G-4 | High availability | 3 voters; automatic failover; no acknowledged data lost with one node down; zero-downtime deploys via leadership transfer |
| G-5 | Simple | one log, one leader, one proposal in flight, a borrowed consensus library, no sharding |
| G-6 | Operable | bootstrap, node replacement, rolling upgrade, backup/restore, observability, migration from postgres deployments |

Non-goals (v1): write scale-out inside a cell (cells scale out); per-queue
storage classes or mixing; cross-cell transactions; Postgres introspection
features (the postgres-stats tab); embedded fleets sharing one database;
witness (vote-only) nodes; multi-raft; changes to the wire protocol or SDK APIs.

Constraints:

- C-1 Small team: every WP must be completable by one agent with clear exit criteria.
- C-2 Build: pure Rust, or native code compiled with `cc` only. No cmake. A
  build-time `protoc` only if the consensus spike picks raft-rs (§12.4).
- C-3 MSRV: `rust-version = "1.88"` (server/Cargo.toml). Every dependency must build on it.
- C-4 Deployment: Kubernetes StatefulSet (`helm_v1/broker`): prod runs 3
  replicas, stage and the default values run 2 (raft needs 3).
- C-5 Wire compatibility with every SDK (clients/), both facades
  (protocols/queen-kafka, protocols/queen-sqs), the S3 sink
  (connectors/queen-s3), the supervisor and the proxy.
- C-6 Performance claims are measured on the Linux VM (§13.6).

---

## 2. Decisions (RATIFIED at G0, 2026-09-18, as recommended; amendments from the spikes are inline)

| id | decision | why |
|---|---|---|
| D1 | Storage mode per deployment: `QUEEN_STORAGE=postgres\|raft` (default `postgres` until GA). No per-queue mixing. | Mixing classes created the two-store coupling behind pgless C17, C18, C20, C21. |
| D2 | One Raft group. Voters = StatefulSet pods (3). Embedded and single-node = 1 voter. Learners possible later. | One log keeps every multi-structure transaction exact. |
| D3 | Entries carry effects, not commands. Apply is deterministic and knows no semantics. | Followers never re-run planning, so caches, clocks and hash-map order cannot diverge replicas. |
| D4 (amended at G0) | A bounded pipeline: at most `QUEEN_RAFT_PIPELINE` (4) entries in flight on the leader. Each cycle plans on committed state plus the overlays of the entries still in flight, in order; on step-down or any propose error every overlay is dropped together and waiters get `Retry`. A timeout keeps its entry in flight (I3). | S3 measured one-in-flight at ~300 entries/s of 64 KiB (3 ms per commit round trip) against ~974/s with 16 in flight; a small bounded pipeline keeps the "nothing speculative survives" property without the throughput cap. |
| D5 | Time is stamped by the planner: `now = max(wall clock, last committed now + 1 µs)`. Every absolute time (lease expiry, deliver_at, expires_at, created_at) is computed by the planner and carried in effects. | Apply has no clock; time never runs backwards across leaders. |
| D6 | Every mutating command carries a 16-byte request id minted once by the receiver. Outcomes are kept in state for `QUEEN_RAFT_REQUEST_ID_WINDOW_S` (600). | A write in flight during a leadership change has an unknown outcome (openraft GH#2095). |
| D7 | Nothing answers before its entry is committed and applied on the leader. A receiver reads pop payloads only after applying the claim locally. | pgless answered pops and renews before their records were durable (`Wait::Enqueued`). |
| D8 | Positions are node-local. They never appear in entries or digests. A snapshot carries positions only together with the files they index. | pgless shipped checkpoints with the owner's positions to a backup whose files had different boundaries. |
| D9 (ratified: heed) | Payloads in per-bucket append-only segment files (256 buckets); the segment index in one immutable index file per sealed segment file, the active file's index in RAM (§6.1, §11.2); everything else in an embedded ordered store: **heed 0.22 (LMDB), everywhere** (3 voters, raft1 and embedded alike; Alice chose one engine over the packet's heed + redb split), behind an adapter with four pins the numbers depend on: `MDB_NOSYNC` (not `MDB_NOMETASYNC`: measured 13% slower and a 123 ms store-commit p99); the adapter exposes a read-transaction handle, never free-standing get/scan (thread-local reader slots: a second read txn on one thread is `MDB_BAD_RSLOT`; `MDB_NOTLS` does not scale, 2 M vs 10 M gets/s at 8 threads); `max_readers` ≥ the blocking pool, `MDB_READERS_FULL` a refusal not a panic; a map-size rule in §11.8 (`MDB_MAP_FULL` on the apply path is a node-local liveness cliff). Single-voter recovery is repair from the node's own last snapshot plus the Raft log (§11.5), never a discard. | S1 (test/raft/spikes/s1-store/MEMO.md): heed is the only candidate that always reopens consistent after kill -9 and after dropped writes, scans 42 M rows/s (10–20× fjall), sustains 20k/49k/59k msg/s; redb capped at ~10k msg/s with 29× write amplification; fjall reopened ahead of its durable point in 4 of 5 dropped-writes runs. Residual risk: one heed run in ten after dropped writes did not reopen and was not reproduced; the crash criterion was met on 20 dropped-writes runs, not 100. At G0 Alice chose to skip the follow-up runs (RAFT_STATUS.md D-01..D-03) and start phase 1: WP-1.2's own crash tests are the check, and the decision is re-opened if they disagree. |
| D10 (ratified: option (a), pending the S2 memo's final numbers) | Dedup exact, disk-backed, RAM bounded: a store index per hash, `(pid, hash) → (offset, created_at)`, pruned by the txns window (option (a) of spike S2: 12–21 µs p50 probes, exact; option (b), blooms over the Append hash lists, could not sustain the offered rate). If the S2 memo lands on (b) with evidence, re-decide before WP-1.2. Hash lists must outlive the segments retention deletes, for the txns window `max(dedup_window, completed_retention, 900 s)` (retention.rs ≈16–17): the dedup probe (003) and ack-by-hash below the cursor (005) read them. | pgless kept ~200 B per message in RAM for the whole window. |
| D11 (ratified: openraft) | Consensus behind a `Replicator` trait (§12.1). Phases 1–2 use `LocalReplicator` (no network). Library: **openraft at git rev `54094270ede0b8a2eb6ed6ae990edc6ca19d98ec`** (0.10.0-alpha.34 plus the GH#2095 `LogEntryDiscarded` fix) with **raft-log 0.4.6** as the log store, re-confirmed at G3 against whatever 0.10 release exists then; fallback raft-rs. | S3 (test/raft/spikes/s3-consensus/MEMO.md): raft-log passes openraft's log-store conformance suite; on the VM, 3 ms commit p50 for 64 KiB entries, a new leader 3.0–3.4 s after kill -9 with 0 acknowledged writes missing in 4 runs, leadership transfer in 28 ms, linearizable reads ~1.1 ms, a 1 GiB snapshot installed at 398 MiB/s, the wiped-voter procedure verified both ways. After the review: raft-log is qualified by crash runs (10 kill -9 + 10 dropped-writes rounds, 20/20 reopened at the matched index), not by the conformance suite alone; the adapter MUST persist `save_committed` durably (openraft's example does not; without it a restart came back with applied state behind what it had answered; cost +2.4 ms on a 64 KiB commit p50). Seven plan changes accepted at G0: §9.4 barriers coalesce into the in-flight one (no fixed 2 ms window: measured 7–8× worse); §12.5 append/snapshot deadlines have a floor of their own, never `soft_ttl()` from the heartbeat; readiness is never `is_leader`/`current_leader` but "applied ≥ the committed index reopened with"; D14 states 3–4 s failover; durable `save_committed` plus a readiness gate; IDENTITY carries a fencing value (highest term seen) so a reverted disk cannot bring back a stale vote store (D21); `propose` has its own deadline (D13). |
| D12 (ratified: framed TCP; S4 measured half the leader CPU per message of HTTP/1.1 at 50k msg/s, 1.35 vs 2.77 µs, and 5% fewer bytes) | Raft traffic and forwarding use a dedicated binary port `QUEEN_RAFT_PORT` (6634) with length-prefixed frames. Authentication is mutual: an HMAC with `QUEEN_RAFT_SECRET` over both sides' nonces, binding cluster_id and the from/to node ids; then (G0 choice: the MAC branch, not TLS) a sequenced MAC on every frame over `dir ‖ seq ‖ len ‖ type ‖ body` with per-direction keys derived from the handshake, the receiver accepting only its expected sequence, so frames can be neither injected nor replayed (S4 measured +1.2 µs/msg of leader CPU without SHA-NI, 0.06 of a core at 50k msg/s). TLS was not chosen because its trust anchor (who issues and rotates certificates in a cell) is undefined; it can return later as an option. Connection pools start at 2 per peer and are sized in WP-4.1 under a commit delay and mixed frame sizes (the "2 is the knee" claim was withdrawn). A node without a matching IDENTITY rejects Raft RPCs (the enforcement point of I9). Multi-node mode refuses to start without a secret. Confirmed by spike S4. | pgless's replica lane ran wide open when its secret was unset (U19). |
| D13 | Receivers hold requests up to `QUEEN_RAFT_HOLD_MS` (8000) while no leader is known, then answer 503 `no_leader`. A `propose` carries its own deadline, `QUEEN_RAFT_PROPOSE_MS` (5000): a node that still holds the leader role but has no quorum-ack lease (openraft GH#2080, reproduced in S3: the write hung 10 s unanswered) fails its waiters with `Retry` instead of hanging them; the entry stays in flight per I3. | JS, Python and PHP consumers exit on a lasting 5xx; Kafka groups stall after 10 s; the proxy passes 502/504 through. |
| D14 (kept at G0) | Heartbeat 100 ms, election timeout 1000–2000 ms, pre-vote on, quorum check (leader lease) on. Measured failover on the VM: 3.0–3.4 s to a new leader (detection = election_timeout_max + a random election timeout; the election itself is ~5 ms); the target is ≤ 4 s p99 (O14). Alice kept these over 400/800 ms, which would trade ~1.5 s failover for spurious elections when a follower's durable point stalls. | Cloud networks; etcd's defaults are 100 ms / 1000 ms. openraft's defaults (50 / 150–300 ms) are LAN-tuned. |
| D15 | Local stale reads for fetch, partitions/changed, browse, DLQ lists, stats, analytics, traces and timer lists. Linearizable reads (read barrier, §9.4) for KV gets and lists and for `POST /streams/v1/state/get`; they judge expiry at the leader-stamped `now` the barrier returns. Two constraints from the spikes: a restarted node serves NO local stale read until it has re-applied the committed index it reopened with; on heed a read begins and ends inside one blocking call and never holds a read transaction across an `.await`. | The Kafka, SQS and S3-sink fences read KV. |
| D16 | Stats, usage and retained bytes are counters updated at apply: O(1) per effect, plus O(subscribed groups) per `Append` for per-group pending counters. No periodic aggregation. | `log_refresh_all_stats_v1` is O(partitions) and was a production wall. |
| D17 | Metrics tables are node-local, not replicated: `system_metrics`, `worker_metrics`, `worker_metrics_summary`, `queue_lag_metrics`, `queue_parked_replica`, plus `retention_history`. Cluster views gather from every node. | A consensus log is a poor fit for per-replica minute metrics. |
| D18 | Traces are replicated data with an age limit (default 7 days). | Today `message_traces` is never purged by age. |
| D19 | No push spool in raft mode: after the hold, 503; SDK buffers retry. | Spool replay lands after newer pushes and re-mints message ids (file_buffer.rs replay path vs the push path in handlers/data.rs ≈553). |
| D20 | Effect kinds are gated by a replicated cluster version; a kind is proposed only when every voter supports it. | Rolling upgrades apply a new leader's entries on old followers. |
| D21 | Each data dir carries `IDENTITY {cluster_id, node_id, generation, disk_uuid, fence}` where `fence` is the highest Raft term this node has seen, rewritten (temp + fsync + rename) whenever the term grows: a REVERTED disk (a PVC restored from a volume snapshot) matches every other field and would bring back a stale vote store; a node whose stored fence is below the term the cluster reports refuses to vote until re-added (S3 also showed openraft's wiped-voter guard is a `debug_assert!`, silent in release builds). A node never votes with a dir that does not match its membership record. A wiped disk rejoins only as a new member (remove, add as learner, promote). | openraft calls a wiped voter "undefined behavior" and has no release-build guard; pgless C01/C22. |
| D22 | The postgres class stays in the binary as the oracle until GA; its removal is a separate decision. | Differential testing needs it. |
| D23 | Branch `raft` starts from `master` (`fc71b65b`). Reusable pgless modules are copied into `server/src/rsm/`. The per-queue native class, its placement, replica lane, RPC and SQL 034–036 are not carried over. | The native class is superseded; starting clean avoids ripping its PG-coupled seams out. |
| D24 | Ephemeral queues stay outside Raft; the mesh carries only ephemeral frames in raft mode. | RAM by definition. |
| D25 | The proxy keeps its own Postgres (`queen_proxy` schema); Queen Cloud cells still run Postgres for the control plane. One proxy change is needed: the new admin tenant-delete route (WP-2.5) must be blocked at the proxy, as pgless did (6e96e228 proxy/src/routes.rs ≈145); on master the proxy classifies every method under `/api/v1/resources` as a read (proxy/src/routes.rs ≈525–533). | fact (proxy/migrations/001_init.sql) |
| D26 | Kafka facade transactions stay single-node-only, as today. | A facade routing limit (protocols/queen-kafka/src/txn.rs ≈84–100), not a storage one. |

---

## 3. Architecture

### 3.1 Picture

```
      clients · proxy · Service · facades on loopback   (any broker, as today)
          │                   │                   │
   ┌──────┴──────┐     ┌──────┴──────┐     ┌──────┴──────┐
   │ broker-0    │     │ broker-1    │     │ broker-2    │
   │ LEADER      │     │ follower    │     │ follower    │
   │ HTTP :6632  │     │ HTTP :6632  │     │ HTTP :6632  │
   │ receiver    │     │ receiver    │     │ receiver    │
   │ planner     │◄────┤ forwarder   │     │ forwarder   │
   │ replicator ─┼────►│ replicator  │     │ replicator  │
   │      └──────┼─────┼─────────────┼────►│             │
   │ apply thread│     │ apply thread│     │ apply thread│
   ├─────────────┤     ├─────────────┤     ├─────────────┤
   │ raft log    │     │ raft log    │     │ raft log    │
   │ store       │     │ store       │     │ store       │
   │ segments    │     │ segments    │     │ segments    │
   │ local.db    │     │ local.db    │     │ local.db    │
   └─────────────┘     └─────────────┘     └─────────────┘
        internal port :6634 (Raft RPCs, forwarding, snapshots), HMAC-authenticated
```

### 3.2 Components on every node

| component | role |
|---|---|
| HTTP API, dashboard, auth, tenant middleware | unchanged routes (main.rs router) |
| facades (Kafka, SQS), S3 sink | unchanged child processes talking to their own broker over loopback |
| receiver pipeline | parse, auth, tenant, quota gate, encryption, message ids, hashes, packing → `Command` (§9.1) |
| forwarder | sends commands to the leader, holds during elections, retries with the same request id (§9.2) |
| batcher + planner | leader only: one entry per cycle (§7) |
| replicator | `LocalReplicator` or the Raft adapter (§12) |
| apply thread | sole writer of store and segment files; resolves waiters; wakes long-polls; runs local file maintenance between entries (§11) |
| read path | local stale reads; read barrier for linearizable reads (§9.4) |
| leader-only loops | timer wheel, retention, KV and trace expiry, chunked deletes, cluster version bump (§10.1) |
| node-local loops | metrics collector, durable points, snapshot triggers, digest reports (§10.2) |
| identity & membership | IDENTITY file, bootstrap, join, replacement (§12.6) |
| local.db | node-local metrics and retention history (D17) |

### 3.3 Threads and tasks

- tokio runtime: HTTP handlers, forwarder client and server, Raft network,
  long-poll parking, loop timers.
- planner thread (std, leader only): owns the overlay; reads committed state
  through store read transactions and RAM indexes; consumes commands from a
  bounded channel whose capacity is reserved before any lock is taken.
- apply thread (std, every node): consumes committed entries in index order;
  the only writer of the store and segment files; resolves the leader's
  waiters; publishes wakes through non-blocking notifiers; runs durable points,
  file GC and compaction between entries.
- snapshot sender/receiver: tokio tasks with `spawn_blocking` for file I/O.
- With one entry in flight (D4) the planner plans the next cycle only after
  the previous entry is applied locally or leadership is lost (a timeout is
  not enough, I3), so planner and apply never race on committed state. Local reads use store read transactions and immutable file bytes.

### 3.4 Module map (new)

```
server/src/rsm/
  mod.rs            Rsm facade used by handlers in raft mode (the storage seam)
  command.rs        Command types built by the receiver (§9.1)
  effect.rs         Effect enum + codec (from native/record.rs)
  entry.rs          Entry header, outcomes, request ids, kind versions (§5)
  planner/          leader-side planning, pure over (committed view, overlay)
    mod.rs push.rs pop.rs ack.rs txn.rs kv.rs timers.rs streams.rs admin.rs retention.rs
  apply.rs          the only mutator (I1)
  state/            committed view: store access + RAM derived indexes (§6.3)
  store/            ordered store adapter and keyspaces (§6, S1)
  segments/         payload segment files (from native/log.rs, no journal)
  dedup/            dedup design per S2
  batcher.rs        cycle driver: drain, plan, propose, await apply (§7.1)
  replicator/       trait + local.rs + raft.rs (§12)
  raftlog/          consensus log storage (§12.3)
  snapshot.rs       durable point, manifest, transfer, install (§11.6)
  net/              port 6634: framing, handshake, connection pools (§12.5)
  forward.rs        receiver → leader commands, hold, retries (§9.2)
  reads.rs          local reads, read barrier (§9.4)
  loops.rs          leader-only and node-local loops (§10)
  identity.rs       IDENTITY, bootstrap, join, replacement (§12.6)
  digest.rs         divergence detection (§12.9)
  local_metrics.rs  node-local metrics store (D17)
  faults.rs         crash points (from native/faults.rs)
  tests/
server/tests/conformance/   dual-backend semantics tests (§13.2)
test/raft/                  difffuzz, checker, crash, kill, flatness harnesses (§13)
```

### 3.5 Reuse from pgless and the postgres broker

| source | fate |
|---|---|
| native/record.rs | → rsm/effect.rs: keep the codec style (len, xxh3, kind, body); replace the lsn/epoch header with the entry header; add kinds (§5.2) |
| native/state.rs | → rsm/state/: keep partition/cursor/ready-index logic; state moves to the store; remove plan-time mutation |
| native/semantics.rs | → rsm/planner/: pure functions over committed view + overlay; delete `stage` (it applied at plan time, hazard D-1) |
| native/log.rs | → rsm/segments/: keep framing, rolling, reads; drop the journal coupling and per-group fsync; verify checksums on read |
| native/journal.rs | basis for `LocalReplicator`'s log (§12.2) |
| native/checkpoint.rs, native/bucket.rs | superseded by store durable points and the single apply thread |
| native/faults.rs | → rsm/faults.rs with new points (§13.5). Not in any commit: it exists only as an untracked file in the pgless working tree (`/Users/alice/Work/queen/server/src/native/faults.rs`); copy it read-only or re-create it (≈50 lines) |
| native/types.rs, native/tests/* | reuse types; port tests to planner/apply tests |
| native/placement.rs, replica.rs, rpc.rs, handlers/native_rpc.rs, sql 034–036 | not carried over (D23) |
| handlers seam (`storage_for`, `Storage::{Postgres,Native,Unknown}`) | replaced by a global `StorageMode`; the ~30 pgless dispatch sites are a map of where handlers branch |
| fusion.rs, ack_fusion.rs, pop_fusion.rs | receiver-side work reused (hashing, packing, repack on duplicates); batching replaced by rsm/batcher.rs |
| hotlist.rs, reconcile.rs, lease.rs, stats.rs, retention.rs, sweeper.rs SQL loops, file_buffer.rs | not started in raft mode (§10.3) |
| admission.rs | retargeted from DB pool size to planner queue depth |
| peerclient.rs, mesh.rs | kept for ephemeral only (D24) |

---

## 4. Invariants

Every WP keeps these. Each names how it is tested.

| id | invariant | tested by |
|---|---|---|
| I1 | Committed state is mutated only by apply. The planner never writes the store, segment files or committed RAM indexes. | property test: plan random command sequences, committed digest unchanged until apply; crash matrix |
| I2 | Apply is a pure function of (committed state, entry): no clock, no randomness, no environment or config reads, no dependence on hash-map iteration order, no floating point. | two fresh states with different `RandomState` seeds apply the same entries → equal state digests; `clippy.toml` disallowed-methods (`SystemTime::now`, `Instant::now`, `rand::*`, `std::env::var`) enforced for rsm/apply.rs, rsm/state, rsm/store |
| I3 | At most `QUEEN_RAFT_PIPELINE` (4) entries in flight on the leader, planned in order on committed state plus the in-flight overlays. An entry leaves the pipeline only when it is applied locally or leadership is lost; a timeout on the proposal does NOT remove it, because the entry may still commit. On step-down every overlay is dropped together. | planner unit test; timeout injection test (a delayed commit must not produce duplicate offsets or double claims); step-down with 4 in flight leaves committed state equal to the log; `queen_raft_inflight` ≤ 4 |
| I4 | No client-visible answer before commit + leader apply. Pop payloads are read only after local applied index ≥ the claim's commit index. | crash points `propose.sent`, `commit.before_apply`; checker: no answered write missing |
| I5 | Planner time is monotone across terms: `now ≥ last committed now + 1 µs`. | clock-jump kill test (leader wall clock −10 s) |
| I6 | A command whose request id is in the window returns its recorded outcome and plans nothing. | duplicate proposal injection; retry after leadership loss |
| I7 | Positions are node-local: never in entries, never in the replicated digest; shipped only with the files they index. | snapshot install onto a node with different file boundaries, then read every segment |
| I8 | No periodic work proportional to stored data; RAM bounded independently of retained volume; durable points cost proportional to change. Exception: a snapshot build for transfer may cost O(state) (a store export), and is bounded by the snapshot policy, not by the write path; S1 prefers engines with incremental checkpoints. | flatness test (§13.6), with snapshot build time reported separately |
| I9 | A node never participates with a data dir whose IDENTITY does not match its membership record; an empty dir joins only as a new member. | wiped-disk kill test; bootstrap-with-stale-flag test |
| I10 | A file is unlinked only after a durable store commit that no longer references it, and only when no snapshot manifest or reader holds it. | crash points `gc.before_unlink`, `gc.after_unlink` + recovery |
| I11 | Every apply commit records the applied index AND the lengths of the segment files it touched, atomically. Recovery trusts the state the store reopens with (after a process crash some engines reopen past the last durable commit), truncates segment files to the lengths recorded in that state, and verifies the checksums of the referenced tail frames. A referenced frame that is missing or corrupt makes the node discard its live state directory and install a snapshot (raft3), or refuse to start (raft1). | crash matrix; kill -9 during non-durable commits; dropped-unflushed-writes test (S1) |
| I12 | Every transaction in the SQL spec maps to exactly one command inside one entry (§8). | dual-backend conformance |
| I13 | A new leader plans only after applying the first entry of its own term. | failover test with commands in flight |
| I14 | When the planner lacks committed state it needs (not yet applied, snapshot installing), it refuses with a retryable error. | fault test; review lens |
| I15 | Every I/O, RPC and store call has a deadline; no blocking on tokio workers; no std mutex across `.await`; channel capacity is reserved before taking locks. | stalled-disk test (dm-delay): `/health`, `/metrics` and ephemeral endpoints stay responsive |
| I16 | Effect kinds above the replicated cluster version are never proposed. Decoding an unknown kind stops the Raft instance on that node (no votes, no acks, no apply), never skips. | mixed-version upgrade test |
| I17 | A snapshot install is atomic: written into a new versioned directory `sm-<index>-<term>/`, verified, fsynced, then activated by atomically replacing a `CURRENT` file that names the live directory (rename(2) cannot replace a non-empty directory). A crash mid-install leaves the old state live. | crash points `snapshot.install_staged`, `snapshot.install_activated` |
| I18 | Planner-assigned counters (partition ids, KV versions) come from bases stored in `meta`; the entry header carries the bases it used and apply asserts they equal `meta` (fatal otherwise) before advancing them. | duplicate-assignment injection test |

---

## 5. Entries and effects

### 5.1 Entry

```rust
struct Entry {
    format: u16,                  // codec version of this layout
    kinds_version: u32,           // highest effect-kind version used inside (I16)
    now_us: i64,                  // planner stamp (D5, I5)
    pid_base: u64,                // meta.next_pid when planned (I18)
    kv_version_base: u64,         // meta.kv_version_next when planned (I18)
    commands: Vec<CommandRecord>, // one per planned command WITH effects, in plan order
    effects: Vec<Effect>,         // in apply order
}
struct CommandRecord {
    request_id: [u8; 16],
    first_effect: u32,
    effect_count: u32,            // ≥ 1: commands without effects are never logged (§5.4)
    outcome: Outcome,             // §5.4
}
```

- Encoding: the hand-rolled style of `native/record.rs` (`Writer`/`Reader`,
  length prefixes, xxh3 checksum of the body). No serde formats on disk or on
  the wire.
- Counters: the planner assigns partition ids as `pid_base + ordinal` and KV
  versions as `kv_version_base + ordinal of the versioned write in this
  entry`. Apply asserts both bases equal `meta` (I18), then advances them. The
  log index cannot be used: outcomes carrying versions are fixed at planning,
  before the library assigns the index, and blank and membership entries
  interleave.
- Size: the batcher cuts a cycle at `QUEEN_RAFT_BATCH_MAX_BYTES` (4 MiB) or
  `QUEEN_RAFT_BATCH_MAX_CMDS` (4096). A single command larger than the cap
  goes alone. The planner refuses (413) a command whose PLANNED size (effects,
  including 16 B of hashes per message) exceeds `QUEEN_RAFT_ENTRY_MAX_BYTES`
  (default 96 MiB). The HTTP body limit alone does not bound an entry: tiny
  messages carry more hash bytes than JSON. `QUEEN_MAX_BODY_BYTES` (64 MiB,
  main.rs ≈1419) stays the HTTP limit.
- Golden files: every kind has fixture bytes under `rsm/tests/golden/`; a
  change to a golden file is a format change and needs a kind version bump.

### 5.2 Effect catalogue

| kind | fields | touches (§6) | origin in SQL |
|---|---|---|---|
| QueueUpsert / QueueDelete | tenant, queue, cfg | queues | 012 `configure_queue_v1`; implicit creation in 003/004/005/007/025; 013 `delete_queue_v1` |
| GroupUpsert / GroupDelete | tenant, queue, group, meta | groups | 004 first contact; 014 |
| PartitionCreate | pid, uuid, tenant, queue, partition, created_at | partitions, partitions_by_key, queue_partitions | 003/004/005/007/025 implicit |
| PartitionDelete | pid | partitions*, segments, cursors, dedup, counters | 006 cleanup, 013, 031 |
| Append | pid, bucket, base_offset, count, created_at, hashes[16×count], blob | segments, seg files (positions local), dedup, counters, wakes | 003, 005 txn, 007 sink, 025 fire, 016 DLQ move |
| CursorSet / CursorDelete | pid, group, full cursor row | cursors, leases_by_worker, pending | 004 claim/seed, 005 ack/nack/renew/DLQ head, 007, 010 seek, 014 |
| DlqInsert / DlqDelete | dlq_id, pid, group, offset, message_id, txn, payload, error, retry_count, failed_at | dlq | 005 `log_dlq_head_v1`, 025 `log_timers_dlq_v1`, 016 |
| Watermark | pid, log_start, txns_start | partitions, segments (range delete), dedup, counters | 006 retention, txns purge, max-wait eviction |
| KvPut / KvDelete | tenant, ns, key, value, version, expires_at, created_at, updated_at | kv, kv_expiry, counters | 024 `kv_apply_v1`, 005 riders, 026 expiry |
| TimerUpsert / TimerDelete / TimerBackoff | tenant, queue, key, partition, deliver_at or visible_at, frame, message_id, attempts, producer_sub | timers, timers_due | 025 |
| StreamsQueryUpsert / StreamsStatePut / StreamsStateDelete | query and state rows | streams_queries, streams_state | 007, 008 |
| TraceAppend / TraceExpire | trace event / cutoff | traces, trace_names, trace_expiry | 010 `record_trace_v1`, D18 |
| FlagSet | key, value | flags | `system_state` via maintenance.rs |
| QuotaSet | kind (kv, ephemeral, streams), tenant, grant | quotas | `kv_quota`, `ephemeral_quota`, `queen_streams.quota` |
| EphemeralConfigSet / Delete | tenant, queue, cfg | eph_config | 030 |
| GarbageAdd | pids moved to the garbage set by a delete | garbage | 013, 014, 031 (see rules below) |
| DeleteChunk | garbage pid set, resume key | bounded deletes of pid-keyed data in key order | 013, 031 |
| RequestIdsExpire | cutoff now | request_ids | D6 |
| ClusterVersionSet | version | meta | D20 |
| MembershipNote | node_id, generation, disk_uuid, address | meta.identity | D21 (alongside the library's membership) |

Rules:

- Counters (D16) are updated by apply as a side effect of the kinds above, not by separate effects.
- Large deletes (a queue, a tenant, a consumer group over many partitions)
  keep the SQL's single-transaction semantics for names.
  - The delete command removes the name-keyed rows at once (`queues`,
    `groups`, `partitions_by_key`, `queue_partitions`) and moves the
    affected pids to the `garbage` set.
  - A leader loop proposes bounded `DeleteChunk` commands that delete the
    pid-keyed data (segments, cursors, DLQ, dedup, counters) of garbage pids.
  - Readers and planners ignore garbage pids. The name is reusable
    immediately: a push, configure or pop right after the delete recreates
    it, as `delete_queue_v1` allows (Kafka DeleteTopics→CreateTopics and the
    client suites rely on this).
  - A queue delete does not touch timers or KV, matching 013.
- Randomness is allowed in the planner (for example, the wildcard candidate
  choice that the SQL makes with `ORDER BY random()`), never in apply. Prefer
  a rotation seeded by the request id so the differential fuzzer stays reproducible.

### 5.3 Versioning

Every kind has `(kind_id, version)`. `kinds_version` in the entry header is
the maximum used. Apply stops (fatal, alert) on an unknown kind or version
instead of skipping it (I16). New kinds and new versions ship behind the
cluster version gate (§12.8).

### 5.4 Request ids and outcomes

- The receiver mints a request id (uuidv7 bytes) once per command and reuses
  it for every forwarding retry. SDK-level retries are new commands; pushes
  stay deduped by transactionId as today.
- The planner first looks the id up in committed `request_ids`, then in the
  entry in flight, then in the overlay. A hit in committed state returns the
  recorded outcome and plans nothing (I6); a hit in the entry in flight waits
  for that entry's apply and returns its outcome.
- Commands whose plan has no effects are never logged and their ids are not
  recorded: empty pops and long-poll re-checks, refusals, lost CAS
  (`applied:false`), `required` aborts, replays. Logging them would make cost
  follow the poll rate (against G-3). Such a command is answered at once if
  it read only committed state, or after the entry whose overlay it read has
  been applied.
- Apply inserts `request_id → (now, outcome)` for every logged command. A
  leader loop proposes `RequestIdsExpire` for ids older than the window.
- Outcomes are compact: push verdicts with offsets, pop claims (partition,
  offset range, lease id, worker, delivery attempt), ack results, transaction
  results, KV results with versions, timer op results, streams cycle results,
  admin results. Payload bytes are never stored in outcomes.

---

## 6. State

### 6.1 Replicated keyspaces

| keyspace | key | value | from Postgres |
|---|---|---|---|
| meta | fixed names | applied index/term, last_now_us, max_created_at_us, next_pid, kv_version_next, cluster_version, membership (incl. the initial voter set), digest chain | — |
| garbage | pid | deleted_at, scope | deletes in progress (§5.2 rules) |
| queues | (tenant, queue) | queue config (all `queen.queues` columns except `storage`, `replication_factor`) | `queues` |
| groups | (tenant, queue, group) | subscription mode, timestamp, conflation, seeded, registered_at | `consumer_groups_metadata` |
| partitions | pid | uuid, key, last_offset, log_start, txns_start, last_write_at, oldest_live_at, created_at, last_created_at | `log_partitions` |
| partitions_by_key | (tenant, queue, partition) | pid | unique key |
| queue_partitions | (tenant, queue, pid) | () | index for wildcard and admin scans |
| partition_files | (pid, file_id) | () | which sealed segment files hold data of this partition; written once per file seal (G0 amendment: the per-segment rows do NOT live in the store) |
| segments (NOT in the store, G0 amendment) | per sealed segment file, one immutable index file `<file>.qidx`: sorted (pid, base_offset) → end, count, created_at, position, binary-searched and memory-mapped; the active file's entries in RAM (bounded by one file's worth) | | `log_segments` minus the blob; the highest-rate keyspace, kept out of the store the way Kafka keeps `.index` files beside `.log` files |
| dedup | per S2 | per S2 | `log_txns` |
| cursors | (pid, group) | committed, batch_end, worker, lease_expires_at, lease_acquired_at, batch_retry_count, attempt_offset, attempt_count, total_consumed, lease_conflated (+ delivered set per O16) | `log_consumers` |
| leases_by_worker | (worker, pid, group) | lease_expires_at | derived index for renew (`log_renew_lease_v1`) |
| pending | (tenant, queue, group, pid) | ready_at | derived index maintained at apply: partitions with work for a group, so ready rings rebuild in O(pending). An `Append` touches one row per subscribed group of its queue (O(groups), not O(1)). |
| dlq | (tenant, queue, dlq_id) and (pid, group, offset) | DLQ row | `log_dlq` |
| kv | (tenant, ns, key) in byte order | value, version, expires_at, created_at, updated_at | `kv` |
| kv_expiry | (expires_at, tenant, ns, key) | () | prune index |
| quotas | (kind, tenant) | grant | `kv_quota`, `ephemeral_quota`, `queen_streams.quota` |
| timers | (tenant, queue, key) in byte order | partition, deliver_at, visible_at (backoff), frame / payload, payload_zstd flag, encrypted flag, txn, message_id, attempts, last_error, producer_sub, created_at, updated_at — every column peek and list return (025 ≈1436–1456) | `log_timers` |
| timers_due | (visible_at, tenant, queue, key) | () | due index |
| eph_config | (tenant, queue) | ephemeral queue config | `ephemeral_queues` |
| streams_queries | query_id | query row | `queen_streams.queries` |
| streams_state | (query_id, partition_id, key) | value, updated_at | `queen_streams.state` |
| flags | key | value | `system_state` |
| traces, trace_names, trace_expiry | (tenant, pid, txn, seq) / (tenant, name, created_at, …) / (created_at, …) | event / () / () | `message_traces`, `message_trace_names` |
| request_ids, request_expiry | id / (now, id) | (now, outcome) / () | — |
| counters | (scope, id, counter) | i64 | `stats`, `kv_usage`, retained bytes (D16) |

Deliberately not ported: `consumer_watermarks`, `hotlist_repairs`,
`maintenance_leases`, `native_txn_applied`, `native_members`,
`native_buckets`, `stats_history` (no writer), the sequence `kv_version_seq`
(versions come from log index and position, §8).

### 6.2 Node-local state

Not replicated, excluded from digests:

- `seg_loc`: (pid, base_offset) → (bucket, file_id, offset, len).
- `files`: (bucket, file_id) → length at the last durable point, sealed flag,
  live bytes, snapshot references.
- dedup blooms (if S2 picks them).
- `local.db`: the node-local metrics tables and `retention_history` (D17).

### 6.3 RAM (derived)

Rebuilt at open or at leadership start, bounded independently of retained volume (I8):

- ready rings per (tenant, queue, group), rebuilt from `pending`;
- lease and visibility deadline heaps (for pending gates and long-poll re-checks);
- hot caches: queue configs, group policies, partition rows of active partitions;
- bounded recent dedup cache (S2);
- timer wheel (leader only), from `timers_due` for the next horizon;
- long-poll notifiers.

### 6.4 Counters

Maintained at apply: per partition and queue — pushed, pending, completed,
failed, DLQ count, retained bytes, last push/pop times; per tenant — KV rows
and bytes, timers count, retained bytes (the proxy storage quota reads
`retainedBytes`); per consumer group — lag inputs. WP-2.6 maps every field
returned by `get_queue_v2`, `get_queue_detail_v2`, `get_queues_v2`,
`aggregate_*`, `get_status_queues_v2`, `log_queue_depth_v1`,
`log_queue_message_stats_v1` and the Prometheus function to a counter or a
bounded read, and proves equality with conformance tests.

---

## 7. Planning and apply

### 7.1 The cycle (leader)

1. Commands arrive on the planner channel from local handlers or from the
   forward server. Each carries: request id, deadline, tenant, producer
   subject, kind, and the receiver's pre-work (hashes, packed blobs,
   encrypted frames, message ids).
2. When no entry is in flight and commands are queued, the batcher drains up
   to the caps. It may hold up to `QUEEN_RAFT_BATCH_HOLD_MS` (2) only when
   the previous cycle was short and small, as fusion does today.
3. For each command, in order: request-id lookup (§5.4); otherwise plan
   against committed state + overlay, producing effects and an outcome, or a
   refusal outcome with no effects. The overlay records the logical results
   (allocated offsets, cursor rows, KV versions, dedup hashes, created
   partitions) for the commands after it in the same cycle.
4. Build the entry, stamp `now`, propose, and wait until apply on this node
   resolves it (I4). The forward server returns each command's outcome with
   the commit index.
5. Clear the overlay. Next cycle.

On a propose error:
- `NotLeader` (refused before append): fail waiters with `Retry` plus the
  leader hint; drop the overlay.
- `OutcomeUnknown` (leadership lost after the entry was appended): fail
  waiters with `Retry`, drop the overlay, stop planning (this node is no
  longer leader). The retry against the new leader finds the id if the entry
  committed, or plans anew; the new leader plans only after I13.
- `Timeout` while still leader: answer the waiters `Retry` but keep the entry
  IN FLIGHT. Plan nothing until the entry is applied locally or the role
  watch reports lost leadership; only then drop the overlay. A command whose
  request id is in the in-flight entry waits on that entry (§5.4). Dropping
  the overlay on a timeout would plan the next cycle without an entry that
  can still commit: duplicate offsets, double claims, two CAS winners.

### 7.2 Overlay rules

- Only the planner reads the overlay; apply never sees it.
- It lives for exactly one cycle.
- A duplicate verdict computed against the overlay is part of the entry and
  answered after commit (fixes pgless's duplicate verdict against an
  uncommitted append).
- A command without effects (§5.4) that read overlay state is answered after
  the entry that produced that state is applied; if the entry fails, it is
  answered `Retry` like the logged commands.

### 7.3 Leadership changes

- Becoming leader: wait until applied ≥ the first entry of the new term
  (I13). Rebuild the timer wheel, the retention and deletion cursors, and the
  pending gates. Start leader loops (§10.1). Accept commands.
- Losing leadership: stop the planner, fail waiters with `Retry`, stop leader
  loops, keep serving local reads and forwarding.

### 7.4 Clock

- The planner computes
  `now = max(SystemTime::now(), meta.last_now_us + 1, meta.max_created_at_us + 1)`;
  apply sets `meta.last_now_us = entry.now_us` and advances
  `meta.max_created_at_us` from `Append` effects. Segment stamps
  (`max(now, last_created_at + 1 µs)`) can run ahead of `now` by one µs per
  segment in a cycle; the third term keeps the next cycle's `now` above them.
- Effects contain absolute times computed by the planner.
- Stale local reads that interpret times (visibility, lease display) use the
  follower's own wall clock. Linearizable reads judge expiry (KV liveness,
  the facades' `qk:node:*` rows, the S3 sink lease) at the `now_us` returned
  with the read barrier (§9.4), never at the receiver's clock.
- Lease expiry is judged by the planner's `now`. A new leader whose clock
  lags keeps leases slightly longer; a new leader whose clock runs ahead
  shortens every remaining lease and TTL by the skew. Nodes report their wall
  clock in `Status`; the leader raises `queen_raft_clock_skew_ms` and an
  alarm above `QUEEN_RAFT_CLOCK_SKEW_ALARM_MS` (500).

### 7.5 Answer rules

- Answers are built from outcomes after leader apply (D7).
- A receiver that needs local state after a write (pop payloads, a read
  following the write) waits for local applied ≥ commit index, up to
  `QUEEN_RAFT_LOCAL_APPLY_WAIT_MS` (1000). Past that, it asks the leader with
  `PayloadRead` (§9.2).

---

## 8. Semantics port: the atomic units

The SQL is the spec. Port each function's behavior exactly, including the
edge cases its comments describe. Each row is one planner module and one
command kind.

| SQL (file: functions) | planner | effects | rules and pitfalls |
|---|---|---|---|
| 003: `log_push_one_v1`, `log_push_multi_v1`, `log_segment_at_v1` | push.rs | QueueUpsert (implicit), PartitionCreate, Append | Dedup verdict returns the ORIGINAL offset and writes nothing. Offsets are gapless. `created_at = max(now, last_created_at + 1 µs)` (PUSHSER: monotone per partition). The receiver packs one blob per partition assuming no duplicates; on a duplicate the planner repacks the survivors (fusion.rs repack path). Timer fires push with the dedup probe skipped (`p_verified`); keep that equivalence. |
| 004: `log_pop_v1`, `log_pop_specific_v1`, `log_pop_wildcard_wire_v1`, `log_pop_wildcard_bin_v1`, `log_pop_list_v1`, `log_pop_discover_wire_v1`, `log_has_pending_v1`, `log_partition_has_pending_v1`, `log_discover_has_pending_v1` | pop.rs | GroupUpsert (first contact), CursorSet, PartitionCreate (a wildcard pop creates a missing queue, 004 ≈1046) | window_buffer quiet debounce; delayed_processing; subscription seeding (stored policy wins, then pop-carried intent all/new/timestamp); conflation (newest visible frame, lease (committed, tail]); claim only without a live lease; attempt tracking (redelivery keeps the first offset); budget walk with head probe; autoAck advances committed. First contact bulk-seeds cursors in SQL. Here seeding is lazy and position-based, never time-based: apply records on the group row the (entry index, effect position) of its registration, and on every segment the (entry index, effect position) of its `Append`; within a planning cycle the plan order stands in for both. A partition without a cursor for the group is seeded on first use: `all` = before log_start; `new` = the last offset appended at a position lower than the registration's; `timestamp` = the first offset whose created_at ≥ ts (004 ≈311–316), clamped to log_start. WP-1.5 proves equivalence with conformance tests. `log_hotlist_reseed*_v1`: not ported. |
| 005: `log_ack_v1`, `log_ack_at_v1`, `log_ack_by_hash_v1`, `log_ack_multi_v1` | ack.rs | CursorSet, DlqInsert, Append (DLQ replay paths) | Positional ack (full-batch fast path when the acked hash set equals the delivered set). Hash ack: implicit ack; explicit signal never skipped; below-cursor honesty; one retry budget charged only by explicit `failed`; forced-DLQ keep-lease handoff; `noopHashes` / `staleHashes` vocabulary; `conflated` count. HAZARD: today an in-memory delivered set picks the fast or slow path, and the two leave different durable counters (retry budget, attempts, total_consumed); resolve per O16. |
| 005: `log_dlq_head_v1` | ack.rs | DlqInsert, CursorSet | File the head frame snapshot (the leader decompresses it from its committed files), advance past it, release the lease. |
| 005: `log_renew_lease_v1` | ack.rs | CursorSet | Renew every live lease of a worker (GREATEST, live only, MIN expiry). Iterate `leases_by_worker` in key order, never a hash map. |
| 005: `log_transaction_wire_v1` | txn.rs | any of the above + KvPut/KvDelete + Timer* | One command. Validate in the SQL lock order: kv (ns, key) → log_timers (queue, key) → queue creation by name → partitions by ascending id → pushes in input order → acks by ascending (partitionId, group) → leaf tables. The whole bundle is refused on: an ack partition of another tenant, a duplicate push, a push whose queue/partition does not resolve (005 ≈1394–1399), an unresolved ack hash, a failed ack, or a KV `required` precondition. A non-array `kv` or `timers` field raises 22023 (005 ≈1256–1263). KV runs in wire mode (`p_in_wire`): `getPrefix` is refused and limits drop to 64 ops / 256 keys (024 ≈618–621, ≈745–756). Timers never create queues (005 ≈1299–1301). KV and timer ops share one `now`. Two acks on the same (pid, group): tiebreak by input position (the SQL has none; document it). DLQ handoff for `dlq:true` acks runs after commit today (handlers/data.rs ≈5675); O7 decides whether it joins the entry. |
| 007: `log_streams_cycle_v1` | streams.rs | StreamsState*, Append, CursorSet | Each array element is an independent atomic unit (SQL savepoints): one CommandRecord per element. Tenant checks on partition and query. State ops stamped with `now`. Sink pushes use the full dedup probe; a duplicate is a soft verdict that writes nothing. When the element carries an ack, check the lease first, then: nack, full-batch reset, or partial ack walking real segment ranges and keeping the lease on the tail. An idle-flush element (`ack: null`) skips the lease check entirely (007 ≈421). Any failure discards that element's state ops and sink pushes. |
| 008: `streams_register_query_v1`; 009: `streams_state_get_v1` | streams.rs, reads.rs | StreamsQueryUpsert | Quota deny-by-default for streams (`queen_streams.quota`). |
| 010: `log_seek_consumer_group_v1`, `log_seek_one_v1`, `log_seek_partition_v1`, `log_delete_consumer_group_v1`, `record_trace_v1`; reads `get_consumer_groups_v4`, `get_consumer_group_details_v1`, `get_dlq_messages_v1`, `get_dlq_signatures_v1`, `get_lagging_partitions_v1`, `list_messages_v1` | admin.rs, reads.rs | CursorSet, CursorDelete, TraceAppend | `hotlist_repair_publish_v1` is not ported: apply updates `pending` and the rings on every node. |
| 011: `get_queue_v2`, `get_queue_detail_v2`, `log_queue_depth_v1`, `log_queue_message_stats_v1`, `log_queue_stats_all_v1`, `get_analytics_v1`, `get_partition_liveness_v1`, `log_oldest_pending_at_v1` | reads.rs | — | Over counters (D16). `log_refresh_all_stats_v1` is not ported. |
| 012: `configure_queue_v1` | admin.rs | QueueUpsert | Merge semantics (server/tests/configure_merge_semantics.rs) and `invalid` markers. The answer keeps echoing `storage: "segments"` (012 ≈269–271, master). The pgless-only `replication_factor` and `_defaultStorage` do not exist. |
| 013: `delete_queue_v1`; 014: `delete_consumer_group_v1`, `delete_consumer_group_for_queue_v1`, `update_consumer_group_subscription_v1` | admin.rs | QueueDelete + GroupDelete + GarbageAdd, then DeleteChunk; CursorDelete | Name-keyed rows go at once, pid-keyed data in chunks (§5.2 rules); the name is reusable immediately. A queue delete leaves timers and KV alone. |
| 015: `get_system_metrics_v1`; 019: worker metrics functions; 022: `get_retention_timeseries_v1` | reads.rs → local.db, plus replicated state | — | Node-local metrics plus gather (D17). In 019, `get_status_v3`, `get_system_overview_v3` and `get_workload_v1` also read replicated tables: those parts come from counters and committed state, not local.db. |
| 016: `delete_message_v1`, `log_dlq_move_v1`, `purge_dlq_v1` | admin.rs | DlqDelete, Append | A DLQ move is ONE command (push + delete) and therefore atomic; pgless did it as two appends. |
| 017: `get_available_trace_names_v1`, `get_message_traces_v1`, `get_traces_by_name_v1` | reads.rs | — | Plus trace expiry (D18). |
| 018: `aggregate_namespace_stats_v1`, `aggregate_system_stats_v2`, `aggregate_task_stats_v1`, `get_namespaces_v2`, `get_queues_v2`, `get_status_queues_v2`, `get_tasks_v2` | reads.rs | — | Over counters. |
| 020: partition counter triggers; 028: `log_refresh_retained_bytes_v1` | apply counters | — | D16. |
| 021: `get_postgres_stats_v1` | — | — | Removed in raft mode; the dashboard tab shows Raft status instead (§14.6). |
| 023: `get_prometheus_metrics_v1` | reads.rs | — | Counters + local metrics + `queen_raft_*`. |
| 024: `kv_apply_v1`, `kv_list_v1`, `kv_namespaces_v1`, helpers `kv_check_names_v1`, `kv_live_v1`, `kv_num_v1`, `kv_prefix_end_v1`, `kv_ver_v1` | kv.rs, reads.rs | KvPut, KvDelete | A first pass validates without writing: at most one write per key, budgets, exactly one of `ttlSeconds` or `forever`. Apply order is (namespace, key) in byte order, multi-key reads last, input order as tiebreak; results by input position. A lost precondition gives `applied:false`, except `required:true`, which aborts the whole command (and an enclosing transaction). One `now` per call; `expires_at = now + ttl`. Live = `expires_at` null or > now; an expired key reads as absent (version 0), so `expect:0` recreates it. `incr` keeps a live row's TTL. Versions: unique, never reused, and strictly increasing across the entry: `kv_version_base + ordinal` of each versioned write (§5.1, I18), so two commands writing one key in one entry get different versions and a stale `expect` cannot win (Kafka `qk:fence`, S3 sink lease). List: byte order, exclusive `after`, limit clamped to 1..1000 (default 100), 4 MiB byte budget, a `limit+1` probe for truncation. Reads inside a write command (the Kafka registry does a conditional put and a prefix read in one call) are evaluated against committed state + overlay. |
| 026: `kv_expire_step_v1`, `kv_usage_step_v1`; 027: `kv_quota_refresh_v1` | loops (leader) / counters | KvDelete chunks | Physical prune only; readers already treat expired keys as absent. |
| 025: `log_timers_apply_v1`, `log_timers_claim_v1`, `log_timers_fire_v1`, `log_timers_fail_v1`, `log_timers_dlq_v1`, `log_timers_due_v1`, `log_timers_peek_v1`, `log_timers_list_v1`, `log_timers_count_v1` | timers.rs + leader wheel | TimerUpsert/Delete/Backoff, Append, DlqInsert | Apply: one op per (queue, timerKey), applied in byte order. `deliver_at = now + delayMs` in integer µs (the SQL computes it in double precision; document any rounding difference). Reschedule resets attempts. Fire: one command for a set of due timers {keys with generation (message id/updated_at), packed segments per (tenant, queue, partition)}. Apply deletes the delivered rows and appends atomically, so claims and random claim tokens (025 ≈778) are not needed. Fail: backoff; attempts count only permanent errors. Dlq: when attempts ≥ min, DLQ row with group `__timer__`, offset -1, and the payload the leader decompressed. `too_late` for cancel/reschedule of a timer that is being fired: parity rule per O8. |
| 030: `eph_config_set_v1`, `eph_config_get_v1`, `eph_config_list_v1`, `eph_config_delete_v1`, `eph_quota_list_v1` | admin.rs, reads.rs | EphemeralConfigSet/Delete | The ephemeral engine itself stays in RAM (D24). |
| 031: `delete_tenant_data_v1` | admin.rs | name-keyed deletes + GarbageAdd, then DeleteChunk | Same rules as 013; also removes the tenant's KV, timers, streams, traces, quotas and ephemeral configs as 031 does. |
| 032: `log_fetch_bin_v1`, `log_fetch_changed_v1`; 033: `log_partitions_changed_v1` | reads.rs | — | Local reads. `safeTime` = last applied `now_us` − 1 µs, exact (replaces the `pg_stat_activity` trick in 033 ≈94–135). `lastWriteAt` is microsecond-exact; `since` is ISO 8601. The Kafka facade relies on contiguous offsets (protocols/queen-kafka/src/handlers/produce.rs ≈1009–1033). |
| 006: `log_retention_step_v1`, `log_retention_boundary_v1`, `log_retention_boundary_windowed_v1`, `log_txns_purge_step_v1`, `log_evict_max_wait_step_v1`, `log_partition_cleanup_step_v1`, `log_partition_dead_v1`, `log_watermark_walk_step_v1` | retention.rs (leader loop) | Watermark, PartitionDelete | Rule 1 retention_seconds (pending) and rule 2 completed_retention_seconds (min committed over ALL groups incl. `__QUEUE_MODE__`) apply only when `retention_enabled`; rule 3 max_wait_time eviction always runs. The txns (dedup) purge has its own cutoff `now − max(dedup_window, completed_retention, 900 s)` and outlives segment deletion (D10). For queues with a sink hold, rules 1–2 are floored at `max(tEnd − 60 s, now − retention_sink_hold_max_seconds)` from the S3 sink pointer in KV (retention.rs ≈130–142, ≈300–320). Empty-partition cleanup refuses while DLQ rows, streams state, live leases or in-window consumer activity exist, and deletes the partition's txns explicitly (006 ≈605–626, ≈641–645). The watermark walk (repair of `oldest_live_at`) is not needed: apply keeps it exact. File deletion is local (§11.7). `retention_history` rows are written only by the leader's loop into its local.db, so the D17 gather does not triple them. |
| 029, 034, 035, 036 | — | — | Not ported. |

---

## 9. Requests end to end

### 9.1 Receiver pipeline (every node)

1. Auth as today: stateless JWT (auth.rs), tenant header middleware
   (main.rs ≈1426–1431). The producer subject comes only from the validated token
   (handlers/data.rs ≈254).
2. Parse and limit checks (as the handlers do today).
3. Quota gates from the local applied `quotas` and `counters` (stale reads,
   like today's 30 s refresh in quota.rs).
4. Pre-work: encryption (encryption.rs), message ids (uuidv7), xxh3 hashes of
   transaction ids, pack + zstd per (tenant, queue, partition) (fusion.rs pack
   path), base64 decoding for timers.
5. Build a `Command` with a fresh request id and a deadline derived from the
   client request's own budget.
6. If this node is leader, send it to the local planner channel; otherwise forward it.

### 9.2 Forwarding protocol (port 6634)

- Handshake: mutual. Each side sends a fresh nonce; each proves knowledge of
  `QUEEN_RAFT_SECRET` with an HMAC over both nonces, the cluster_id and the
  (from, to) node ids. After the handshake, frames travel over TLS (rustls)
  or carry a per-frame MAC keyed from the handshake, so nothing can be
  injected mid-stream. A node without a matching IDENTITY rejects Raft RPCs
  (I9). Fail closed when the secret is unset in multi-node mode (pgless U19).
- Frames: `len u32 | xxh3 u64 | type u8 | body`. One reader task per
  connection forwards whole frames to a channel. Never read frames directly
  inside `select!` (cancellation safety; pgless lane bug).
- Connections: separate pools for Raft RPCs, forwarding and snapshots, so a
  snapshot stream cannot delay heartbeats.
- Requests:
  - `Command{…}` → `Outcome{request_id, commit_index, outcome}` | `Retry{leader_hint}` | `Refused{error}`
  - `PayloadRead{pid, ranges}` → segment bytes (fallback of §7.5)
  - `ReadBarrier{}` → `BarrierIndex{index, now_us}` (`now_us` stamped by the leader's planner clock, §7.4)
  - `Status{}` → `{node_id, generation, cluster_id, role, term, applied, commit, supported_kinds_version, disk_usage, wall_clock_us}`
- Deadlines: every request carries its remaining budget; no fixed 30 s peer
  deadline (pgless U17).

### 9.3 Hold during elections (D13)

- On `Retry`, connection errors or no known leader, the receiver retries
  with backoff (50 → 500 ms) until the earlier of the hold deadline and the
  request deadline, re-reading the leader hint from the replicator and the
  `Retry` frames.
- Long-poll pops count the hold inside their own wait budget.
- Past the hold: 503 `no_leader` with `Retry-After: 1`.

### 9.4 Reads

- Local stale reads (D15) come from store read transactions and immutable file bytes.
- Linearizable reads (KV gets and lists, and any endpoint a facade uses as a
  fence) take a read barrier:
  - the receiver asks the leader for a barrier;
  - the leader coalesces barrier requests into the library linearizable-read
    call already in flight and starts the next one as soon as it returns (no
    fixed window: S3 measured a 2 ms window 7–8× worse than coalescing,
    1.3–1.5 ms vs 0.16–0.23 ms p50; openraft sends one RPC round per call, so
    coalescing is the application's job) and answers `{index, now_us}`;
  - the receiver waits until local applied ≥ index, then reads locally,
    judging every expiry (KV `expires_at`, leases) at the barrier's `now_us`,
    never at its own clock.

### 9.5 Long-poll and wakes

- Pops park at the receiver on notifiers keyed by (tenant, queue) and
  (tenant, queue, partition).
- The apply thread notifies after applying `Append` effects and after cursor
  releases. Parked pops also re-check at the earliest lease or visibility
  deadline from the local heaps.
- On wake, the receiver checks the local pending gate and, if work may exist,
  sends a pop command.
- These wakes replace MESSAGE_AVAILABLE and HOTLIST_DIRTY mesh frames.

### 9.6 Endpoint classes

| class | routes (main.rs router) |
|---|---|
| write command | POST /api/v1/push; GET /api/v1/pop, GET /api/v1/pop/queue/:queue, /api/v1/pop/queue/:queue/partition/:partition (claims are writes even on GET); POST /api/v1/ack, /api/v1/ack/batch; POST /api/v1/lease/:leaseId/extend; POST /api/v1/transaction; POST /api/v1/configure; POST /api/v1/kv when the batch holds any write (its reads are evaluated inside the same command); PUT and DELETE /api/v1/kv/:ns/*key; POST /api/v1/timers; DELETE /api/v1/timers/:queue/*timerKey; POST /streams/v1/cycle, /streams/v1/queries (register); /api/v1/consumer-groups/* (delete, subscription, seek); /api/v1/messages/:partitionId/:transactionId (delete, retry); /api/v1/dlq/:id/replay; DELETE /api/v1/resources/queues/:queue; POST /api/v1/traces (`record_trace_v1`); DELETE /api/v1/dlq (`purge_dlq_v1`); POST /api/v1/system/maintenance*, POST /api/v1/system/ephemeral and POST /api/v1/system/kv-timers (persisted flags → `FlagSet`; handlers/maintenance.rs); DELETE /api/v1/resources/tenant and a quota grant endpoint (both added by WP-2.5; neither exists on master) |
| linearizable read | POST /api/v1/kv when the batch holds only reads; GET /api/v1/kv/:ns/*key; POST /api/v1/resources/kv/list; /api/v1/resources/kv/namespaces; POST /streams/v1/state/get |
| local stale read | /api/v1/fetch, /api/v1/partitions/changed, GET /api/v1/messages, GET /api/v1/dlq, GET /api/v1/traces*, GET /api/v1/timers/:queue and /api/v1/timers/:queue/*timerKey, GET /api/v1/resources/*, /api/v1/status*, GET /api/v1/consumer-groups (incl. lagging), /api/v1/analytics/* (except postgres-stats), GET /api/v1/system/*, /metrics, /metrics/prometheus |
| local only | /health, /auth/*, /api/v1/ephemeral/* (ephemeral engine), /api/v1/system/shared-state, static files |
| kept as a no-op | POST /api/v1/stats/refresh answers 200 (every SDK's Admin API calls it: client-go admin.go ≈380, client-js Admin.js ≈291, client-py admin.py ≈356, client-php Admin.php ≈190, client-rust admin.rs ≈337); counters are always current |
| removed in raft mode | /api/v1/analytics/postgres-stats (the pgless-only `/internal/api/native/*` and `/api/v1/native/placement` do not exist on master) |
| new | /api/v1/raft/status (admin), /api/v1/raft/members (admin), /api/v1/raft/replace (admin, §12.6), /internal/raft/step-down (local, preStop) |

WP-1.7 and WP-2.6 must confirm this table against the router and handlers
before implementing, and fix it here if it is wrong.

---

## 10. Loops

### 10.1 Leader-only

Each proposes bounded commands through the normal cycle. None runs before I13.

| loop | cadence | proposes |
|---|---|---|
| timer wheel | due-driven, ≤ 1 s sleep | Fire / Backoff / Dlq commands |
| retention (rules 1–3, txns purge, empty partitions) | 5 s | Watermark / PartitionDelete chunks |
| chunked deletes (queue, tenant, group) | continuous while the `garbage` set is not empty | DeleteChunk |
| KV expiry | 1 s | KvDelete chunks |
| trace expiry | 60 s | TraceExpire |
| request id expiry | 10 s | RequestIdsExpire |
| cluster version check | 30 s | ClusterVersionSet when all voters support it |
| digest compare | on reports | alert (§12.9) |

### 10.2 Every node

- metrics collector (syscollect.rs) → local.db
- counters exposure for `/metrics/prometheus`
- durable points (§11.4), snapshot trigger (§11.6)
- file GC and compaction slots on the apply thread (§11.7)
- deferred promotions for pending gates
- admission control retargeted to planner queue depth
- JWKS refresh, facade supervision
- ephemeral engine and its mesh

### 10.3 Not started in raft mode

reconcile.rs (60 s re-reads), hot-list reseed and wheel (main.rs ≈644–815),
stats.rs refresh loops, lease.rs maintenance leases, sweeper.rs SQL loops
(timer claim/fire, KV prune and usage), retention.rs SQL steps, file_buffer.rs
spool drain, quota.rs grant re-reads, mesh config and maintenance frames,
native placement and replica loops.

---

## 11. Storage on disk

### 11.1 Data dir layout

```
$QUEEN_RAFT_DIR/                       default /var/lib/queen/raft
  LOCK                                 flock held by the process; refuse to start if held (pgless U24)
  IDENTITY                             {cluster_id, node_id, generation, disk_uuid, created_at, format}; temp + fsync + rename + dir fsync
  log/                                 consensus log (§12.3)
  CURRENT                              names the live state directory; replaced atomically (I17)
  sm-<index>-<term>/                   live state directory (the first one is sm-0-0)
    store/                             ordered store (engine per S1)
    seg/b000 … b255/                   payload segment files, append-only
    dedup/                             blooms or index per S2
  snapshots/
    <index>-<term>/MANIFEST + store export + hard links to sealed segment files
  local.db                             node-local metrics (D17)
```

### 11.2 Segment files

- Frame: `len u32 | xxh3 u64 | pid u64 | base_offset u64 | count u32 | created_at i64 | hashes | blob`.
  Files are self-describing, so `seg_loc` can be rebuilt by scanning.
- The apply thread appends without fsync; files roll at
  `QUEEN_RAFT_SEGMENT_BYTES` (64 MiB); sealed files never change.
- Reads go by position and verify the frame checksum. pgless's `read_blob`
  did not verify.

### 11.3 Store

- G0 amendment: NOT one store transaction per applied entry. The apply thread
  keeps one write transaction open and commits it (non-durably) every
  `QUEEN_RAFT_STORE_COMMIT_MS` (4) or `QUEEN_RAFT_STORE_COMMIT_ENTRIES` (256),
  whichever comes first; the Raft log is the write-ahead log, so nothing is
  lost between commits, and S1 showed per-entry commits are what drove the
  write amplification (29× on redb). A durable commit at each durable point
  (§11.4). Readers see the last committed transaction; local reads that need
  the very latest entry wait for the next commit (≤ 4 ms).
- Read transactions for local reads, isolated from the writer.
- Iteration in key order for digests, chunked deletes and snapshot export.
- Node-local keyspaces in the same database or a second one, as S1 decides.

### 11.4 Durable point protocol (apply thread)

Every `QUEEN_RAFT_DURABLE_EVERY_MS` (1000) or `QUEEN_RAFT_DURABLE_EVERY_BYTES` (256 MiB):

1. fsync every segment file written since the last durable point (sealed files once) and their directories;
2. durable store commit containing `meta.applied_index/term/membership` and the file lengths (every apply commit already records the lengths of the files it touched, I11);
3. report the durable index to the replicator. It bounds recovery replay, not log size: with openraft the log is purged only behind snapshots (§11.6), and `LocalReplicator` may truncate its own log behind durable points.

Cost is proportional to what changed (I8).

### 11.5 Recovery (node start)

1. Take LOCK. Read IDENTITY. An empty dir takes the new-member flow; a
   mismatch with membership refuses to start as a voter (I9, §12.6). Read
   CURRENT; delete any `sm-*` directory it does not name (an interrupted
   install).
2. Open the store. Do not assume which commit it reopens at: after a process
   crash some engines reopen past the last durable commit, after a power
   loss they return to it. Use the applied index the reopened state reports.
3. Truncate every segment file to the length recorded in that reopened state
   and delete files it does not know. Verify the checksums of every frame the
   state references past the last durable point. A missing or corrupt
   referenced frame means store and files disagree: discard the state
   directory and install a snapshot from the leader (raft3); with a single
   voter (raft1, embedded) REPAIR instead: restore the node's own newest
   snapshot and re-apply the Raft log after it (G0: never a bare refusal
   when the log and a local snapshot exist) (I11).
4. Rebuild the RAM derived structures (§6.3).
5. Give the applied index to the replicator. It re-applies committed log
   entries after it (openraft: from `applied_state()`).
6. Join the cluster. `/health` reports ready once a leader is known and the
   apply lag is under the threshold.

A crash at any step must be safe to repeat.

### 11.6 Snapshots

- Build (library trigger, or manual by log bytes and time), in this order:
  1. seal the active segment files (roll every bucket to a new active file);
  2. take a durable point at index N that records the seals;
  3. write the store export at N (method per S1: a consistent file copy or a
     logical dump written to disk; never a read transaction held open for
     the transfer, which a restart would lose) into
     `snapshots/<N>-<term>/`;
  4. hard-link every sealed segment file into it;
  5. write `MANIFEST` (files with sizes and xxh3, node-local positions for
     those files per D8, meta, membership); fsync every file and the
     directory;
  6. only then return from `build_snapshot`. openraft counts a snapshot as
     done when the call returns, and calls `get_current_snapshot` at boot, so
     the snapshot must be complete on disk by then. A snapshot directory
     without a complete, fsynced MANIFEST is deleted at boot, and
     `get_current_snapshot` returns none, so the library rebuilds.
- Transfer: stream the manifest's files one by one with a per-file xxh3.
  Resumable per file. Throttled by `QUEEN_RAFT_SNAPSHOT_MBPS`.
- Install: import into a new directory `sm-<N>-<term>/`, verify every
  checksum, fsync, atomically replace `CURRENT` (write temp, fsync, rename,
  fsync the directory), delete the old state directory, then tell the
  library (I17).
- Retention: keep the newest snapshot and any snapshot still being sent;
  unlink older ones (hard links keep file data alive for readers).

### 11.7 File GC and compaction (local)

- Apply maintains live bytes per file. A file stays live while any segment
  in it is retained OR any `Append` in it lies inside its partition's txns
  window (above `txns_start`): the hash lists outlive retention (D10), for
  the dedup probe and ack-by-hash below the cursor. GC, compaction and
  snapshots keep those hash lists (compaction may copy only the hash lists of
  retention-deleted segments).
- A committed pop claim pins the files holding its claimed segments until
  the claim's payloads have been read or `QUEEN_RAFT_LOCAL_APPLY_WAIT_MS`
  plus the forward deadline has passed, so retention or a delete cannot
  unlink bytes between the claim's apply and the payload read (I4).
- A file with no live bytes, not referenced by any snapshot manifest, pin or
  open reader, is unlinked after the next durable point that no longer
  references it (I10).
- Compaction copies the live segments of a mostly-dead sealed file into a
  new local file on the apply thread between entries, then updates `seg_loc`.
  It never produces entries: pgless compaction emitted replicated `Append`
  records chosen from local file layout (hazard D-4).

### 11.8 Disk full

- The planner uses the HIGHEST disk usage any voter or learner reports in
  `Status`, not only its own: every node stores the full data, and a full
  follower would otherwise hit ENOSPC while the leader keeps accepting.
- Above `QUEEN_RAFT_DISK_HIGH_PCT` (85) the planner refuses pushes, timer
  schedules, KV puts and trace records with 507 `storage_full`. Acks, deletes,
  retention and admin commands still flow. An alert fires.
- Normal service resumes below `QUEEN_RAFT_DISK_LOW_PCT` (80).
- ENOSPC on the consensus log is fatal for that node (library `Fatal`). It
  stops, and the operator frees space or replaces the disk.

---

## 12. Consensus

### 12.1 The seam

```rust
#[async_trait]
pub trait Replicator: Send + Sync + 'static {
    /// Leader only. Resolves after the entry is committed AND applied on this node.
    async fn propose(&self, entry: Bytes, deadline: Instant) -> Result<AppliedAt, ProposeError>;
    fn role(&self) -> Role;                       // Leader{term} | Follower{leader} | Learner | Candidate | Stopped
    fn watch_role(&self) -> watch::Receiver<Role>;
    async fn read_barrier(&self, deadline: Instant) -> Result<u64, ProposeError>; // linearizable read index
    fn applied_index(&self) -> u64;
    async fn transfer_leadership(&self, to: Option<NodeId>, deadline: Instant) -> Result<(), ReplError>;
    async fn membership(&self) -> Membership;
    async fn change_membership(&self, change: MembershipChange, deadline: Instant) -> Result<(), ReplError>;
    fn metrics(&self) -> ReplMetrics;
}
pub enum ProposeError { NotLeader { hint: Option<NodeId> }, OutcomeUnknown, Timeout, Refused(String), Fatal(String) }

pub trait StateMachine: Send + 'static {       // implemented by the apply thread handle
    fn apply(&mut self, index: u64, term: u64, entry: &[u8]) -> ApplyResult;
    fn applied(&self) -> (u64, u64, Membership);
    fn durable_point(&mut self) -> io::Result<u64>;
    fn build_snapshot(&mut self) -> io::Result<SnapshotHandle>;
    fn install_snapshot(&mut self, staged: StagedSnapshot) -> io::Result<()>;
}
```

### 12.2 LocalReplicator (phases 1–2, embedded)

- Appends each entry to a local log (`len | xxh3 | index | term=1 | bytes`),
  one fsync per group, applies it, resolves the waiter.
- Recovery replays log entries after the store's durable index. The log is
  truncated behind durable points.
- No network, no elections. The rest of the RSM cannot tell it from Raft.

### 12.3 openraft adapter (default candidate; confirm at G3)

Re-verify every name against the pinned version: the API changed between
0.10 alphas (Appendix F).

- **TypeConfig:** `NodeId = u64`, `Node = {addr}`, app data = entry bytes,
  `SnapshotData` = an app-defined manifest handle (0.10 full-snapshot API).
- **Log storage:** the `raft-log` crate (by openraft's author, used by
  Databend) if S3 validates it, else the `LocalReplicator` log format.
  It must pass openraft's log-store conformance suite (`testing::log::Suite`)
  and honor the contract:
  - call the flush callback only after fsync;
  - `save_vote` durable before returning;
  - `truncate_after` exclusive, removing from the tail backward;
  - `purge` inclusive;
  - `get_log_state` returns the last purged id when the log is empty;
  - `save_committed` persisted.
- **State machine adapter:**
  - `apply` sends entries to the apply thread and awaits results;
  - `applied_state` reads meta;
  - `build_snapshot` = §11.6 build;
  - install through the full-snapshot API.
- **Config:**
  - heartbeat and election per D14;
  - pre-vote and quorum check (leader lease) on;
  - snapshot policy `Never` + manual trigger by log bytes/time;
  - `max_in_snapshot_log_to_keep` sized to cover a follower restart;
  - `purge_batch_size` > 1;
  - `enable_leader_restore = false` (its docs call it unsafe with a volatile state machine).
- **Client writes:** one `client_write` per cycle (D4). Map `ForwardToLeader`
  before append → `NotLeader`; `LogEntryDiscarded` (or alpha.34's
  `ForwardToLeader` after append) → `OutcomeUnknown`. openraft has no
  internal timeout on client writes: our deadline wrapper maps to `Timeout`,
  which keeps the entry in flight (§7.1, I3). The pending future resolves
  when the entry is applied locally, possibly under the next leader.
- **Reads:** `ensure_linearizable` with ReadIndex, batched by §9.4. Do not
  use LeaseRead: unsafe across quick restarts in alpha.34.
- **Leadership transfer:** `trigger().transfer_leader`, plus the network
  `transfer_leader`. A failed transfer leaves no leader until an election
  (GH#2088); the caller waits for the role change with a deadline and moves
  on either way.
- **Membership:** `add_learner` (non-blocking, then wait for catch-up via
  metrics) and `change_membership`. Removed nodes are shut down by us.
- **Metrics:** `raft.metrics()` → `/health`, `/metrics/prometheus`, dashboard.
- **Tests to add** (openraft's do not model these): lost fsyncs, wiped disks,
  clock jumps, slow disks (§13.6).

### 12.4 raft-rs adapter (fallback)

- A `RawNode` ready loop on a dedicated thread: persist entries and hard
  state (fsync) BEFORE sending messages; apply committed entries in order;
  snapshots through `Storage::snapshot`.
- `read_index` with context ids for barriers; `transfer_leader`;
  `pre_vote = true`, `check_quorum = true`, `read_only_option = Safe`.
- Log storage: `raft-engine` (TiKV) or the local format.
- Build: raft-proto generates protobuf code at build time. It needs a
  matching system `protoc` (`PROTOC` env in the Dockerfile), or `protobuf-src`
  compiles one from C++ (slow; conflicts with C-2).
- The driver loop is ours: budget it as the largest WP of phase 3.

### 12.5 Transport

§9.2 framing and handshake apply to Raft traffic too:

- Raft RPCs (append/stream, vote, pre-vote, transfer_leader) on their own
  connections; heartbeats must never wait behind forwarding or snapshots.
- Bounded per-connection queues with backpressure.
- Deadlines per call derived from the library's TTLs (`soft_ttl` / `hard_ttl` in openraft 0.10).

### 12.6 Membership, bootstrap, identity, replacement

- **Node ids:** `node_id = ordinal * 1000 + generation`; generation starts
  at 1 and increases when a pod's disk is replaced. Addresses:
  `<pod>.<headless service>:6634`. Kafka node ids (ordinal+1) are unrelated
  and unchanged.
- **Bootstrap** of a brand-new cluster happens only when ALL configured peers
  (`QUEEN_RAFT_PEERS`) answer `Status` with an empty dir and no cluster_id:
  - ordinal 0 writes and fsyncs its IDENTITY (with the new cluster_id)
    BEFORE calling `initialize`, so a crash cannot leave an initialized log
    with no identity;
  - it initializes a one-voter cluster; its first entry records the INITIAL
    VOTER SET (all configured ordinals) in `meta`;
  - it adds the other ordinals of the initial voter set as learners and
    promotes them once caught up, without admin approval (they are members
    by the first entry, not scale-ups);
  - the planner refuses client commands (503 `bootstrapping`) until the
    voter count reaches the configured size, so no write is acknowledged
    while it is held on a single disk;
  - a stale bootstrap flag can never create a second cluster: if any peer
    reports a cluster_id, bootstrap is refused.
- **Join:** a node with an empty dir whose ordinal is not in membership
  (scale-up) is added as a learner by the leader after admin approval, and
  promoted after catch-up.
- **Replacement:** a node with an empty dir whose ordinal IS in membership
  under an older generation never votes and waits for the operator. Operator
  flow: `POST /api/v1/raft/replace {ordinal}` →
  1. verify the old member is unreachable or has reported an empty disk;
  2. `change_membership` removing the old node id;
  3. add `ordinal*1000 + generation+1` as learner;
  4. install a snapshot;
  5. promote.

  Never reuse a node id with an empty disk (I9). Replacement needs a live
  quorum (2 of 3). Losing two disks at once is not a replacement but a
  restore from backup (WP-5.2) into a new cluster id. Runbook in WP-5.5.
- **Scale-down:** leadership transfer if needed, `change_membership` removing
  the node, then stop it.

### 12.7 Leadership transfer on shutdown

preStop calls `POST /internal/raft/step-down` (localhost only). If this node
is leader, transfer to the most up-to-date follower and wait for the role
change up to 3 s. Then stop accepting new commands (the receiver forwards
them), drain in-flight work, take a durable point, and exit.
`terminationGracePeriodSeconds` stays 90. A PodDisruptionBudget keeps two
pods available.

### 12.8 Cluster version and rolling upgrades

- Each binary declares `SUPPORTED_KINDS_VERSION` and reports it in `Status`.
- The leader loop proposes `ClusterVersionSet(v)` once every voter AND every
  learner has reported ≥ v for `QUEEN_RAFT_VERSION_SETTLE_S` (300).
- The planner never emits kinds above the cluster version.
- Rollback to the previous binary is possible until the version is bumped.
- WP-5.1 tests N → N+1 under load, and rollback before the bump.

### 12.9 Divergence detection

- **Chain digest:** apply keeps `digest = xxh3_128(digest ‖ entry bytes)` in
  meta. Nodes report (index, digest) every `QUEEN_RAFT_DIGEST_EVERY`
  (100 000) entries. A mismatch is settled by majority: with three nodes the
  divergent one may be the leader itself. The node in the minority stops its
  Raft instance (no votes, no appends acknowledged, no apply), so it can
  never become or stay leader with a divergent state, and raises an alert;
  it is then replaced (§12.6). Without a majority (all three differ), the
  whole cluster stops accepting commands and pages.
- **State digest:** a hash of every replicated keyspace iterated in key order
  at a durable point (node-local keyspaces excluded). Computed on demand,
  compared across nodes in tests and nightly in production. This catches
  apply nondeterminism that the chain digest cannot see.

---

## 13. Verification

### 13.1 Unit and property tests

- planner functions (ported native tests plus new ones per §8 row)
- codec golden files
- apply idempotence across the recovery boundary (§11.5)
- determinism (I2): two states, different hash seeds, same entries → equal state digests
- overlay equivalence: N commands planned in one cycle vs one per cycle → identical committed state and outcomes (modulo `now`)
- request-id replay (I6)

### 13.2 Dual-backend conformance

- Build `server/tests/conformance/`: scenarios over a `Backend` trait
  implemented by the embedded broker in postgres mode (throwaway Postgres on
  :5479) and in raft mode (`LocalReplicator`, temp dir). Every scenario runs
  on both and compares answers and final views.
- Port into it: configure_merge_semantics, conflation_semantics,
  delivery_attempt_semantics, dlq_move_semantics, ephemeral_semantics,
  kv_semantics, kv_console_list, kv_console_routes, kv_handler_isolation,
  kv_quota_refresh, kv_sql_helpers (behavioral parts), partitions_changed,
  log_fetch_timezone, queue_list_stats, retention_sink_hold,
  retention_work_list, streams_tenant_isolation, tenant_wipe,
  timer_fire_delivery, timers_count, timers_fault_injection,
  timers_semantics, timers_tenant_isolation, kafka_group_mirror, embedded_smoke.
- Postgres-internal tests stay postgres-only: hotlist_repairs,
  hotlist_reseed_window, schema_apply_boot_under_load, kv_vacuum_gate,
  kv_collation_42p22, pg_tls_root_cert, procedures_timezone,
  retention_lock_scope, kv_timers_boot_idempotence, kv_timers_source_pins.

### 13.3 Client suites (test/run.sh)

- **New topologies:**
  - `raft1`: one broker, `QUEEN_STORAGE=raft`, no Postgres container;
  - `raft3`: three brokers. The runner targets a follower; a variant uses all three URLs where the SDK accepts a list.
  - `raft3-tenanted` for the tenancy suite.
- **Parity gate:** `RAFT PARITY` compares single↔raft1 and ha↔raft3. Model it on master's TENANCY PARITY gate in test/run.sh; pgless's NATIVE PARITY gate (`git show 6e96e228:test/run.sh`) is the closest precedent but does not exist on master.
- **Suites:** js go py cli cpp laravel rust-client, http, conflation, s3sink,
  tenancy; Kafka and SQS facade compat lanes (protocols/*/compat).
- The `mesh` suite does not apply. Replace it with a `raft` suite:
  - leader known;
  - a push through a follower is readable on all nodes;
  - a long-poll parked on a follower wakes;
  - a leader kill during a suite run keeps the suite green.

### 13.4 Differential fuzzer (test/raft/difffuzz, Go)

- **Operations:** seeded random sequences of
  - push with deliberate duplicates;
  - pop: pinned, wildcard, discovery, auto and manual ack;
  - ack ok/failed/dlq, by hash and by position; renew; nack;
  - transaction bundles with KV riders, timers and deliberate failures;
  - KV CAS, long TTLs, incr, prefix lists;
  - timer schedule and cancel with long delays (avoid time races);
  - configure changes, seek, group delete, DLQ move and purge.
- **Comparison:** the same sequence against a postgres broker (`single`) and
  a raft broker (`raft1`). Normalize ids and timestamps, then compare every
  response and the final views (depth, groups, messages list, KV list,
  timers list, DLQ list).
- **Runs:** CI nightly with N seeds; a failing seed becomes a regression fixture.

### 13.5 Crash points and the crash matrix

- **Points** (rsm/faults.rs, `QUEEN_TEST_FAULTS="point[:nth],…"`, off unless set):
  - batching and planning: `batcher.drained`, `planner.planned`, `propose.sent`;
  - log and apply: `log.appended`, `log.flushed`, `commit.before_apply`, `apply.mid_entry`, `apply.segment_written`, `apply.store_committed`;
  - durable points: `durable.files_synced`, `durable.store_committed`;
  - snapshots: `snapshot.sealed`, `snapshot.built`, `snapshot.file_sent`, `snapshot.install_staged`, `snapshot.install_activated`;
  - GC and compaction: `gc.before_unlink`, `gc.after_unlink`, `compaction.copied`, `compaction.loc_committed`;
  - leadership, identity, membership: `transfer.started`, `identity.written`, `membership.learner_added`, `membership.changed`.
- **Harness** (test/raft/crash): for raft1 and raft3, arm each point, drive a scenario, restart, then check:
  - every answered write delivered;
  - unanswered writes at most once, and their retries exactly once;
  - transaction riders all or nothing;
  - one leader, equal applied indexes, equal state digests;
  - no error lines except the fault's own.

### 13.6 Linux VM

- **Host:** `root@164.90.215.224`, Ubuntu 24.04, 8 vCPU, 15 GB, ext4,
  fdatasync ≈1 ms that scales in parallel. Use only `/root` and the local
  Postgres. Ask Alice before long runs.
- **Build:** `. ~/.cargo/env && cd /root/queen/server && cargo build --release --bin queen`.
  Sources are rsynced without `target/`.
- **Regimes:** goload openloop A20k/A50k/B1/C1000/D1 (Appendix G) for
  postgres vs raft1 vs raft3. Three brokers on one VM share disk and use
  loopback, so those numbers serve correctness and relative comparison. Final
  numbers need three VMs (O13).
- **Flatness test (G-3, I8):**
  1. Preload 300 million messages over 1 million partitions with a 1-hour
     dedup window and retention off (a bulk loader in test/raft/flatness).
  2. Run A20k and C1000 for 60 minutes each.
  3. Compare against the same regimes on an empty store: p50, p99, p999,
     ack round trip, CPU cores, disk MB/s, durable point and snapshot times.
  4. Acceptance: every metric within ±15% of the empty-store run, and RSS
     flat (±5% after warm-up) across the 60 minutes.
- **Kill tests under load** (A20k, 64 partitions; test/raft/kill):
  - kill times randomized across at least 3 durable-point intervals and 2 snapshot builds;
  - scenarios: kill -9 the leader; kill -9 a follower; SIGSTOP the leader
    for 5 s; partition the leader (iptables drop on 6634); wipe a follower's
    disk and run the replacement flow; clock jump −10 s and +10 s on the
    leader (leases and TTLs must follow §7.4, the skew alarm must fire); slow
    disk on a follower (dm-delay 50 ms); ENOSPC on a follower (small loop
    device); rolling restart with leadership transfer;
  - each scenario ≥ 5 runs, and the checkers must pass;
  - record time to a new leader and client-visible unavailability.
- **Lesson:** pgless's kill tests killed at 25 s, before the first 30 s
  checkpoint, and never exercised checkpoints. Kill schedules must cover
  every periodic boundary.

### 13.7 Checkers (test/raft/checker, Go)

- every acknowledged push delivered at least once (transaction id + payload hash);
- delivered offsets monotone per partition within a lease;
- no payload mismatch;
- transaction bundles all or nothing;
- KV compare-and-set histories linearizable (porcupine model);
- each timer schedule generation fired exactly once;
- streams state equal to the processed message count;
- nothing delivered after its partition was deleted;
- DLQ rows carry the payload that was pushed.

### 13.8 Adversarial reviews

- **When:** before committing a ⚠ WP, and at each gate.
- **Lenses:** acknowledged data, crash recovery at every crash point,
  determinism, liveness (blocking, deadlines), identity and membership, edges
  (clients, proxy, facades).
- **Refuters:** each finding goes to two refuters told to answer "refuted"
  when in doubt.
- **Record:** findings go to `RAFT_STATUS.md` with ids (R-xx) and verdicts.
  Confirmed findings are fixed, or accepted in writing by Alice, before the gate.

---

## 14. Edges: what changes around the broker

### 14.1 Health and metrics

- **`/health`:**
  - keeps today's fields (`status`, `version`, `engine`, …): the dashboard
    (app/src/components/Sidebar.vue ≈128) and the CLI `ping`
    (clients/client-cli/cmd/ping.go ≈37) require `status` to be `healthy`;
  - adds `raft: {role, leader, term, applied, commit, lag}`;
  - 200 with `status: "healthy"` when a leader is known and the lag is under
    `QUEEN_RAFT_READY_LAG_MS` (2000); otherwise 503 with `status: "settling"`;
  - liveness stays TCP.
- **New `queen_raft_*` metrics:**
  - role and term;
  - commit and applied indexes;
  - log bytes;
  - snapshot index and age;
  - per-follower match lag;
  - latency histograms: propose, forward, durable point;
  - batch size and in-flight count;
  - hold count and hold time;
  - request-id hits;
  - digest mismatches;
  - disk usage;
  - apply errors.

### 14.2 Helm (helm_v1/broker — gitignored; changes land in that tree)

- **Pods:** `replicas: 3` everywhere (stage and the defaults run 2 today).
- **Storage:** PVC `data` sized for the retained data plus headroom, separate from `spool`.
- **Availability:** PodDisruptionBudget `minAvailable: 2`; required pod
  anti-affinity by hostname, preferred by zone (today's spread is a soft
  `ScheduleAnyway`).
- **Ports:** add 6634 to the headless Service (which already publishes not-ready addresses).
- **Env:** `QUEEN_STORAGE=raft`, `QUEEN_RAFT_*` (Appendix H) and
  `QUEEN_RAFT_SECRET` from a secret; drop `PG_*`, `DB_POOL_SIZE` and the
  Postgres egress label in raft mode. `QUEEN_MESH_PEERS` stays for ephemeral only.
- **Lifecycle:** preStop runs step-down (§12.7) then sleeps;
  `podManagementPolicy: Parallel` is fine because bootstrap is ordinal-0-driven with the all-empty check.
- **Probes:** readiness keeps `/health` (new meaning). The startup probe
  becomes TCP: today it polls `/health` for up to ≈20 minutes
  (templates/statefulset.yaml ≈247–253), which would kill a replacement node
  in the middle of installing a large snapshot.

### 14.3 SDKs

Nothing is required if brokers forward and hold. Recommended fixes (WP-5.4), which matter more once elections exist:

- CLI push sends no transactionId, so its retries duplicate (clients/client-cli/cmd/push.go ≈225–238).
- JS consumers rethrow non-403 errors and the worker dies (clients/client-js/client-v2/consumer/ConsumerManager.js ≈296–304); Python 503 kills the worker (clients/client-py/queen/consumer/consumer_manager.py ≈384); PHP `consume` ends (clients/client-php/src/Consumer/ConsumerManager.php ≈442). Retry with backoff instead.
- PHP and C++ treat a 3xx as success (client-php HttpClient.php ≈227, 288–290; client-cpp queen_client.hpp ≈1740–1766); C++ swallows errors (≈4545–4553).
- Go disables failover whenever URLs are passed (clients/client-go/queen.go ≈56–58, load_balancer.go ≈258–260).
- A pop whose response is lost hides its messages for the lease time (300 s default). That is today's behavior too; document it.

### 14.4 Facades and the S3 sink

- Unchanged: loopback to their own broker.
- **Kafka:**
  - The node registry and group fences use KV conditional writes: commands, therefore linearizable.
  - Registry prefix reads use the read barrier (D15).
  - EndTxn is one `/api/v1/transaction`.
  - Produce relies on contiguous offsets.
- **SQS:** registry and delete-sets use expect/required writes; DLQ redrive and SNS fan-out need one atomic transaction (§8 transaction wire).
- **S3 sink:** `safeTime` from §8; the KV lease with expect + required; TTL on the leader's clock.
- Every facade lane must pass on raft1 and raft3.

### 14.5 Proxy

- Unchanged: routes to the cell Service, keeps `queen_proxy`, polls
  `resources/queues?stats=cached` every 60 s for the storage quota
  (`retainedBytes` comes from counters).
- Runbooks that run broker SQL on a cell need admin endpoints in raft mode:
  - `queen.delete_tenant_data_v1`: no endpoint exists on master. WP-2.5
    adds DELETE /api/v1/resources/tenant (admin, path-exact) and ports the
    proxy block pgless added (6e96e228 proxy/src/routes.rs ≈145), because on
    master the proxy classifies every method under `/api/v1/resources` as a
    read (proxy/src/routes.rs ≈525–533);
  - `queen.kv_quota` grants (server/src/quota.rs ≈162–178): new admin endpoint in WP-2.5, also blocked at the proxy.

### 14.6 Dashboard

- The postgres-stats tab becomes a Raft tab: members, roles, lag, log and snapshot sizes, digest status, disk usage.
- Analytics endpoints are re-sourced per §8 (counters, local metrics gathered from every node).
- DB pool gauges and the `database`/`dbHealthy` fields are removed in raft mode.
- Worker, system, lag and parked-replica series are gathered from all nodes.
- Retention history is local.

### 14.7 Embedded mode

- `QUEEN_STORAGE=raft` with `LocalReplicator` and a required data dir.
- The same state machine and planner as the server. Keep server/src/main.rs
  and server/src/embedded/boot.rs in sync.
- Embedded fleets over one Postgres have no raft-mode equivalent; document it.

### 14.8 Ephemeral and mesh

- The ephemeral engine is unchanged.
- Placement by hash over live members; liveness from mesh heartbeats.
- In raft mode the mesh carries only ephemeral frames (EPH_ADMIN and heartbeats).
- Grants and configs are read from local applied state instead of Postgres (main.rs ≈522–539).

### 14.9 Docs and CI

- **webdoc:** the deploy page (webdoc/src/content/docs/deploy/index.mdx says brokers are stateless) gets a raft-mode section; operations runbooks; the config reference generator gets the knobs.
- **CHANGELOG.**
- **CI:**
  - `raft1` lane in the default matrix;
  - `raft3`, difffuzz and the crash matrix nightly;
  - the upgrade test weekly.
- One release image, as today.

---

## 15. Phases, work packages, gates

### 15.0 RAFT_STATUS.md shape

```
# RAFT_STATUS.md
## Gates        (G0..G5: date, decision, evidence links)
## Decisions    (D-id, change, date, reason)
## Work packages (WP-id: state, commit, evidence: commands + summarized output + numbers)
## Findings     (R-id: source, severity, verdict, fix commit)
## Measurements (regime tables with date, commit, host)
## Open questions for Alice
```

### Phase 0 — Groundwork and spikes (no product code)

| WP | goal | deliverables | done when |
|---|---|---|---|
| WP-0.1 | Branch and record | branch `raft` from `fc71b65b` in the checkout (done 2026-09-17); PLAN_RAFT.md committed on it; create RAFT_STATUS.md; baseline `cargo build --release --bin queen` and `cargo test --lib` counts | status file has the baseline |
| WP-0.2 | Baselines on the VM | postgres class at A20k/A50k/B1/C1000/D1 + a push-only fat-batch run + CPU/RSS; native single-node numbers from PLAN_NATIVE_LOG.md §8.1 for reference | table in RAFT_STATUS.md |
| WP-0.3 ⚠ | Spike S1: store engine | candidates redb, fjall, heed (LMDB); a harness replaying a synthetic apply stream (segment index writes per push batch, cursor updates per pop/ack, a KV mix up to 40k ops/s, timers, request ids) at 20k/50k/100k msg/s for 2 h on the VM. Measure: write amplification, non-durable and durable commit p99, RSS, file growth and reclamation, ordered iteration speed, consistent export for snapshots (cost, and whether it can be incremental, I8) and the writer pause it needs, build dependencies (no cmake), MSRV 1.88, license, maintenance activity. PASS/FAIL (mandatory, not "if feasible"): after 100 kill -9 runs during non-durable commits AND after dropped unflushed writes (dm-flakey or an equivalent fault-injecting block device), the applied index the reopened store reports and the segment bytes it references agree (I11); record for each engine whether it reopens past its last durable commit after a process crash | decision memo with a recommendation |
| WP-0.4 ⚠ | Spike S2: dedup | options (a) a store index per message; (b) Append hash lists + per-file blooms + bounded recent cache, in the S1 harness. Measure probe p99 at 50k msg/s with a 1 h window, RAM, disk, rebuild time after restart, exactness with duplicates injected at random ages. Include the case where retention has deleted a segment whose hashes are still inside the txns window: a re-push must still be a duplicate, and an ack-by-hash below the cursor must still resolve (noop/stale, as postgres answers) | memo |
| WP-0.5 ⚠ | Spike S3: consensus | openraft (latest 0.10 release, or the pin that contains `LogEntryDiscarded`), 3 nodes over real TCP on the VM. Measure: commit latency with 64 KiB entries at 200/500/1000 entries/s; leader kill recovery time; `transfer_leader` success and failure behavior; batched `ensure_linearizable` cost; streaming a 10 GiB manifest snapshot (throughput, resume); the wiped-voter procedure, and what happens when done wrong; restart with `enable_leader_restore=false`. Also: re-read open issues GH#2080, #2088, #2095; estimate the raft-rs driver effort from its examples | memo with library and exact version pin |
| WP-0.6 | Spike S4: transport | framed TCP vs HTTP/1.1 binary bodies for forwarding at 20k/50k msg/s across processes: latency and CPU | memo |
| WP-0.7 | Harness skeletons | `test/raft/{difffuzz,checker,crash,kill,flatness}/` with READMEs and minimal runnable programs | each skeleton builds and runs |

**G0 (Alice):** ratify D1–D26 as amended by the spikes; choose the store, the dedup design and the transport; set the library direction (final at G3); decide O7, O8, O15 and O16 (phases 1–2 apply them); set performance targets (O14); approve phase 1.

### Phase 1 — Single node, message path (LocalReplicator)

| WP | goal | files | done when |
|---|---|---|---|
| WP-1.1 ⚠ | Entry and effect codec, versioning, golden files | rsm/effect.rs, rsm/entry.rs, rsm/tests/golden | round-trip and golden tests; fuzzed decoder never panics |
| WP-1.2 ⚠ | Store adapter and message-path keyspaces (queues, groups, partitions + indexes, segments, dedup, cursors, leases_by_worker, pending, dlq, request_ids, counters, meta, seg_loc, files) | rsm/store/, rsm/state/ | CRUD, range scans and durable-point tests; read isolation test |
| WP-1.3 ⚠ | Segment files | rsm/segments/ | append/roll/seal/read with checksum; truncate-to-length recovery tests |
| WP-1.4 ⚠ | Apply thread: message-path effects, counters, wakes, waiters, durable points (§11.4), recovery (§11.5), file GC (§11.7, no compaction yet) | rsm/apply.rs | determinism property (I2); crash-boundary idempotence tests |
| WP-1.5 | Planner: push, pop (all variants, with index-based lazy seeding), ack (all variants), renew, DLQ head, nack; overlay; apply the O16 decision taken at G0 and prove it with conformance tests | rsm/planner/push.rs, pop.rs, ack.rs | ported native tests + new rows of §8 green |
| WP-1.6 ⚠ | LocalReplicator + batcher cycle (D4–D7) | rsm/replicator/local.rs, rsm/batcher.rs | cycle tests; request-id replay; one in flight |
| WP-1.7 | Handler seam: `QUEEN_STORAGE` in config.rs; `StorageMode` in AppState; message-path handlers (handlers/data.rs push/pop/ack/lease extend) routed to rsm; receiver pre-work from fusion paths; no Postgres pool in raft mode; embedded boot in sync | config.rs, main.rs, handlers/, embedded/boot.rs | broker boots with no Postgres; smoke push/pop/ack |
| WP-1.8 ⚠ | Crash points + raft1 crash matrix (message path). Start from the untracked pgless `server/src/native/faults.rs` (§3.5) | rsm/faults.rs, test/raft/crash | all points pass |
| WP-1.9 | `raft1` topology in test/run.sh + RAFT PARITY gate | test/run.sh, test/compose | message-path tests of every client suite identical to `single` |
| WP-1.10 | Differential fuzzer v1 + checker v1 (message path) | test/raft/difffuzz, test/raft/checker | 10 000 seeds clean |
| WP-1.11 | VM: regimes on raft1 vs baselines; flatness test (message path) | test/raft/flatness | numbers recorded; thresholds of §13.6 met |

**G1 (Alice):** message-path parity, crash matrix, differential and flatness results, raft1 performance vs postgres against the O14 targets.

### Phase 2 — Single node, the whole feature set

WPs 2.2–2.9 can run in parallel after G1; WP-2.1 (the transaction wire)
embeds KV and timer planning, so it starts after WP-2.2 and WP-2.3. Each WP
owns its planner module; the integrator owns config.rs, main.rs,
handlers/mod.rs and AppState.

| WP | goal | done when |
|---|---|---|
| WP-2.1 ⚠ | Transaction wire (planner/txn.rs), O7 applied | conformance + transaction-semantics tests green |
| WP-2.2 | KV: planner/kv.rs, linearizable reads (local barrier = applied index in single node), versions, TTL, prefix lists, `required`, quota counters, expiry loop | kv_* conformance green; http suite green |
| WP-2.3 ⚠ | Timers: planner/timers.rs, leader wheel, fire/backoff/DLQ, O8 applied | timers_* conformance green; timer_fire_delivery green |
| WP-2.4 | Streams: cycle, register, state get | streams conformance + JS streams tests green |
| WP-2.5 | Admin: configure merge, delete queue (name-keyed rows at once, pid data in chunks), consumer groups (delete, subscription, seek), messages (delete, retry, DLQ move/purge), flags and maintenance mode (O10) incl. the ephemeral and kv-timers switches, tenant purge, ephemeral config. New endpoints (none exist on master): DELETE /api/v1/resources/tenant (admin, path-exact) and a quota grant endpoint, both blocked at the proxy (port pgless's block, 6e96e228 proxy/src/routes.rs ≈145) | conformance green; proxy route test |
| WP-2.6 | Reads: fetch, partitions/changed (safeTime), messages list/detail, DLQ lists/signatures, consumer group views, lagging, traces (record, reads, expiry), status/queues/namespaces/tasks, analytics over counters, Prometheus; §9.6 table confirmed | conformance + dashboard endpoint tests green |
| WP-2.7 ⚠ | Retention loop: rules 1–3, max-wait eviction, empty partitions, txns/dedup purge, sink hold, local retention history | retention conformance + s3sink suite green |
| WP-2.8 | Local metrics store and collectors; dashboard endpoint re-sourcing; Raft/RSM tab data | dashboard views populated on raft1 |
| WP-2.9 ⚠ | Compaction (local) and disk watermarks (§11.8) | crash points `compaction.*`; ENOSPC test |
| WP-2.10 | Embedded mode on RSM | embedded_smoke on both backends |
| WP-2.11 | Dual-backend conformance suite complete (§13.2) | every listed test ported and green on both |
| WP-2.12 | Every suite, facade lane, differential v2 (full surface), crash matrix v2 and full flatness on raft1 | all green; flatness thresholds met |

**G2 (Alice):** full parity on raft1 (RAFT PARITY identical to single), differential clean, crash matrix and flatness pass, performance vs O14.

### Phase 3 — Raft with one voter

| WP | goal | done when |
|---|---|---|
| WP-3.1 | Pin the library per S3; TypeConfig; Raft `Replicator` adapter (rsm/replicator/raft.rs); `QUEEN_RAFT_REPLICATOR=local\|raft` | builds on MSRV 1.88; unit tests |
| WP-3.2 ⚠ | Consensus log storage passing the library's conformance suite, plus torn-write and lost-tail tests | suite green |
| WP-3.3 ⚠ | State machine adapter: apply via the apply thread, applied state, local snapshot build/install, `enable_leader_restore=false`, `save_committed` | adapter tests |
| WP-3.4 ⚠ | Recovery through the library: restart matrix at every crash point, including log flush and snapshot build, on raft1 | matrix pass |
| WP-3.5 | Every suite on raft1 with the Raft replicator; parity with LocalReplicator | RAFT PARITY identical |

**G3 (Alice):** confirm the library, re-checking its release status and the open issues (GH#2080, GH#2088, GH#2095 or their successors). Crash matrix and parity pass.

### Phase 4 — Three voters

| WP | goal | done when |
|---|---|---|
| WP-4.1 ⚠ | Transport: port 6634, HMAC handshake, framing, reader tasks, pools, deadlines; the library's network traits | transport tests incl. cancellation and slow-peer backpressure |
| WP-4.2 ⚠ | Forwarding: commands, outcomes, PayloadRead, request-id retries, hold (D13), budget-derived deadlines | forward tests incl. leader change mid-command |
| WP-4.3 ⚠ | Membership: bootstrap (all-empty check), IDENTITY, join as learner + promote, replacement endpoint, refusal on mismatch (I9) | bootstrap/join/replace tests; stale bootstrap flag refused |
| WP-4.4 ⚠ | Leadership: role watch, leader loops start/stop, I13 barrier, step-down endpoint, pre-vote and quorum check config | failover tests with commands in flight |
| WP-4.5 | Reads: batched read barrier; follower stale reads; follower long-poll wakes; local pop payload reads with fallback | tests on 3 in-process nodes |
| WP-4.6 ⚠ | Snapshot transfer and install (§11.6), log purge policy | install tests incl. crash mid-install; 10 GiB transfer on the VM |
| WP-4.7 | Digests: chain + state digest compare, alerting | injected divergence detected |
| WP-4.8 | `/health`, metrics, Raft tab live data | dashboards show a live 3-node cluster |
| WP-4.9 | `raft3` topologies + parity gate ha↔raft3; every suite and facade lane | RAFT PARITY identical |
| WP-4.10 ⚠ | Crash matrix on raft3 + VM kill tests (§13.6) + checkers; fix loop | every scenario ≥ 5 runs green |
| WP-4.11 | Helm changes (§14.2) in the helm_v1 tree; deploy to a throwaway namespace only with Alice's OK | chart renders; namespace test if approved |

**G4 (Alice):** raft3 parity; kill matrix with zero acknowledged loss and failover unavailability within target; equal digests; performance within the O14 budget.

### Phase 5 — Operations and release readiness

| WP | goal | done when |
|---|---|---|
| WP-5.1 ⚠ | Cluster version and rolling upgrade (N → N+1 under load, rollback before bump) | upgrade test green |
| WP-5.2 ⚠ | Backup/restore: snapshot export to S3 and restore into a new cluster id; drill. The broker has no S3 client: reuse the SigV4 implementation in `connectors/queen-s3/src/s3/sigv4.rs` (extract a shared crate or copy it) | drill documented and passing |
| WP-5.3 ⚠ | Migration from postgres deployments: exporter reading Postgres at one REPEATABLE READ snapshot → an RSM snapshot manifest (queues, groups, partitions, segments → segment files, cursors, DLQ, KV, timers, streams, traces, flags, quotas); importer; view-comparison verifier; offline cutover runbook (O11) | migration of a stage copy verified equal |
| WP-5.4 | SDK resilience fixes (§14.3) in each client with tests | client suites green on raft3 with a leader kill mid-run |
| WP-5.5 | Docs: deploy, runbooks (bootstrap, replace node, upgrade, backup/restore, disk full), config reference, CHANGELOG, dashboard docs | docs gate green |
| WP-5.6 | CI lanes (§14.9) | lanes running |
| WP-5.7 ⚠ | Final adversarial review of the whole RSM and fixes | no open confirmed finding |

**G5 (Alice):** go / no-go for stage.

### Phase 6 — Stage and production

A separate plan with Alice; nothing starts without her explicit OK. Outline:
- stage to 3 pods;
- migration drill on a copy of stage data;
- soak of at least 7 days with the kill schedule;
- then a production plan.

### 15.1 Dependency graph

```
0.1 ─► 0.2
0.1 ─► 0.3 ─┐
0.1 ─► 0.4 ─┼─► G0
0.1 ─► 0.5 ─┤
0.1 ─► 0.6 ─┤
0.1 ─► 0.7 ─┘
G0 ─► 1.1 ─► 1.2 ─┬─► 1.4 ─► 1.5 ─► 1.6 ─► 1.7 ─┬─► 1.8 ─┐
                  └─► 1.3 ─┘                    ├─► 1.9 ─┼─► 1.11 ─► G1
                                                └─► 1.10 ┘
G1 ─► {2.2 … 2.9 in parallel} ─► 2.10 ─► 2.11 ─► 2.12 ─► G2
      2.2, 2.3 ─► 2.1 ─► 2.10
G2 ─► 3.1 ─► 3.2 ─► 3.3 ─► 3.4 ─► 3.5 ─► G3
G3 ─► 4.1 ─┬─► 4.2 ─┐
           ├─► 4.3 ─┤
           ├─► 4.4 ─┼─► 4.5 ─► 4.7 ─► 4.8 ─► 4.9 ─► 4.10 ─► G4
           └─► 4.6 ─┘                 4.11 (after 4.4, parallel)
G4 ─► {5.1 … 5.6 in parallel} ─► 5.7 ─► G5
```

---

## 16. Risks

| id | risk | mitigation |
|---|---|---|
| K1 | Consensus library immaturity: openraft features alpha-only; safety fixes June–July 2026; unknown outcomes reported as `ForwardToLeader` in alpha.34; open liveness bug under partial partition | S3; exact pin; our own fault tests (lost fsync, wiped disk, clock jump); re-check at G3; raft-rs fallback; request ids (D6) |
| K2 | Single-leader throughput below the postgres class on fat batches | WP-0.2 baseline; phase 1 measures the planner + apply ceiling. Levers: receiver-side packing; parallel apply per bucket inside an entry (deterministic); pipelining with overlays (revisit D4); larger batches |
| K3 | Store engine crash semantics or write amplification | S1 with kill and dropped-write tests; engine behind an adapter |
| K4 | Snapshot size makes node replacement slow, with reduced redundancy meanwhile | resumable throttled transfer; PVC sizing; runbook; measured in S3 and WP-4.6 |
| K5 | Elections break clients | hold (D13); SDK fixes; measured unavailability target |
| K6 | Determinism bugs in apply | effects-only apply; lints; property tests; digests |
| K7 | Semantic drift while porting ~20k lines of SQL | conformance + differential + suites; the SQL as spec; timebox per WP |
| K8 | Rolling upgrade incompatibility | cluster version gate; upgrade test |
| K9 | Disk loss mishandled | IDENTITY + refusal + replacement flow + test |
| K10 | Two backends to maintain | D22 removal decision after GA |
| K11 | Cost: 3 pods everywhere and 3 full data copies | capacity plan before G5 |
| K12 | Dashboard and metrics regressions | endpoint checklist (§9.6) + dashboard e2e |
| K13 | Clock skew | monotone stamps (D5); clock-jump test |
| K14 | Disk full | watermarks; ENOSPC test |
| K15 | SDK-level retries after a lost pop response still hide messages for the lease time | same as today; document; optional pop idempotency key later |
| K16 | Queen Cloud cells still need Postgres for the proxy | documented (D25); out of scope |

---

## 17. Open decisions for Alice

**Answered at G0 (2026-09-18): every item below as recommended**, with one
change: O14's failover unavailability target is ≤ 4 s p99 (S3 measured
3.0–3.4 s to a new leader with 1–2 s election timeouts). O17–O20 (bounded
planning time 5 ms per batch, planner metrics and a slow-command log, the
noisy-neighbor test in WP-1.11, decompression and repacking off the planner)
were also accepted; O21–O23 (store commit cadence, the segment index outside
the store, the bounded pipeline) are the D4, D9 and §11.3 amendments above.
The deferred long runs happen after phase 1. The postgres class stays in the
binary as the oracle until G2 (D22). Second round, after the G0 packet's
review (same day): heed everywhere rather than heed + redb; the per-frame MAC
rather than TLS; the evidence-gap runs (RAFT_STATUS.md D-01..D-03, D-09) skipped
by decision, phase 1 starts at once; D14 kept at 1–2 s with ≤ 4 s failover.
The G0 packet itself is in RAFT_STATUS.md (findings R-01..R-64).

| id | question | recommended default |
|---|---|---|
| O1 | Branch base and dropping the per-queue native class (D23) | `raft` from `fc71b65b`, copy modules from `6e96e228` |
| O2 | Storage mode per deployment (D1) | yes |
| O3 | Hold duration (D13) and election timeouts (D14) | 8 s hold; 100 ms heartbeat; 1–2 s election |
| O4 | Stage to 3 pods; PVC sizes; zone anti-affinity | 3 pods; size from retained bytes × 1.5; preferred by zone |
| O5 | Consensus library: openraft 0.10 stable, a pinned commit, or raft-rs | decide at G3 with S3 data; prefer a released openraft 0.10 |
| O6 | Store engine (S1) and dedup design (S2) | per memos |
| O7 | Put the DLQ handoff for `dlq:true` acks inside the transaction entry (atomic; differs from today only in failure windows) | yes |
| O8 | `too_late` for timers without claims | a cancel or reschedule planned in the same cycle as (after) that timer's Fire, or while the entry holding the Fire is in flight, answers `too_late`; otherwise it succeeds (a fired timer's row is gone, so a later cancel answers as the SQL does for a missing timer); confirm with conformance |
| O9 | Traces age limit (D18) | 7 days |
| O10 | No spool in raft mode (D19); push behavior in maintenance mode (today it spools) | maintenance mode answers 503 `maintenance` to pushes in raft mode |
| O11 | Migration: offline snapshot export with a maintenance window, or online copy; is a window acceptable for Smartness? | offline first |
| O12 | When to remove the postgres class (D22) | decide after 3 months of GA |
| O13 | Three VMs for final performance numbers | yes, for G4 |
| O14 | Performance targets for G1, G2 and G4 | raft1 p50/p99 ≤ postgres at A20k/A50k/C1000; raft3 p50 ≤ raft1 + 2 ms; failover client-visible unavailability ≤ 3 s p99; push-only fat-batch throughput ≥ 70% of postgres |
| O15 | Replicator for embedded mode | LocalReplicator |
| O16 | The ack fast path: record the delivered hash set in the claim (deterministic, matches today's fast path when the RAM set was present) or always compute the slow-path accounting | record the delivered set in the cursor at claim, bounded by batch size |

---

## 18. Lessons from pgless (do not repeat)

| lesson | pgless evidence | rule here |
|---|---|---|
| State ahead of the log | C00 (checkpoint froze hw), C02 (cancelled push skipped publish), C03 (leaked in_flight); `stage` applied at plan time | I1, D4, D7 |
| Re-claim by name without data | C01, C22 destroyed both replicas | I9, D21 |
| Positions shipped without their files | checkpoint shipped verbatim to a backup with different file boundaries (task filed 2026-09-17) | D8, I7 |
| Replication-triggered checkpoint above the log tail wedged the bucket | C07 | I11, I17: recovery falls back; never refuse forever |
| Blocking runtime threads under locks | C11 took the whole process down | I15 |
| Database calls without timeouts | C13 made the lease judgement unenforceable | I15 |
| Exactly-once ledger keyed per attempt | C17 | D6: id minted once at the receiver, reused on retry |
| Guessing when the control plane cannot answer | C18 skipped a backlog | I14 |
| Answering before durability | `Wait::Enqueued` for pop and renew | D7 |
| Kill tests that never crossed a periodic boundary | killed at 25 s, first checkpoint at 30 s | §13.6 randomized kill schedules |
| Laptop smokes at zero load | catch-up races only under load on Linux | §13.6 kill tests under load |
| `select!` over non-cancellation-safe reads | half-read frames lost | one reader task per socket |
| Position file written with truncate + write | kill left it empty | temp + fsync + rename + dir fsync for every small durable file |
| Harness only in /private/tmp | lost with the session | `test/raft/` |
| Compaction emitting replicated records from local layout | hazard D-4 | §11.7 local only |
| Lane authentication open when the secret was unset | U19 | D12 fail closed |
| Two processes on one data dir | U24 | LOCK file |
| A review found 9 real defects tests had not | dbf5613c | §13.8 before every ⚠ commit |

---

## Appendix A. Postgres durable objects → RSM

34 objects (33 tables + one sequence). Writers and readers as found on 2026-09-17.

| object | purpose | class | RSM |
|---|---|---|---|
| queues (schema.sql) | identity and config | R | queues |
| consumer_groups_metadata | group mode, seeded | R | groups |
| consumer_watermarks | wildcard discovery scan time | X | not ported |
| log_partitions (001) | allocator, watermarks, times | R+L | partitions |
| log_segments (001) | compressed blobs | R+L | segment files + segments index |
| log_txns (001) | txn hashes per segment | R (derivable from Append hashes) | dedup per S2 |
| log_consumers (001) | cursor, lease, retry, attempts | R | cursors |
| log_dlq (005) | DLQ snapshots | R | dlq |
| log_timers (025) | scheduled messages + fire lease | R+L | timers, timers_due (no claims) |
| kv (024) | tenant KV | R+L | kv, kv_expiry |
| kv_version_seq (024) | version tokens | R | index + position |
| kv_quota (024), ephemeral_quota (030), queen_streams.quota (002) | grants | R | quotas |
| kv_usage (024) | measured usage | L | counters |
| ephemeral_queues (030) | ephemeral configs | R | eph_config |
| queen_streams.queries, .state (002) | streams | R | streams_queries, streams_state |
| system_state (schema.sql) | operator flags | R | flags |
| message_traces, message_trace_names | trace events | R | traces, trace_names (+ expiry, D18) |
| stats (schema.sql) | cached counters, retained bytes | L | counters |
| stats_history | dead (no writer) | — | not ported |
| retention_history | retention audit | L | local.db |
| system_metrics, worker_metrics, queue_lag_metrics, queue_parked_replica, worker_metrics_summary | per-replica metrics | N | local.db |
| hotlist_repairs (001) | cursor moved back notice | X | not ported |
| maintenance_leases (029) | fenced scheduler | X | not ported (leader loops) |
| native_txn_applied (034), native_members (035), native_buckets (035/036) | pgless native class | X | not ported |

Values followers could not reproduce in SQL, all planner-stamped here:
`clock_timestamp()` (003, 004), `gen_random_uuid()` (025),
`nextval(kv_version_seq)`, `ORDER BY random()` (004 wildcard candidates).
Writes hidden in reads that must stay writes here: a wildcard pop creates a
missing queue (004); storage-class resolution upsert (034, not ported).

## Appendix B. Background loops → RSM

| loop (file) | today | RSM |
|---|---|---|
| retention (retention.rs) | rules, txns purge, max-wait eviction, empty partitions, metrics purge; 5 s; lease + advisory 737_001 | leader loop §10.1; metrics purge → local.db |
| watermark walk (retention.rs) | 24 h repair of oldest_live_at | not needed |
| stats refresh (stats.rs) | 10 s / 10 min; lease + advisory 737_002/737_003 | counters (D16) |
| timers (sweeper.rs) | claim then fire, ≤ 1 s, SKIP LOCKED on every broker | leader wheel |
| KV prune and usage (sweeper.rs) | 1 s / 5 min on every broker | leader expiry loop / counters |
| quota and ephemeral grant re-reads (quota.rs, ephemeral.rs) | 30 s | read local state |
| reconcile (reconcile.rs) | 60 s flags, hot-list repairs, cache clears | not started |
| hot-list wheel, wake, trim, idle sweep, reseed (main.rs, reconcile.rs) | 50 ms … 5 min | apply-driven rings and wakes |
| mesh frames and heartbeats (mesh.rs) | 1 s / 5 s | ephemeral only |
| spool drain (file_buffer.rs) | 100 ms | not started (D19) |
| metrics, gauges, log rates, JWKS (syscollect.rs, metrics.rs, obs.rs, main.rs) | 60 s etc. | node-local |
| admission (admission.rs) | 500 ms | retarget to planner queue depth |
| fusion, ack_fusion, pop_fusion | 3 ms hold group commit | receiver pre-work + batcher |
| native checkpoint, compaction, placement, replica, txn ledger prune (native/, main.rs) | pgless | not carried over |
| facade and sink supervisors (kafka_facade.rs, sqs_facade.rs, s3_sink.rs) | backoff restarts | unchanged |

`server/src/embedded/boot.rs` wires its own copies of some loops: keep it in sync.

## Appendix C. Coordination mechanisms removed

| mechanism | solves today | with one leader |
|---|---|---|
| advisory locks 737_001/002/003 (retention.rs, stats.rs) | single-runner backup during mixed rollouts | gone |
| advisory 778_120_010 (schema.rs) | one broker applies DDL at boot | gone (versioned format, D20) |
| advisory 778120035 (035) | native bucket rows race | gone |
| hash try-lock (004), blocking lock (007) | pop consumer-row livelock; stream shard serialization | gone (apply is serial) |
| maintenance_leases (lease.rs) | fenced cluster cadence on the DB clock | leader loops; term fences |
| partition row locks + the six-space lock order (003, 005, 025) | offset order = commit order; no deadlocks | log order; lock order kept only as validation order (§8) |
| SKIP LOCKED claims (004 pop, 025 timers, 026 KV prune) | several brokers drain the same work | gone; pop leases remain cursor state |
| system_state + 60 s re-read | durable flags healing lost mesh frames | FlagSet effects applied everywhere |
| hotlist_repairs | cursor moved back notice | gone |
| mesh config/maintenance frames | cache invalidation | apply |
| push spool | accept pushes through a DB outage | gone (D19) |
| native placement, replica lane, txn ledger | pgless | gone |

## Appendix D. Determinism hazards found in pgless `native/`

| id | hazard | where (6e96e228) | consequence here |
|---|---|---|---|
| D-1 | planning is applying: `stage` advances `applied_lsn`; `apply` skips lsn ≤ applied | semantics.rs ≈134–138; state.rs ≈549, ≈582 | would skip a new leader's records and leave phantom offsets, leases, DLQ ids, dedup hashes → I1, D4 |
| D-2 | in-memory delivered set chooses the ack path with different durable results; the postgres path has the same twin (ack_registry.rs ≈229 → 005 ≈258–266 vs ≈796–805) | bucket.rs ≈426–493; semantics.rs ≈992–1127 | O16 |
| D-3 | dedup window pruned by clock without a record; follower windows only grow; `occurrences` has no time filter | semantics.rs ≈1484–1487; state.rs ≈219–227; semantics.rs ≈848 | dedup via S2 with planner-stamped cutoffs |
| D-4 | node-local positions inside serialized state and shipped checkpoints; compaction emits replicated Append from local layout | state.rs ≈900–926; bucket.rs ≈647, ≈668–760; replica.rs ≈1077, ≈1277; log.rs ≈711–719 | D8, I7, §11.7 |
| D-5 | clock and uuid read at plan time | bucket.rs; util.rs ≈215–229; semantics.rs ≈161, ≈1195 | planner-stamped (D5) |
| D-6 | HashMap iteration: renew record order; rebuilt rings differ from live rings | semantics.rs ≈1251, ≈685; state.rs ≈1105 | key-ordered indexes (`leases_by_worker`, `pending`) |
| D-7 | queue config and group policy from node caches, DDL defaults on a miss; group registered "now" on DB error | handlers/mod.rs ≈404–482 | config is committed state; I14 |
| D-8 | pop and renew answered before records were durable | bucket.rs ≈386, ≈407, ≈568 | D7 |

## Appendix E. Edges inventory (2026-09-17)

- **Helm** (helm_v1/broker):
  - Deployment and replicas: StatefulSet with a headless Service, `podManagementPolicy: Parallel`; replicas 2 in the defaults and stage, 3 in prod.
  - Volumes: the only PVC is `spool`; read-only root filesystem.
  - Probes and shutdown: startup and readiness on `/health` (a Postgres ping), TCP liveness; preStop `sleep 5`, grace 90 s; no PDB; soft spread.
  - Identity: `QUEEN_SERVER_ID` = pod name; Kafka node id = ordinal + 1; `QUEEN_MESH_PEERS` is a static list built from `replicas`.
  - Proxy database: same database on stage, same secret in prod.
- **SDKs:** no leader hints or redirects followed (Rust follows 307/308). Every SDK except the CLI mints the transactionId before retrying. Per-SDK issues are in §14.3.
- **Kafka facade:**
  - Talks to its own broker over loopback: one URL, no retries, 10 s timeout.
  - Leader = hash over live facade ids (advertised only).
  - Node registry in KV (`qk:node:*`, 2 s heartbeat, 10 s TTL); group generation in memory; offset commits with a `qk:fence` expect + required write.
  - Transactions refused in cluster mode; EndTxn = one `/api/v1/transaction`.
  - Endpoints used: resources/queues, configure, push, fetch long-poll, kv, transaction.
  - 408/429/502–504 map to COORDINATOR_NOT_AVAILABLE.
- **SQS facade:** stateless; KV `qs:*` registry; HMAC receipt handles with no node id; FIFO dedup id = transactionId; registry and delete-sets with expect (2 attempts); DLQ redrive and SNS fan-out as one atomic transaction.
- **Proxy:**
  - Own Postgres schema `queen_proxy` plus a LISTEN connection.
  - One `cells.base_url` per cluster; no retry or failover (502/504 pass through).
  - Storage quota via `resources/queues?stats=cached` every 60 s.
  - Stateless JWT cookies with a revocation table.
- **Embedded:** requires Postgres today; "N embedded instances over one Postgres" fleets coordinate by polling `hotlist_repairs`.
- **Dashboard:**
  - Served from the binary; every analytics handler is one SQL function.
  - Removed or re-sourced: postgres-stats, DB pool gauges, `database`/`dbHealthy`.
  - Need a new store: the five metrics tables, `retention_history`, `stats`, traces.
- **Mesh:** static full mesh, HMAC handshake, frames dropped when a peer's queue is full. Frames: MESSAGE_AVAILABLE, HOTLIST_DIRTY, maintenance ×2, QUEUE_CONFIG_*, EPH_ADMIN.
- **Auth:** broker JWT is stateless. A forwarded write must carry the tenant and the producer subject from the validated token.
- **Supervisor:** polls depth, trying URLs in order.
- **S3 sink:** one QUEEN_URL; fetch, partitions/changed, kv; time windows rely on per-partition monotone timestamps; `safeTime` from `pg_stat_activity`; KV lease with expect + required.
- **Docs:** webdoc deploy page says brokers are stateless.
- **Test infrastructure:** the HA test stack is Postgres-bound.

## Appendix F. Consensus library facts (checked 2026-09-17)

openraft (sources read: 0.9.25 and 0.10.0-alpha.34 in ~/.cargo/registry; repo at main 54094270, 2026-09-15):

- **Releases and stability:**
  - stable 0.9.25 (2026-07-28); latest pre-release 0.10.0-alpha.34 (2026-08-14);
  - the README says the API is not stable and 0.10 is in alpha;
  - alpha.34 builds on Rust 1.88, not 1.86;
  - license MIT OR Apache-2.0.
- **Features only in 0.10 alphas:**
  - pre-vote (alpha.22, off by default);
  - quorum-check leader lease (alpha.32);
  - `trigger().transfer_leader`;
  - `ReadPolicy` / `ensure_linearizable` (ReadIndex: one empty append per call to each voter, 50 ms timeout, no batching; LeaseRead unsafe across quick restarts);
  - app-defined snapshot data and transport (`full_snapshot`, example `dir-transfer`);
  - `client_write_many` / `client_write_ff`;
  - streaming replication, only with an eager `stream_append`.
- **Storage contract:**
  - the leader counts its own entry only after the flush callback;
  - followers ack after flush;
  - apply follows cluster commit, so a node may apply before its own flush;
  - restart installs the last snapshot if newer than `applied_state()`, then re-applies the log up to the saved committed;
  - purge only behind snapshots, minus `max_in_snapshot_log_to_keep` (default 1000);
  - `build_snapshot` counts as done when it returns;
  - `enable_leader_restore` defaults to on and is documented unsafe with a volatile state machine without a saved committed.
- **Safety fixes landed late:**
  - commit without a real quorum, latent since 0.8.0 (2023), fixed in alpha.25 / 0.9.25 (GH#1802);
  - stale membership after snapshot install losing commits (GH#1808, alpha.26 / 0.9.25);
  - a leader read error corrupting a follower's log (GH#1795, alpha.24);
  - new leader not flushing its no-op (0.9.11).
- **Open or recent issues:**
  - `ForwardToLeader` may mean "may still commit" (GH#2095; `LogEntryDiscarded` only on main 54094270);
  - liveness deadlock under partial partition (GH#2080);
  - a failed transfer leaves no leader until an election (GH#2088).
- **Disk loss:** undefined behavior per its FAQ; detection is a `debug_assert!` only; safe procedure is remove + re-add as learner + promote (GH#898).
- **Testing:**
  - turmoil deterministic simulation (April 2026; in-memory store, so no lost fsyncs; 5 iterations in CI);
  - its own Jepsen-style harness (July 2026; no disk faults).
- **Performance:** the published benchmark is framework overhead only (in-memory, function-call network, empty payloads, M1 Max): 33k writes/s with one client; 3.55M with 4096 clients; 5.6M with batches of 4.
- **Threading:** one core task, one replication task per peer, serial apply task.
- **Log storage:** `raft-log` 0.4.6 (2026-09-01, by openraft's author, used by databend-meta); older rocks/sled stores stale.
- **Users:** Databend's meta-service runs 0.10.0-alpha.29 + raft-log 0.4.6, for metadata.

raft-rs (TiKV):

- **Releases:** crates.io 0.7.0 (2023-03-07); TiKV depends on git master; commits through 2026-05-13.
- **Features:**
  - config: `pre_vote`, `check_quorum`, `read_only_option` (Safe / LeaseBased), `max_inflight_msgs`, `batch_append`, `priority`;
  - RawNode: `transfer_leader`, `read_index`;
  - joint consensus.
- **API:** the Ready-loop driver is the application's job.
- **Build:** protobuf codegen at build time, via a matching system `protoc` or `protobuf-src` compiling protoc from C++.
- **Log storage:** `raft-engine` (TiKV) active on git (2026-09-10); crates.io 0.4.2 (2024).

## Appendix G. Baselines and regimes

goload `-mode openloop` (coordinated-omission-correct), 256-byte payloads,
manual acks unless stated.

| regime | shape |
|---|---|
| A20k | 20 000 msg/s, push batch 10, 100 partitions, wildcard pop |
| A50k | 50 000 msg/s, same shape |
| B1 | 2 000 msg/s single pushes, 16 partitions, pinned pop |
| C1000 | 3 000 msg/s single pushes, 1 000 partitions |
| D1 | 500 msg/s, one partition, one consumer |

Linux VM, 2026-09-16 (PLAN_NATIVE_LOG.md §8.1), same fsync for both:

| regime | postgres class | native single node (journal, fsync) |
|---|---|---|
| A20k | p50 8.5 ms, p99 36.6, ack 8.4 | p50 4.2 ms, p99 13.8, ack 3.4 |
| A50k | p50 24.5, p99 92, ack 31.8 | p50 7.1, p99 29, ack 6.1 |
| B1 | p50 4.0, p99 28 | p50 2.1, p99 22.7 |
| C1000 | p50 6.2, p99 40.7, ack 25 | p50 3.0, p99 15.7, ack 2.6 |
| D1 | p50 2.4, p99 6.8 | p50 1.7, p99 10.9 |

Reference, not from the VM: the postgres class reached 1.78 M msg/s push-only
in fat batches (Postgres ≈5.6 cores) and a 600k msg/s soak (broker ≈15 cores
+ Postgres ≈6) on the larger campaign hosts. WP-0.2 re-measures the fat-batch
shape on the VM.

## Appendix H. Environment knobs (proposed)

| knob | default | meaning |
|---|---|---|
| QUEEN_STORAGE | postgres | `postgres` or `raft` (D1) |
| QUEEN_RAFT_DIR | /var/lib/queen/raft | data dir (§11.1) |
| QUEEN_RAFT_PORT | 6634 | internal port (D12) |
| QUEEN_RAFT_SECRET | — | HMAC secret; required with peers |
| QUEEN_RAFT_PEERS | — | `ordinal=host:port,…` initial members |
| QUEEN_RAFT_ORDINAL | from pod name | node ordinal (§12.6) |
| QUEEN_RAFT_REPLICATOR | raft (local for embedded) | `local` or `raft` |
| QUEEN_RAFT_HEARTBEAT_MS | 100 | D14 |
| QUEEN_RAFT_ELECTION_MIN_MS / MAX_MS | 1000 / 2000 | D14 |
| QUEEN_RAFT_HOLD_MS | 8000 | D13 |
| QUEEN_RAFT_PROPOSE_MS | 5000 | propose deadline, D13 (GH#2080) |
| QUEEN_RAFT_BATCH_HOLD_MS | 2 | §7.1 |
| QUEEN_RAFT_PIPELINE | 4 | entries in flight (D4, I3) |
| QUEEN_RAFT_PLAN_MAX_MS | 5 | planning time per batch before the drain is cut (O17) |
| QUEEN_RAFT_STORE_COMMIT_MS / ENTRIES | 4 / 256 | store commit cadence (§11.3) |
| QUEEN_RAFT_BATCH_MAX_BYTES | 4 MiB | §5.1 |
| QUEEN_RAFT_BATCH_MAX_CMDS | 4096 | §5.1 |
| QUEEN_RAFT_ENTRY_MAX_BYTES | 96 MiB | largest PLANNED command, §5.1 |
| QUEEN_RAFT_CLOCK_SKEW_ALARM_MS | 500 | §7.4 |
| QUEEN_RAFT_LOCAL_APPLY_WAIT_MS | 1000 | §7.5 |
| QUEEN_RAFT_REQUEST_ID_WINDOW_S | 600 | D6 |
| QUEEN_RAFT_SEGMENT_BYTES | 64 MiB | §11.2 |
| QUEEN_RAFT_DURABLE_EVERY_MS / BYTES | 1000 / 256 MiB | §11.4 |
| QUEEN_RAFT_SNAPSHOT_LOG_BYTES | 4 GiB | §11.6 |
| QUEEN_RAFT_SNAPSHOT_MAX_INTERVAL_S | 1800 | §11.6 |
| QUEEN_RAFT_SNAPSHOT_MBPS | 200 | §11.6 |
| QUEEN_RAFT_DISK_HIGH_PCT / LOW_PCT | 85 / 80 | §11.8 |
| QUEEN_RAFT_READY_LAG_MS | 2000 | §14.1 |
| QUEEN_RAFT_DIGEST_EVERY | 100000 | §12.9 |
| QUEEN_RAFT_VERSION_SETTLE_S | 300 | §12.8 |
| QUEEN_RAFT_TRACE_TTL_DAYS | 7 | D18 |
| QUEEN_TEST_FAULTS | — | crash points (§13.5) |
