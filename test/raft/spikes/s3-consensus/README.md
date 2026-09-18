# Spike S3 — consensus (PLAN_RAFT.md WP-0.5)

A 3-node openraft cluster where every node is a **separate process** listening
on **real TCP** (length-prefixed MessagePack frames, no HTTP), with a
raft-log-backed Raft log, a file-based state machine, and snapshots as an
**app-defined manifest** streamed over the same transport.

Standalone crate on purpose: it is not a member of `server/Cargo.toml` and must
never become one.

    cargo build --release          # openraft is a git dependency, pinned
    cargo test                     # openraft's testing::log::Suite on our log
                                   # store, once per save_committed setting —
                                   # it passes for all three, which is why it
                                   # is NOT evidence of durability
    ./run.sh laptop                # the seven scenarios, small scale
    ./run.sh vm                    # the same at the sizes the WP row asks for
    ./run.sh vm-today              # the budgeted VM pass of 2026-09-17:
                                   # 1 GiB snapshot instead of 10 GiB, 3 kill
                                   # repetitions, one of them with a follower
                                   # SIGSTOPped for 5 s
    ./run.sh vm-refute             # the refutation pass of 2026-09-18:
                                   # scenarios 8 and 9, scenario 7 with kill -9
                                   # and a stale durable point, and the cost of
                                   # each save_committed setting (~3 min)
    ./run.sh laptop-refute         # the same, small scale
    sudo ./flaky-log.sh run 10     # Linux only: scenario 8 with dropped
                                   # unflushed writes under the victim's log
    sudo ./flaky-log.sh selftest   # proof that the injector injects

Results: `RESULTS-vm.md` (Linux VM — **the numbers to quote**, §13.6) and
`RESULTS-laptop.md` (macOS — behaviour only). Raw output and node logs are in
`results/vm-2026-09-17/`, `results/vm-2026-09-18-refute/`,
`results/laptop-2026-09-17/` and `results/laptop-2026-09-18-refute/`.
The decision memo of the WP-0.5 row is `MEMO.md`; the effort estimate for the
raft-rs fallback is `RAFTRS.md`.

## The pin

    openraft = { git = "https://github.com/databendlabs/openraft.git",
                 rev = "54094270ede0b8a2eb6ed6ae990edc6ca19d98ec" }   # main, 2026-09-15
    raft-log = "0.4.6"

That commit is version `0.10.0-alpha.34` plus the fix for GH#2095: it has
`ClientWriteError::LogEntryDiscarded`, the variant that says "your entry was
appended and then discarded, the outcome is unknown" instead of a plain
`ForwardToLeader`. PLAN_RAFT.md §12.1 maps it to `ProposeError::OutcomeUnknown`;
on the published alpha.34 that state is indistinguishable from "never
appended", which would make D6's request-id window the only defence.

It builds and runs on Rust 1.88 (MSRV) — verified with `cargo +1.88 check`.

## What is in here

| file | what it is |
|---|---|
| `src/types.rs` | the openraft `TypeConfig`: opaque byte entries, `u64` node ids, `NodeInfo` nodes, advanced leader id |
| `src/logstore.rs` | `RaftLogStorage` on `raft-log` 0.4.6, adapted from openraft's own `examples/log-wal` (MIT OR Apache-2.0). `CommittedDurability` makes `save_committed` a knob (`none` = the upstream example, `buffered`, `fsync` = §12.3 as written) |
| `src/sm.rs` | the state machine: entry payloads appended to rolled data files, applied index + membership in a two-slot meta file, snapshots = a directory of immutable files + `MANIFEST.json` with a per-file xxh3 |
| `src/net.rs` | `RaftNetworkFactory` / `RaftNetworkV2` over the framed TCP transport, including a real `pre_vote` RPC and the manifest snapshot sender |
| `src/server.rs` | the node process: Raft + the admin/client calls the scenarios need + the linearizable-read batchers |
| `src/cluster.rs` | spawning, `kill -9`, graceful stop, wiping and per-node directory overrides (one node on a dm-flakey filesystem) |
| `src/scenarios.rs` | the nine scenarios |
| `run.sh` | driver: runs them all and keeps the output and the node logs |
| `flaky-log.sh` | Linux/root: loop device + dm-flakey under the victim follower's log, `drop`/`up` verbs driven by scenario 8, and a self-test of the injector |

## Scenarios

| # | subcommand | question |
|---|---|---|
| 1 | `latency` | commit latency for 64 KiB entries at 200/500/1000 entries/s, one `client_write` in flight (D4); `--writers N` shows what pipelining would buy |
| 2 | `kill-leader` | `kill -9` the leader under load: time to a new leader, to the first committed write, and whether any acknowledged write is missing. `--stop-follower-secs N` first SIGSTOPs a follower for N s in the middle of the pre-kill load (quorum on the other two, then catch-up) |
| 3 | `transfer` | `trigger().transfer_leader` to a healthy target `--rounds` times, then to an unreachable one (GH#2088) |
| 4 | `linearizable` | `ensure_linearizable` cost: one call per read vs a 2 ms window vs coalescing into the in-flight barrier. `--concurrency` is the closed-loop sweep; `--rates` measures offered read rates open loop (the VM pass uses 1000 and 5000/s) |
| 5 | `snapshot` | a manifest snapshot streamed to an empty learner: throughput, and resume after a killed transfer |
| 6 | `wiped-voter` | restart a voter with an empty directory the wrong way (same id), then the right way (remove, add learner, promote) — D21, §12.6 |
| 7 | `restart` | restart every node with `enable_leader_restore=false` and check the applied state. `--kill9 true` makes it a crash instead of a graceful shutdown, which is the only way it says anything about recovery |
| 8 | `log-crash` | `kill -9` a FOLLOWER under load N times, restart it every time, and check what the RAFT LOG reopened with against the index the leader had counted as matched, plus a read-back of every index it claims. `--victim-dir` + `--flakey-cmd` put that node's log on a dm-flakey filesystem and drop its unflushed writes before each kill |
| 9 | `one-way-partition` | both followers process the leader's appends and never answer: the lease interaction behind openraft GH#2080. Measures how long the cell is write-dead, what a client sees, and what each operator action costs |

Node ids follow §12.6: `ordinal * 1000 + generation`, so the three voters are
1001/2001/3001 and a replaced disk comes back as 2002.

## Deliberate simplifications

- No HMAC handshake and no TLS on the transport: that is spike S4 (D12).
- Client/admin calls share the Raft port; snapshots get their own connection
  per transfer so a stream never queues in front of a heartbeat (§12.5).
- The state machine is not the RSM: no ordered store, no positions, no
  retention, no dedup. It exists so that apply, durable points, snapshots and
  recovery are real file work of the right shape (§11).
- `--fsync periodic` (default) takes a durable point on a timer, as §11.4
  prescribes; `--fsync batch` takes one per apply batch and shows what a
  per-entry durable point costs; `--fsync never` isolates consensus from disk.
- `--committed none` is the default because it is what openraft's example does
  and what the 2026-09-17 pass ran; the recommendation for the real adapter is
  `fsync` (MEMO.md, plan change 5).
- `--wait-recovery true` calls `Raft::wait_for_recovery` at start-up and
  reports how long it blocked (`StatusResp::recovery_ms`).
