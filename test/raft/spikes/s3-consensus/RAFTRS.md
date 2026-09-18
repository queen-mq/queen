# raft-rs as the fallback (PLAN_RAFT.md §12.4) — effort estimate

Sources read: `tikv/raft-rs` `examples/five_mem_node/main.rs` (435 lines, the
whole driver pattern), `proto/build.rs`, and the repository metadata (fetched
2026-09-17 with `gh api`). Latest commit on master `ad13f3d9`, 2026-05-13;
crates.io still at 0.7.0 (2023-03-07), so a dependency would be a git pin.

## What raft-rs gives us

`RawNode` with pre-vote, check-quorum, `read_only_option = Safe`,
`transfer_leader`, joint consensus, `max_inflight_msgs`, `batch_append` — the
protocol, and nothing else. There is no network, no storage implementation
beyond `MemStorage`, no snapshot transport, and no conformance suite for the
`Storage` trait (openraft ships `testing::log::Suite`, which this spike used to
validate `raft-log` in an afternoon).

## What the driver must do (all ours)

1. **The Ready loop.** `has_ready` → `ready()` → send `take_messages()` →
   apply the snapshot if any → apply committed entries → **append entries and
   persist the hard state** → send `take_persisted_messages()` → `advance()` →
   from the light ready: update commit index, send messages, apply committed
   entries → `advance_apply()`. The ordering is the correctness contract:
   persisted messages may only go out after the entries and the hard state are
   on disk, and `advance_apply` may only move after the state machine has the
   entries. The example runs it on a dedicated thread with a 10 ms poll and a
   100 ms `tick()`; ours would be the apply thread of §3.3.
2. **`Storage`**: `initial_state`, `entries(low, high, max_size, context)`,
   `term`, `first_index`, `last_index`, `snapshot(request_index, to)`, plus the
   writer side (append, compact, set hard state, apply snapshot). The error
   kinds are load-bearing: `Compacted`, `Unavailable`, `SnapshotOutOfDate`,
   `SnapshotTemporarilyUnavailable` mean specific things to the algorithm and
   getting one wrong is a silent safety bug. `raft-engine` (TiKV's) is the
   ready-made option; it is active on git, last release 0.4.2 (2024).
3. **Snapshots.** `Snapshot.data` is a protobuf `Vec<u8>` inside the message —
   there is no app-defined snapshot transport. A 10 GiB manifest means doing
   what TiKV does: put a reference in `data`, ship the files out of band on our
   own transport, and implement the receive/install/retry side by hand,
   including the "snapshot is being built, come back later" path. This is the
   single biggest difference from openraft 0.10, which hands us
   `full_snapshot` and the `SnapshotData` type (the spike's manifest streamer
   is ~200 lines because of it).
4. **Read barriers.** `read_index(ctx)` with an application context, results in
   `ready.read_states()`, matched back by context bytes, then wait for applied
   ≥ index. Our batching layer (§9.4) sits on top, same as here.
5. **Membership, transfer, metrics.** `propose_conf_change` +
   `apply_conf_change` on the committed entry + store the `ConfState`;
   `transfer_leader(id)` with no completion signal (poll the role);
   `raft.status()` for `/health` and `/metrics`.
6. **Build.** `proto/build.rs` runs `protobuf_build::Builder` at compile time:
   it needs a matching system `protoc` (the Dockerfile, CI and every laptop) or
   `protobuf-src`, which compiles protoc from C++ — which C-2 rules out. This
   is a build-system change, not a code change, and it touches the release
   pipeline.

## Estimate

| piece | agent-days | risk |
|---|---|---|
| Ready loop, tick thread, role watch, `Replicator` impl | 3–4 | medium: the ordering contract |
| `Storage` on raft-engine or our WAL, plus the conformance tests we must write ourselves | 5–7 | **high**: no upstream suite |
| Snapshot build, out-of-band transfer, install, retry | 4–5 | high |
| read_index barriers + batching | 2 | low |
| Membership, transfer, metrics, health | 2–3 | low |
| protoc in Dockerfile/CI/dev setup | 1 | medium (C-2) |
| Hardening: in-flight bounds, panics on storage misbehaviour, apply order | 3–5 | high |
| **total** | **20–27** | |

For comparison, the openraft adapter in this spike — log store, state machine,
snapshots with a manifest, network, three processes over TCP, seven scenarios —
took about one agent-day of code, because openraft owns the driver loop, the
storage contract comes with a test suite, and the snapshot API is app-defined
by design. A production-quality openraft adapter inside `server/src/rsm/` is
maybe 6–9 agent-days.

## Verdict

Stay on openraft at the pinned commit. Keep raft-rs as a genuine fallback only
for the case where openraft's alpha status turns into a blocker (a safety bug
we cannot get fixed, or the 0.10 API changing under us); budget **4–5 calendar
weeks** for the switch and expect the storage and snapshot code, not the
protocol, to be where the bugs are — which is exactly the lesson the pgless
investigation already paid for once.
