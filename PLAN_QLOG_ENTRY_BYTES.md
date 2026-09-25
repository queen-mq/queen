# PLAN_QLOG_ENTRY_BYTES — entry bytes linear in the entry, not queues × entry

Status: design + prototype (2026-09-25). Prototype behind `QUEEN_QLOG_ENTRY_LAYOUT=stub`,
default unchanged (`copies`). Nothing committed. Alice decides the design.

## 1. The problem

Benchmark 2026-09-25 (single-node openraft, 16 vCPU DO VM, `QUEEN_LANES=1`,
50k msg/s, push batch 10, ~300 B JSON, one long-poll consumer per queue,
1 partition per queue; bytes split by record kind):

| shape | qlog bytes / 60 s | messages | entry records | avg entry | copies (weighted) | outcome |
|---|---|---|---|---|---|---|
| 1 queue × 1000 partitions | 639 MB | 427 MB | 210 MB | 848 B | 1 | healthy, push p99 9 ms |
| 100 queues × 1 | 5.3 GB | 426 MB | 4.83 GB | 9.8 KB | 22 (max 100) | survives, 285 MB/s |
| 1000 queues × 1 | 34.4 GB | 266 MB | 34.2 GB (99.2%) | 112 KB | 286 (max 828) | collapse: 23k/50k accepted, push p50 8 s |

Cause: `QlogWrite::write_group_nosync` writes the SAME payload-free entry
record (every command, outcome and effect of the cycle — hashes, pop claims,
cursor sets, group upserts) into EVERY queue log the entry touches, with
`copies` = number of logs. An entry of `B` bytes touching `k` logs costs
`k × B`. Both `k` and `B` grow with the number of queues a cycle drains, so
bytes per group grow like `queues²`, groups slow down, the batcher drains more
per cycle, entries get bigger and touch more queues: a feedback loop.

## 2. What must not change

1. **Durability rule.** An entry is durable only when EVERY touched log holds
   its part (the group fsyncs every touched log once; recovery keeps the
   gapless prefix of complete entries and truncates the rest).
2. **Payloads before parts.** In each log an entry's payload records precede
   its part, so a complete entry has every payload.
3. **I2.** Recovery hands apply exactly `encode_entry_payload_free(entry)`, the
   same bytes the leader applied live.
4. **Follower rehydration.** `LogStore::read_range` rebuilds a whole entry
   (payload-free record + every `Append` payload as stored) from the queue logs.
5. **Retention floor.** No file holding a record above the recovery floor is
   unlinked.

## 3. Designs

### A. Per-queue slices

Each touched log gets the entry header plus only ITS commands and effects,
each tagged with its ordinal in the entry; recovery reassembles by seq and
checks the re-framed body against the original checksum carried in every slice.

- Bytes: `B + k × (49 record header + ~46 slice header) + 8 × (commands + effects)`.
  Linear, but the per-slice header is paid by every log, so a 1000-queue cycle
  costs about 2 × B.
- Needs the entry codec to emit per-command/per-effect byte spans, a
  command → log assignment (a command's effects can span queues), and a
  re-framing reassembler. The most code, all of it on the I2 path.
- Upside: each queue log holds only its own history (locality, per-queue
  moves between cells). Nothing reads per-queue entry history today.

### B. One whole record + stubs (prototyped)

The lowest touched log id gets the whole record; every other touched log gets
a 61-byte stub: the same record header (`seq`, `copies`, term, `now_us`) and
`xxh3_64(entry bytes) | entry_len`. `copies` still counts every touched log.

- Bytes: `B + (k − 1) × 61`. Smallest of the three.
- Same logs touched, same fsyncs, same durability rule: a stub is a part.
- Recovery: per seq, `found == copies`, exactly the whole records identical,
  every stub's digest and length equal to the whole record's, else refused.
  Zero whole records with all stubs present is refused (corruption).
- The reader accepts both layouts, mixed, so the knob can flip on a live
  directory. The follower wire format is untouched (the layout is node-local).
- Where the whole record goes (reader does not care, so this can change later):
  - **lowest touched id** (prototype): no extra log, no extra fsync; the system
    log (id 0) wins whenever it is touched. Skew: the few lowest-id queues home
    most multi-queue entries. A slow consumer on such a queue keeps those bytes
    until its own files are reclaimable — never more than today, where every
    touched log keeps a whole copy.
  - **system log always (k ≥ 2)**: entry bodies never live in queue logs and
    are reclaimed by the floor alone; costs one more fsync per multi-queue
    group (+50% at k = 2, noise at k ≥ 20).
  - **spread by seq** (`touched[seq % k]`): even file sizes, same coupling as
    lowest-id.

### C. Cap distinct queues per entry (batcher)

Cut a cycle's entry when it would touch more than `K` queues (e.g. 8).

- Bytes: at most `K × B`. Still multiplicative, just bounded.
- Does NOT reduce fsyncs per group (the group still touches every queue it
  touched) and costs more entries per cycle; it changes batcher/planner shape.
- Useful only as defence in depth on top of A or B, not as the fix.

### Comparison

| | bytes per entry | fsyncs per group | recovery change | code touched | compat |
|---|---|---|---|---|---|
| today (copies) | k × B | k | — | — | — |
| A slices | ~B + 95k + 8·(cmds+effs) | k | reassemble + re-frame | entry codec, writer, readers | new kind |
| **B stubs** | **B + 61(k−1)** | k (+1 if system home) | merge by digest | writer, readers | new kind, reader-first |
| C cap | ≤ K·B | k | none | batcher/planner | none |

Recommendation: **B, home = lowest touched id, together with the
preallocation gate of §6** (alone, stubs turn preallocation on for every log
and the zeros replace the copies). Ship the reader first, then flip both.

## 4. The prototype (B)

- `qlog/record.rs`: `REC_ENTRY_STUB = 3`, `encode_entry_stub_into`,
  `is_entry_kind`, `entry_digest`, `stub_fields`; decode and the dedup prefix
  read accept kind 3.
- `qlog/mod.rs`: `WriteRecord::EntryStub`, `EntryStub`, `EntryPart`; one parts
  scanner `QLog::entry_parts_between` (replaces the two copy-pasted readers;
  `entry_records_from/between` keep their meaning, whole records only); the
  index build, the recovery cut and compaction treat stubs as entry records.
- `qlog/set.rs`: `EntryLayout` (`QUEEN_QLOG_ENTRY_LAYOUT=copies|stub`,
  default copies); `EntryMerge` + `Parts::verdict`, shared by
  `QLogSet::scan_entries` and `QLogReader::entry_records_range` (they had two
  copies of the merge).
- `replicator/local.rs`: `QlogWrite.layout`; `write_group_nosync` writes the
  whole record in the lowest touched log and stubs in the rest (digest computed
  once per entry, only when k ≥ 2); `LocalReplicator::open_with_entry_layout`
  for tests. `raft/log_store.rs` reads the env at open.
- `qlog/mod.rs`, preallocation: `QUEEN_RAFT_QLOG_PREALLOC_MIN_SYNCS`
  (default 0 = today) and the pure rule `prealloc_run` (§6).
- Rollout: the reader must be on every node before any node writes stubs (a
  binary without kind 3 refuses the files, including ones a snapshot ships).
  Stubs and the preallocation gate go on together (§5, §6).

Tests (all in `qlog/`; 56/56 green with the existing ones before the
preallocation test was added — see §8 for the full run):

- `tests::prealloc_waits_for_sustained_traffic_and_sizes_the_run_to_it`

- `record::an_entry_stub_round_trips_and_is_not_a_message`
- `stub_parts_merge_into_whole_entries` (stubs across sealed files, mixed with
  the copies layout, range read names every part's log)
- `a_missing_stub_or_whole_record_leaves_the_entry_not_durable` (both cases:
  scan stops, range read stops, cut drops the surviving part + its payload)
- `stubs_that_do_not_name_the_whole_record_or_stand_alone_are_refused`
- `the_stub_layout_writes_each_entry_once` (real `LocalReplicator`: placement,
  exact byte saving vs copies, recovery bytes = payload-free encoding)
- `stub_layout_a_lost_part_is_cut_and_its_index_reused` (real writer bytes;
  the stub or the whole record of the last entry cut with its payload record;
  replay from an older checkpoint lands on the state before it, digest-equal;
  the index is reused)
- `the_queue_logs_alone_recover_acked_work_stub_layout` (+ pipelined): the
  existing end-to-end replay proof, on stubs
- `read_range_rehydrates_stub_layout_entries` (openraft `LogStore`: evicted
  entries and payload-free recovered entries come back whole — record and
  every payload; a lost stub shortens the log to the durable prefix)

## 5. Measurements (local harness)

Laptop (Apple Silicon, APFS, `F_FULLFSYNC`), the prototype binary
(`fastrel`), single-node openraft, `QUEEN_LANES=1`, one queue-log lane, push
batch 10, ~300 B JSON, one long-poll consumer per queue, 1 partition per queue,
40 s per run on a fresh data dir; bytes split by record kind with
`harness/qlogbytes.py` (the 09-25 parser plus kind 3). Nothing was reclaimed
during a run, so bytes on disk = bytes written. Harness and raw results:
`benchmark-queen/2026-09-25-qlog-entry-stubs/`.

### 5.1 Layout only (preallocation as today)

Bytes per message = bytes on disk ÷ messages in the logs. "entries" = whole
entry records + stubs; "zeros" = the preallocated tail.

| shape (5k msg/s unless noted) | layout | MB | B/msg | messages | entries | zeros | avg entry | copies | push p50 / p99 ms |
|---|---|---|---|---|---|---|---|---|---|
| 1 queue × 1000 partitions | copies | 44.7 | 238 | 155 | 75 | 8 | 760 B | 1.0 | 8 / 15 |
| 1 queue × 1000 partitions | stub | 44.7 | 238 | 155 | 74 | 9 | 845 B | 1.0 | 10 / 32 |
| 100 queues × 1 | copies | 577 | 3,077 | 155 | **2,484** | 438 | 12.7 KB | 35.5 | 122 / 224 |
| 100 queues × 1 | stub | 115 | 612 | 155 | **80** | 376 | 4.9 KB | 14.1 | 93 / 210 |
| 1000 queues × 1 | copies | 1,139 | 6,081 | 155 | **5,471** | 455 | 26.6 KB | 75.6 | 124 / 1,384 |
| 1000 queues × 1 | stub | 1,058 | 5,695 | 155 | **82** | **5,459** | 36.2 KB | 100.8 | 1,417 / 2,310 |
| 1000 queues × 1, 10k msg/s | copies | **10,853** | 44,065 | 155 | 43,905 | 4 | 313 KB | 695.6 | 9,372 / 17,433 — collapse: 4.7k/s pushed, 3,964 push + 386 pop errors |
| 1000 queues × 1, 10k msg/s | stub | 1,069 | 2,897 | 155 | 69 | 2,673 | 94 KB | 221.5 | 2,867 / 4,227 — 9.96k/s pushed, 0 errors, consumers keep up |

- Entry bytes per message fall 31× (100 queues), 67× (1000) and 637× (1000
  at 10k); with one queue they are identical (k = 1 writes no stub).
- At 1000 queues the saving is eaten by zeros: every log now preallocates
  (§6). Total bytes only drop once preallocation is gated (§5.2).

### 5.2 Stubs + the preallocation gate (`MIN_SYNCS=64`)

| shape | before | after | ratio |
|---|---|---|---|
| 1000 queues × 1, 5k msg/s, consumers (runs back to back) | copies + gate: 3,877 MB, 20,678 B/msg, push p99 4.75 s, 4.5k/s pushed | stub + gate: **44 MB, 237 B/msg**, push p99 2.57 s, 4.9k/s pushed | 88× bytes |
| 1000 queues × 1, 5k msg/s, push only | stub, prealloc as today: 1,052 MB (1,012 zeros) | stub + gate: **40 MB, 214 B/msg**, 0 zeros | 26× |
| 2000 idle queues + parked consumers, 25 s | stub, prealloc as today: 2,099 MB (all zeros) | stub + gate: **1.0 MB** | 2,000× |
| default config vs full fix, 1000 queues × 1, 5k msg/s | copies, prealloc as today: 1,139 MB, 6,081 B/msg | stub + gate: 44 MB, 237 B/msg | 26× |

With both, a message costs its record (155 B) plus ~80 B of entry parts, at
1 or 1000 queues: linear in the traffic, not in queues × entry.

Not fixed: 1000 queues × 1 at **20k msg/s**, stub + gate, still collapsed on
the laptop (4.0k/s pushed, 14.6k push errors) while its bytes stayed at
219 B/msg (50 MB). Its groups fsync hundreds of logs each: the fsync-count
wall of §7, which no layout changes.

### 5.3 Latency on this laptop: fsync-bound, and it drifts

`F_FULLFSYNC` on APFS costs a device flush per call, so a group's time
follows the NUMBER of logs it fsyncs, which stubs do not change (§7), not its
bytes: the group fsync averaged 80 ms (copies) and 85 ms (stub) in the two
back-to-back gated runs above, although one wrote 88× the bytes of the other.
The device also slowed as the sweep wrote tens of GB: the same copies run took
124 ms push p50 as the 3rd run of the day and 1,012 ms as the 13th. Single
runs are not comparable across the sweep; the ABBA pairs below are.

ABBA order (stub, copies, copies, stub), preallocation OFF, 20 s cooldown
before each run, 1000 queues × 1, 5k msg/s:

| run | MB | B/msg | entries B/msg | copies | pushed/s | push p50 / p99 ms | group fsync | propose round trip | cycle size (cmds) |
|---|---|---|---|---|---|---|---|---|---|
| stub 1 | 44.2 | 236 | 81 | 42 | 4,810 | 1,335 / 2,310 | 80 ms | 213 ms | 109 |
| copies 1 | 2,780 | 14,906 | 14,750 | 189 | 4,795 | 545 / 2,310 | 74 ms | 205 ms | 72 |
| copies 2 | 5,020 | 26,773 | 26,618 | 341 | 4,823 | 1,434 / 2,343 | 82 ms | 222 ms | 102 |
| stub 2 | 43.9 | 236 | 80 | 38 | 4,685 | 1,466 / 2,900 | 76 ms | 198 ms | 93 |

| 100 queues × 1: stub | 44.2 | 236 | 80 | 18 | 5,002 | 107 / 177 | 29 ms | | |
| 100 queues × 1: copies | 448 | 2,390 | 2,235 | 33 | 5,004 | 109 / 171 | 30 ms | | |

- Bytes: stubs write 10× less at 100 queues and 63–114× less at 1000, and
  stay at 236 B/msg in every shape, while copies drift from 14.9 to
  26.8 KB/msg between two identical 1000-queue runs (the loop in §1).
- Latency on this laptop: no gain and no clear loss. The engine's own
  timings (group fsync, propose round trip, plan, apply) match within a few
  percent. Push p50 swings 545 → 1,434 ms between two identical copies runs.
  The group time is the number of `F_FULLFSYNC`s, which is the same.

The VM (ext4, `fdatasync`, 900 MB/s of copies at collapse) is where the bytes
turn into latency; the laptop only shows the bytes and the fsync-count floor.

## 6. Preallocation

Today (`prealloc_after_write`): a log gets a 1 MiB zero run as soon as it has
seen ONE sync and its bytes-per-sync estimate is under 32 KiB; the run is
refilled when under a quarter is left. The 09-25 VM data shows what that costs
when logs are many and cold: 782 MB of zeros for 1000 push-only queues, 12.9 GB
after 10k idle queues' create + first contact (≈1.3 MB per queue).

The stub layout makes this worse, not better: with copies, a log of a
1000-queue cycle took ≥ 32 KiB per sync (the copies), which switched
preallocation OFF; with stubs every log is small per sync, so all of them
preallocate. Measured above: zeros went from 85 MB to 1,013 MB at 1000 queues
and the group fsync from 39 to 223 ms. **Stubs need a preallocation gate.**

Proposal: preallocate only after sustained traffic, sized to it.

- Gate on the log's sync count: nothing until it has been synced `N` times
  (`QUEEN_RAFT_QLOG_PREALLOC_MIN_SYNCS`, prototyped; proposed default 64). A
  sync count needs no clock (the qlog reads none, I2 hygiene) and a queue's
  create + first contact never reaches it.
- Size the run to `N × bytes-per-sync`, clamped to [64 KiB, 1 MiB]: about `N`
  syncs of overwrite. A growing `fdatasync` on ext4/xfs costs one journal
  commit (~24 KB + a second flush), so a run of `N × bps` zeros saves about
  `N` commits; at bps ≤ 16 KiB the bytes are a wash and the flushes are the win.
- On APFS (copy-on-write) an overwrite allocates anyway, so preallocation
  saves nothing and should stay off on macOS.

Measured (laptop, stub layout, `MIN_SYNCS` 0 = today vs 64):

| shape | today | gate 64 | push p50 / p99 today → gate |
|---|---|---|---|
| 1000 queues push-only, 5k msg/s, 40 s | 1,052 MB (1,012 MB zeros) | 40 MB (0 zeros) | 30 / 66 → 32 / 54 ms |
| 2000 idle queues + parked consumers, 25 s | 2,099 MB (2,098 MB zeros) | 1.0 MB (0 zeros) | — |
| 1000 queues + consumers, 5k msg/s | 1,058 MB (1,013 MB zeros) | 44 MB (0 zeros) | see §5.3 (fsync-bound) |

What the gate does NOT settle: whether hot logs on ext4 still gain from the
run once they pass 64 syncs. That is the VM measurement to make: a
1-queue × 1000-partition run (the healthy shape, one hot log) with the gate
at 0 and 64, `fdatasync` count and device writes per group.

Recommendation: yes, start preallocation only after sustained traffic
(`MIN_SYNCS` default 64 on Linux, preallocation off on macOS), and ship it
with the stub layout or before it.

## 7. The next wall: fsync fan-out

Stubs fix bytes, not the NUMBER of fsyncs: a group still fsyncs every touched
log (286 per group at 1000 queues). `QLogSyncer::sync` also spawns one scoped
thread per extra log per group (3–7k spawns/s measured). Two follow-ups:

1. A persistent fsync pool (N workers, N ≈ device queue depth) instead of a
   scoped spawn per log per group.
2. Fewer logs per group for cold queues: queues below a traffic threshold
   share one log (their payload records included), so a group's fsync count is
   `hot queues + 1`. Bigger change: per-queue locality is lost for cold queues.

## 8. Verification

- Library suite, default layout: 1,341 passed, 1 failed:
  `raft_cluster::a_follower_behind_the_purge_point_gets_a_snapshot`, a 60 s
  timing assertion. It passed 3/3 alone and passed in the next full run.
- Library suite with `QUEEN_QLOG_ENTRY_LAYOUT=stub
  QUEEN_RAFT_QLOG_PREALLOC_MIN_SYNCS=64` for EVERY test (every replicator,
  cluster, snapshot, crash-replay and facade test on stubs): 1,341 passed,
  1 failed: `qlog::tests::prealloc_zero_run_is_cut_at_roll_and_reopen`. That
  test asserts a zero run after 20 syncs, which the gate forbids by design. It
  passes with stubs alone, and needs `≥ 64` syncs (or the gate pinned off) if
  the gate becomes the default.
- Restart on the laptop, 1000 queue logs after a 40 s run at 5k msg/s (gate
  on): copies 4,714 MB → 4.87 s to healthy; stub 44 MB → **0.25 s**. Both
  recovered cleanly (no refusal, no tail cut).
