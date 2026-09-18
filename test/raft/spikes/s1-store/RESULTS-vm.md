# S1 VM campaign — redb 2.6.3 vs fjall 2.11.2 vs heed 0.22.1 (LMDB)

WP-0.3 part 2, the Linux half. Every number below was produced on the project
VM; the laptop half (60 s smoke, 10 kill runs per engine, 10M-key iteration) is
in `RESULTS-laptop.md` and is smoke only (macOS serialises `F_FULLFSYNC`).

> **⚠ 2026-09-18: §1 measures the PRE-G0 store shape.** These cells used one
> store write transaction **per applied entry**, `segments` rows in the store and
> no dedup keyspace. G0 then amended §11.3 (commit every 4 ms / 256 entries),
> §6.1 (`segments` is not in the store) and ratified D10 option (a) (a dedup
> index of random `(pid, hash)` keys). Re-measured in that shape,
> `RESULTS-refutation.md` §2: **fjall and heed are unchanged; redb is not** —
> 20k → 19 466 msg/s (was 10 046), 50k → 25 895 (was 10 260), kernel write
> amplification 8.66x (was 29.31x), with a 2×2 isolating the cause to the commit
> cadence. §1's redb "achieved" and "WA" figures are historical. §2–§6 (the crash
> criteria) are unaffected by the shape change and still stand as written.

**Host** `root@164.90.215.224` (`queenpgless-01`), Ubuntu 24.04, kernel
6.8.0-124-generic, 8 vCPU, 15 GB RAM, ext4 on `/dev/vda1` (193 GB, 187 GB free
at the start), Rust 1.98.1 stable. The VM's Postgres was not used and not
touched. Date: 2026-09-17.

**Build**

```
rsync -a --exclude target --exclude node_modules --exclude results \
      test/raft/spikes/s1-store/ root@164.90.215.224:/root/raft/s1-store/
ssh root@164.90.215.224 '. ~/.cargo/env && cd /root/raft/s1-store && cargo build --release --locked'
```

rc=0 in **16.3 s** (`/root/raft/s1-store/build.log`), no cmake anywhere: heed
compiles LMDB with `cc`, redb and fjall have no build script. This is the first
time the crate's Linux `cfg` paths ran (`sys.rs` reads `/proc/self/status` and
`/proc/self/io`, `seg.rs` uses `fdatasync`/`fsync`); both work — the write
amplification below comes from the kernel's own per-process counter.
MSRV was not re-checked here (the VM has only the stable toolchain); the
`cargo +1.88 check` evidence is the laptop's `results/msrv-1.88.log`.

**Budget.** Alice's budget for 2026-09-17 was 45 minutes of VM wall clock
beyond the build. The campaign took **33 min 16 s** (14:00:00 → 14:33:16 UTC,
`results-vm/campaign-20260917-140000.log`) and three extra runs (§6) took the
total to **39 min 34 s** (ended 14:39:34 UTC). The long runs WP-0.3 actually
asks for are **deferred**, listed in §7. Nothing was left running on the VM and
no loop or dm device survived (`losetup -a` and `dmsetup ls` both empty, no
`flakey` mount, disk back to 6.8 GB used of 193 GB).

```
./vm-campaign.sh short           # step 1 matrix 852 s, 2 kill 287 s, 3 flaky 221 s, 4 soak 636 s
```

`vm-campaign.sh` is in this crate: `./vm-campaign.sh full` runs the deferred
long version of exactly the same four steps.

---

## 1. Apply stream: 3 engines × {20k, 50k, 100k} msg/s × 90 s

```
EXTRA="--segment-bytes 262144" ENGINES="redb fjall heed" RATES="20000 50000 100000" \
  OUT=./results-vm DATA=/root/raft/s1-store/data ./run-matrix.sh 90
```

Defaults: batch 10 (→ rate/10 entries/s), payload 512 B, 4096 partitions, 64
queues, 1 group, KV = rate×0.4 ops/s over 200k keys, timers = rate/50, durable
point every 1000 ms, 1 fsync thread, 256 MiB store cache. `--segment-bytes
262144` (not the plan's 64 MiB) so files roll inside 90 s and reclamation is
observable — the same choice as the laptop smoke, identical for all engines.

| engine | rate | achieved msg/s | non-dur p50 | non-dur p99 | durable p50 | durable p99 | of which seg fsync p50 | WA (kernel) | WA (files) | store growth | seg growth | RSS max | KV get p99 | KV list p99 | ordered scan | reopen |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| redb | 20k | **10 046** | 0.399 ms | 0.543 ms | 1409 ms | 1802 ms | 336 ms | **29.31x** | 2.42x | 855 MiB | 453 MiB | 250 MiB | 0.024 ms | 0.041 ms | 10.5 M rows/s | 2 ms |
| redb | 50k | **10 260** | 0.399 | 0.543 | 1311 | 1671 | 311 | 29.36x | 2.37x | 855 | 468 | 251 | 0.024 | 0.041 | 11.5 M/s | 1 ms |
| redb | 100k | **9 909** | 0.399 | 0.543 | 1376 | 1933 | — | 29.28x | 2.46x | 867 | 449 | 250 | 0.024 | 0.041 | 11.1 M/s | 2 ms |
| fjall | 20k | **20 000** (100%) | 0.047 | 0.295 | 360 | 606 | 352 | **1.38x** | 0.89x | 58.7 | 901 | 155 | 0.037 | 0.119 | 3.25 M/s | 3 ms |
| fjall | 50k | **49 905** (100%) | 0.042 | 0.175 | 573 | 885 | 541 | 1.35x | 0.89x | 141 | 2246 | 554 | 0.032 | 0.106 | 4.75 M/s | 4 ms |
| fjall | 100k | **67 641** (68%) | 0.044 | 0.171 | 688 | 1032 | — | 1.36x | 0.89x | 187 | 3044 | **717** | 0.038 | 0.097 | 2.12 M/s | 5 ms |
| heed | 20k | **20 000** (100%) | 0.065 | 0.122 | 475 | 786 | 344 | 3.30x | 1.00x | 172 | 901 | 182 | **0.003** | **0.011** | **44.2 M/s** | 0 ms |
| heed | 50k | **48 783** (98%) | 0.067 | 0.116 | 901 | 1475 | 573 | 2.86x | 1.00x | 420 | 2197 | 445 | 0.002 | 0.011 | 41.7 M/s | 0 ms |
| heed | 100k | **58 958** (59%) | 0.068 | 0.119 | 1016 | 1409 | — | 2.80x | 1.00x | 504 | 2654 | 528 | 0.002 | 0.011 | 42.2 M/s | 0 ms |

Raw: `results-vm/<engine>-<rate>.log`, one `RESULT` line each, collected in
`results-vm/matrix-20260917-140000.tsv`.

### What the table says

1. **redb tops out at ~10 000 msg/s (1 000 entries/s) whatever you offer it —
   IN THIS SHAPE ONLY; see the warning at the top and `RESULTS-refutation.md`
   §2, where the same engine does 19 466 msg/s under §11.3 as amended.**
   Its *non-durable* commit is fine (p50 0.4 ms); the wall is the durable point:
   1409 ms p50 of which only 336 ms is the segment fsyncs, so **redb's own
   durable commit is ≈ 1.07 s** and, scheduled every 1 s, it eats more than half
   the wall clock (39 durable points in 90 s). 29x write amplification measured
   by the kernel confirms why: every `Durability::Immediate` commit rewrites the
   copy-on-write path of 13 trees plus the region tracker.
2. **fjall's durable point is essentially free**: 360 ms of which 352 ms are the
   segment fsyncs → **8 ms of store commit** at 20k, 32 ms at 50k.
   heed sits between: 475 − 344 = **131 ms** at 20k, 328 ms at 50k (msync of the
   dirty mmap pages, and it pays 2.8–3.3x write amplification for page-granular
   COW on random keys).
3. **The durable point is dominated by the segment-file fan-out, not the store.**
   Measured file fsyncs per durable point: 293 (redb, 20k) · 304 → 477 (fjall,
   20k → 100k) · 308 → 484 (heed) — i.e. all 256 buckets every second plus the
   files that rolled, at ≈ 1.2–1.8 ms each on this ext4. This is the laptop
   finding reproduced with real `fdatasync`: **§11.4 has to bound how many
   buckets a durable point may touch**, or an entry's payloads must land in
   fewer buckets between durable points. It costs 340–580 ms per second of wall
   clock today, for every engine.
4. **Ordered reads separate heed from fjall by an order of magnitude**: 42–44 M
   rows/s versus 2–5 M rows/s, KV get p99 2–3 µs versus 32–38 µs, prefix list
   p99 11 µs versus ~100 µs. That is the number the replicated digest (§12.9),
   chunked deletes, `pending`/ready-ring rebuilds and logical exports pay.
5. **Neither fjall nor heed reaches 100k msg/s** in this single-threaded harness
   (68k and 59k): above ~50k the durable point starts to overlap the next one.

### Snapshot export (§11.6 step 3) and reclamation (§11.7), 20k cells

| | redb | fjall | heed |
|---|---|---|---|
| consistent export | logical dump 58.7 MiB in 324 ms (181 MiB/s) | logical dump 115.7 MiB in 939 ms (123 MiB/s) | **`mdb_env_copy` 172.1 MiB in 309 ms (556 MiB/s)**, compacting copy 170.7 MiB in 312 ms |
| incremental? | no (savepoints keep an older version inside the same file) | **yes in principle** (SSTs are immutable → hard-link sealed segments) but no checkpoint API in 2.11 | no, always O(state) |
| writer pause | none (MVCC read txn; held pages are not freed) | none (seqno snapshot) | none (read txn; the free list is pinned, so the file grows while it runs) |
| chunked delete of 52–93k rows | 316 ms | 145 ms | 178 ms |
| store before → after delete → after maintenance | 855 → 855 → **146 MiB** (`compact()` 843 ms) | 55.4 → 60.1 → **39.7 MiB** (`major_compact` 1316 ms) | 172 → 172 → **172 MiB** (no in-place compaction; a compacting copy would be 169 MiB in 116 ms) |
| segment files unlinked | 933 files, 233 MiB | 1736 files, 434 MiB | 1718 files, 429 MiB |

At the 100k cells the same operations cost: redb `compact()` 853 ms, fjall
`major_compact` **3885 ms**, heed compacting copy 305 ms. fjall's compaction
pause grows with the data; see the soak (§5), where it reached 12.3 s.

---

## 2. PASS/FAIL A — `kill -9` during non-durable commits (15 runs per engine)

```
ENGINE=<e> RATE=20000 MODE=mixed DATA=/root/raft/s1-store/data/kill OUT=./results-vm ./kill-loop.sh 15
```

Mixed mode: 8 runs killed externally at a random moment in [3 s, 12 s], 7 runs
raising `SIGKILL` inside the harness at `append` / `precommit` / `postcommit` of
an entry. After each kill the store is reopened and `verify --verify-all` checks
every `seg_loc` row against the bytes on disk.

| engine | runs | PASS | FAIL | reopened past the last durable commit | torn entries | store would not reopen | reopen after kill |
|---|---|---|---|---|---|---|---|
| fjall 2.11.2 | 15 | **15** | 0 | **14 / 15** | 0 | 0 | 234–967 ms (journal replay) |
| heed 0.22.1 | 15 | **15** | 0 | **12 / 15** | 0 | 0 | 0–70 ms |
| redb 2.6.3 (§6) | 15 | **15** | 0 | **0 / 15** | 0 | 0 | 57–465 ms |

No engine ever produced a torn entry across the 13 keyspaces, and every segment
the reopened state referenced was on disk with a matching xxh3 — as expected,
because a process kill leaves the page cache intact. The column that matters is
the third: fjall and heed reopen **past** their last durable commit (up to 2327
segments for fjall and 2456 for heed beyond the recorded file lengths), which is exactly the
state §11.5/I11 is written for. Raw: `results-vm/kill-<engine>-*.log`.

---

## 3. PASS/FAIL B — dropped unflushed writes (dm-flakey), 5 runs per engine

This is the half the laptop cannot produce, and it is where the engines part.

```
ENGINE=<e> RATE=20000 RUN_S=20 BASE=/root/raft/s1-store/flaky OUT=./results-vm ./flaky.sh 5
```

Per run: a 4 GiB file `/root/raft/s1-store/flaky/s1-flakey-<pid>.img` → loop
device → `dm-flakey` (up) → `mkfs.ext4` → mount → 20 s of load → switch the dm
table to `drop_writes` (`dmsetup suspend --noflush --nolockfs`, `load`,
`resume`) → `kill -9` → lazy unmount (its writeback is dropped too) → clean
table back → mount → `verify --verify-all`. From the table switch on, every
write the filesystem issues is silently discarded: a power loss for everything
that was never fsynced.

| engine | runs | PASS | FAIL (state disagrees with the files) | store would not reopen | reopened past the last durable commit |
|---|---|---|---|---|---|
| fjall 2.11.2 | 5 | 1 | **4** | 0 | 4 / 5 |
| heed 0.22.1 | 5 + 5 (§6) | 9 | 0 | **1** | 0 / 10 |
| redb 2.6.3 (§6) | 5 | **5** | 0 | 0 | **0 / 5** |

**fjall fails the mandatory criterion.** In the 4 failing runs the reopened
state was ahead of the last durable commit (journal records that reached the
platter replay) while the segment files were short of what that state
references: `shortfile=256 … 1700` rows whose `(file, offset, len)` lies beyond
the end of the file on disk, `beyond_recorded=1085 … 1891`. No checksum was
wrong and nothing was torn — the bytes are simply not there, because a segment
file is only fsynced at a durable point. Example line:

```
run 5 -> VERIFY FAIL agree=false torn=false applied=38895 durable=37195 past_durable=true
        checked=38896 rows=38896 missing=0 badsum=0 shortfile=1700 cross_table_mismatch=0
        beyond_recorded=1568 open_ms=1898 verify_ms=857
```

The one PASS is the run whose kill landed close enough to a durable point that
applied == durable.

**heed returns to its last durable commit — except when it does not open at
all.** 4 of the 5 runs (runs 1, 2, 3, 5 of
`results-vm/flaky-heed-20260917-142056.log`; plus 5 of 5 in §6's rerun, so 9 of
10 in all, which is what the table above says) reopened with `applied == durable`
and every referenced byte present: LMDB's meta page that reached the platter is
the one the durable `mdb_env_sync` wrote. But one run **would not reopen**
(`VERIFY DID NOT RUN`): with `MDB_NOSYNC` the OS may write back a newer meta
page whose data pages never landed, which is the corruption LMDB's own
documentation warns about for that flag. The rerun in §6 names the failure mode.

What this means for the design is in `MEMO.md` §4; in one line: **after a power
loss, an engine that reopens past its durable point can reference payload bytes
that do not exist, and neither fjall nor LMDB can be rolled back to the durable
point**, so the node must discard its state directory and install a snapshot
(raft3) or refuse to start (raft1) — I11's escape hatch becomes the normal path,
not the exception.

No loop device, dm device or mount survived: `losetup -a` and `dmsetup ls`
printed nothing after the step (in the campaign journal).

---

## 4. Soak — fjall, 50k msg/s, 10 minutes, with a delete phase at minute 6

```
./target/release/s1-store run --engine fjall --dir /root/raft/s1-store/data/soak-fjall \
  --rate 50000 --duration 600 --segment-bytes 4194304 --sample-ms 5000 \
  --growth-every-s 30 --delete-at-s 360 --delete-pct 30
```

(`--sample-ms`, `--growth-every-s`, `--delete-at-s`, `--delete-pct` were added
to the harness for this run; `MIDDELETE` and `GROWTH` are printed lines.)

Sustained **48 621 msg/s** (97% of target) for 600 s: 2 917 232 entries,
29 172 320 messages, 402 durable points. Non-durable commit p50 0.052 ms,
p99 0.203 ms, p99.9 2.0 ms; durable point p50 442 ms, p99 688 ms, **max 2400 ms**;
WA(kernel) 1.74x (up from 1.35x in the 90 s run — compaction rewrites).
402 durable points fsynced 106 305 segment files in all (264 per point).

| t (s) | 30 | 90 | 150 | 210 | 270 | 330 | 375 | 435 | 495 | 585 |
|---|---|---|---|---|---|---|---|---|---|---|
| RSS (MiB) | 152 | 566 | 717 | 779 | 765 | 827 | 823 | 850 | 901 | **978** |
| store (MiB) | 65 | 145 | 210 | 278 | 355 | 423 | 297 | 409 | 490 | 594 |
| segments (MiB) | 751 | 2250 | 3749 | 5260 | 6778 | 8283 | 6828 | 8337 | 9838 | 12 071 |
| segment files | 281 | 687 | 1063 | 1436 | 1816 | 2214 | 1840 | 2399 | 2585 | 3140 |

**RSS is not flat.** It rose from 4 MiB to 986 MiB over ten minutes and was
still climbing at the end, roughly with the number of live rows (the 256 MiB
block cache is only a quarter of it). The delete phase did not bring it down
(823 MiB before → 823 MiB after). I8 asks for RAM bounded independently of
retained volume; over 10 minutes at 50k msg/s fjall does not show that, and a
2 h soak is needed to say whether it plateaus (deferred, §7).

**Delete phase at t = 374.5 s** (`MIDDELETE`): dropped the oldest 30% —
540 008 of 1 797 240 segment rows — in **1.777 s** of chunked deletes, then
unlinked **541 sealed segment files (2162.8 MiB)**, then `rotate_memtable +
major_compact`:

| | before | after the deletes | after maintenance |
|---|---|---|---|
| store | 472.5 MiB | 479.2 MiB (grows: tombstones) | **289.9 MiB** |
| segment files | 8994.3 MiB | 6828.4 MiB (files unlinked) | 6828.4 MiB |

Reclamation works and is cheap on the file side (unlink) — but fjall's
`major_compact` took **12.3 s**, single-threaded and stop-the-world for the
writer. On the apply thread (the only mutator, §3.3) that is a 12-second stall
of the whole node; at the end of the run the same call took 15.3 s. §11.7's
compaction must therefore be incremental and budgeted, never a major compact.

Other soak numbers: consistent export (a snapshot's store part) 1125 MiB in
**12.1 s** (93 MiB/s, O(state)); full ordered scan of 2.38 M `segments` rows at
**1.35 M rows/s** (3.25 M/s in the 90 s run — it degrades as the LSM deepens);
clean reopen 13 ms.

---

## 5. Cut, and honestly

- The matrix is **90 s per cell, not the 2 h WP-0.3 asks for**; the soak is
  **10 min, not 2 h**; the kill loop is **15 runs, not 100**; the flaky loop is
  **5 runs per engine, not 20**. Alice's budget for today was 45 minutes of VM
  time. §7 lists what that leaves unanswered.
- The matrix used `--segment-bytes 262144`, not the plan's 64 MiB, so that files
  roll and reclamation can be measured inside 90 s. It inflates the number of
  files fsynced per durable point (rolled files are fsynced once) and therefore
  the durable-point cost; the soak used 4 MiB and shows the same shape.
- The 10M-key ordered-iteration run that the laptop could not finish for redb
  (`RESULTS-laptop.md` §4) was **not** repeated here: the budget went to the two
  PASS/FAIL criteria instead. The redb iteration figure is still missing.
- No `cargo +1.88 check` on the VM (stable toolchain only); MSRV evidence stays
  the laptop's.
- Single VM, single process, one fsync thread; three brokers on one disk (§13.6)
  was not simulated.

---

## 6. Extra runs after the campaign (the rest of the 45 minutes)

The `short` campaign carries only the top two engines by throughput (fjall,
heed) into the two crash steps. That left redb without a Linux verdict on the
criterion that decides WP-0.3, and left heed's single "would not reopen" run
unexplained, so the remaining budget bought three more runs (14:33 → 14:39:34,
`results-vm/extra.log`; total VM wall clock **39 min 34 s** of the 45 allowed):

```
ENGINE=redb RATE=20000 RUN_S=20 BASE=/root/raft/s1-store/flaky OUT=./results-vm ./flaky.sh 5
ENGINE=heed RATE=20000 RUN_S=20 BASE=/root/raft/s1-store/flaky OUT=./results-vm ./flaky.sh 5
ENGINE=redb RATE=20000 MODE=mixed DATA=/root/raft/s1-store/data/kill OUT=./results-vm ./kill-loop.sh 15
```

- **redb, dropped writes: 5 / 5 PASS, 0 / 5 past the durable commit.** Every run
  reopened with `applied == durable`, `shortfile=0`, `beyond_recorded=0`. What
  the laptop saw under `kill -9` holds under real dropped writes: `Durability::None`
  never publishes a crash-visible root, so redb comes back exactly at its last
  durable commit and can never reference segment bytes that were not fsynced.
- **redb, `kill -9`: 15 / 15 PASS, 0 / 15 past the durable commit**, no torn
  entries, reopen 129–140 ms.
- **heed, dropped writes, second 5 runs: 5 / 5 PASS, 0 past durable** — the
  "would not reopen" of §3 did **not** reproduce, so heed stands at 9 PASS /
  1 no-reopen over 10 runs. The rerun used a patched `flaky.sh`/`kill-loop.sh`
  that records the exit status and the last lines of a failed `verify`; since
  the failure did not recur, **its failure mode is still unnamed** — the
  original run left no error text, which is consistent with the process dying on
  a signal (a faulting mmap) rather than returning an LMDB error, but that is an
  inference, not a measurement. Naming it needs the 20-run loop (§7).

---

## 7. Deferred — what these short runs cannot show

**2026-09-18 update.** The 2026-09-18 refutation round (`RESULTS-refutation.md`)
spent its 10-minute budget on the store shape, the untested LMDB flag and the
read path, so **none of the rows below were run**. The first two are the ones
that decide D9 and the first is this spike's own stated ratification
precondition; `MEMO.md` §6.9 lists them in priority order.

| deferred run | command | what it would answer |
|---|---|---|
| **2 h matrix** per engine per rate | `./vm-campaign.sh full matrix` | steady-state write amplification, whether fjall's compaction backlog catches up or diverges, whether heed's file keeps growing, durable-point cost once the store is tens of GiB |
| **2 h soak** | `./vm-campaign.sh full soak` | whether fjall's RSS plateaus or keeps climbing (§4 is 10 minutes and still rising), the compaction pause distribution over hours, disk growth vs reclamation over many delete cycles |
| **100 kill -9 runs** per engine | `./vm-campaign.sh full kill` | the rare kill positions: inside the engine's own durable commit, during a compaction, during a file roll. 15 runs cover the common ones only |
| **20 dropped-write runs** per engine | `./vm-campaign.sh full flaky` | the *rate* of each failure mode. With 5 runs we know fjall fails and LMDB can fail to open, not how often |
| 10M-key ordered iteration for redb | `./target/release/s1-store iter --engine redb --dir … --iter-keys 10000000` | the one laptop number that had to be abandoned |
| dm-delay / ENOSPC | not in this crate | I15 (stalled disk) and §11.8, which belong to the broker, not to the store spike |
