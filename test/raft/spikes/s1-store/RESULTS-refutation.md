# S1 refutation round — 2026-09-18

Two adversarial reviews of `MEMO.md` came back `refuted` (one *major*, one
*blocker*). This file is the measurement half of the answer: what was re-run,
on what, with which commands, and what came out. `MEMO.md` §6 lists every
finding and what happened to it; nothing here is a summary of that list.

The two reviews agree on one thing above all, and it is correct:

> **the first campaign measured a store the G0 amendments deleted.**

`RESULTS-vm.md` was produced on 2026-09-17. G0 ratified on 2026-09-18 and
amended two things the whole S1 comparison rests on:

| what G0 changed | PLAN_RAFT.md | what the 2026-09-17 campaign measured |
|---|---|---|
| the store transaction is **not** per applied entry: one write txn held open, committed every `QUEEN_RAFT_STORE_COMMIT_MS` (4) or `QUEEN_RAFT_STORE_COMMIT_ENTRIES` (256) | §11.3 | one non-durable write txn **per entry** (2 000/s at the 20k cell) |
| `segments` is **not in the store**: one immutable `.qidx` per sealed file, the active file's index in RAM | §6.1 | `segments` rows in the store — and the memo's ordered-scan pillar was measured on exactly that table |
| D10 ratified dedup option (a): a store index `(pid, hash) → (offset, created_at)` pruned by the txns window | §2, §6.1 | no dedup keyspace at all (13 tables, none of them dedup) |

redb was dropped on a ~10 000 msg/s ceiling and 29.31x kernel write
amplification that `RESULTS-vm.md` §1 finding 1 attributes, in its own words, to
"every commit rewrites the copy-on-write path of 13 trees plus the region
tracker" — i.e. to the per-commit cost that a 256:1 batch amortizes. `MEMO.md`
§3 even wrote its own escape clause ("it deserves a re-measurement with a longer
durable interval and fewer keyspaces before anyone believes the ceiling is
final"). That condition fired at G0 and the re-measurement had never been run.
It has now.

---

## 0. What was added to the harness

All of it is in this crate, all of it is off by default so the 2026-09-17 cells
stay reproducible byte for byte.

| flag | what it does |
|---|---|
| `--shape legacy\|ratified` | `legacy` = the 2026-09-17 shape. `ratified` = §11.3 batching + no `segments` rows + a `dedup` keyspace |
| `--store-commit-ms`, `--store-commit-entries` | §11.3's commit cadence (4 / 256). `1` / `0` = the old per-entry shape |
| `--segments-in-store`, `--dedup-rows`, `--dedup-window-s` | the three shape switches on their own |
| `--heed-flags nosync\|nometasync\|both\|none` | which LMDB durability flag the env is opened with. The spike had only ever measured `nosync` |
| `--heed-no-tls`, `--heed-max-readers` | heed's `read_txn_without_tls()` (MDB_NOTLS) and LMDB's reader-slot table size |
| `reads` subcommand | the D15 read path from **N threads**, in both reader modes, plus a reader-slot ceiling probe |

`dedup` rows are one per **message**, key `(pid BE, 8 uniformly random bytes)`,
value `(offset, created_at)` — D10 option (a)'s `(pid, hash)`. At batch 10 that
is 10 random-key inserts per entry, which makes the store's write volume in the
ratified shape *higher* than in the legacy shape, not lower: one `segments` row
per entry goes away, ten `dedup` rows arrive.

---

## 1. The VM run

**Host** `root@164.90.215.224` (`queenpgless-01`), Ubuntu 24.04, kernel
6.8.0-124-generic, 8 vCPU, 15 GB RAM, ext4 on `/dev/vda1`, Rust 1.98.1 stable.
Date 2026-09-18. The VM's Postgres was not used and not touched; nothing under
`/root/queen` was read or written.

**Before starting**, `pgrep -af raft` showed another agent's job running
(`/root/raft/wp06-refute/s4-transport/run-vm-refute.sh`, PID 147042, started
06:16:57 UTC). It was NOT mine, so nothing was killed and nothing was started
until it printed `done` at 06:25:05 and its processes were gone. My campaign ran
**06:26:12 → 06:34 UTC, about 8 minutes of VM wall clock**, inside the 10 minutes
this task allows.

```
rsync -a --exclude target --exclude results --exclude results-vm --exclude data \
      test/raft/spikes/s1-store/ root@164.90.215.224:/root/raft/s1-store/
ssh root@164.90.215.224 '. ~/.cargo/env && cd /root/raft/s1-store && cargo build --release --locked'   # rc=0, 2.2 s (incremental)
ssh root@164.90.215.224 'cd /root/raft/s1-store && nohup ./vm-refute.sh &'
```

`vm-refute.sh` is in this crate. Afterwards: no `s1-store` process left,
`dmsetup ls` "No devices found", `losetup -a` empty, `data-refute/` removed,
disk back to 8.5 GB used of 193 GB (8.4 GB at the start). Raw logs are in
`results-vm/ratified-*.log`, `results-vm/iso-*.log`, `results-vm/reads-heed.log`
and the journal `results-vm/refute-20260918-062612.log`.

**Cells are 40 s, not the 90 s of the 2026-09-17 matrix and not the 2 h WP-0.3
asks for.** Where a 2026-09-17 number is quoted beside a new one, the older one
came from a 90 s cell without a dedup keyspace. Both are short.

---

## 2. The ratified store shape, three engines

```
ENGINE=<e> ./target/release/s1-store run --engine <e> --dir <d> --shape ratified \
  --store-commit-ms 4 --store-commit-entries 256 --rate <r> --duration 40 \
  --segment-bytes 262144 --export 0 --reclaim 0
```

Everything else as the 2026-09-17 matrix: batch 10, payload 512 B, 4096
partitions, 64 queues, 1 group, KV = rate×0.4 ops/s over 200k keys, timers =
rate/50, durable point every 1000 ms, 1 fsync thread, 256 MiB store cache.

| engine | rate | achieved msg/s | (2026-09-17, legacy shape) | store commit p50 / p99 | commits/s | entries per commit | durable p50 | of which seg fsync p50 | ⇒ engine's own durable commit | WA (kernel) | (legacy WA) | store growth | RSS max | get p99 | list p99 | ordered scan | reopen |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| redb 2.6.3 | 20k | **19 466** (97%) | 10 046 | 3.52 / 32.26 ms | 51 | 38.3 | 721 ms | 385 ms | ≈ 336 ms | **8.66x** | 29.31x | 578 MiB | 260 MiB | 21 µs | 40 µs | 7.07 M rows/s | 1 ms |
| redb 2.6.3 | 50k | **25 895** (52%) | 10 260 | 29.70 / 39.94 | 17 | 148.6 | 1049 | 492 | ≈ 557 | 7.88x | 29.36x | 657 | 261 | 14 µs | 34 µs | 7.42 M/s | 1 ms |
| fjall 2.11.2 | 20k | **20 000** (100%) | 20 000 | 0.61 / 9.47 | 138 | 14.5 | 369 | 352 | ≈ 17 | **1.33x** | 1.38x | 49 | **129** | 31 µs | 126 µs | 2.71 M/s | 231 ms |
| fjall 2.11.2 | 50k | **49 019** (98%) | 49 905 | 1.70 / 17.92 | 84 | 58.2 | 541 | 508 | ≈ 33 | 1.33x | 1.35x | 119 | 195 | 29 µs | 124 µs | 2.05 M/s | 457 ms |
| heed 0.22.1 | 20k | **20 000** (100%) | 20 000 | 0.66 / 11.78 | 134 | 14.9 | 492 | 352 | ≈ 139 | 3.91x | 3.30x | 145 | 163 | **4 µs** | **13 µs** | **41.5 M/s** | **0 ms** |
| heed 0.22.1 | 50k | **47 425** (95%) | 48 783 | 1.79 / 16.38 | 65 | 73.1 | 770 | 573 | ≈ 197 | 3.00x | 2.86x | 320 | 349 | 4 µs | 14 µs | 41.5 M/s | 1 ms |
| heed, **MDB_NOMETASYNC** | 20k | **17 309** (87%) | never run | **7.04 / 122.88** | 18 | 95.0 | 885 | 885 | **≈ 0 ms** | 7.04x | — | 132 | 153 | 7 µs | 17 µs | 41.4 M/s | 4 ms |

The ordered scan in the ratified shape is over `seg_loc` (same key as the old
`segments`, §6.2) because `segments` is no longer in the store. The `dedup` scan
is reported separately in each log.

### What changed, and what did not

1. **fjall and heed are unmoved.** Same achieved rates (±2%), same write
   amplification (±0.6 points), same latencies. Batching the store commit and
   swapping one `segments` row per entry for ten random-key `dedup` rows per
   entry costs them nothing measurable at 40 s.
2. **redb is a different engine in this shape**: 10 046 → **19 466** msg/s at
   the 20k target (97% of offered) and 10 260 → **25 895** at 50k, with kernel
   write amplification 29.31x → **8.66x**. `MEMO.md` §3's "it sat at ~10 000
   msg/s whatever we offered it" is false for the store PLAN_RAFT.md now
   describes.
3. **The dedup keyspace is not free, and it shows up exactly where a COW B-tree
   is weakest.** heed's write amplification goes 3.30x → 3.91x at 20k on the
   same offered rate; its store grows 145 MiB in 40 s at 20k against 172 MiB in
   90 s in the legacy shape, i.e. roughly double the rate of growth. This is
   800 010 uniformly random 16-byte keys in 40 s, all of them still in a
   145 MiB file that fits in page cache — the regime D10's real window
   (~72 M rows at A20k with a 1 h window) will not be in. Unmeasured, still.
4. **The durable point is still the segment-file fan-out**, unchanged by any of
   this: 352–573 ms of every second in segment fsyncs (6 700–10 300 file fsyncs
   per 40 s cell). `MEMO.md` item 5 stands as written.

### Isolating why redb moved: a 2×2 at 20k, 40 s each

```
--shape legacy   --store-commit-ms 4 --store-commit-entries 256   # keyspaces old, cadence new
--shape ratified --store-commit-entries 1 --store-commit-ms 0     # keyspaces new, cadence old
```

| keyspaces | store commit cadence | achieved msg/s | WA (kernel) | store growth |
|---|---|---|---|---|
| legacy (`segments`, no dedup) | per entry (2026-09-17) | 10 046 *(90 s cell)* | 29.31x | 855 MiB |
| legacy | §11.3 batched | **19 539** | **8.33x** | 485 MiB |
| ratified (dedup, no `segments`) | per entry | **9 843** | **26.81x** | 772 MiB |
| ratified | §11.3 batched | **19 466** | **8.66x** | 578 MiB |

Raw: `results-vm/iso-redb-legacyshape-batched-20000.log`,
`results-vm/iso-redb-ratifiedshape-perentry-20000.log`.

The cadence explains the whole effect and the keyspaces explain none of it. Per
entry, redb does 985 commits/s and writes 26.8 bytes per logical byte; batched,
it does 62 commits/s at 31 entries each and writes 8.3. **The "~10k msg/s
ceiling and 29x write amplification" that dropped redb from D9 were properties
of one store transaction per applied entry — the thing §11.3 no longer does.**

---

## 3. MDB_NOMETASYNC — the LMDB flag the spike never measured

`MEMO.md` §1 explains heed's one unopenable store as "the corruption LMDB's own
documentation warns about for that flag". The header is more specific than that,
and the difference matters. `lmdb-master-sys-0.2.6/lmdb/libraries/liblmdb/lmdb.h`,
lines 590–602, on **MDB_NOSYNC** (the flag the harness uses):

> Don't flush system buffers to disk when committing a transaction. This
> optimization means a system crash can corrupt the database or lose the last
> transactions if buffers are not yet flushed to disk. […] **However, if the
> filesystem preserves write order and the #MDB_WRITEMAP flag is not used,
> transactions exhibit ACI (atomicity, consistency, isolation) properties and
> only lose D (durability)** […]

`src/engines/heed_eng.rs` opens with `EnvFlags::NO_SYNC` and **without**
`WRITE_MAP`. So for this exact configuration the header predicts integrity, not
corruption, *provided the filesystem preserves write order* — which ext4 does
not promise for data-vs-data ordering. The memo's causal claim is therefore not
supported by the document it cites: either the failure has another cause, or
ext4 on this VM did not preserve write order, and the second reading is a
production hazard rather than a test artifact. **The failure stays unexplained.**

The same header, lines 582–589, on **MDB_NOMETASYNC**:

> Flush system buffers to disk only once per transaction, omit the metadata
> flush. […] This optimization **maintains database integrity**, but a system
> crash may undo the last committed transaction. I.e. it preserves the ACI
> (atomicity, consistency, isolation) but not D (durability) database property.

No conditional, no corruption clause, and "may undo the last committed
transaction" is exactly §11.4's durable-point semantics. It had never been run.
It has now, in the ratified shape at 20k, 40 s:

| | MDB_NOSYNC | MDB_NOMETASYNC |
|---|---|---|
| achieved | **20 000** msg/s (100%) | **17 309** msg/s (87%) |
| store commit p50 / p99 | 0.66 / 11.78 ms | **7.04 / 122.88 ms** |
| store commits held per second | 134 | 18 |
| durable point p50 | 492 ms | 885 ms |
| of which segment fsyncs | 352 ms | 885 ms |
| ⇒ the engine's own durable commit | ≈ 139 ms | **≈ 0 ms** (the data pages are already on the platter) |
| WA (kernel) | 3.91x | **7.04x** |
| reopen after a clean close | 0 ms | 4 ms |

Read: NOMETASYNC moves the store's fsync cost out of the durable point and into
every commit. It costs **13% of the offered rate and 1.8x the kernel writes**,
and its p99 store commit is **123 ms** — on the apply thread, the only mutator
(§3.3), that is a 123 ms stall of the node at p99, which is a worse number than
the throughput loss. It is not a free upgrade, and it is not measured under a
power loss yet (the dm-flakey loop was not re-run; §5).

---

## 4. The D15 read path, from more than one thread

`MEMO.md` reason 1 for heed is "recovery behaviour and ordered-read cost". The
ordered-read half was measured single-threaded with heed's default
`EnvOpenOptions::new()`, which is `WithTls` (heed 0.22.1
`src/envs/env_open_options.rs:29,39,462`). In that mode `RoTxn` is **not `Send`**
— `src/txn.rs:237` implements `Send` only for `RoTxn<'_, WithoutTls>` — so it
cannot be held across an `.await` or moved between tokio workers, and a nested
read transaction on one thread is illegal. The harness had already hit this and
worked around it (`src/main.rs` ≈776: "LMDB binds a read txn to the thread, so a
get inside a scan is not allowed"), which is exactly the scan-then-lookup shape
the planner and the D15 read path need.

`read_txn_without_tls()` sets MDB_NOTLS and lifts both limits. What it costs is
in `mdb.c`'s `mdb_txn_renew0`: with no thread-local slot, **every read
transaction begin takes `env->me_rmutex`, linearly scans `mti_numreaders` for a
free slot, and releases it** (`MDB_END_SLOT MDB_NOTLS` at mdb.c:1645 releases
the slot at txn end, so the scan happens every time). Measured on the VM against
the store the heed 20k ratified cell left behind, 5 s per point:

| reader threads | `WithTls` gets/s | `WithoutTls` (MDB_NOTLS) gets/s | TLS prefix lists/s | NOTLS prefix lists/s | NOTLS get p99 |
|---|---|---|---|---|---|
| 1 | 2 102 067 | 2 058 214 | 262 758 | 257 277 | < 1 µs |
| 4 | **8 177 933** | 1 968 794 | 1 022 242 | 246 099 | 4 µs |
| 8 | **9 962 765** | 2 078 464 | 1 245 346 | 259 808 | 10 µs |

**In the only reader mode where `RoTxn` is `Send`, heed's read path does not
scale past one core**: 2.06 → 1.97 → 2.08 M gets/s at 1, 4 and 8 threads, a flat
line, while the thread-local mode reaches 9.96 M at 8 threads. The gap at 8
threads is **4.8x**. The same measurement on the laptop (macOS, `results/refutation/reads-heed-laptop.log`)
is far worse — NOTLS goes *backwards*, 1.31 M at 1 thread to 0.29 M at 8, an
11x collapse and a 39x gap — so the size of the penalty is an OS-scheduler
property, but its direction is the same on both.

Two ceilings, both measured rather than inferred:

```
READERS engine=heed asked=200 opened=1  err="MDB_BAD_RSLOT: Invalid reuse of reader locktable slot"   (WithTls, VM)
READERS engine=heed asked=100 opened=64 err="MDB_READERS_FULL: Environment maxreaders limit reached"  (WithoutTls, max_readers=64, laptop)
```

- With thread-local slots, the **second simultaneous read transaction on one
  thread fails immediately**. Not a latency, a hard error.
- With MDB_NOTLS, the slot table is the limit: LMDB's default `max_readers` is
  126, the harness never set it, and the broker never exercised it. A node that
  runs more concurrent read transactions than slots gets `MDB_READERS_FULL` on
  a read path D15 makes ordinary (browse, DLQ lists, stats, analytics, traces).

Neither kills heed. Both are constraints the design has to take on: a local read
must begin and end inside one blocking call on a thread that keeps its slot
(`spawn_blocking` on a bounded pool, `max_readers` ≥ pool size), and a `RoTxn`
must never be held across an `.await`. That is a rule for WP-1.2, and the
2–3 µs / 11 µs / 44 M rows/s figures are only true under it.

---

## 5. What was NOT re-run, and why

The task's VM budget for this round was **10 minutes**, and the two runs both
reviews ask for loudest do not fit in it. They are named here so nobody reads
this file as closing them.

| not run | why it matters | what it costs |
|---|---|---|
| **20 dropped-write runs per engine** (`./vm-campaign.sh full flaky`) | `MEMO.md` §4 makes this its own ratification precondition, `RESULTS-vm.md` §7 lists it as deferred, and D9 was ratified without it. At 5 runs for fjall and redb and 10 for heed, a single observed failure has a 95% interval of roughly 0.3%–45%: these samples cannot distinguish "1 in 10" from "1 in 3" | ≈ 45 min for three engines at the current 20 s run length |
| the same loop with **`vm.dirty_expire_centisecs=100`, `vm.dirty_writeback_centisecs=50`, `RUN_S ≥ 120`** | with the VM's defaults (dirty_expire 30 s, dirty_background ≈ 1.5 GiB) and a 20 s run, almost nothing reaches the platter except at the explicit `force_sync` of each durable point. **"heed never reopened past its durable point, 0/10" is partly a property of the run length**, not only of LMDB. This is the configuration that can actually produce heed's dangerous case | ≈ 1 h, plus a sysctl change, which is outside `/root` and needs Alice |
| **MDB_NOMETASYNC under dm-flakey** | §3 measured its cost, not its crash behaviour. The reason to want it is the header's unconditional integrity claim, and that claim is still untested here | ≈ 15 min for 20 runs |
| **2 h soak on heed at 50k** | `MEMO.md` §2's own row reads "RSS over a 10-min soak at 50k: fjall 4 → 986 MiB still rising; heed not run", and §3 then uses "its memory is page cache the kernel can reclaim" as a reason to prefer heed over the one engine whose long-run memory WAS measured. G-3 (§13.6) asks for RSS flat ±5% over 60 minutes on a store of 300 M messages; for an mmap engine RSS is resident mapped pages and climbs with the touched working set. Nothing in the memo says how heed is expected to pass that | 2 h |
| **ordered scan beyond page cache** | every heed store in both campaigns is 145–504 MiB on a 15 GB VM, always cached. `heed-20000.log`'s 44.2 M rows/s is 180 001 rows / 9.6 MiB. LMDB's mmap scan is exactly what degrades once the store exceeds RAM, and G-3 targets hundreds of millions of messages | part of the 2 h soak |
| **100 kill -9 runs**, 10M-key iteration for redb, dm-delay/ENOSPC | already deferred in `RESULTS-vm.md` §7; unchanged | — |

One thing both reviews say about `kill -9` is worth repeating because it bounds
what 45 runs can ever prove: **a process kill leaves the page cache intact**
(`RESULTS-vm.md` §2 says so itself), so all 45 runs passed for all three
engines and the test cannot falsify anything about durability. Every bit of
discrimination in WP-0.3's mandatory criterion comes from the 20 dm-flakey runs.

---

## 6. What these numbers do to D9

| the memo said | the evidence now says |
|---|---|
| redb "sat at ~10 000 msg/s whatever we offered it" and is "the only one that cannot carry the load" | **False for the ratified store.** 19 466 msg/s at the 20k target (97%), 25 895 at the 50k target, WA 8.66x. The ceiling was the per-entry commit, isolated by a 2×2 |
| heed wins on "ordered-read cost": 42–44 M rows/s, 2–3 µs gets, 11 µs lists | **True, and 15–20x ahead of fjall in the ratified shape too — but only with thread-local reader slots.** In the mode where `RoTxn` is `Send`, the read path is flat at ~2 M gets/s from 1 to 8 threads |
| heed's "memory is page cache the kernel can reclaim rather than heap that grows with the data" | **Still unmeasured.** No heed soak exists. At 40 s in the ratified shape heed's RSS max is *above* fjall's (163 vs 129 MiB at 20k; 349 vs 195 at 50k) |
| the one unopenable heed store is "the corruption LMDB's own documentation warns about for that flag" | **Not supported for this env.** `lmdb.h` narrows the corruption warning to write-order-preserving filesystems *with* `MDB_WRITEMAP`; the harness uses neither. The failure is unexplained |
| the §11.5 log-tail repair "is what makes a 1-in-10 or 4-in-5 'reopened past the durable point' tolerable at all" | **It makes fjall's 4-in-5 tolerable and does nothing for heed's 1-in-10**, which is not a past-durable reopen but an unopenable store |
| (unstated) MDB_NOMETASYNC is the flag whose documented guarantee matches §11.4 | **Measured for the first time: 87% of the offered 20k rate, 1.8x the kernel writes, store-commit p99 123 ms on the apply thread.** Not free, and its crash behaviour is still unmeasured |

`MEMO.md` §4 (revised) and §6 carry the consequences. In one line: **redb is
re-admitted as the documented alternative and as the engine for single-voter /
embedded, heed stays for the 3-voter broker with its flags and its read path
pinned, and the §11.5 repair has to be decided before WP-1.2 freezes an engine.**
