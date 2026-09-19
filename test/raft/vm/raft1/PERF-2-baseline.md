# PERF-2 — the baseline profile at pipeline=4 (where the time goes, before any lever)

Phase-1 performance package (2026-09-19). The measured baseline that PERF-3/4/5
levers must beat. **No product code changed** — PERF-2 is a profile task; it adds
no lever. The one knob it exercises is PERF-1's instrumentation ablation
**`QUEEN_RAFT_METRICS`** (default on): every run here had it on; a lever's VM pass
ablates it to price the instrumentation (PERF-1 measured net +81 ns/entry ≈ 0.16%).

## Provenance

- **Binary** `queen` release built on the VM from an rsync of `server/` + `crates/`
  (no `target/`, warm night cache) at branch `raft` **HEAD `1a9dcf63`** (PERF-1
  timing instrumentation; the night's `29bc7cdd` binary predates it and does **not**
  emit the `queen_raft_*` families). `cargo build --release --bin queen` → `Finished
  in 2m19s`, exit 0, `rustc 1.98.1`. **md5-12 `a2208d19ad18`** (differs from M7's
  `cdc5bb59af27` only by the PERF-1 timing commit + this build; storage class,
  flags, all else identical).
- **Loader** `/root/goload` md5 `2c97cccb0d1e` — the exact WP-0.2 / M1 / M6 / M7
  loader, so the goload columns line up under M7.
- **VM** `root@164.90.215.224` (`queenpgless-01`), Ubuntu 24.04, Linux 6.8.0-124,
  8 vCPU, 15 GB, ext4 `/dev/vda1` (~180 GB free). `ulimit -n 262144`,
  `QUEEN_STORAGE=raft`, **`QUEEN_RAFT_PIPELINE=4`**, LocalReplicator, no Postgres,
  dedup 3600 s, retention off. `perf` = `linux-tools 6.8.0-124` (already present).
- **Host clock** all runs 2026-09-19 08:14–08:29 UTC. VM left **clean**: no
  queen/goload/perf procs, no loop/dm, every broker data dir deleted, disk 14 GB
  used, MemAvailable 14.6 GB. Raw `perf.data` (0.9–1.3 MB each, 8.2–11.4 k samples)
  kept on the VM under `/root/raft/perf/runs/<regime>/`; only the small CSV/stage/
  top-40 tops are copied to the repo (`perf/`, per §0.3).

## Method, and two honest caveats up front

For each regime a **fresh** broker (own store) on port 6699 at pipeline=4 was driven
by the byte-identical `measure-raft1.sh` goload flags for 75 s; `perf record -g -F 99`
sampled the broker for **60 s** of it (after a 5 s ramp), while `timing.sh` scraped
`/metrics/prometheus` every 5 s. Exact commands: `bash prof-run.sh <regime> 60`
(driver = `profile.sh` + `timing.sh`, both copied from `test/raft/vm/`). The
cold-store runs used `bash prof-cold.sh {A20k,C1000} 60 8000000`: preload 8 M msgs /
1000 partitions push-only into an at-rest queue `flbg` (the `flatness-night.sh`
shape), `sync; echo 3 > drop_caches` to make the store cold on disk, then profile a
fresh traffic queue on top.

1. **`profile.sh`'s fallback fold was broken and is replaced.** Its awk did
   `sub(/ 1$/,"",k)` and *then* read `c[k]`, mutating the array key before the
   lookup, so every count was lost (`tot=0` → division-by-zero → **empty** top-40).
   `perf.data` itself is intact. All top-40 tables here were regenerated with a
   correct offset-stripped fold (`perf/fold40.sh`) that aggregates by **function**
   across offsets and samples. (A one-line fix for `profile.sh` is noted in Blockers.)
2. **The CPU top-40 under-captures the fsync stall; the timing histograms are the
   authoritative stage attribution.** `perf record` samples **on-CPU**; the durable
   point spends most of its wall time **off-CPU** in `fdatasync`/`fsync` I/O-wait, so
   it barely appears in the CPU top-40 (it shows only the on-CPU `mdb_page_flush`
   copy + the ext4 write path). Read the *tail/throughput* story from the
   `queen_raft_*_seconds` histograms (below) and the *on-CPU cost* from the top-40.
   Also: the histograms are **log2-bucket** summaries (p50/p99 are bucket **upper**
   bounds, `2^k−1` ns — 268.435 ms, 536.871 ms, 1073.74 ms are adjacent buckets), so
   read them as "≤" and cross-check against the exact `max`. And **[unknown]
   38–57 %** of samples are unsymbolised (frame-pointer unwinding on a release
   binary loses glibc `pwrite`/`fsync`/`pthread` and the LMDB mmap) — the named
   fractions are of the whole, not of the symbolised part.

---

## The runs (goload `overall`, this profiling methodology)

75 s sustained, perf attached; **not** the O14 numbers (M7 owns those — these run
longer/attached than M7's §1 burst and drop the pop-lag mask, so p99 sits between
M7 §1 and the 5-min sustained figure). 0 broker error/panic lines in every run.

| regime | push p50 | push p99 | push p999 | pushed / popped | note |
|---|---|---|---|---|---|
| A20k        | 9.02 ms | 232.45 ms | 288.77 ms | 1.277 M / 1.136 M | fresh |
| A50k        | 125.44 ms | 626.69 ms | 684.03 ms | 3.175 M / 1.569 M | fresh, consume can't keep up (lag 1.6 M) |
| C1000       | 89.60 ms | 514.05 ms | 741.38 ms | 194 k / 194 k | fresh, fully drains |
| FAT100      | 4489 ms | 6586 ms | 7045 ms | 6.05 M / 0 (push-only) | ≈100 k msg/s achieved, sheds 12.7 M |
| A20k-cold   | 9.92 ms | 296.96 ms | 354.30 ms | 1.279 M / 0.972 M | on cold 8 M store |
| C1000-cold  | 72.19 ms | 354.30 ms | 432.13 ms | 194 k / 194 k | on cold 8 M store |

---

## Stage histograms per regime (p50 / p99 / max, ms; count)

Full CSV: `perf/stages-all.csv`; per-regime `perf/<regime>/stage-summary.txt` +
`timing.csv`. Latencies are ns→ms, log2-bucket (read p50/p99 as "≤", `max` exact).

### A20k (fresh) — 100 partitions, push-batch 10, 32 consumers

| stage | p50 | p99 | max | count |
|---|---|---|---|---|
| plan | 0.066 | 4.19 | 8.19 | 98 820 |
| arrival→proposed | 4.19 | **268.4** | **297.7** | 264 272 |
| propose_roundtrip | 4.19 | 33.6 | 304.3 | 44 461 |
| proposed→committed | 2.10 | 16.8 | 108.3 | 44 462 |
| log_fsync | 2.10 | 8.39 | 55.7 | 28 966 |
| apply_entry | 0.066 | 2.10 | 16.0 | 44 461 |
| apply_segment (write_all) | 0.008 | 0.262 | 9.75 | 122 428 |
| apply_other (LMDB+dispatch) | 0.066 | 2.10 | 13.0 | 44 461 |
| store_commit | 0.524 | 4.19 | 9.70 | 10 321 |
| **durable_point** | **268.4** | **536.9** | **285.5** | 64 |
| durable_seg_fsync | 67.1 | 134.2 | 70.9 | 64 |
| durable_dir_fsync | 0.000 | 4.19 | 3.09 | 64 |
| pop_read | 0.004 | 0.524 | 8.45 | 173 791 |

Sizes/depth: drain_commands p50 1 / p99 63; drain_messages p50 1 / p99 511;
group_entries p50 1 / p99 3; group_bytes p50 8 KiB / p99 128 KiB;
apply_channel_depth p50 1 / p99 3. Work: 42.7 k entries, 113 k appends, 1.13 M
messages, 59 durable points. slow_commands 0.

### A50k (fresh) — 100 partitions, push-batch 10, 48 consumers, push>consume

| stage | p50 | p99 | max | count |
|---|---|---|---|---|
| plan | 0.066 | 8.39 | 11.1 | 23 715 |
| arrival→proposed | 16.8 | 268.4 | 595.8 | 360 108 |
| propose_roundtrip | 16.8 | **536.9** | 589.6 | 10 619 |
| log_fsync | 2.10 | 8.39 | 46.2 | 8 013 |
| apply_entry | 0.131 | 16.8 | 23.0 | 10 619 |
| apply_other | 0.131 | 16.8 | 21.4 | 10 619 |
| store_commit | 2.10 | 8.39 | 34.4 | 5 880 |
| **durable_point** | **536.9** | **1073.7** | **559.2** | 64 |
| durable_seg_fsync | 67.1 | 268.4 | 145.0 | 65 |

Sizes: drain_commands p50 1 / **p99 2047** (the 4096 cap is close); drain_messages
p99 16 383; group_bytes p50 128 KiB / p99 1 MiB. Work: 10.3 k entries, 282 k
appends, 2.82 M messages. apply_channel_depth p99 3.

### C1000 (fresh) — 1000 partitions, push-batch 1, 64 consumers, pop-batch 50

| stage | p50 | p99 | max | count |
|---|---|---|---|---|
| plan | 0.016 | 8.39 | 7.96 | 48 076 |
| arrival→proposed | 8.39 | **536.9** | 752.0 | 235 781 |
| propose_roundtrip | 2.10 | 67.1 | 323.9 | 35 058 |
| apply_entry | 0.066 | 4.19 | 37.0 | 35 058 |
| apply_segment | 0.004 | 0.033 | 30.6 | 185 922 |
| apply_other | 0.066 | 4.19 | 13.9 | 35 058 |
| store_commit | 0.131 | 4.19 | 125.4 | 9 952 |
| **durable_point** | **134.2** | **536.9** | **280.9** | 64 |
| durable_seg_fsync | **134.2** | 268.4 | 186.4 | 64 |

Sizes: drain_commands p99 511; group_bytes p50 1 KiB. **1 message per append**
(172 402 appends = 172 402 messages), ~5.3 appends/entry. The durable point here is
seg-fsync-**dominated** (134 ≈ 134), the opposite of A50k/FAT100.

### FAT100 (fresh, push-only) — 100 partitions, push-batch 100, rate 300 k

| stage | p50 | p99 | max | count |
|---|---|---|---|---|
| plan | 8.39 | 8.39 | 14.3 | 3 273 |
| arrival→proposed | 16.8 | **1073.7** | 1015.2 | 58 201 |
| propose_roundtrip | 33.6 | 1073.7 | 1091.9 | 3 269 |
| log_fsync | 4.19 | 16.8 | 45.3 | 2 945 |
| apply_entry | 8.39 | 16.8 | 18.7 | 3 269 |
| apply_segment | 0.033 | 0.524 | 12.0 | 58 142 |
| apply_other (LMDB) | 4.19 | 16.8 | 14.1 | 3 269 |
| store_commit | 8.39 | 16.8 | 27.5 | 2 738 |
| **durable_point** | **1073.7** | **1073.7** | **1004.9** | 64 |
| durable_seg_fsync | 134.2 | 268.4 | 200.3 | 64 |

Sizes: drain_commands p50 **127**, drain_messages p50 **16 383**, group_bytes p50
1 MiB / p99 2 MiB, **apply_channel_depth p50 3 / p99 7** (the only regime that keeps
the pipeline full). Work: 3074 entries, 54 401 appends, **5.44 M messages** in 60 s
(≈100 appends/append-batch, ≈1770 messages/entry).

### Cold-store A20k / C1000 (8 M preloaded, caches dropped) vs fresh

| stage | A20k fresh | A20k cold | C1000 fresh | C1000 cold |
|---|---|---|---|---|
| durable_point p50 / max | 268 / 285 | 268 / **1793** | 134 / 281 | 268 / **1638** |
| durable_seg_fsync p99 / max | 134 / 71 | **537 / 504** | 268 / 186 | **537 / 423** |
| durable_point count | 64 | **139** | 64 | **133** |
| arrival→proposed max | 298 | **1786** | 752 | 1642 |
| push p50 / p99 (goload) | 9.02 / 232 | 9.92 / 297 | 89.6 / 514 | 72.2 / 354 |

The cold 8 M store roughly **doubles the durable-point count** (more `DURABLE_EVERY_
BYTES` trips: appends land in the same 256 bucket files and the same `data.mdb` that
already hold 8 M rows) and **widens the durable-point tail 6× at `max`** (seg-fsync
`max` 71→504 ms; durable_point `max` 285→1793 ms), which propagates to
arrival→proposed `max` (298→1786 ms). But A20k's **median is isolated** (p50 9.0→9.9;
on-CPU top-40 essentially unchanged) — the cold penalty is the widened tail, not the
median, matching the night's I8 (A20k p50 +8 %, p999 +155 %).

---

## Top-40 hot functions (CPU self-time; full tables in `perf/<regime>/top40-{self,inclusive}.txt`)

Names are Rust v0-mangled; decoded below. `mdb_*` = LMDB (heed store); `serde_json`
= push-payload parse; `store::keys::{read,push}_name` = store-key codec; `Derived::*`
= per-partition RAM state; `File::write_all` = segment append; `mdb_page_flush` =
LMDB copying dirty B-tree pages at (durable) commit.

**A20k self** — [unknown] 41.6 %; `RawVecInner::finish_grow` 4.41 %; **`mdb_page_flush`
4.26 %**; tokio `TcpStream::poll_write_vectored` 3.92 %; **`store::keys::read_name`
3.15 %**; `mdb_node_search` 2.70 %; `serde_json::Value::deserialize` 2.06 %;
`mdb_txn_begin` 1.55 %; `File::write_all` 1.01 %. Inclusive: syscall path 21 %,
`malloc` 13.9 %, `cfree` 7.4 %, futex 5.1 % (waiting), `ext4_*write` 4.8 %.

**A50k self** — [unknown] 45.3 %; **`mdb_page_flush` 8.68 %**; `RawVecInner::finish_grow`
4.58 %; `mdb_node_search` 4.05 %; tokio write 3.42 %; `serde_json::Value::deserialize`
3.41 %; `File::write_all` 1.43 %. Inclusive: syscall 22 %, **`malloc` 15.8 % + `cfree`
8.8 %**, **`mdb_page_flush` 8.75 %**, **ext4 buffered-write path (`ext4_file_write_iter`/
`vfs_write`/`generic_perform_write`/`__libc_pwrite`) ≈ 8 %**.

**C1000 self** — [unknown] 38.2 %; **`store::keys::read_name` 10.53 %** (dominant);
tokio write 3.55 %; `mdb_txn_begin` 3.26 % (many per-partition read txns);
`store::keys::push_name` 2.34 %; `mdb_node_search` 2.21 %; `mdb_page_flush` 1.99 %;
**`Derived::set_pending_inner` 1.63 % + `Derived::note_lease` 1.25 % + `Derived`
drop_glue 1.59 % + BTreeMap remove 1.09 %** (per-partition RAM state). Inclusive:
`read_name` 10.8 %, `Derived::note_lease` 8.1 %, `malloc`+`realloc`+`cfree` ≈ 30 %.

**FAT100 self** — [unknown] 45.5 %; **`mdb_page_flush` 13.19 %** (dominant);
`mdb_node_search` 5.34 %; `serde_json::Value::deserialize` 5.27 %;
`RawVecInner::finish_grow` 4.25 %; `push_impl` 2.06 %; serde_json `Value` BTreeMap
insert 2.01 %; `File::write_all` 1.54 %; `mdb_cmp_memn` 0.79 %. Inclusive: syscall 22 %,
**`malloc` 17.5 % + `cfree` 9.0 %** (a `serde_json::Value` per message), **`mdb_page_flush`
13.3 %**, **ext4 buffered-write ≈ 12 %** (`__x64_sys_pwrite64` 9.2 %).

**Cold** self-tables are within noise of fresh (A20k-cold `mdb_page_flush` 4.63 %,
`read_name` 3.67 %; C1000-cold `read_name` 9.42 %, `Derived::note_lease` 0.97 %) plus
a new `heed_store::HeedRead::scan_pending` 0.72 % and hashbrown `reserve_rehash` — i.e.
the cold penalty is **off-CPU** (mmap page-ins + longer fsyncs), invisible to on-CPU
sampling, confirming caveat 2.

---

## Diagnosis

### (a) What makes the p99 tail — **the durable point, and it lines up exactly**

The tail *is* the §11.4 durable point stalling the single apply thread. Cadence: 64
points over ~70 s ≈ **one every ~1.1 s** (the `DURABLE_EVERY_MS=1000` tick). Duration
scales with the regime and **equals the push p99**:

| regime | durable_point (p50/max) | push p99 | the tail stage |
|---|---|---|---|
| A20k | 268 / 285 ms | 232 ms | arrival→proposed p99 **268** ms |
| A50k | 537 / 559 ms | 627 ms | propose_roundtrip p99 **537** ms |
| FAT100 | 1074 / 1005 ms | (inflight-capped) | arrival→proposed p99 **1074** ms |

Mechanism, confirmed in code (`apply.rs::run` → `durable_point_inner`): the durable
point runs **on the apply thread**; while it fsyncs, **no entry is applied**, so with
`QUEEN_RAFT_PIPELINE=4` the ≤4 in-flight entries can't be answered, the batcher
(inline propose, F-1) can't propose the next, and new arrivals queue on the facade
channel → `arrival→proposed` (and, once the store fills, `propose_roundtrip`) balloons
to ~the durable-point duration. Within the durable point, **the LMDB `data.mdb`
durable commit dominates, not the segment fsync**: durable_point − seg_fsync − dir_fsync
≈ **A20k 211 ms, A50k 409 ms, FAT100 800 ms** of LMDB fsync vs seg-fsync 67/67/134 ms
(C1000 is the exception — 94 ms LMDB but 186 ms seg-fsync, because it ingests few
messages but touches all 256 bucket files). So the tail lever is the durable-point
cadence / making the durable fsync cheaper or off the apply thread, not the log fsync
(a separate thread, 2–4 ms) or the segment write_all (8–33 µs).

### (b) What bounds FAT100 throughput — **the single apply thread, on the store path**

FAT100 achieves ≈100 k msg/s at only ~1.4 cores of 8 (M7) — it is **not** CPU-bound
across cores; it is **serialised on the one apply thread**, whose 60 s is ≈**45 s of
durable point** (64 × ~700 ms, p50/max ~1 s each) + ~13 s of per-entry apply
(apply_entry p50 8.4 ms × 3074), leaving little time to apply. Within that, the store
is the cost, split: **off-CPU** the LMDB+segment **durable fsync** (~800 ms of each ~1 s
point), and **on-CPU** `mdb_page_flush` **13.2 %** (LMDB copying dirty pages) plus the
ext4 **buffered-write** path for `File::write_all` (~12 % inclusive, `pwrite` into page
cache) plus **`serde_json::Value` alloc/free ~26 %** (`malloc` 17.5 % + `cfree` 9 %:
one heap `Value`/`BTreeMap` per message on the facade/planner side). What is **not**
the bound: the planner (plan p50 8.4 ms, 3273 cycles — cheap), the log fsync (separate
writer thread, 4–17 ms), the segment `write_all` (apply_segment p50 33 µs/entry). Levers
in priority: cut the durable-point store cost (cadence / cheaper durable commit / move
the store fsync off the apply thread), then the per-push `serde_json::Value` allocation.

### (c) What the C1000 fan-out pays per entry, and what the cold store adds

C1000 pays in **breadth, not depth**: **1 message per append** (no push-batch
amortization) fanned across 1000 partitions / 256 buckets. Per entry (~5.3 appends)
that is ~5 `write_all` syscalls into distinct bucket files + the **store-key codec**
(`store::keys::read_name` **10.5 %** + `push_name` 2.3 % — the top on-CPU cost, from
encoding/decoding many distinct `(pid,…)` keys) + **many LMDB B-tree ops**
(`mdb_txn_begin` 3.3 % — a read txn per partition pop-probe, `mdb_node_search` 2.2 %)
+ **per-partition RAM state** (`Derived::set_pending_inner`/`note_lease`/drop +
BTreeMap ≈ 5.5 %). The durable point is **seg-fsync-bound** here (fsyncs all 256
touched bucket files: 134 ms) rather than LMDB-bound. **The cold 8 M store adds** (i)
~2× more and ~6× longer durable points at the tail (seg-fsync `max` 186→423 ms, more
`DURABLE_EVERY_BYTES` trips — the appends grow the shared 256 bucket files and the
shared `data.mdb`), and (ii) **page-cache misses** on first touch of the LMDB mmap
(`store/data.mdb`, the `(pid,hash)` dedup + per-partition state keyspaces) and the
`.qidx`/`seg_loc` index — **off-CPU**, so they show in the widened durable-point/
arrival→proposed **tails** and `HeedRead::scan_pending` appearing on-CPU, not in the
CPU top-40 (identical to fresh). **Caveat (stated plainly):** at this **75 s
profiling** scale the cold-store end-to-end goload latency did **not** reproduce the
night's 6-min flatness **C1000 +546 %** — cold p50 72 ms was *below* fresh p50 90 ms
(both ~10× the night's steady-state empty 9.79 ms), i.e. the 75 s window is
warmup-dominated for the C1000 shape and is not a clean steady-state latency
measurement; the profile answers *where the fan-out spends*, not *by how much the
loaded store slows steady-state C1000* (the night/M7 owns that verdict).

---

## What was cut or failed

- **`profile.sh`'s fold was broken** (empty top-40, root-caused above); regenerated
  correctly from the kept `perf.data`. The fix belongs in `profile.sh` (Blockers).
- **CPU top-40 has 38–57 % `[unknown]`** (frame-pointer unwinding on a release binary)
  and **under-samples the off-CPU fsync stall** — the timing histograms are the
  authoritative stage attribution; the top-40 is the on-CPU cost only. No DWARF/
  `--call-graph dwarf` run (heavier; frame-pointer `-g` was enough to name the hot
  functions).
- **Cold-store C1000 did not reproduce the night's steady-state +546 %** at 75 s
  (caveat above) — the profile still delivers the per-entry fan-out attribution asked.
- Single VM, co-resident loader (O13: final numbers need three VMs). Histograms are
  log2-bucket (± one bucket).
