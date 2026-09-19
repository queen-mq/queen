# PERF-7 — the round-3 SHORT measure (G batcher/writer scheduling, H ring re-arm, I O(claimed) wildcard pop)

Phase-1 performance package, round 3 (2026-09-19). PERF-7 measures the binary that
carries **PERF-G** (batcher/writer scheduling — `DRIVER_NOTIFY` on, `WRITER_PIPELINE`
off), **PERF-H** (ready-ring re-arm audit; knob `PENDING_TRANSITIONS`, kept off) and
**PERF-I** (the wildcard/pinned/discovery claim now O(claimed), `CLAIM_FROM_RING` on)
on top of round-2's best config (`BATCH_COUNTERS=1 DEDUP_INDEX=txns DEDUP_FRONT=1
DURABLE_ASYNC=1 SEG_BUFFERED=1 BUCKETS=16`, `PIPELINE=4`). Alice's directive: iterate,
short VM runs first, long only if good. This is the **60/75/60 s** consume-matched,
drained short measure — three regimes, a one-knob-off ablation on A20k and C1000, and
the `PIPELINE=8` D4 datum. **No product code changed here**; PERF-7 sets only knobs.

## Bottom line — `proceed=false`

**Round 3's G/H/I are correctness / scaling hardening that, on these short fresh-store
measures, are performance-neutral-to-parity with round 2 and regress nothing — but they
do not move either push-latency bound, so the aggressive proceed gate misses on the two
gates that sit on work round 3 did not do.** The two remaining bounds are both the
`arrival→proposed` propose path: A20k's ~4 ms batcher hold (push p50 floor) and C1000's
1073 ms per-partition propose serialization — untouched by G, H and I.

- **A20k: parity, clean tail, p50 floor intact.** Round-3 default push **p50 6.05 /
  p99 17.28 / p999 25.22 ms**, fully drains (phase-2 lag → −770 k), pop **6.3 k/s**.
  That is parity with round-2 b=16 (6.62 / 22.14; PERF-6 b16-txns 18.3). `writer_pickup`
  is 0.52/2.10 ms — but so is the **minus-G** column (§4), so it is the VM's ~0 writer
  queueing (PERF-2: `proposed→committed ≈ log_fsync`), not a `DRIVER_NOTIFY` gain — and
  `arrival→proposed` **p50 stays 4.194 ms**: the batcher still holds a command ~4 ms before
  proposing it, and that is the push-p50 floor. `WRITER_PIPELINE` is off by default (the
  laptop A/B showed it regresses p50 on a ~1 ms-fsync box). **p50 6.05 > 4.0 ms: gate
  misses on the p50 floor.** The pop rate (6.3 k) is **not evaluable on one 60 s run** —
  PERF-6 established a 3.4–11.4 k single-run A20k pop variance (minus-I drew 11.7 k/s here) —
  so the robust fact is **drained=yes**, not a 9 k pop miss. Alice's item (1) is
  **partially addressed** — scheduling tighter, tail clean, p50 unmoved.
- **C1000: push latency unchanged, bound is upstream.** Round-3 default push **p50 211.97
  / p99 790.53 ms** — parity with round-2 b=16 (218.11 / 765.95). It fully drains at 75 s
  (pushed = popped = 220 490, lag 0) — but so does **every ablation variant and the PERF-2
  baseline at 75 s**, so draining is a **window property, not a round-3 gain** (PERF-4's
  60 s window was the one that saturated). The push bound is `arrival→proposed` **p99 1073
  ms**, the per-partition propose serialization, **unchanged old↔round-3↔every knob** and
  upstream of the store and the claim. **p50 211.97 > 13, p99 790.53 > 66**: gate misses.
- **FAT100: 90.7 % of the knee, PASS.** Round-3 default **260 498 msg/s = 90.7 %** of the
  287 k postgres knee (a second run read 266 335 = 92.7 %) — parity with round-2 (261 k).
  Push-only; the consume/claim paths do not run, so G/H/I are no-ops here. Clears ≥ 250 k
  and the ≥ 70 % O14 bar.
- **The ablation isolates nothing measurable, and that is the finding.** On A20k, minus-G,
  minus-H and minus-I are all within run-to-run noise of the default (p50 5.66–6.18, p99
  17.0–18.6, durable_point 16.8/33.6 everywhere); the pop-rate spread (min-i popped **more**,
  11.7 k/s) is the known A20k scheduling variance (PERF-6 3.4–11.4 k), not a knob. On C1000
  the same: p50 142–222, p99 774–823, `arrival→proposed` p99 1073 in all four. **minus-I**
  (baseline full-history claim scan) shows **no C1000 penalty at 75 s** — each partition
  holds only ~225 messages, so the O(history) scan is as cheap as O(claimed) (`pop_wildcard`
  plan sub-2 ms p99 either way). PERF-I's design win (bounding the scan) is real and
  I2-transparent, but it only **bites on an aged/large store** (PERF-2's cold 8 M), which a
  fresh 75 s run does not build. PERF-H moves nothing (AB-0 already settled this).
- **PIPELINE=8 (D4 datum) regresses A20k.** push **p50 14.02 / p99 28.03** (vs pipe-4
  6.05 / 17.28), `arrival→proposed` p50 16.8 ms (vs 4.2). Deeper pipeline just buffers more
  arrivals in front of the single apply thread — the system is **apply-thread-bound, not
  slot-starved** — so **PIPELINE=4 is the right default** and the bound is confirmed to be
  the propose/apply serialization, not pipeline depth.

## Provenance

- **Binary** `queen` release, VM-built from an rsync of `server/` + `crates/` (no `target/`)
  at branch `raft` **HEAD `f006c3d4`** (PERF-I tip; G `9e1ebff8`, H `c65c8c2c`, I `f006c3d4`
  on round-2 report `7e5f0d61`). `cargo build --release --bin queen` exit 0 in ~3 m,
  `rustc 1.98.1`. **md5-12 `f22bf1e19c3f`**.
- **Loader** `/root/goload` md5 `2c97cccb0d1e` — the exact M1/PERF-2/3/4/6 loader.
- **VM** `root@164.90.215.224`, Ubuntu 24.04, 8 vCPU, 15 GB, ext4 `/dev/vda1`.
  `ulimit -n 262144`, `QUEEN_STORAGE=raft`, `QUEEN_RAFT_PIPELINE=4` (a `=8` datum), METRICS
  on, LocalReplicator, no Postgres, dedup 3600 s, retention off. **Round-3 default knobs**
  (explicit): `DRIVER_NOTIFY=1 WRITER_PIPELINE=0 PENDING_TRANSITIONS=0 CLAIM_FROM_RING=1`
  on the round-2 best config. **Host clock** 2026-09-19 19:22–19:46 UTC. Work under
  `/root/raft/perf7/`, distinct port 6713; each run booted a FRESH broker + fresh data dir,
  killed **only its own** PID, deleted the data dir after. **0 broker error/panic lines in
  every kept run** (`ERRLINES=0` × 10).
- **The settle guard, and a discarded first batch.** The first batch launched **98 s after
  the build** read A20k push **p99 1318 ms** — `durable_seg_fsync` tail **1073 ms (max 724)**
  on a ~1 ms-fsync box, i.e. the durable-point fdatasync stalled behind the build's **3.7 GB
  `target/` writeback**. Medians were unaffected (p50 7.14 ms; durable_point **p50 16.8 ms**).
  That batch is **discarded**; the driver now runs `sync` and waits for `Dirty < 20 MB` before
  each boot (`SETTLE dirty_kb=0`), and every number below is the settle-guarded rerun.

## The configs

| column | knobs (on round-2 best config + `PIPELINE=4 METRICS=1`) |
|---|---|
| **round 3** | `DRIVER_NOTIFY=1 WRITER_PIPELINE=0 PENDING_TRANSITIONS=0 CLAIM_FROM_RING=1` |
| **minus G** | `DRIVER_NOTIFY=0` (WRITER_PIPELINE stays 0) |
| **minus H** | `PENDING_TRANSITIONS=1` (prices the transition-gated append re-arm H keeps OFF) |
| **minus I** | `CLAIM_FROM_RING=0` (the baseline full-history claim scan) |

---

## (1) A20k — 100 partitions, push-batch 10, 32 consumers, pop-batch 200 (60 s + 80 s drain, consume-matched)

Push latency = goload `overall` (phase 1, under consume). Every kept run fully drains in
phase 2 (lag → negative). `pop /s` = phase-1 popped / 60 s.

| metric | pg M1 | round-2 b16 | **round 3** | minus G | minus H | minus I |
|---|---|---|---|---|---|---|
| push p50 ms | 9.15 | 6.62 | **6.05** | 6.18 | 5.66 | 6.18 |
| push p99 ms | 27.01 | 22.14 | **17.28** | 17.02 | 18.30 | 18.56 |
| push p999 ms | 52.48 | 50.94 | **25.22** | 27.78 | 28.29 | 28.29 |
| pop /s (ph1) | — | 8.0k | **6.3k** | 6.5k | 5.75k | 11.7k |
| drained? | — | (satur.) | **yes** | yes | yes | yes |
| durable pt p50/p99 | — | 16.8/24.8 | **16.8/33.6** | 16.8/33.6 | 16.8/33.6 | 16.8/33.6 |
| arrival→proposed p50/p99 | — | — | **4.19/16.8** | 4.19/16.8 | 2.10/16.8 | 4.19/16.8 |
| writer_pickup p50/p99 | — | — | **0.52/2.10** | 0.52/2.10 | 0.52/4.19 | 0.52/2.10 |
| pop_wildcard plan p50/p99 | — | — | **0.066/0.131** | 0.066/0.131 | 0.016/0.066 | 0.066/0.524 |
| CPU cores / RSS MB | 0.93/55 | 1.49/152 | **1.30/166** | 1.30/167 | 1.23/170 | 1.31/173 |

**PIPELINE=8 (D4):** push **p50 14.02 / p99 28.03 / p999 40.19**, pop 3.1 k/s (fully drains),
`arrival→proposed` **p50 16.8 / p99 33.6**, durable_point 16.8/33.6, CPU 1.34, RSS 167 —
a **regression** vs pipe-4 (deeper queue in front of the apply thread).

## (2) C1000 — 1000 partitions, push-batch 1, 64 consumers, pop-batch 50 (75 s + 40 s drain)

| metric | pg M1 | round-2 b16 | **round 3** | minus G | minus H | minus I |
|---|---|---|---|---|---|---|
| push p50 ms | 6.62 | 218.11 | **211.97** | 156.67 | 222.21 | 142.34 |
| push p99 ms | 33.02 | 765.95 | **790.53** | 790.53 | 823.30 | 774.14 |
| push p999 ms | 54.02 | 815.10 | **831.49** | 847.87 | 1105.92 | 864.26 |
| ph1 pushed / popped / lag | — | (lag ~68k) | **220490/220490/0** | 220501/220501/0 | 220498/220496/2 | 220491/220486/5 |
| drained? | — | (satur.) | **yes** | yes | yes | yes |
| durable pt p50/p99 | — | 67.1/75.2 | **67.1/134** | 67.1/134 | 67.1/134 | 67.1/134 |
| arrival→proposed p50/p99 | — | ~/1073 | **8.39/1073** | 8.39/1073 | 8.39/1073 | 8.39/1073 |
| plan p50/p99 (overall) | — | — | **0.016/4.19** | 0.016/8.39 | 0.016/1.05 | 0.016/8.39 |
| pop_wildcard plan p50/p99 | — | ~8.4 (base) | **0.033/1.05** | 0.033/1.05 | 0.016/0.52 | 0.033/1.05 |
| CPU cores / RSS MB | 0.91/82 | 1.38/437 | **1.50/474** | 1.52/489 | 1.53/539 | 1.51/485 |

C1000 push p50 varies 142–222 ms run-to-run (the propose-queue depth is timing-sensitive,
one run per cell), so the p50 spread is **not** evidence either way. The direct measure that
`CLAIM_FROM_RING=0` carries **no** claim-path penalty at 75 s is `pop_wildcard` **plan p99
1.05 ms, identical CFR on/off** (a per-op measure, not the noisy p50): the O(history) scan is
as cheap as O(claimed) when each partition holds only ~225 messages. minus-I's 142 ms p50 is
disclosed as the lowest of the four, but it is one unreplicated draw, not the support.

## (3) FAT100 — push-only, push-batch 100, rate 300 k (knee 287 363)

| metric | pg M1 | round-2 b16 | **round 3** |
|---|---|---|---|
| rate msg/s | 287363 | 261470 | **260498** |
| % of knee | 100 % | 91.0 % | **90.7 %** |
| durable pt p50/max ms | — | 67.1/88.2 | **67.1/111** |
| durable_seg_fsync p50/p99 | — | — | **33.6/67.1** |
| CPU cores / RSS MB | 1.38/255 | 2.66/2955 | **2.64/2977** |

## (4) O14 ratios and the proceed gate

| target | round 3 | pg M1 | O14 ratio | proceed gate | verdict |
|---|---|---|---|---|---|
| A20k p50 | 6.05 | 9.15 | **0.66×** (≤ pg) | ≤ 4.0 ms | **FAIL** (6.05) |
| A20k p99 | 17.28 | 27.01 | **0.64×** (≤ pg) | ≤ 27 ms | **PASS** |
| A20k pop/s (drained) | 6.3k | — | — | ≥ 9k | **n/e** — drained=yes; 6.3k is one draw in PERF-6's 3.4–11.4k |
| C1000 p50 | 211.97 | 6.62 | **32.0×** | ≤ 13 ms | **FAIL** |
| C1000 p99 | 790.53 | 33.02 | **23.9×** | ≤ 66 ms | **FAIL** |
| FAT100 tput | 260,498 | 287,363 | **90.7 %** | ≥ 250k | **PASS** |

`proceed=false`. Note the O14 **strict** bar (p50 AND p99 ≤ pg) A20k **passes on both**
(0.66× / 0.64×) — round 3 keeps A20k under postgres; it is only the round-3 **aggressive
4 ms p50** gate that A20k misses (the 9 k pop bar is a round-3-invented target, not O14, and
is **not evaluable** on one run — drained=yes is the robust fact). C1000 fails O14 and the
gate on the same upstream bound as round 2. FAT clears both.

## (5) Diagnosis — where each failing gate is bound (from the stage histograms)

- **A20k p50 (fails 4.0 ms) — the batcher hold.** The store side is clean and cheap:
  durable_point 16.8 ms, writer_pickup 0.52 ms, log_fsync ~1 ms, apply_entry 0.07 ms. The
  floor is `arrival→proposed` **p50 4.194 ms** — a command waits for the current plan cycle /
  a freed pipeline slot before it is proposed. The **median hold is structural to the
  single-plan-cycle batcher** and unmoved by G/H/I. The tail is clean (p999 **25.22** vs the
  single round-2 run's 50.94), but the ablation (§4) does **not** attribute that to PERF-G:
  minus-G reads p999 **27.78** with the same `writer_pickup` 0.52/2.10, so the tail
  improvement is run-to-run against one round-2 datum, not a demonstrated G gain. PIPELINE=8
  makes the median worse (16.8 ms), proving the hold is apply-thread queueing, not slot
  starvation. Next lever = the propose path.
- **C1000 p50/p99 (fails 13/66 ms) — the per-partition propose serialization.**
  `arrival→proposed` **p99 1073 ms**, identical old↔round-3↔every knob: 1 partition = 1
  propose lock while 64 consumers *do* now drain 1000 partitions. PERF-I moved the plan cost
  (O(claimed) claim) and PERF-G the interleaving, but neither touches the propose ingress —
  the C1000 bound is upstream of the store and the claim, exactly as PERF-2/4 found. Closing
  it needs per-partition propose parallelism, a separate work item.

## (6) 60 s CPU profile of the two failing regimes (round-3 defaults, `perf record -g -F 99`)

Fresh settle-guarded broker per regime, perf attached 60 s under load (so p99 sits a little
above §1/§2 — C1000 profile p50 276 ms, A20k 7.78 / 27.01; both fully drained in-profile).
`perf-event-paranoid=4` loses kernel frames; names v0-demangled by hand. **The point: both
failing bounds are OFF-CPU** (the A20k batcher hold, the C1000 propose serialization), so
neither shows as a hot function — the profile confirms there is no CPU hotspot to blame, only
serialization; what the top-of-CPU shows is the per-message / per-partition *work*.

- **C1000 (self-time top): the cardinality/breadth cost, matching PERF-2.** Allocator
  **≈ 14.7 %** (`malloc` 8.06 % + `realloc` 3.51 % + `cfree` 3.18 %); the store-key codec
  **`store::keys::read_name` 3.44 % + `push_name` 2.81 % ≈ 6.3 %** (encoding/decoding the many
  distinct per-partition keys); **`Derived::note_lease` 2.68 %** + `BTreeMap<String>::remove`
  1.37 % (per-partition lease/RAM state); `core::str::from_utf8` 1.31 %; hashing a tracing
  span Id 1.49 %; syscall path ~12.8 % inclusive. **No claim-scan symbol is hot** — PERF-I's
  O(claimed) claim keeps the wildcard pop off the CPU path (`pop_wildcard` plan sub-2 ms).
  The 1073 ms `arrival→proposed` bound is the off-CPU wait behind the per-partition propose
  lock, invisible to on-CPU sampling — consistent with a serialization, not a compute, bound.
- **A20k (self-time top): allocator + LMDB, lighter codec.** Allocator **≈ 12.6 %** (`malloc`
  7.95 % + `cfree` 3.18 % + `realloc` 1.45 %); LMDB `mdb_node_search` 1.65 % + `mdb_page_search`
  1.06 %; `store::keys::{push,read}_name` ≈ 2.2 % (only 100 partitions, so far below C1000's
  6.3 %); `SipHasher::write` 1.37 %; `from_utf8` 1.50 %. The 4 ms `arrival→proposed` median
  hold is the batcher waiting on the plan cycle / a pipeline slot — again off-CPU, no hot
  function — which is why the lever is the propose path, not any symbol above.



## (7) What was cut / not done

- Short runs only (Alice: iterate); one replicate per cell (A20k pop rate is 3.4–11.4 k
  run-to-run — PERF-6). The first (build-writeback) batch discarded; settle-guarded rerun kept.
- The C1000 "fully drains" is **not** attributed to round-3 code: the PERF-2 baseline and
  every ablation variant also drain at 75 s; only PERF-4's 60 s window saturated. I did **not**
  re-run the round-2 binary at 75 s to control the window, so I make no drain-win claim.
- minus-I did not stress PERF-I: a fresh 75 s store has ~225 msg/partition, so the O(history)
  claim scan is already cheap. PERF-I's win needs an aged/large store (PERF-2 cold 8 M),
  unmeasured here; PERF-I is I2-transparent (identical outcomes, property-proven).
- Single VM, co-resident loader (O13: final numbers need three VMs); histograms log2-bucket
  (± one bucket). `apply_stats` gauge reads 0 mid-run in this build (an emission gap; work is
  read from goload counts + `apply_receives_total` + durable_point count, all populated).
