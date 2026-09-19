# NIGHT-A — the pipeline=4 pass (the owed re-run of WP-1.11)

PLAN_RAFT.md §7.1–7.3, I3/I5/D4, §13.6, Appendix G, O14, O18, O19, G-3/I8.
The re-run WP-1.11 owed once the **F-1** fix (batcher submits each propose inline
on the one driver task, in plan order) was committed. Night of 2026-09-18/19,
VM `root@164.90.215.224` (`queenpgless-01`, Ubuntu 24.04, 8 vCPU, 15 GB, ext4,
`/dev/vda1`). Everything under `/root/raft/night/`.

- **Binary** `queen` release from branch `raft` @ `29bc7cdd` (HEAD; the F-1 fix
  is committed at `f8bfa672`). Built on the VM from an rsync of `server/` +
  `crates/` (no `target/`) into `/root/raft/night/queen/`, warm-cache incremental
  (`Finished release in 2m17s`, exit 0). **md5-12 `cdc5bb59af27`**, which
  **differs from WP-1.11's `be80d860b4a1`** (step-0 provenance: the fix is in the
  binary). `rustc 1.98.1`. `QUEEN_STORAGE=raft`, LocalReplicator, no Postgres.
- **Loader** `/root/goload` md5 `2c97cccb0d1e` — the exact WP-0.2 / WP-1.11 loader
  (openloop mode), so the columns line up under M1 and M6.
- **Oracle** postgres-class numbers = WP-0.2 / M1 (same VM + loader). WP-1.11
  pipeline=1 = M6 (same VM + loader + binary family).
- **ulimit -n 262144** on every broker (F-2). `dedup_window_seconds=3600`,
  retention off on both classes (config parity, unchanged from WP-1.11).

## TL;DR

1. **The F-1 fix holds on the VM.** At `QUEEN_RAFT_PIPELINE=4` the node **survived
   every regime plus a 5-minute sustained A20k with ZERO poison lines and ZERO
   broker error lines** — a 20 s smoke, the six-regime sweep, the flatness sweep
   and the O19 run all ran the fixed batcher under real concurrent load and none
   poisoned. The fix was only ever exercised on a laptop (where F-1 does not
   reproduce, §WP-1.11); this is the first proof it holds under the VM scheduler
   that produced F-1. **The pipeline=4 pass is unblocked; F-1 is discharged on the
   VM.**
2. **O14 is still NOT met at pipeline=4 (FAIL).** Push **p50 beats/matches
   postgres** on the A-regimes (A20k 6.24 vs 9.15, A50k 19.58 vs 23.17), and A50k
   is **dramatically better than pipeline=1** (p50 148→19.6, p99 1106→444). But
   the push **p99 tail is ~6× postgres** on both A-regimes, **C1000 (fan-out)
   REGRESSES badly** at pipeline=4 (p50 238 vs 13 at pipeline=1, vs 6.6 postgres),
   and **FAT100 tops at ≈90k msg/s ≈ 30 % of the 300k knee** (O14 wants ≥70 %).
   Every O14 target line is judged below.
3. **O18 slow-command log / per-kind planner metrics are inert.** `QUEEN_RAFT_SLOW_COMMAND_MS`
   and `plan_budget_ms` are plumbed into `PlanConfig` from env and `CommandKind`
   exists, but **nothing consumes them** — a 5-minute A20k at `QUEEN_RAFT_SLOW_COMMAND_MS=5`
   with sustained plan+commit times of p50 44 ms emitted **zero** slow-command
   lines. Staged, not wired, in phase 1.
4. **O19 isolation FAILS at pipeline=4 (real DLQ storm).** A real active DLQ storm
   (pop + ack `status:"dlq"`) at only **463/s** on the hot partition raised the
   quiet partitions' p99 **+90.6 %** (87.55 → 166.91 ms), and the 2 M backlog
   **at rest** (storm stopped) still held it **+53.2 %** (→ 134.14). Both blow the
   ±15 % threshold. No transcription this time — raw CSVs kept.
5. **I8 flatness: mixed, FAIL on the strict gate.** For the **A20k** shape
   (100 partitions, wildcard consumers) p50/p99/CPU/disk **are flat** against an
   8.1 M at-rest store (+8 %/+5 %/0 %/+3 %, all PASS) — the good I8 property holds
   — but the **p999 tail +155 %** and **RSS is not flat**. For the **C1000** shape
   (1000 single-message partitions) latency **scales hard with the store** (p50
   **+546 %**, p99 +15 %, ack +122 %, all FAIL). **At rest ~387 B/msg** at
   pipeline=4 (8.14 M → ~3.0 GB) — **lower than R-105's 610** (pipeline=1) but
   still **linear in stored count, not flat**; 100 M at rest ≈ 36 GB » 15 GB, so
   the task's 100 M / 1 M-partition size is infeasible on this VM.

---

## Finding N-1 — the F-1 fix survives pipeline=4 on the VM (the headline)

The 20 s smoke (`smoke4.sh`, A20k shape, `QUEEN_RAFT_PIPELINE=4`) pushed 369 950,
popped 262 680, ended `role:leader storageReady:true`, **0** poison / `TimeWentBackwards`
/ error lines. The full six-regime sweep (§1), the 5-minute A20k (§1b), the
flatness sweep (§2) and the O19 run (§3) then ran the same binary under
concurrent load for ~40 minutes of wall time total. **Poison lines across all of
tonight's pipeline=4 runs: 0. Broker `error`/`panic` lines: 0.** The exact failure
WP-1.11 documented (apply thread stops at `applied≈33–670`, `role:stopped`) did
not occur once. F-1 is discharged on the VM.

---

## (1) The six regimes at pipeline=4, under postgres (M1) and pipeline=1 (M6)

`QUEEN_BIN=…/night/queen OUTDIR=…/night/raft1-pipe4 bash measure-raft1.sh pipe4
"A20k A50k B1 C1000 D1 FAT100" "QUEEN_RAFT_PIPELINE=4"`. goload openloop, 256 B,
manual ack, the Appendix-G shapes. Push latency from goload `overall`. One shared
broker across all six regimes (RSS accumulates; store data reaches 4.3 G).

**raft1 pipeline=4 (tonight, md5 `cdc5bb59af27`):**

| regime | p50 ms | p99 ms | p999 ms | ack ms | pushed / popped (window) | queen cores | queen RSS max | disk MB/s |
|---|---|---|---|---|---|---|---|---|
| A20k  | **6.24** | 162.82 | 197.63 | 6.90 | 749 910 / 314 980 | 1.48 | 254 MB | 72.1 |
| A50k  | **19.58** | 444.42 | 518.14 | 23.82 | 1 874 450 / 149 960 | 1.72 | 839 MB | 131.7 |
| B1    | 4.45 | 36.61 | 85.50 | 4.39 | 59 990 / 31 515 | 1.44 | 843 MB | 49.5 |
| C1000 | 238.59 | 749.57 | 831.49 | 165.06 | 84 389 / 51 688 | 1.33 | 908 MB | 43.5 |
| D1    | 4.26 | 40.19 | 54.02 | 3.95 | 9 994 / 2 476 | 1.03 | 901 MB | 25.5 |
| FAT100 (push-only) | 4079.62 | 6782.98 | 7045.12 | — | 5 402 600 / 0 (shed 11.4 M) | 1.42 | 3238 MB | 250.5 |

**postgres oracle (M1, WP-0.2):**

| regime | p50 | p99 | p999 | ack | queen cores | pg cores | queen RSS | pg PSS |
|---|---|---|---|---|---|---|---|---|
| A20k  | 9.15 | 27.01 | 52.48 | 9.06 | 0.93 | 3.01 | 55 MB | 1211 MB |
| A50k  | 23.17 | 75.26 | 103.94 | 32.92 | 1.17 | 2.65 | 105 MB | 1384 MB |
| B1    | 3.34 | 14.27 | 35.07 | 1.72 | 0.84 | 2.45 | 109 MB | 1427 MB |
| C1000 | 6.62 | 33.02 | 54.02 | 21.36 | 0.91 | 3.86 | 82 MB | 1481 MB |
| D1    | 1.99 | 4.08 | 18.30 | 0.79 | 0.28 | 0.78 | 54 MB | 1490 MB |
| FAT100 | 23.17 | 209.92 | 415.74 | — | 1.38 | 1.39 | 255 MB | 1540 MB |

**raft1 pipeline=1 (M6, WP-1.11) for reference:**

| regime | p50 | p99 | p999 | ack |
|---|---|---|---|---|
| A20k  | 6.18 | 175.10 | 205.82 | 7.04 |
| A50k  | 148.48 | 1105.92 | 1220.61 | 93.47 |
| B1    | 3.73 | 68.10 | 116.22 | 3.56 |
| C1000 | 13.12 | 138.24 | 193.54 | 16.43 |
| D1    | 3.28 | 24.96 | 46.34 | 2.67 |
| FAT100 | 5275.65 | 7897.09 | 7962.62 | — |

### O14 verdict, per target line

Target (Appendix G / O14): raft1 **p50 and p99 ≤ postgres** at A20k / A50k /
C1000; **fat-batch ≥ 70 % of the 300k knee**.

| line | raft1 pipe4 | postgres | verdict |
|---|---|---|---|
| A20k p50  | 6.24  | 9.15  | **PASS** (0.68×) |
| A20k p99  | 162.82 | 27.01 | **FAIL** (6.03×) |
| A50k p50  | 19.58 | 23.17 | **PASS** (0.85×) |
| A50k p99  | 444.42 | 75.26 | **FAIL** (5.91×) |
| C1000 p50 | 238.59 | 6.62  | **FAIL** (36.0×) |
| C1000 p99 | 749.57 | 33.02 | **FAIL** (22.7×) |
| FAT100 throughput | ≈90 043 msg/s (5.40 M / 60 s) ≈ **31 %** of the postgres knee | 287 363 msg/s (17.24 M / 60 s) | **FAIL** (<70 %) |

**O14: FAIL.** The pipeline lifts A50k enormously over pipeline=1 and keeps push
p50 competitive on the A-regimes, but the push **tail (p99)** never comes within
range of postgres, **C1000 regresses** at pipeline=4, and **FAT100** stays far
below the knee.

Two honest caveats on the raw comparison, both of which still leave O14 failing:

- **C1000's 238 ms p50 is partly a shared-broker confound, not only fan-out.**
  The regimes run sequentially against one broker; at pipeline=4 the A50k regime
  just before it left a **1.72 M pop backlog** (it popped 150 k of 1.87 M), and
  C1000 runs while that backlog is still draining. This is consistent with the
  inline-driver submission serialising a high-fan-out command stream, but the
  accumulated-backlog drain confounds the absolute number. The postgres oracle
  runs sequentially too, but its A50k did not leave a backlog, so its C1000 is
  clean — the comparison is unfavourable to raft either way, and the standalone
  5-minute A20k (§1b) shows the same tail without any prior-regime backlog.
- **The low popped fractions under burst (A20k 42 %, A50k 8 %, C1000 61 %,
  D1 25 %) are a warm-up artifact, not a store cap** — the 5-minute A20k (§1b)
  drains to lag 730 of 5.9 M. The push latency, not the popped count, is the O14
  metric.

## (1b) 5-minute A20k, sustained (O14 tail + I8 RSS + O18 slow-command)

`fivemin.sh`, fresh broker, `QUEEN_RAFT_PIPELINE=4 QUEEN_RAFT_SLOW_COMMAND_MS=5
LOG_LEVEL=info`, A20k for 300 s.

- **Consume keeps pace over 5 min:** pushed **5 945 350**, popped **5 944 620**,
  lag **730**, ackErr 0. So the short-run pop lag in §1 is warm-up; at steady
  state pipeline=4 sustains 20k/s push **and** 20k/s consume.
- **Sustained latency is worse than the burst:** p50 **44.29**, p99 **419.84**,
  p999 501.76, ack 24.66 ms — because sustained load makes push and pop compete.
  Against postgres A20k (9.15 / 27.01) this is **FAIL p50 and p99**, the same
  verdict as §1 and more honest (no pop lag masking push cost).
- **RSS is not flat — it tracks cumulative pushed (the 3600 s dedup index):** RSS
  climbed monotonically **163 → 1275 MB over 300 s** even though pending ≈ 0
  (everything was consumed). ~**220 B per cumulative message** retained in the
  dedup window. Extrapolated, a sustained 20k/s A20k fills ~11 GB of dedup index
  in ~40 min → the dedup window is the RAM wall (see §2 / R-105).
- **O18 slow-command log: 0 lines.** With the threshold at 5 ms and sustained plan
  times of p50 44 ms, the broker log carried **zero** slow-command / per-kind
  planner lines (the only two `grep` hits were boot `config:` lines). Confirmed
  against the source: `slow_command_ms` / `plan_budget_ms` are read from env into
  `PlanConfig` and `CommandKind` exists, but **no consumer** measures plan elapsed
  or emits the metric. **O18 is staged, not wired, in phase 1.**

---

## (2) Flatness (I8, §13.6, G-3) at pipeline=4

`flatness-night.sh 6 8000000` (the pipeline=4 variant of `flatness-raft1.sh`;
broker env `QUEEN_RAFT_PIPELINE=4`, **verified** — note the `.results` metadata
line still prints the literal `pipeline=1`, a label string the pipeline sed did
not touch; the broker ran pipeline=4). Preload **8 140 400** msgs push-only into
an at-rest queue (`flbg`, 1000 partitions) in 71 s; then A20k and C1000 for 6 min
each on that loaded store and on a fresh empty store. 0 broker error lines, no
OOM (`dmesg` clean).

**Per-run numbers (goload `overall`; RSS from the broker sampler):**

| run | p50 ms | p99 ms | p999 ms | ack ms | cores | disk MB/s | rss start→end MB |
|---|---|---|---|---|---|---|---|
| empty A20k      | 56.58 | 452.61 | 561.15 | 28.18 | 1.31 | 112.9 | 8 → 1520 |
| **loaded** A20k | 61.18 | 477.18 | 1433.60 | 29.06 | 1.31 | 116.2 | **3003** → 4279 |
| empty C1000     | 9.79 | 536.58 | 733.18 | 13.19 | 1.46 | 58.2 | 8 → 1067 |
| **loaded** C1000| 63.23 | 618.50 | 765.95 | 29.23 | 1.38 | 62.1 | **4250** → 4718 |

**`flatness compare` (gate: every gated metric within ±15 %, RSS drift ±5 %):**

| metric | A20k Δ | verdict | C1000 Δ | verdict |
|---|---|---|---|---|
| p50    | +8.1 %  | PASS | **+545.9 %** | **FAIL** |
| p99    | +5.4 %  | PASS | **+15.3 %** | **FAIL** |
| p999   | **+155.5 %** | **FAIL** | +4.5 % | PASS |
| ack_rtt| +3.1 %  | PASS | **+121.6 %** | **FAIL** |
| cpu_cores | 0.0 % | PASS | −5.5 % | PASS |
| disk_mbps | +2.9 % | PASS | +6.7 % | PASS |
| RSS drift within run | empty 8→1520, loaded 3003→4279 (+42.5 %) | **FAIL** | empty 8→1067, loaded 4250→4718 (+11 %) | **FAIL** |
| **overall** | | **FAIL** | | **FAIL** |

**What flatness shows at pipeline=4:**

- **CPU and disk ARE flat** against store size in both shapes (G-3's core claim
  holds: work follows the write rate, not the stored volume).
- **The A20k shape's median and p99 latency are isolated** from an 8 M at-rest
  store (+8 %/+5 %) — a large cold store does **not** leak into fresh A20k
  p50/p99. Its **p999 tail does** (+155 %).
- **The C1000 shape is NOT isolated** — p50 +546 %, p99 +15 %, ack +122 %. A large
  store leaks heavily into the 1000-single-message-partition latency (the same
  fan-out weakness §1 shows).
- **RSS is not flat, in any shape.** At rest **~387 B/msg** (8.14 M → rss_start
  ~3003 MB; on disk 4.0 GB ≈ 528 B/msg), scaling **linearly** with stored count.
  And it climbs **within every run** — even the empty ones (A20k 8→1520 MB,
  C1000 8→1067 MB) — because the 3600 s dedup index retains every pushed message,
  so the ±5 % drift gate fails for the empty baselines too. **This is not a
  store-isolation failure; it is the dedup-window memory model (R-105) showing up
  at pipeline=4.**

**R-105 (~610 B/msg): does it hold? No — at pipeline=4 the at-rest resident is
~387 B/msg**, ~⅓ lower than pipeline=1's 610, consistent with the batched-commit
/ lean-dedup store. But the *shape* of the concern is confirmed: **resident memory
scales with stored volume, it is not flat.**

### The 100 M / 1 M-partition size was not run, and why

The task's 100 M messages over 1 M partitions is **infeasible on this 15 GB VM**,
on three independent grounds:

- **At-rest RSS.** ~387 B/msg (measured above) × 100 M ≈ **36 GB** resident » 15 GB
  (WP-1.11's 610 B/msg would put it at ~57 GB).
- **Per-partition RAM (R-105).** 1 M active partitions multiplies the per-frame
  active-file footprint; C1000's ~0.9 GB for 1000 partitions already shows the
  per-partition cost, so 1 M partitions alone is multi-GB before any payload.
- **Dedup-index growth caps run duration too.** RSS grows ~200–390 B per pushed
  message in the 3600 s window, so a 20-minute loaded run at 20k/s adds ~5 GB on
  top of the preload — the run length, not only the preload, is bounded by RAM.

So the comparison ran at the largest at-rest size leaving headroom for two loaded
runs on one broker: **8 M over 1000 partitions, 6-minute runs**. This is a cut
from the task's 100 M / 1 M / 20 min — stated plainly, with the extrapolation
above standing in for the size that will not fit here. The three-VM O13 rig is
where the plan volume belongs.

---

## (3) Noisy neighbour (O19) at pipeline=4, with a REAL active DLQ storm

`noisy-night.sh` + `dlqstorm.py`. One broker, `QUEEN_RAFT_PIPELINE=4`. Quiet
workload = 3000 msg/s, 200 partitions, 48 consumers on queue `quiet`. The DLQ
storm is a real pop→ack loop: a keep-alive client wildcard-pops leased batches
from the hot queue (queue-mode pop seeds `all`, so it drains the pre-existing
backlog — a **named** consumerGroup would default to `new` and see nothing, the
bug that made the first attempt inert) and acks each `status:"dlq"`
(`success:true, dlq:true, leaseReleased:true` — real DLQ filing), paced to ~500/s.

Phases: **before** (empty store) → build a 2 M backlog on the hot partition →
**during** (quiet + active storm) → **after** (quiet, storm stopped, backlog at
rest). Raw CSVs kept (`storm.csv`, the `quiet-*.gl` per-5 s report series) and
copied to `test/raft/vm/raft1/night/`.

| phase | quiet p50 | quiet p99 | quiet p999 | Δ p99 vs before | verdict (±15 %) |
|---|---|---|---|---|---|
| before (empty store) | 9.28 | **87.55** | 114.18 | — | baseline |
| during (active DLQ storm, 463/s, 45 350 rows filed) | 13.38 | **166.91** | 236.54 | **+90.6 %** | **FAIL** |
| after (storm stopped, 2 M at rest) | 15.30 | **134.14** | 164.86 | **+53.2 %** | **FAIL** |

Storm: `popcalls=909 empties=2 acked_dlq=45 350 ackErr=0 achieved_rate=463/s`
(paced to 500; the client can sustain **12 400/s** unpaced — measured in the probe
— so 463/s is a deliberately light storm). Quiet throughput itself never lagged
(pushed ≈ popped ≈ 265 k every phase); only its **latency** degraded.

**O19: FAIL.** A light real DLQ storm on one partition nearly **doubles** the cold
partitions' p99, and even the **at-rest** 2 M backlog leaves them **+53 %** after
the storm stops. The single shared replicated log + store does not isolate a hot
partition's churn or volume from a cold partition's latency at pipeline=4. This
is a clean numeric verdict (contrast WP-1.11's withdrawn transcription-error
cell); 0 broker error lines throughout.

---

## Commands

```
# build (warm-cache incremental)
cp -a /root/raft/wp111/queen /root/raft/night/queen
rsync -az --delete --exclude target/ server/  VM:/root/raft/night/queen/server/
rsync -az --delete            crates/ VM:/root/raft/night/queen/crates/
cd /root/raft/night/queen/server && cargo build --release --bin queen   # md5 cdc5bb59af27

# (1) six regimes
QUEEN_BIN=…/night/queen/…/queen OUTDIR=…/night/raft1-pipe4 \
  bash measure-raft1.sh pipe4 "A20k A50k B1 C1000 D1 FAT100" "QUEEN_RAFT_PIPELINE=4"
# (1b) 5-min A20k + slow-command capture
bash fivemin.sh                        # QUEEN_RAFT_SLOW_COMMAND_MS=5 LOG_LEVEL=info
# (2) flatness  (pipeline=4 variant of flatness-raft1.sh)
QUEEN_BIN=… OUTDIR=…/night/raft1-flatness FLAT=…/wp111/flatness \
  bash flatness-night.sh 6 8000000
# (3) O19 with real DLQ storm
OUTDIR=…/night/raft1-noisy STORM_TARGET=500 QSECS=90 NOISY=2000000 \
  bash noisy-night.sh                  # uses dlqstorm.py (wildcard pop, ack status:dlq)
```

## Harness notes / deviations

- **dlqstorm.py** (new tonight): the real DLQ generator O19 needed. Wildcard
  queue-mode pop (seeds `all`) + `ack status:"dlq"`, keep-alive, paced. The
  facade does **not** thread the `subscriptionMode` query param in phase 1
  (`facade/real.rs`: "a richer subscriptionMode is threaded by a later WP"), so a
  pre-existing backlog is only reachable via a group-less (queue-mode) pop.
- The pinned path `…/partition/0` returns empty against a goload `-partitions 1`
  queue: the real partition is `p0` / partitionId `1`, and the wildcard pop is
  what drains it. (Recorded so the next agent doesn't re-hit it.)
- `noisy-night.sh`'s broker-RSS sampler captured only its header — it read `$QPID`
  from a subshell forked before the pid was assigned. The gated numbers come from
  the goload `overall` p99, which is unaffected; only the broker-RSS side column
  is missing. `storm.csv` (the storm's own ack-rate series) is intact.
- All numbers pipeline=4, single-VM, co-resident loader (O13: final numbers need
  three VMs).
