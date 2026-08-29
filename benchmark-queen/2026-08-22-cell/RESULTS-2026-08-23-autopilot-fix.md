# Pop autopilot, second live A/B (20:57 UTC) — the fix is a NO-OP on this shape

Rolled onto the running cell without wiping Postgres. Same PG, same 827 000
partitions, same 35 full-quota tenants, same loader processes (untouched — the
SDK did not change, and the loader was already on the autopilot arm
`-pop-partitions 1 -pop-batch 0`). Within-subjects against the broken build
measured 25 minutes earlier on the same cell.

**Verdict: FAIL against the success criterion, but not a regression.** The fix
neither helped nor hurt: p99 227.8 vs 238.5 ms and broker CPU 0.949 vs 0.953
cores against the broken build, both inside the noise. W still converges to 1.

## Timeline (UTC)

| time | event |
|---|---|
| 20:48:06 | `build-broker.sh` starts — rsync + release build ON the cell, competing with the soak for CPU |
| 20:52:2x | build done, `queen:soak` = `sha256:0cb45ef9…` (source verified by sha256 against the Mac working tree) |
| 20:53:55 | roll `cell-broker-b` (passive). First boot **failed** on `procedures/001_log_schema.sql: db error`; container self-healed via one restart, healthy at 20:53:59 |
| 20:57:13 | roll `cell-broker-a` (active). Health 200 at **20:57:16 = T0**, zero restarts |
| 20:57–21:00 | HAProxy failover: 569 5xx (93 push / 465 pop / 11 ack) — the only errors of the entire run |
| 21:05:11 | first fully clean post-roll interval |
| 22:30:11 | last interval of the ≥90 min window (T+93) |

Cell left running with `QUEEN_POP_AUTOPILOT=on` on both brokers. No contingency
was triggered.

## Result

Checkpoints at this DB size spread writes over **1620 s of every 1800 s**
(`checkpoint_completion_target` × `checkpoint_timeout`), so "inter-checkpoint" is
only a ~3 min gap twice an hour and **no 300 s interval is ever fully quiet**.
Bucketing on "in a checkpoint or not" separates nothing. What separates is which
edge an interval straddles — see the note below.

| | baseline (W=10 B=100) | broken autopilot (W→1) | **fix (this run)** |
|---|---:|---:|---:|
| intervals measured | — | 5 (20:25–20:45) | **18 (21:05–22:30)** |
| e2e p50 | 13.5 ms | 14.6 ms | **14.0 ms** |
| e2e p99 overall | **112 ms** | 238.5 ms | **227.8 ms** |
| e2e p99 worst-queue | — | 277.3 ms | **275.7 ms** |
| p99 quiet-tail (best inter-ckpt proxy) | 90–100 ms | 167.8 ms (n=1) | **160.4 ms (n=3)** |
| p99 steady write phase | — | 249.2 ms (n=3) | **226.5 ms (n=12)** |
| p99 checkpoint onset | 160–220 ms | 277.0 ms (n=1) | **300.9 ms (n=3)** |
| throughput | 1 060 msg/s | 1 060 msg/s | **1 060 msg/s, all 18 intervals** |
| cell CPU | ~78% of 800 | 74.9% | **73.8%** |
| broker CPU (a+b) | 0.89 active† | 0.953 cores | **0.949 cores** |
| loss / duplication | — | — | **0 / 0** |

† Baseline broker CPU is not directly comparable: attempt 3 quotes only
`broker-a 89%` and does not give the passive broker, so the a+b figure for the
baseline arm does not exist. Treat it as "same order, ~0.9–1.0 cores".

Against the stated criterion: inter-checkpoint p99 160 ms vs baseline 90–100 ms
is **1.6–1.8× worse**; overall 228 ms vs 112 ms is **2.0× worse**; the CPU saving
is not retained because at this cell state it does not exist. FAIL on three of
four; throughput and correctness pass.

## W behaviour: it still converges to 1, and that is arithmetically correct

* `cands_visit` (global rates, broker-a): **1.0 in 83 of 90 samples**, 1.1 ×6, 1.2 ×1.
* Logged decisions over a 30-min sample: **W=1 ×22, W=2 ×2**. `B=200` throughout.
* A transient `W=4` appears in the first 10 s after boot, while the lane is cold
  and `width()` is free-running the division; it commits to 1 immediately after.
* `ring_depth` (global) is mostly 0–7 and bursts to 131 / 216 / 162.

The fix does what it says — `ready` became a widening TARGET instead of a cap —
but its input is the **per-lane** ready count, and `width()` returns
`clamp_w((ready / m).round())` with `m` = that lane's in-flight poppers. This
workload has **229 lanes**, so a global `ring_depth` of 5 is ≈0.02 ready per lane
and even a 216-deep global burst is <1 per lane. The controller cannot widen
onto a ready set that is empty per lane.

**This means the original diagnosis mislocated the burst.** The "28 partitions
ready" that motivated the fix was the GLOBAL ring depth across 229 lanes, not one
lane's ready set. Making the smoothed ready count a widening driver was therefore
inert on this shape by construction.

Corroborating that the tail is not a pop-width phenomenon: p50 is flat at
13–15 ms in the baseline, the broken build and the fix alike, and the tail tracks
the checkpoint edge monotonically — **onset 301 > steady 227 > quiet-tail 160** —
in *both* the broken and fixed builds. That is a storage write-wall signature,
not a claim-width signature.

## The comparison the criterion asks for cannot be made from today's data

The baseline numbers (112 ms, 78% CPU) were measured at **~2M segments** during
the 11:11 run. This cell is now at **43.3M segments / 17 GB**, with checkpoints
flushing 4.4–7.4 GB apiece. The only matched-state comparison available is fix vs
broken, 35 minutes apart, and those are equal within noise.

**There is no baseline arm in today's cell state.** Settling "is autopilot worse
than W=10 B=100 at 43M segments" needs a broker rolled to
`QUEEN_POP_AUTOPILOT=off` for 30+ min on this cell. Not run — outside the brief.

Two secondary consequences of the same cross-state problem:

* The handoff's "broken autopilot = 380 ms" is the **top of its range, not its
  level**: re-measured over 25 min immediately before this roll, the same build
  averaged 238.5 ms p99 with 380 ms as its single worst interval.
* The handoff's "~17% CPU saving" is not visible here at all. Broker CPU is only
  ~1.0 core of the ~5.9-core cell; a 1.04-core cell-level saving cannot come from
  the brokers. It would have to be a Postgres-side effect of B=200 vs B=100, and
  both arms measured today run B=200, so it is untested at this state.

## Correctness

* **Watermark backlog: 12 messages** across 827 000 (partition, group) pairs, worst
  partition 1 behind — normal in-flight state, not loss and not a stall.
* `cross_tenant` 0, `err_429` 0, `err_403` 0, `err_timeout` 0, `err_conn` 0,
  `shed` 0, `dlqMessages` 0, `ackFailed` 0.
* Error counters are **cumulative** in `*-interval.csv`, not per-interval. Total
  569, all incurred in the 20:57 failover, and **flat from 21:00:11 to 22:30:11**
  — 90 minutes with zero new errors.
* `verdict.py` cannot judge an in-flight soak: it reports
  `runs with a verify block: 0` because the 30-day run has produced no final
  artifacts. The watermark check above is the substitute the rerun doc prescribes.
* `pop_empty_pct` 0.1, `ready_age_p95` 5 ms, `pool_waiting` 0 throughout.

## Per-statement cost at 43.3M segments (unchanged by this build)

| statement | calls/window | ms/call |
|---|---:|---:|
| `log_hotlist_reseed_window_v1` | 1 618 | 17.6 |
| `log_queue_stats_all_v1` | 70 | 88.7 |
| `log_refresh_all_stats_v1` | 3 | 6 669 |
| pop `SELECT meta/blobs/states` | 178 183 | 3.35 |

Hot-list reseed still flat (17.6 ms at 43M segments vs 8–17 ms at 2M) — the
attempt-3 fix holds. `log_queue_stats_all_v1` has drifted 43 → 89 ms with
partitions pinned at 827 000, worth a look but unrelated to this change.

## Rig traps found this session — both cost real measurement

1. **`build-broker.sh` destroys the rollback anchor.** Under the containerd
   snapshotter, re-pointing the `queen:soak` tag drops the previous untagged
   image. `bd0e0be8…` — the ID the handoff tells you to record — was pinned only
   by the running containers and could not be re-tagged or re-run, so **image
   rollback was unavailable for this entire deploy**. The handoff's "verify by
   image ID, not tag" implies the old ID is a rollback anchor; it is not.
   Fix: `docker tag queen:soak queen:soak-prev` **before** every build. This run
   was gated instead by rolling the passive broker first, and the config escape
   hatch (`QUEEN_POP_AUTOPILOT=shadow`) remained available on the new image.
2. **`roll-broker.sh` blinds `sampler.sh`.** The sampler resolves each
   container's cgroup path once at startup and caches it; `docker rm -f` +
   `docker run` changes the container ID, so **both broker CPU columns in
   `samples/soak.csv` have read 0.000 since the rolls**. pg/proxy/lb columns stay
   valid. All broker CPU here was read directly from cgroup v2 instead
   (`cpu-cgroup-samples.txt`). A roll must restart the sampler, or the sampler
   must re-resolve.
3. Minor: `cell-broker-b`'s first boot failed with
   `schema apply failed: procedures/001_log_schema.sql: db error` and recovered
   on the automatic restart. Rolling a broker re-applies the schema against a
   live cell; the retry papers over it, but it is a real race worth a look.

## Raw data

`raw/2026-08-23-autopilot-fix/` — `loader-soak/` (27 queue interval CSVs + logs),
`cell-samples/{soak.csv,soak-db.csv,spstrend.csv}`, `broker-a-autopilot.log`,
`broker-a-global-rates.log`, `broker-b-autopilot-and-errors.log`,
`checkpoints.log`, `cpu-cgroup-samples.txt`, `lattrend-final.txt`,
`broker-images.txt`.

---

# Matched-state baseline arm (22:42–23:38 UTC) — the write-wall does NOT own the tail

The missing measurement from the section above: hand-tuned `W=10 B=100` **at
today's cell state**. Loader restarted with the knobs PINNED
(`POP_PARTS=10 POP_BATCH=100`), so the SDK sends `batch=100&partitions=10` with
no autopilot parameter — the broker's manual path, on the fixed build. Broker
untouched throughout: same image `0cb45ef9…`, `QUEEN_POP_AUTOPILOT=on`.

| time | event |
|---|---|
| 22:42:00 | loader → pinned arm (`-pop-partitions 10 -pop-batch 100`), 27 procs, fix series kept as `soak-autopilotfix-2242` |
| 22:44 | push/s back to 1 060, all 27 queues flowing |
| 22:52–23:37 | 10 clean intervals (the 22:47 interval is the startup transient) |
| 23:39:47 | loader → autopilot arm restored (`-pop-partitions 1 -pop-batch 0`), baseline kept as `soak-baseline-2339` |
| 23:40 | push/s 1 060 confirmed — **cell restored to its prior state** |

## A confound had to be controlled first

The two arms did not draw equal checkpoints. The fix arm spanned flushes of
**21.5 / 32.5 / 65.6 / 20.5 %** of `shared_buffers`; the baseline arm only
**20.7 / 19.8 %**. Comparing raw window means would credit the baseline for
drawing easier checkpoints. The MATCHED row below is the honest comparison:
steady-phase intervals governed by a ≤21.5 % checkpoint, which both arms have.

## Four-way, all at their own segment counts

| | stale baseline | broken autopilot | fix autopilot | **matched baseline** |
|---|---:|---:|---:|---:|
| pop config | W=10 B=100 | W→1 B=200 | W→1 B=200 | **W=10 B=100** |
| segments | ~2M | 36.5M | 37–43.7M | **43.9–47.4M** |
| intervals | — | 5 | 20 | **10** |
| e2e p50 | 13.5 ms | 14.6 ms | 14.1 ms | **15.9 ms** |
| **e2e p99 overall** | **112 ms** | 238.5 ms | 224.5 ms | **146.2 ms** |
| p99 worst-queue | — | 277.3 ms | 272.1 ms | **155.2 ms** |
| p99 quiet-tail | 90–100 ms | 167.8 (n=1) | 173.5 (n=4) | **137.0 (n=1)** |
| p99 steady | — | 228.9 (n=4) | 222.5 (n=13) | **147.3 (n=9)** |
| p99 onset | 160–220 ms | 277.0 (n=1) | 300.9 (n=3) | **— (none drawn)** |
| **p99 MATCHED (steady, ≤21.5 % ckpt)** | — | — | **199.3 (n=8)** | **147.3 (n=9)** |
| throughput | 1 060 | 1 060 | 1 060 | **1 060, all 10** |
| cell CPU | ~78 % | 74.9 % | 73.8 % | **77.1 %** |
| broker CPU (a+b) | — | 0.953 | 0.949 | **0.977** |

## The decisive reading — both effects are real, and they are separable

**W=10 B=100 does NOT recover 112 ms at 43M segments; it lands at 146 ms.** So
~34 ms (~30 %) of the gap from the stale baseline is owed to the cell state —
20× the segments, 17→19 GB — and no pop configuration will give that back.

**But the checkpoint write-wall does not own the tail either.** At matched
checkpoint intensity, pop configuration alone moves p99 from **147.3 → 199.3 ms
(+35 %)**. That swing is autopilot's, it is separable from the write-wall, and
it is the larger of the two terms.

The trade is now priced with both arms at matched cell state: autopilot buys
**0.26 cores (3.3 points of cell CPU, 73.8 % vs 77.1 %)** and pays **+35 % p99**.
The handoff's "~17 % CPU for 3.4× tail" was wrong in both directions — the CPU
saving is ~4 %, not 17 %, and the tail cost is +35 %, not +240 %. It is still
clearly the wrong side of the trade.

Shape corroborates: the baseline trades a slightly WORSE median (p50 15.9 vs
14.1) for a far tighter tail (p99max 155.2 vs 272.1) and never exceeds 200 ms in
any interval (worst 199.6), while the fix arm breached it repeatedly (393, 440).
That is the distribution shape a 200 ms SLO wants.

## Divergence diagnostic — validated, works as designed

58 lines over 58 minutes on `cell-broker-a` (0 on passive `cell-broker-b`):

* **58 lines, 58 DISTINCT tenant+queue lanes — not one repeat.** Once per lane.
* Every line carries both numbers: `explicit=10 controller=1`, all 58 identical.
* Inter-line gap exactly **60 s, 57 of 57** — a global rate limiter admits one
  lane's first-time diagnostic per minute, so 229 lanes drain over ~229 min.
* `suppressed` climbs 0 → **63 177**, i.e. ~every pop diverges and all but one
  per minute are elided. **It does not repeat per-pop.**
* Emitted at WARN, correctly stating the explicit value is the one being used.

It also confirms the earlier finding independently: with the client explicitly
asking for 10 partitions, `cands_visit` stayed at **1.0** — the broker cannot
visit 10 partitions because the per-lane ready set does not contain 10. W is not
the lever; `B=100` vs `B=200` and the resulting claim shape is.

## Anomalies

* None in the load. Throughput 1 060 in all 10 intervals, `pool_waiting` 0,
  `ready_age_p95` 5 ms, no errors, no shed.
* `dead` tuples swung 21k → 276k → 21k across the arm (autovacuum cycling on
  `log_partitions`); unrelated to the arm, noted so it is not read as a leak.

## Raw data

`raw/2026-08-24-baseline-matched/` — `loader-soak-baseline/` (27 queue interval
CSVs + logs), `divergence.log`, `checkpoints-baseline.log`,
`broker-a-global-rates-baseline.log`, `cpu-cgroup-samples-baseline.txt`,
`lattrend-baseline.txt`, `spstrend.csv`. The fix-arm series is also kept on the
loader as `/root/soak/soak-autopilotfix-2242` and the baseline as
`/root/soak/soak-baseline-2339`.

---

# Autopilot B=100 (00:03 UTC 2026-08-24) — parity NOT demonstrated, and the cell OOM'd

`QUEEN_POP_AUTOPILOT_BATCH`, default 100 (`AUTO_BATCH_DEFAULT`). Explicit client
batch untouched, W law unchanged. Loader NOT touched — already on the delegating
arm (`-pop-partitions 1 -pop-batch 0`), which is the arm under test.

| time | event |
|---|---|
| 23:56 | `docker tag queen:soak queen:soak-prev` — rollback anchor secured **before** the build (the trap from the first deploy) |
| 23:57–00:01 | build; new image `ee7d2d64…` ≠ `0cb45ef9…`; source sha256-verified, `AUTO_BATCH_DEFAULT: i32 = 100` present |
| 00:01:06 | roll passive `cell-broker-b` — **restarts=0, 0 errors**; the schema race did not recur |
| 00:02:54 | roll active `cell-broker-a`; health 200 at **00:03:19 = T0**; restarts=1, the known `001_log_schema.sql` race (within the stated envelope) |
| 00:09:50–01:29:50 | 17 clean intervals (80 min) |
| **01:35:14** | **global OOM kill → Postgres crash recovery** (see below) |
| 01:37:35 | PG ready; cell self-recovered to 1 060 msg/s by 01:44 |

`pop_autopilot_batch=100` confirmed in both brokers' boot config and in every
`pop autopilot chose … batch=100` decision line.

## Five-way

| | stale baseline | broken autopilot | fix B=200 | matched baseline | **autopilot B=100** |
|---|---:|---:|---:|---:|---:|
| pop config | W=10 B=100 | W→1 B=200 | W→1 B=200 | W=10 B=100 | **W→1 B=100** |
| segments | ~2M | 36.5M | 37–43.7M | 43.9–47.4M | **49.5–54.7M** |
| intervals | — | 5 | 20 | 10 | **17** |
| p50 | 13.5 | 14.6 | 14.1 | 15.9 | **16.5** |
| p99 overall | **112** | 238.5 | 224.5 | 146.2 | **168.2** |
| p99 worst-queue | — | 277.3 | 272.1 | 155.2 | **197.1** |
| p99 quiet-tail | 90–100 | 167.8 | 173.5 | 137.0 | **173.3** |
| p99 steady (all) | — | 228.9 | 222.5 | 147.3 | **152.7** |
| p99 onset | 160–220 | 277.0 | 300.9 | — | **219.9** |
| **p99 MATCHED (≤21.5 % ckpt)** | — | — | 199.3 | **147.3** | **175.0** |
| worst single interval | — | 379.9 | 440.0 | 199.6 | **348.3** |
| throughput | 1 060 | 1 060 | 1 060 | 1 060 | **1 060, all 17** |
| cell CPU | ~78 % | 74.9 % | 73.8 % | 77.1 % | **77.3 %** |
| broker CPU (a+b) | — | 0.953 | 0.949 | 0.977 | **0.972** |

## Prediction vs measured

| prediction | measured | held? |
|---|---|---|
| matched-bucket p99 ≈ 147 ms | **175.0** (all-steady 152.7) | **NO** on the pre-registered bucket |
| p50 ~15–16 ms | 16.5 | yes (marginally over) |
| worst interval < ~200 ms | **348.3** | **NO** |
| throughput 1 060 flat | 1 060, all 17 | yes |
| cell CPU 76–77 % | 77.3 % | yes |

**Re-based criterion = FAIL.** Matched-bucket 175.0 is +18.8 % against 147.3 and
above the stated 165 ms failure line. CPU 77.3 % vs 77.1 % is parity, not below.
Loss/dup zero. So one of three sub-criteria passed cleanly.

Three things must be said alongside that, none of which rescue it:

1. **The intensity control is falsified in this window.** The 86.8 % checkpoint
   produced the BEST intervals (110.7 / 103.5 / 67.4) and the 12.9 % checkpoint
   the WORST (149.7 / 207.1 / 348.3 / 188.5). "MATCHED" therefore excludes the
   three best intervals purely because their checkpoint was heavy — the premise
   is inverted here. By the all-steady comparator the arm is 152.7 vs 147.3
   (+3.7 %, inside the band). Both numbers are in the table; the pre-registered
   one governs the verdict.
2. **The arm ran at +15 % segments** (49.5–54.7M vs 43.9–47.4M) — a harder
   condition, and segment count demonstrably drives cost on this cell.
3. **An OOM was building throughout the window** (below), so the tail in the
   degraded middle band (00:34–01:14) is not attributable to B alone.

## What B=100 did buy, and what it cost

B=100 moved the autopilot arm from 199.3 → 175.0 matched-bucket (−12 %) and
222.5 → 152.7 all-steady (−31 %) against the B=200 fix. It also **gave back the
entire CPU saving**: 73.8 % → 77.3 %, landing exactly on the hand-tuned baseline's
77.1 %. That is the coherent finding of the night: **B is the single live lever,
and tail and CPU trade against each other along it.** Autopilot at B=200 is
cheap and slow; at B=100 it is baseline-priced and near-baseline-fast. There is
no configuration in this campaign that is both.

## ANOMALY (serious): global OOM killed a Postgres backend

```
01:35:14 bash invoked oom-killer: ... global_oom, task=postgres, pid=805118
         Out of memory: Killed process 805118 (postgres)
01:35:15 client backend (PID 39544) was terminated by signal 9: Killed
         DETAIL: Failed process was running: SELECT queen.log_ack_multi_v1(...)
         LOG: terminating any other active server processes
01:37:35 database system is ready to accept connections
```

Postmaster-wide crash recovery, ~2 min 20 s. Throughput dipped to 536 msg/s for
one interval, p99 3 349 ms, then fully recovered to 1 060. No data loss: fsync
and `synchronous_commit` are on, recovery completed cleanly, and the watermark
check afterwards is clean. Below the >10 %-for->5-min rollback threshold, so no
rollback was taken.

**This is `RERUN-2026-08-23.md`'s open memory item finally biting**, ~14.4 h into
the soak against its own ~16 h estimate. RSS at 01:42:

| container | RSS |
|---|---:|
| cell-pg | 10 794 MB (4 GB shared_buffers inside) |
| cell-broker-a | 1 592 MB (rolled 00:03, ~100 min old) |
| **cell-broker-b** | **1 757 MB — PASSIVE, `push_s=0`/`pop_s=0`** |
| cell-proxy | 724 MB |
| sum | 14 867 MB of 15 991 MB |

**The passive broker holds more anonymous memory than the active one at zero
traffic.** That cannot be a workload working set. `dedup_cache_mb=512` is a fixed
per-broker reservation, leaving ~1.1–1.2 GB per broker unexplained and
accumulated in ~100 minutes — far faster than the ~300 MB/h the earlier
open item recorded. This now outranks autopilot as the thing to fix: the cell
has ~2.1 GB available and will OOM again within hours.

## Correctness

Watermark **21 messages** in flight across 827 000 (partition, group) pairs,
worst 1 behind. `cross_tenant` 0. Error counters (cumulative) flat since the
00:03 roll at 2 139 5xx total, all from the failover. Zero loss, zero duplication.

## Cell end state

Both brokers on `ee7d2d64…` (fix + B=100), `QUEEN_POP_AUTOPILOT=on`, loader
delegating both knobs, 1 060 msg/s, `pool_waiting` 0. Rollback anchor
`queen:soak-prev` = `0cb45ef9…` (B=200 fix) is on the box if it is wanted.

## Raw data

`raw/2026-08-24-autopilot-b100/` — `loader-soak-b100/`, `cell-samples/`,
`checkpoints-b100.log`, `kernel-oom.log`, `pg-oom-crashrecovery.log`,
`container-rss-oom-context.txt`, `cpu-cgroup-samples-b100.txt`,
`broker-a-autopilot-b100.log`, `broker-a-global-rates-b100.log`,
`broker-a-errors.log`, `lattrend-b100.txt`.

---

# Overnight mitigation 01:55 UTC — restart reclaimed 3.3 GB, and found the mechanism

Protective only; all measurement arms were finished. Rationale: the 01:35 OOM
plus ~3.3 GB apparently reclaimable from the two brokers made a restart cheap
insurance, and the re-accrual was the free leak diagnostic.

`docker restart` (not a re-`run`), so image and env are preserved by definition —
verified anyway on both: image `ee7d2d64…`, `QUEEN_POP_AUTOPILOT=on`,
`pop_autopilot_batch=100`, health 200. Each broker took the known
`001_log_schema.sql` boot race exactly once and self-healed — within envelope.

## RSS before / after

| | 01:55:24 before | 02:00:31 after both | 02:31:33 T+30 |
|---|---:|---:|---:|
| cell-pg | 10 101 MB | 11 399 MB | 11 822 MB |
| cell-broker-a | 1 626 MB | **161 MB** | 657 MB |
| cell-broker-b | 1 757 MB | 1 519 MB | 1 584 MB |
| cell-proxy | 725 MB | 786 MB | 778 MB |
| **system available** | **1 945 MB** | **3 109 MB** | **1 333 MB** |

Failover cost: broker-a's restart returned in 3 s; `push/s` was back at 1 060
within 60 s, and no interval registered a throughput dip. Cheaper than the
`roll-broker.sh` path, which does `docker rm -f` + `docker run`.

## Re-accrual verdict: TIME-DRIVEN, and the mechanism is the hot-list ready ring

The passive broker's own `sizes:` line names it:

| time | broker-a (ACTIVE, push_s=1060) | broker-b (PASSIVE, push_s=0) | avail |
|---|---|---|---:|
| 01:55 pre | `229rings/0ready/5wheel`, 1 626 MB | `229rings/827000ready/0wheel`, 1 757 MB | 1 945 MB |
| 02:00 +0 | `…`, 161 MB | 38 MB → 1 519 MB | 3 109 MB |
| 02:04 +5 | `229rings/0ready/1055wheel`, 329 MB | `229rings/308531ready`, 1 536 MB | 2 593 MB |
| 02:10 +10 | `229rings/1ready/1056wheel`, 507 MB | `229rings/567927ready`, 1 582 MB | 2 142 MB |
| 02:15 +15 | `229rings/2ready/1056wheel`, 623 MB | `229rings/777977ready`, 1 588 MB | 1 806 MB |
| 02:24 +23 | `229rings/3ready/1076wheel`, 653 MB | `229rings/**827000**ready`, 1 597 MB | 1 496 MB |
| 02:31 +30 | `229rings/1ready/1062wheel`, 657 MB | `229rings/827000ready`, 1 584 MB | 1 333 MB |

**It re-accrued to the full 827 000-entry ring in ~24 minutes with `push_s=0` and
`pop_s=0` throughout — zero traffic. The accrual is time/background-driven, not
traffic-driven.** The reseed lane (`hotlist_reseed_ms=30000`, full walk every
`hotlist_reseed_full_ms=300000`) runs on a timer regardless of load and seeds
every partition in the cell into the ready ring. The ACTIVE broker drains its
ring — `ready` stays 0-3 — so it sits at ~650 MB. **The PASSIVE broker has no
drain path, so the ring fills to the entire cell partition count and stays
there**: ~1.6 GB (~1.9 KB/entry) of memory for a ring it can never consume.
Before the restart, `ring_oldest_ms` on broker-b had climbed to 6 718 073 ms
(112 min) and advanced exactly +10 s per 10 s — the oldest entry was never taken.

This is bounded, not unbounded: it plateaus at `partitions × ~1.9 KB` per broker.
But a standby broker paying ~1.6 GB for it is a real defect, and it scales with
cell cardinality — at 827k partitions it is 10 % of the box.

## Survival projection to 08:00 UTC: NO

With both brokers now flat (broker-a 653→657 MB, broker-b plateaued), available
memory is still falling — 1 806 → 1 496 → 1 333 MB, i.e. ~21-29 MB/min, driven by
Postgres, whose working set tracks segments (58.2M and rising ~65k/min, DB
23.1 GB). Postgres holds ~11.8 GB and is untouchable.

Linear at the observed ~21 MB/min, 1 333 MB is gone by **~03:35 UTC**; if it
decelerates to ~10 MB/min, ~04:45. Either way the cell does **not** reach 08:00
without a second OOM. Expect one between roughly 03:30 and 05:00.

The previous OOM was survivable — PG crash-recovered in 2 m 20 s, no data loss,
throughput self-restored — so the risk is to measurement continuity, not data.

**Honest assessment of the mitigation: net-negative within 30 minutes.** It
reclaimed 1 164 MB at 02:00, but restarting the passive broker forces a failover
that makes it briefly active, which triggers the very ring-seeding that consumes
the memory; 30 minutes later available memory (1 333 MB) is *below* where it
started (1 945 MB). The value delivered was the diagnosis, not the headroom.

The fix is not a restart: it is to stop a broker with no drain path from holding
a full-cell ready ring — either don't seed rings on a standby, or bound/evict the
ready ring by age when nothing is consuming it.

## End state

Both brokers `ee7d2d64…` (fix + B=100), `QUEEN_POP_AUTOPILOT=on`,
`pop_autopilot_batch=100`, loader untouched and delegating, 1 060 msg/s,
`pool_waiting` 0, `readyAge95` 5 ms. One OOM total on the box (01:35:14); none
since. Anchor `queen:soak-prev` = `0cb45ef9…` still on the box.
