# CM-BENCH — pgmq A/B, pgmq-head ceiling, Kafka@1000 clean — 2026-08-03

Two questions from today's campaign table forced this session:

1. Is "pgmq FAIL — order violations" a real structural verdict, or an artifact?
2. Yesterday's fair matrix recorded pgmq@l1000 at **262 ms PASS**; today's clean run
   did **623 ms FAIL** with nominally identical config. Which number is real?

Same rig as the campaign: broker 104.248.245.59, loader 159.89.104.168, 8 vCPU /
16 GB each, R = 2000 ev/s, P = 1000 properties, lanes = 1000, 180 s rated window,
drain 90, fresh volumes (`down -v`) before every run below, queen stack stopped,
pgmq image `ghcr.io/pgmq/pg17-pgmq` @ **pgmq 1.12.0** (the only image this VM has
ever had — `docker images -a` shows no other), binary = the committed
`cmbench-linux-amd64` (md5 `aa5f239b…` matches the loader's copy bit-for-bit).

## The A/B

| run | read fn | ramp | e2e A p50 | p95 | p99 | verdict | broker cores | disk write | reads |
|---|---|---|---|---|---|---|---|---|---|
| ab-pgmq-head | **read_grouped_head** | 10 | **185.4** | 311.7 | 571.7 | **PASS 0/0/0** | 7.06 | 29.8 MB/s | 374 280 |
| clean-pgmq (campaign) | read_grouped_rr | 10 | 623.5 | 1247.0 | 1482.9 | FAIL — 9 viols | 7.22 | 29.3 MB/s | 191 926 |
| ab-pgmq-rr-ramp20 | read_grouped_rr | 20 | 623.5 | 1247.0 | 1617.1 | FAIL — 21 viols | 7.2 | 29 MB/s | 215 152 |
| ab-rr20-rep | read_grouped_rr | 20 | 741.5 | 1482.9 | 1923.1 | FAIL — 59 viols | 7.2 | 29 MB/s | 202 632 |
| ab-rr-ramp60 | read_grouped_rr | 60 | 571.7 | 1143.5 | 1617.1 | FAIL — 23 viols | not sampled | | 326 993 |

**head PASSes, and it is ~3.4× faster than rr at the same CPU and the same disk.**
Counting today's campaign runs, rr is 6/6 in the degraded-FAIL state on clean
rigs: 9, 11, 15, 21, 23, 59 violations. Even a 60 s ramp — the gentlest start
tested — tips into it (571 ms, FAIL). This is not noise, and on this rig rr has
no clean path to the healthy equilibrium at this workload.

## Why rr violates ordering: a demonstrated upstream race

`read_grouped_rr`'s per-group exclusivity rests on three legs that do not compose
(SQL identical in 1.11.1 and 1.12.0, and identical to pgmq main):

1. the per-group **advisory xact lock is released when the read commits**, not
   when processing finishes;
2. the group-head visibility gate is evaluated on the statement's **MVCC
   snapshot**, which is stale for any read that started just before another
   read's commit;
3. the **final guard** `m.vt <= clock_timestamp()` in the UPDATE re-checks rows
   individually under EvalPlanQual — it drops the head the other reader just
   took and **serves the survivors**, i.e. the group's NEXT messages, while the
   head is still in flight elsewhere.

Adversarial repro (`repro-read-grouped-rr-race.go`, faithful mirror of the
adapter's readLoop): **4096 exclusivity breaches in 90 s** on rr (`read_ct=1`,
0 dups — concurrent delivery of the same lane, not redelivery, e.g. "reader 6
read g001 seq 14 while seqs 9–13 in flight on reader 4"); **0 in
read_grouped_head** under identical adversarial load. head cannot serve a
non-head message by construction: when EvalPlanQual kills its only candidate,
the group yields nothing.

The race window is proportional to the `fifo_groups` scan (which scans the whole
table **regardless of visibility**), so it opens with backlog. The verifier only
sees a violation when the two handlers' recordings then invert — which is why a
degraded run shows 9–59 first-occurrence violations out of 2.1 M deliveries
rather than thousands, and why `ackErr=0` (making them fatal per the contract).

NOTE: the pgmq adapter's package comment claimed the OPPOSITE safety story
("rr's advisory lock makes exclusivity true; head would reorder"). That comment
is wrong for the shipped SQL and has to be rewritten before anyone trusts the
default again.

## Yesterday's 262 ms PASS is a contaminated measurement — discard it

Two independent facts:

- **The two fair pgmq runs overlapped.** `fair-pgmq-l200/result.json` was written
  at 12:22:58 UTC; `fair-pgmq-l1000/result.json` at 12:26:09 — 191 s apart, and a
  run's process lives ≥ 200 s (ramp 20 + rated 180) before drain even starts. So
  the l1000 process started while the l200 process was still alive and reading:
  its setup, warmup and early ramp ran co-tenant with the previous run's tail
  (and with 96 extra readers polling the same, un-prefixed queue names). The
  262 ms row was not measured under the campaign's conditions.
- **The degraded state is the reproducible one.** Five clean runs today, three of
  them bit-identical in invocation to the fair row (rr, ramp 20, fresh volumes,
  quiet VM, Kafka and Rabbit reproducing yesterday's numbers exactly on the same
  box) all landed at 623–741 ms FAIL. rr's healthy equilibrium was never
  reachable today from a cold, clean start.

Mechanism of the degraded state (measured via 2 s sampling of
`pg_relation_size` over the queue tables, `broker-ab-rr20-rep.size`): the queue
tables stabilise at **~40 MB / ~110 k standing rows**, every rr read re-scans
them in `fifo_groups`, the read loop slows to ~2× (191–215 k reads vs 335 k),
which sustains the very backlog that slows it — a self-reinforcing equilibrium.
Same CPU (7.1–7.2 cores), same disk; the cores go into fatter scans instead of
more reads. head never enters this state: its reads stay cheap, the loop runs at
374 k reads and the standing lag stays ~700 messages.

## What this changes in the campaign table

- The honest pgmq row at 1000 lanes is **head: 185 ms p50, PASS, 7.06 cores,
  29.8 MB/s** — quoting rr's FAIL as "pgmq cannot pass" would be refuted by any
  maintainer in an afternoon, and quoting yesterday's 262 would be quoting a
  contaminated run.
- The Queen-vs-pgmq verdict flips on p50 (Queen 312 vs pgmq-head 185) and must be
  re-argued on the axes where the architecture actually differs: fan-out
  materialisation (6 inserts/ingress event), no consumer groups, polling, no
  dedup, and — decisive at higher rates — head has **no depth batching**: a
  group's backlog drains at most one message per read that touches it. At 2 ev/s
  per lane that ceiling is invisible; the §6.2 ceiling sweep is where it bites.
  Run pgmq-head through the ceiling sweep before writing the narrative.
- rr remains reportable as a finding: pgmq's only depth-batching grouped read has
  a demonstrated ordering race under concurrent readers, and its degraded
  equilibrium is self-reinforcing. Upstream issue worth filing with the repro.

## pgmq-head ceiling (SPEC.md §6.2) — the knee is at ~2500 ev/s

Fresh volumes per point, 300 s rated, ramp 20, drain 60, P = 1000, lanes = 1000,
`read_grouped_head`. Grid chosen as a search (head at 2000 was already at
7.06/8 cores, so the §6.2 default start of 5000 would have skipped the knee):

| rate | e2e A p50 | p95 | p99 | order | inflight at cutoff | shed | broker cores | disk | served the rate |
|---|---|---|---|---|---|---|---|---|---|
| 2000 (A/B run) | 185 ms | 312 | 572 | 0/0 | 6 | 0 | 7.06 | 29.8 MB/s | yes |
| 2500 | 312 ms | 1247 | 1763 | 0/0 | 168 | 0 | 7.15 | 36.0 MB/s | **yes — last PASS** |
| 2750 | 7 054 ms | 15 385 | 16 777 | 0/0 | 227 316 | 0 | 7.14 | 37.6 MB/s | no — runaway backlog |
| 3000 | 30 770 ms | 61 539 | 67 109 | 0/0 | 786 156 | 70 244 | 7.24 | 34.4 MB/s | no — collapse + shed |

Three facts worth keeping:

- **The ceiling is CPU-bound and sharp.** All four points sit at ~7.1–7.2 cores;
  at 2500 the offered rate still fits inside that budget (with the p95 already
  warning at 1.2 s), at 2750 it no longer does and the backlog runs away.
- **head fails SAFE.** Even in full collapse it never produced a single ordering
  violation, gap or dup — it degrades by lag only. rr at the same workload
  produces violations well below its throughput ceiling. The failure MODES, not
  just the numbers, are the A/B.
- **Giving up depth-batching did not lower the ceiling.** rr saturated at
  ~2400–2450 ev/s on this rig (2026-08-02 pool-160 measurement); head serves
  2500. rr's depth advantage is consumed by its fatter scans; head's cheap reads
  buy back what one-message-per-group-per-read costs.

For the campaign narrative: at R = 2000 this box is at 80% of pgmq-head's
ceiling. Queen's matched-lanes ceiling on this rig is still unmeasured (July
data says it serves 25 000 ev/s on the same 1000-partition shape, i.e. its
economics IMPROVE with rate); Kafka at 2.6 cores has ~3× CPU headroom. Run
those two ceilings before writing "X scales further than Y".

## Kafka @ 1000 lanes, clean re-run (today's binary, fresh volumes)

Same shape as the campaign table (R = 2000, ramp 10, 180 s): **p50 142.9,
p95 170.0, p99 202.1, PASS 0/0/0**, 4000 partitions provisioned, full rate
served, **2.59 cores** mean (p95 4.27), 13.1 MB/s disk. Bit-for-bit consistent
with yesterday's fair-matrix row (143/170/202 at 2.41 cores) and with today's
200-lane row (142.9) — Kafka's lane-count insensitivity reconfirmed on a clean
rig, and the campaign table's Kafka column needs no correction.

## Queen ceiling @ 1000 lanes — wildcard and targeted (campaign config)

Same rig discipline (fresh volumes per point, 300 s rated, ramp 20, drain 60,
P = lanes = 1000). Config pinned to what produced the campaign's 311.7 ms row:
image `queen:trace9`, fusion ON (shards 4 / concurrency 4 / max-jobs 8), Vegas
push+pop windows 16/4/64, `-queen-pop-partitions 40`, 96 pop workers.

| mode | rate | e2e A p50 | p95 | p99 | inflight at cutoff | shed | broker+PG cores | disk | served |
|---|---|---|---|---|---|---|---|---|---|
| wildcard | 2000 (campaign anchor) | 312 ms | 572 | 742 | — | 0 | 6.0 | 24 MB/s | yes |
| wildcard | 2500 | 1 049 ms | 3 234 | 4 988 | 8 418 | 0 | 1.51+4.10 = 5.61 | 25.2 | **yes — last PASS** |
| wildcard | 3000 | 1 360 ms | 12 937 | 21 757 | 69 948 | 0 | 5.77 | 25.7 | marginal — no |
| wildcard | 5000 | 51 748 ms | 61 539 | — | 786 684 | 559 035 | 5.59 | 25.1 | collapse |
| targeted | 2000 | 7 054 ms | 11 863 | 12 937 | **12** | 0 | 5.03 | 21.2 | yes (lag-free, lap-bound) |
| targeted | 3000 | 4 194 ms | 7 054 | 7 692 | **36** | 0 | 5.65 | 19.9 | **yes — last PASS** |
| targeted | 5000 | 14 108 ms | 36 591 | — | 629 664 | 27 306 | 5.87 | 20.8 | no |
| targeted | 4000 | aborted mid-run (operator stop) | | | | | | | not measured |

Correctness: 0 violations / 0 gaps / 0 dups / 0 redeliveries at every point, both
modes, including the collapses.

Findings:

- **Queen's ceiling here is NOT a window/config wall — tested and refuted the
  same day** (see "Vegas probe + forced-wide A/B" below). Every ceiling point
  sits at 5.6–5.9 total cores with 2+ cores idle — against pgmq's 7.1–7.2/8 —
  and forcing the windows wide moved ~1 core into PG while making everything
  worse. The idle cores are structural to the pop path at this shape (the
  synchronous WAL-bound write-tx per pop and the lane-revisit geometry), not
  reclaimable concurrency. July's 25 000 ev/s on this shape ran on different
  hardware; what that rules in/out here is still open.
- **Wildcard's knee is at ~2500** (1 049 ms — already 3.4× the 2000 point) and
  3000 does not drain. pgmq-head at the same rates: 312 ms at 2500, dead at
  2750. On this box, in these configs, pgmq-head dominates Queen-wildcard's p50
  across the whole servable range at a similar ceiling — Queen's answer has to
  come from the tuning axis (or the pop-path work in flight), not from this
  table.
- **Targeted is lag-free but lap-bound at sparse lanes**: it drains everything
  (inflight 12–36!) at a p50 that IS the sweeper's lap (96 workers × ~125
  partitions × ~48 ms ≈ 6–7 s at 2000, tightening to 4.2 s at 3000 as yield per
  visit rises). It out-serves wildcard on throughput (3000 clean vs wildcard's
  marginal) and dies at 5000 like everything else on this box. The fair-matrix
  warning about quoting targeted latency numbers effectively still stands at
  these densities.

## Vegas probe + forced-wide A/B — the window was the defence, not the wall

Hypothesis under test: the same limiter that leaves 2+ cores idle at Queen's
ceiling also doubles its p50 against pgmq-head at 2000 ("widen the windows and
both improve").

**Probe first, not a run** (30 s of broker telemetry under a 2500 load,
`vegas-probe/`): at full tilt — push 4 955/s, pop 14 948/s, ack 14 833/s —
`vegas_push=6, vegas_pop=7` on min 4 / init 16 / max 64 (glued near the MIN,
the max never approached), `pool=110/160, pool_waiting=0` (pool exonerated),
`hot="4/4"` on both hot queues, per-op p50_push 9.2 ms / p99_pop 68.9 ms.
So raising the MAX could never have mattered; the controller was *choosing* to
sit at 6–7. The A/B that follows forces the MIN.

Forced-wide config: SEG push+pop min 64 / init 128 / max 512, fusion
concurrency 8, max-jobs 32 (`wideenv`), image and everything else identical:

| run | vegas held at | e2e A p50 | p95 | pool | broker+PG cores | outcome |
|---|---|---|---|---|---|---|
| narrow @2000 (campaign) | 6–7 | **312 ms** | 572 | 110/160 | 6.0 | serves |
| **wide @2000** | 128 | **961 ms** | 2 287 | 159/160 | 6.55 (PG 4.5→5.1) | serves, 3× worse |
| narrow @5000 | — | 51 748 ms | — | — | 5.6 | collapse |
| **wide @5000** | 128 | 47 453 ms | — | — | 6.6 | same collapse, **fewer pops** (212 k vs 276 k) |

Verdict: **hypothesis refuted on both sides.** The Vegas back-off to 6–7 was
the *defence* — per-op latency at width 128 tripled (push p50 9.2→25.9 ms,
p99_pop 68.9→177.7 ms), the extra in-flight became queueing in front of the
same WAL/commit path, and the ceiling did not move. The two idle cores are not
retrievable via concurrency; the binding constraint is the pop path's
synchronous write-tx (the floor measured on 2026-08-02) plus the lane-revisit
geometry — which is also why pgmq-head's 185 ms at 2000 is not a tuning gap:
its 96 free-running pollers pay per-consumer commits with no ring lap, a
different delivery geometry, not a better window setting.

What still moves the needle for Queen at this shape, in lever order: cheaper /
better-amortised pop write-tx (the low-latency plan's ~100–130 ms floor), more
yield per visit (pp40 + batched claim already bought 1049→312), and only then
controller-signal work (fsync-floor vs queueing separation — roadmap, NOT
implemented; today's telemetry says it is about robustness, not the current
130 ms). Open questions parked with data attached: what exactly caps pops/s at
~650–900 (p99_pop 68.9 ms × 96 workers implies ~3× more headroom than
observed — per-pop wait breakdown is the next probe, `waits-*.psv` style), and
what `hot="4/4"` actually counts (it did not move when fusion concurrency went
4→8; suspect it displays shards).

## Rig defects to fix before the next campaign

1. `pgmq.go` package comment: safety story inverted (see above).
2. `docker-compose.pgmq.yml` pins `:latest`. Pin the digest; today the VM ran
   pgmq 1.12.0 while the dev laptop's months-old `:latest` was 1.11.1.
3. cmbench has no build stamp; only an md5 against the committed binary proved
   what the campaign ran. Embed git hash + build time in `result.json`.
4. Nothing stops two cmbench processes overlapping on one rig — that is exactly
   how the fair pgmq row got contaminated. Add a lockfile or a
   previous-process check to the runner or run-campaign.sh.
