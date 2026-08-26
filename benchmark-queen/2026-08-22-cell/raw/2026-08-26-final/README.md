# 2026-08-26 final capture — soak cell before power-off

Raw data for the 1.2.0 cell campaign (2026-08-23 11:11 UTC → 2026-08-26 08:38 UTC).
Both machines were destroyed after this capture; nothing else survives.

## Topology
8 vCPU / 15.6 GiB cell (Postgres 18.6, both brokers, LB, proxy in Docker) +
4 vCPU loader running 27 goload processes. 36 tenants, 230 queues,
827,000 partitions, 832,000 (partition, group) consumer rows, ~1060 msg/s.

## The publishable window
Brokers self-report **1.2.0**, both started 2026-08-24 16:01/16:02 UTC and never
restarted after. Config frozen from that point; the loader was untouched. That
window (16:04 → 2026-08-26 06:37) is the one clean, single-configuration stretch:
zero errors end to end, and the database grew 41 → 100 GB underneath it, which is
what makes it a controlled measurement of latency against data volume.

Everything BEFORE 16:04 on 08-24 is a debugging session, not a benchmark: six
broker rolls, DB_POOL_SIZE 300→96, retention parallelism 1→4, a retention
subsystem replaced mid-flight, autopilot knobs toggled. Do not quote it as a
steady-state result. It is kept because the before/after pairs are the evidence
for what the campaign fixed.

## Retention collapse test (final hours)
At 2026-08-26 06:37:53 UTC all 195 remaining queues were dropped from 3-day and
7-day retention to 24 hours in one statement, making ~810k partitions eligible at
once. Sustained ~3,000 segment deletes/s (from ~90/s) with the serving path
unaffected: push/pop 1066, readyAge95 5 ms, pool wait 0, zero errors. 17.1M
segments deleted total; 241.6M → 228.8M remaining at capture. The burn was still
running when the machines were powered off — it was NOT allowed to complete, and
no partition had yet been cleared to the 24 h boundary (oldest_live_h 69.45).

## Contents
- `cell/final-state.txt` — sizes, counts, versions, disk, memory at capture
- `cell/env-*.txt` — full broker/proxy environment: the config every number was produced under
- `cell/queue-retention.csv` — per-queue retention config as it stood at the end
- `cell/pg-stat-statements-top40.txt` — per-statement cost at ~229M segments (calls|total_ms|mean_ms|query)
- `cell/table-sizes.txt` — relation sizes, live/dead tuples, last autovacuum
- `cell/rates-*.log.gz` — broker `rates:` lines (scope="global"): throughput, ready_age, cands_visit,
  admission lanes, and the burst telemetry added in 1.2.0 (ring_depth_max, max_lane_ready, pop_wait_max,
  ready-entry provenance)
- `cell/sizes-*.log.gz` — broker `sizes:` lines: RSS, hot-list ring occupancy, pool, dedup
- `cell/events-*.log.gz` — retention, schema-apply, autopilot and watermark-walk events
- `cell/samples/` — soak.csv (per-container CPU/RSS), soak-db.csv (storage trend), spstrend.csv
- `loader/soak/` — THE clean-window series: 27 per-queue interval CSVs
  (t_sec, wall_utc, offered/pushed/popped/acked, e2e p50/p99, push p50/p99, error counters)
- `loader/soak-baseline-2339/` — pinned W=10 B=100 arm (matched-state autopilot A/B)
- `loader/soak-autopilotfix-2242/` — autopilot arm of the same A/B
- `loader/soak-preautopilot/` — the original pre-autopilot series from campaign start

## Known data caveats
- `samples/soak.csv` broker CPU columns read 0.000 after any `roll-broker.sh` — the sampler caches
  cgroup paths at start and a rolled container gets a new one. Broker CPU was read directly from
  cgroup v2 instead; pg/proxy/lb columns stay valid throughout.
- `samples/soak-db.csv` has a ~19 h gap (2026-08-24 14:39 → 2026-08-25 09:31): the collector died
  and was restarted. Its `partitions`/`segments` columns are `reltuples` ESTIMATES that only refresh
  on autovacuum, so treat them as approximate; `db_bytes` is exact.
- Error counters in the interval CSVs are CUMULATIVE, not per-interval. The campaign total of
  ~3.27M 5xx is four bursts (two broker rolls, two Postgres OOMs), all identified in RESULTS; the
  last 38+ hours are entirely error-free.
- The two OOMs (2026-08-24 01:35, 03:39) predate the memory work; the box was never OOM-free until
  the standby-ring trim, intern footprint and DB_POOL_SIZE=96 changes landed together.
