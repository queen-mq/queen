# Fusion hysteresis sweep

Local experiment to find **where the queen fusion engine starts reducing the
per-message load on Postgres** — and whether the up-sweep and down-sweep trace
the same curve or a hysteresis loop.

## The idea

PG cost decomposes roughly as:

```
CPU_pg ≈ commits/s × fixed_commit_cost(fsync+WAL)  +  rows/s × per_row_cost  +  maintenance(~O(partitions))
```

- **Low offered rate** → each push is (almost) its own commit, `msgs/commit ≈ 1`,
  the fixed per-commit **fsync dominates**, so **PG cost *per message* is high**.
- **Rising rate** → the engine batches (FIRE on `preferred_batch_size=50` OR
  `max_hold_ms=20`, self-clocked group commit). `msgs/commit` climbs, `commits/s`
  and `fsync/s` plateau, and **cost per message falls**.
- **Too much** → PG saturates: `achieved` peels away from `offered` (the cliff).

Absolute PG cores usually rise monotonically; the thing to watch for is a *dip*
near the knee, and a *loop* between up- and down-sweep (genuine hysteresis from
the backlog-driven self-clocking).

## Why a new load tool (pacer.mjs)

Every existing harness in this repo (autocannon, goload, `queenctl bench`) is
**closed-loop**: a fixed client pool blocks on each response. In closed-loop you
*cannot offer more than PG serves* — when PG slows, clients slow with it, so you
never reach or observe saturation. `pacer.mjs` is **open-loop**: it dispatches
exactly `RATE` req/s on a wall clock, independent of latency. When PG falls
behind, in-flight grows to a cap and the offered/achieved gap is the signal.

Push-only, one message per request (`push_batch=1`), so the PG-side
`msgs/commit` == requests/commit == the pure **server-side fusion ratio**.

## Run it

```bash
cd benchmark-queen/fusion-hysteresis

# 1. pinned Postgres (2 cores so the cliff is reachable on a laptop), port 5439
docker compose up -d --wait

# 2. the locally-built broker, pointed at it (run from anywhere; the script cds
#    to the repo root so the schema bootstraps). Needs server/bin/queen-server.
PG_PORT=5439 PORT=6632 ./run-broker.sh        # leave running in its own terminal

# 3. the sweep (up then down). ~11 min with the default ladder.
PG_PORT=5439 ./sweep.sh

# -> runs/run-<stamp>/results.csv, results.json, report.html
open runs/run-*/report.html
```

## Knobs

| env | default | effect |
|-----|---------|--------|
| `PG_CPUS` (compose) | 2.0 | moves the saturation cliff |
| `SYNC_COMMIT` (compose) | on | **the control variable** — see below |
| `RATES_UP` / `RATES_DOWN` | 50…80000 | offered-rate ladder |
| `WARMUP` / `MEASURE` / `SETTLE` | 8 / 20 / 4 s | per-step timing |
| `PARTITIONS` | 8 | kept small so O(partitions) maintenance stays negligible |
| `QUEEN_PUSH_MAX_HOLD_MS` (broker) | 20 | fusion hold window |
| `QUEEN_PUSH_PREFERRED_BATCH_SIZE` (broker) | 50 | early-fire threshold |
| `QUEEN_PUSH_MAX_CONCURRENT` (broker) | 24 | in-flight commits (lower = more fusion) |
| `QUEEN_PUSH_BATCH_INFLIGHT_THRESHOLD` (broker) | 0 | 1 = fire immediately when idle (no fusion until backlog) — reshapes the low-load knee |

## Experiments worth running

1. **Baseline** — default config. The headline curves are `msgs/commit` and
   `cores/Mmsg` vs offered rate.
2. **Durability control** — `SYNC_COMMIT=off docker compose up -d --wait`, re-run.
   With async commit there is no per-commit fsync to amortize, so the fusion
   benefit on PG load should largely vanish. This isolates *what* fusion buys you.
3. **Hold / threshold sweep** — re-run with `QUEEN_PUSH_MAX_HOLD_MS=0` or
   `QUEEN_PUSH_BATCH_INFLIGHT_THRESHOLD=1` on the broker to see the low-load end
   change shape (latency down, fusion later).

## Gotchas baked into the setup

- **Background stats maintenance is O(total-partitions) and load-independent**;
  at low rate it would dominate PG CPU and mask the per-commit signal. `run-broker.sh`
  raises `STATS_RECONCILE_INTERVAL_MS`/`STATS_INTERVAL_MS` and `RETENTION_INTERVAL`
  to silence it, and the pacer uses few partitions.
- **`synchronous_commit` is the variable that makes fusion matter.** Keep it `on`
  for the real measurement; flip it `off` only as the control arm.
- **Postgres 17 on purpose.** `pg_stat_wal` still exposes `wal_sync` / `wal_write`
  directly here. On PG18 those moved into `pg_stat_io` (filter `object='wal'`,
  column `fsyncs`); `snapshot-pg.sh` would need that swap if you bump the image.
- **Ground truth, not client counts.** Achieved throughput is read from
  `Δqueen.messages.n_tup_ins`, fusion from `Δn_tup_ins / Δxact_commit`, fsync from
  `Δpg_stat_wal.wal_sync` — all server-side, so client-side artifacts don't lie to you.

## Files

| file | role |
|------|------|
| `docker-compose.yml` | pinned Postgres 17, `synchronous_commit` toggle, WAL I/O timing on |
| `run-broker.sh` | launches local `queen-server` with a clean-measurement env |
| `pacer.mjs` | open-loop, rate-paced, push-only generator (no deps) |
| `snapshot-pg.sh` | one-shot JSON of pg_stat_database + pg_stat_wal + queen.messages |
| `sweep.sh` | drives the pacer up then down, snapshots PG per step, samples PG CPU |
| `analyze.mjs` | computes metrics → `results.csv` + `report.html` (up vs down overlay) |
