# test-perf — scenario-based performance harness

A reproducible harness that runs a set of operational scenarios against a locally-built `queen-server`, captures throughput + latency + server-log signals, and produces a markdown report you can compare across campaigns (e.g., before vs. after a libqueen change).

## What it does, per run

1. Tear down any previous bench PG container and its volume.
2. Start a fresh PostgreSQL 16 in Docker with CPU/memory caps from the scenario (`--cpus=N`, `--memory=Mg`) **and** `--cpuset-cpus=0..N-1` to pin PG to specific CPUs inside the Docker VM. `shared_buffers = 25% of RAM`, `max_connections = 300`, fsync on, etc.
3. Wait for PG to accept real queries (`psql -c "SELECT 1"`, twice, 1 s apart — `pg_isready` alone is racy).
4. Start `./server/bin/queen-server` with `NUM_WORKERS`, `DB_POOL_SIZE` (total shared AsyncDbPool), `SIDECAR_POOL_SIZE` (total, split across workers) from the scenario. Log goes to `server.log`.
5. Wait for `/health`.
6. Warn if the number of "heavy threads" (producer + consumer + queen workers) exceeds host CPUs − 2. Flagged in `config.json` as `host_oversubscribed`.
7. Run a 10 s producer warm-up (results discarded).
8. Snapshot PG counters (`pg_stat_database`, `pg_stat_user_tables` for `queen.messages`, per-status row counts) → `pg-stats-pre.json`.
9. Start background `stats-stream.sh` sampling `docker stats` for the PG container and `ps` for the server PID every ~2 s → `stats-stream.jsonl`.
10. Run the producer and consumer **in parallel** for 60 s (configurable).
11. Stop the streamer. Snapshot PG counters again → `pg-stats-post.json`.
12. Fetch `/api/v1/resources/queues/...`, `/metrics` (if present), one-shot `docker stats`, grep `server.log` for lag / errors / queue_full.
13. Kill the server, tear down PG.

Three runs per scenario, then `analyze.js` aggregates into `aggregate.json` + `report.md`.

### Ground-truth metrics (not just autocannon)

Autocannon tells us HTTP request rate and error counts. That's not enough — an empty pop returns 200 even though 0 messages were delivered. So we also diff `pg_stat_*` across the measurement window to get:

- `xact_commit/s` — actual PG commits per second
- `queen.messages` inserts/s = **actual messages pushed** (ground truth, from `pg_stat_user_tables.n_tup_ins` — the table is append-only so every insert = one pushed message)
- `queen.partition_consumers` updates/s = **pop call rate** (ground truth for how often the consumer cursor advanced; not messages popped, since a batch=100 pop is still one update)
- `tup_fetched/s`, `buffer_hit_ratio`, `deadlocks`, `xact_rollback`

And we sample CPU during the window so we can say whether PG (docker) or the server (host) was the bottleneck.

## One-time setup

```bash
nvm use 24

# Build the server once (deps must already be downloaded).
cd server && make deps   # only if vendor/ is empty
make build-only
cd ..

# Install workload node modules
(cd test-perf/workload && npm install)

# Mark scripts executable
chmod +x test-perf/scripts/*.sh
```

Requirements: Docker running locally, Node.js 24+, and the `queen-server` binary built at `server/bin/queen-server`.

## Usage

Run all 7 scenarios × 3 runs (~32 min):

```bash
nvm use 24 && ./test-perf/scripts/run-all.sh
```

Run a subset:

```bash
nvm use 24 && ./test-perf/scripts/run-all.sh 1 4 6
```

Smoke test (1 run per scenario, ~11 min):

```bash
nvm use 24 && RUNS_PER_SCENARIO=1 ./test-perf/scripts/run-all.sh
```

Even shorter for debugging the harness itself:

```bash
nvm use 24 && RUNS_PER_SCENARIO=1 WARMUP_SECONDS=3 MEASURE_SECONDS=10 \
  ./test-perf/scripts/run-all.sh 1
```

## Scenarios

Declared in `scenarios.json`. The 3 presets are all **non-oversubscribed** on a 10-CPU host (heavy threads < 10):

| # | PG | Queen | Producer | Consumer | Heavy threads | Purpose |
|---|---|---|---|---|---:|---|
| S0 | 2c / 4g | 1 worker  | 2w / 100c | 1w / 25c | 4 | Single-worker baseline |
| S1 | 2c / 4g | 2 workers | 2w / 100c | 1w / 25c | 5 | Same load, 2 queen workers (S0→S1 = effect of +1 queen worker) |
| S3 | 4c / 8g | 2 workers | 4w / 200c | 1w / 50c | 7 | Bigger PG + doubled load, same queen (S1→S3 = PG+load scaling) |

Common to all: `SIDECAR_POOL_SIZE=200` (total, split across workers), `DB_POOL_SIZE=20` (total shared AsyncDbPool), PG `max_connections=300`, push batch = 10 items/request, pop batch = 100, **measurement window 120 s** (tight stddev).

**Partitions per scenario** = `max(500, 2 × producer.connections)` → S0/S1 use 500, S3 uses 500 (since 2×200=400). Keeps producer concurrent connections per partition ≤ 0.5, so there's no write contention.

**Consumer uses wildcard long-poll pop** (`GET /api/v1/pop/queue/<q>?batch=...&wait=true&timeout=5000`). This is the production consumer path:

- **Wildcard** — server picks any available partition via `FOR UPDATE SKIP LOCKED` on `queen.partition_lookup`. Real consumers don't know/care which partition to pull from.
- **Long-poll (`wait=true`, 5 s server timeout)** — request parks on the server until a message arrives (via internal poll or UDP notify from a producer on another worker) or the timeout fires. Exercises the `pop_backoff_tracker` + UDP notification path in libqueen — exactly the code the hot-path plan touches.

The `MAX_PARTITIONS` setting is now purely a producer-side concern: consumers never "poll" a specific partition, so the per-scenario partition count just bounds producer write-contention.

Latency interpretation changes with long-poll:

- Under load: pop latency ≈ "time for the first message to arrive at an available partition" — usually low-ms.
- Under drain: pop latency can climb to the server timeout (5 s). If most pops hit the timeout, the consumer has outpaced the producer.

Clean single-axis deltas:

- **3 → 4**: +2 queen workers (same PG, same load)
- **4 → 5**: +2× client load (same Queen, same PG)
- **5 → 6**: +2× queen workers (same load)
- **6 → 7**: +2× client load (same Queen)

## Output layout

```
test-perf/results/2026-04-20_19-05-12/
├── meta.json                       # campaign-wide: git sha, branch, dirty, scenarios, timings, host info
├── scenario-1_pg2c4g_q2w_prod2w100c/
│   ├── run-1/
│   │   ├── config.json             # fully resolved scenario+timings+host CPU budget
│   │   ├── producer.json           # autocannon raw
│   │   ├── producer-warmup.json    # (discarded) warmup run
│   │   ├── consumer.json           # autocannon raw
│   │   ├── server.log              # full server stdout+stderr
│   │   ├── server-analytics.json   # /api/v1/resources/queues/<q>
│   │   ├── server-metrics.txt      # /metrics if present
│   │   ├── docker-stats.json       # one-shot PG container stats (fallback)
│   │   ├── stats-stream.jsonl      # time series: PG CPU%, server CPU%, every 2s
│   │   ├── pg-stats-pre.json       # PG counters snapshot before measurement
│   │   ├── pg-stats-post.json      # PG counters snapshot after measurement
│   │   ├── log-summary.txt         # grep of event-loop lag, errors (cosmetic warnings filtered)
│   │   └── run.log                 # combined stdout+stderr of this run
│   ├── run-2/ ...
│   └── run-3/ ...
├── scenario-2_.../
├── ...
├── aggregate.json                  # per-scenario mean / stddev / min / max + PG ground truth + resource usage
└── report.md                       # human-readable comparison table
```

## How to interpret results

1. **Open `report.md`.** Top table has the headline; per-scenario detail has PG ground truth + resource usage.
2. **Primary signal = `pg_ins/s`** — actual messages inserted into `queen.messages`, measured from `pg_stat_user_tables.n_tup_ins`. `push msgs/s` is a client-side estimate and isn't reliable when the server returns 2xx then drops the write, or when retries inflate the count.
3. **`pop_upd/s` is your pop-truth check.** `queen.partition_consumers` is only UPDATEd when a pop actually returns messages (advancing the cursor). If `pop req/s` is high but `pop_upd/s` is near zero, the consumer was spinning on empty partitions — no real work done.
4. **Treat deltas as noise** if `|Δ|` < 2 × stddev on that metric. The stddev column shows cross-run variability.
5. **⚠️ host-oversubscribed rows are invalid for server-side claims.** The laptop was bottlenecked on the client-side bench, not the server. We flag these but keep them for reference.
6. **Who's the bottleneck?** Look at the PG CPU% and server CPU% in the per-scenario section:
   - PG CPU% ≈ `PG_CPUS × 100%` → **PG is bound** (disk, CPU, locks). Libqueen improvements may not help until PG scales up.
   - PG CPU% well below cap + high push_p99 → **queen is bound** (its internal hot path, slot contention, or intake). This is where the libqueen plan should move the needle.
   - Both low + high p99 → **client-side** is the bottleneck (autocannon saturated, or host scheduler).
7. **Any non-zero `errors`/`non2xx`** invalidates the run. Common causes: PG hit `max_connections`, server OOM, socket exhaustion on the client. Re-run; if persistent, tune the scenario.
8. **`queue_full hits`** in the per-scenario detail → the harness triggered a backpressure guard (only relevant once the libqueen hot-path changes land).

## Comparing two campaigns (before vs. after a change)

There's no diff tool yet — the workflow is:

1. Run the full campaign on your baseline branch. Copy the resulting `results/<stamp>` somewhere durable.
2. Apply your change, rebuild (`make build-only`), run the campaign again.
3. Open both `report.md` side by side, or `diff` their `aggregate.json`.

A `compare-campaigns.js` script can be added later if we find ourselves doing this often.

## Troubleshooting

- **PG fails to start**: check `docker logs queen-pg-bench`. Usually the volume wasn't wiped; `./test-perf/scripts/pg-down.sh` then retry.
- **Server dies at boot**: the server needs the `lib/schema/` directory relative to its CWD. `server-up.sh` `cd`s to the repo root before launching; don't change that.
- **Port conflicts**: the harness uses `6632` (server) and `5434` (PG). If something else owns those, override via `SERVER_PORT` / `PG_PORT` before calling `run-all.sh`.
- **Node.js version**: workload uses ESM + top-level await; needs Node 24+.
- **Autocannon non-2xx spam**: usually means PG hit connection limit. Lower `DB_POOL_SIZE_PER_WORKER` or raise `PG_MAX_CONN`.
- **macOS memory caps**: Docker Desktop has its own VM memory limit. Scenario 7 wants 8 GB for PG — check Docker Desktop → Settings → Resources has enough.

## Why these scenarios, not a full grid

A full sweep of the original axes (Queen × PG × Producer × Consumer) is ~10,000 combos × 3 runs ≈ weeks. The 7 scenarios were chosen so consecutive pairs isolate a single axis change (see table above), giving clean "what-caused-this" deltas while finishing in under an hour.
