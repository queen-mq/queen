# How to run these benchmarks

This document describes how to reproduce any of the runs in this folder. All scripts are in `[_runner/](./_runner/)` and were used as-is on `root@138.68.70.90`.

## Prerequisites

### Host requirements

- Linux x86_64 (tested on Ubuntu 24.04, kernel ≥ 6.0)
- ≥ 32 vCPU recommended (16 will work but limits peak throughput)
- ≥ 64 GiB RAM (Postgres `shared_buffers=24GB` + working set + Queen + autocannon clients ≈ 35 GiB used at peak)
- No swap recommended (we ran with `Swap: 0B`)
- Docker with BuildKit (`--ulimit nofile=65535`)
- `bash`, `node ≥ 24`, `jq`, `curl`, `python3` for log analysis

### Software

```bash
# Verify
docker --version          # ≥ 24
node --version            # ≥ 24 (the published run used v22.22.2)
nvm use 24 && which node  # if using nvm
which jq                  # for analytics-endpoint parsing
```

### Node modules (autocannon, axios)

The benchmark scripts use the existing `/home/queen/examples/` `package.json` which already pins:

```json
{
  "type": "module",
  "dependencies": {
    "autocannon": "^8.0.0",
    "axios": "^1.13.2"
  }
}
```

If you're starting fresh:

```bash
mkdir -p /home/queen/examples
cd /home/queen/examples
echo '{"type":"module","dependencies":{"autocannon":"^8.0.0","axios":"^1.13.2"}}' > package.json
npm install
```

### Docker images

```bash
# Pull whichever queen versions you want to test
docker pull smartnessai/queen-mq:0.14.0.alpha.3
docker pull smartnessai/queen-mq:0.12.19
docker pull postgres:latest
```

### File layout on the test host

```
/root/bench-runs/
├── bench-producer.js          # parameterized producer (also copied to /home/queen/examples/)
├── bench-consumer.js          # parameterized consumer (also copied to /home/queen/examples/)
├── run_test_v3.sh             # single-test runner (most general)
├── run_master.sh              # 0.14.0.alpha.3 axis orchestrator (10 tests)
├── run_v12_master.sh          # 0.12.19 axis orchestrator (5 tests)
├── run_cg_master.sh           # consumer-group axis orchestrator (2 tests)
└── results/
    └── <test-name>/           # per-test artifacts (logs, JSON dumps, metadata)
```

The producer and consumer **must** be copied to `/home/queen/examples/` so that Node's module resolution finds the `node_modules` there:

```bash
cp /root/bench-runs/bench-producer.js /home/queen/examples/
cp /root/bench-runs/bench-consumer.js /home/queen/examples/
```

## The runner script — `run_test_v3.sh`

This is the workhorse. **One invocation = one full test cycle**: cleanup, container start, warm-up, run, metric collection, summary.

### Signature

```bash
./run_test_v3.sh <test_name> <max_partition> <msgs_per_push> [duration] \
                 [prod_workers] [prod_conn] [cons_workers] [cons_conn] \
                 [cons_batch] [queue_count] [queen_image_tag] [cg_count]
```


| Position | Arg               | Default          | Meaning                                                              |
| -------- | ----------------- | ---------------- | -------------------------------------------------------------------- |
| 1        | `test_name`       | required         | name used for queue (`bench-<name>`) and output dir                  |
| 2        | `max_partition`   | required         | producer's `MAX_PARTITION` (partitions = MP+1)                       |
| 3        | `msgs_per_push`   | required         | producer's `MSGS_PER_PUSH` (batch size)                              |
| 4        | `duration`        | 900              | seconds to run autocannon                                            |
| 5        | `prod_workers`    | 1                | producer Node `cluster.fork()` count                                 |
| 6        | `prod_conn`       | 50               | autocannon connections per producer worker                           |
| 7        | `cons_workers`    | 1                | consumer Node cluster forks per consumer process                     |
| 8        | `cons_conn`       | 50               | autocannon connections per consumer worker                           |
| 9        | `cons_batch`      | 100              | consumer GET `?batch=N`                                              |
| 10       | `queue_count`     | 1                | how many queues to spread load across                                |
| 11       | `queen_image_tag` | `0.14.0.alpha.3` | docker image tag                                                     |
| 12       | `cg_count`        | 0                | number of distinct consumer groups (0 = no group / `__QUEUE_MODE__`) |


### What it does (in order)

1. **Cleanup**: `docker stop/rm -v queen postgres` + `docker volume prune -f` + recreate `queen` Docker network.
2. **Start Postgres** with the full tuning (`shared_buffers=24GB`, autovacuum aggressive, etc. — see script for the complete list).
3. **Wait for Postgres ready** (`pg_isready`, up to 60 s).
4. **Start Queen** with `NUM_WORKERS=10`, `DB_POOL_SIZE=50`, `SIDECAR_POOL_SIZE=250`, `nofile=65535`.
5. **Wait for Queen ready** (HTTP 200 on `/api/v1/status`, up to 120 s).
6. **5 s settle** to let the system stabilize.
7. **Capture `start_time`**.
8. **Spawn producer** (one process: `node bench-producer.js`) with appropriate env vars.
9. **Spawn consumer(s)**: if `cg_count > 0`, spawn `cg_count` consumer processes each with `CONSUMER_GROUP=cg-1..cg-N`. Otherwise spawn one consumer with no group.
10. **Wait `duration` seconds**, logging `docker stats` once per minute.
11. **30 s grace** for clients to finalize.
12. **Capture `end_time`**, kill any stragglers.
13. **Collect metrics**: hit `/api/v1/status`, `/analytics/{retention,queue-ops,system-metrics,postgres-stats}`, `/resources/queues/<name>`, `docker logs queen`, `docker stats` final snapshot.
14. **Write `metadata.json`** with the full configuration.

### Outputs

```
/root/bench-runs/results/<test_name>/
├── metadata.json              # full configuration record
├── start_time.txt             # ISO8601
├── end_time.txt               # ISO8601
├── producer.log               # autocannon-style result JSON line(s)
├── consumer.log               # (if cg_count=0) OR
├── consumer-cg1.log           # (if cg_count>0)
├── consumer-cg2.log           # ...
├── status.json                # GET /api/v1/status snapshot for window
├── retention.json             # /analytics/retention
├── queue-ops.json             # /analytics/queue-ops
├── system-metrics.json        # /analytics/system-metrics
├── postgres-stats.json        # /analytics/postgres-stats
├── queue-resource-first.json  # /resources/queues/<first-queue-name>
├── queues-list.json           # /resources/queues
├── docker-stats.log           # one-per-minute snapshots
├── docker-stats-final.txt     # docker stats at end
└── queen.log                  # full docker logs queen output
```

## Common recipes

### Single test, default settings (production-realistic)

```bash
# bp-10: 1 worker × 50 conns, batch=10, 1001 partitions, 15 min
/root/bench-runs/run_test_v3.sh bp-10 1000 10
```

### Specific producer/consumer scaling

```bash
# hi-part-1: 5×100 producer, 2×50 consumer, 2 partitions, batch=1, 15 min
/root/bench-runs/run_test_v3.sh hi-part-1 1 1 900 5 100 2 50 100 1
```

### Specific image version

```bash
# Same bp-10 setup but on 0.12.19
/root/bench-runs/run_test_v3.sh v12-bp-10 1000 10 900 1 50 1 50 100 1 0.12.19
```

### Multi-queue (10 queues)

```bash
# q-10: 1×50 producer rotating across 10 queues
/root/bench-runs/run_test_v3.sh q-10 1000 10 900 1 50 1 50 100 10
```

### Consumer groups (5 groups, each in its own process)

```bash
# bp-10-cg5: same as bp-10 but with 5 consumer groups
# Note arg 12 = 5 means: spawn 5 consumer processes, each with consumerGroup=cg-1..cg-5
/root/bench-runs/run_test_v3.sh bp-10-cg5 1000 10 900 1 50 1 50 100 1 0.14.0.alpha.3 5
```

## Running an axis (multiple tests in sequence)

The orchestrator scripts run several configurations back-to-back. They `nohup` cleanly so you can disconnect.

```bash
# Run the full 0.14.0.alpha.3 axis (10 tests, ~160 min wall time)
cd /root/bench-runs
nohup ./run_master.sh >/dev/null 2>&1 & disown
tail -f /root/bench-runs/master.log     # watch progress

# Or the 0.12.19 comparison axis (5 tests, ~80 min)
nohup ./run_v12_master.sh >/dev/null 2>&1 & disown

# Or the consumer-group axis (2 tests, ~32 min)
nohup ./run_cg_master.sh >/dev/null 2>&1 & disown
```

Each orchestrator is a simple bash array of test specs. Editing it to add a new run:

```bash
# Edit /root/bench-runs/run_master.sh, add a new line to the TESTS=() array.
# Format: name:maxPart:msgs:prodW:prodC:consW:consC:consBatch:queueCount
TESTS=(
  "my-new-test:1000:10:1:50:1:50:100:1"
  ...
)
```

The cg orchestrator has an extra field for cg_count. The v12 orchestrator hard-codes `IMAGE_TAG=0.12.19`.

## Stopping a running benchmark

The orchestrator and its current test are separate processes. To stop the orchestrator without disturbing the in-flight test:

```bash
# Find the orchestrator PID
ps -ef | grep -E 'run_master|run_v12_master|run_cg_master' | grep -v grep

# TERM the parent only — the in-flight run_test_v3.sh continues to completion
# but no new tests will start
kill -TERM <pid>
```

To kill the in-flight test too:

```bash
ps -ef | grep -E 'run_test_v3|bench-producer|bench-consumer' | grep -v grep
kill -9 <pids>
docker stop queen postgres
```

## Interpreting results

### From the producer log (`producer.log`)

```json
{ "role": "producer", "event": "result",
  "totalRequests": 3515078,
  "totalMessages": 35150780,
  "reqPerSec": 3906,
  "msgPerSec": 39060,
  "latency": { "p50": 11, "p90": 15, "p99": 38, "avg": 12.31, "max": 664 },
  "errors": 0, "non2xx": 0, "timeouts": 0,
  "durationSec": 900 }
```

- `msgPerSec` is the headline producer throughput (`reqPerSec × MSGS_PER_PUSH`)
- `latency.p99` is HTTP push latency from the producer's POV
- `errors` and `timeouts` are autocannon-side; check `queen.log` for server-side errors

### From the server status (`status.json`)

```bash
# Steady-state ingest (avg over high-throughput buckets)
jq '[.throughput[] | select(.ingestedPerSecond > 5000) | .ingestedPerSecond] | add/length' status.json

# Total messages pushed/popped (lifetime)
jq '.messages.requests.push, .messages.requests.pop' status.json

# Errors
jq '.errors' status.json
```

### Heartbeat aggregation (from `queen.log`)

Worker heartbeats look like:

```
[Worker 2] [libqueen] push(q=0 f=1/24 p99rtt=23ms) pop(q=0 f=1/16 p99rtt=39ms) custom(...) slots=23/25 jobs/s=348 drains=32106520 evl=0ms
```

Aggregating push/pop p99rtt percentiles:

```bash
grep 'libqueen' queen.log | python3 -c "
import re, sys
push, pop = [], []
for ln in sys.stdin:
  m = re.search(r'push.*p99rtt=(\d+)ms.*pop.*p99rtt=(\d+)ms', ln)
  if m: push.append(int(m.group(1))); pop.append(int(m.group(2)))
def stats(a):
  a = sorted(a); n = len(a)
  return {'min': a[0], 'p50': a[n//2], 'p99': a[int(n*0.99)], 'max': a[-1], 'avg': round(sum(a)/n, 1)}
print('push_p99rtt:', stats(push))
print('pop_p99rtt:', stats(pop))
"
```

### Common log searches

```bash
# Server-side errors
grep -E 'error|ERROR' queen.log | grep -v 'libqueen' | sort | uniq -c | sort -rn

# StatsService cycle times (look for max approaching 30000 ms = timeout)
grep -oE 'cycle completed in [0-9]+ms' queen.log | grep -oE '[0-9]+' | sort -n | tail -3

# PartitionLookupReconcileService activity
grep -oE 'fixed [0-9]+ partition_lookup rows' queen.log | awk '{sum+=$2; count++} END {print "events:", count, "rows:", sum}'

# Deadlocks
grep -i 'deadlock' queen.log | head
```

## Tweaking the test environment

### Changing Postgres tuning

Edit the `docker run -d --name postgres ...` block in `run_test_v3.sh`. The flags follow `-c key=value`. For a different host size you'd typically scale:

- `shared_buffers` to 25 % of RAM
- `effective_cache_size` to 75 % of RAM
- `work_mem` to RAM / max_connections / 2 (ish)

### Changing Queen environment

Edit the `docker run -d --name queen ...` block. The relevant env vars are:

- `NUM_WORKERS` — Node cluster forks (default 10)
- `DB_POOL_SIZE` — PG connection pool (default 50)
- `SIDECAR_POOL_SIZE` — libqueen sidecar capacity (default 250)
- Add `-e QUEEN_VEGAS_MIN_LIMIT=...`, `-e QUEEN_VEGAS_MAX_LIMIT=...`, etc., if you want to override the libqueen Vegas adaptive concurrency parameters (see `lib/queen/concurrency/vegas_limit.hpp` for the full env vocabulary).

### Changing duration

Pass arg 4 to `run_test_v3.sh`:

```bash
# 5-min smoke test
./run_test_v3.sh my-quick-test 1000 10 300
```

### Pulling artifacts to a local machine

```bash
mkdir -p ./local-results/my-test/raw
scp root@<host>:/root/bench-runs/results/my-test/{producer.log,consumer.log,metadata.json,status.json,retention.json,queue-ops.json,system-metrics.json,postgres-stats.json,queue-resource-first.json,docker-stats-final.txt} ./local-results/my-test/raw/
```

For consumer-group runs, pull `consumer-cgN.log` for each group. Wildcards on remote scp need explicit expansion or one-by-one:

```bash
for i in $(seq 1 10); do
  scp root@<host>:/root/bench-runs/results/<test>/consumer-cg${i}.log ./local-results/<test>/raw/
done
```

## Writing a per-test summary

A summary should at minimum contain:

- **Setup** — full parameters echoed back (queue, partitions, prod/cons, batch, image)
- **Producer autocannon result** — totalRequests, msgPerSec, latency p50/p99/avg/max, errors
- **Consumer autocannon result** — same, plus server-reported pop batch efficiency
- **Server-side window aggregates** — steady-state ingest/pop msg/s, max event-loop lag, max msg dwell-time, queen CPU avg, RSS max
- **Postgres state at end** — cache hit ratio, `messages` size, HOT-update %
- **Queue final state** — total / completed / pending
- **Errors observed** — counts by type, with file-buffer failover note if applicable
- **Headline takeaways** — 3–6 bullets

The existing per-test markdown files in this folder are good templates.

## Troubleshooting


| Symptom                                                            | Likely cause                                                                                                                           | Fix                                                                                                                               |
| ------------------------------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------- |
| `docker run` fails with "name already in use"                      | previous test didn't clean up                                                                                                          | `docker stop/rm -v queen postgres; docker volume prune -f`                                                                        |
| Producer hangs at "ready" forever                                  | Queen container not actually listening                                                                                                 | `curl -sf http://localhost:6632/api/v1/status` to verify; check `docker logs queen`                                               |
| Consumer reports thousands of timeouts                             | client-side autocannon 10 s timeout, not server bug                                                                                    | check server-side `maxLagMs` in `status.json`. If server is fine (low lag), it's just long-poll exceeding client timeout — ignore |
| `StatsService` 30 s timeout fires                                  | known bug in 0.14.0.alpha.3 under heavy load                                                                                           | non-fatal; advisory lock prevents pile-up. One-line fix: `SET LOCAL statement_timeout = 0;` at start of the procedure             |
| Producer process at 99 % CPU but not pushing                       | benchmark client building a too-large `requests` array (e.g. `q-100`: 100 queues × 1001 partitions = 100 100 templates → 22 GB memory) | reduce `MAX_PARTITION` for high-`queue_count` tests, or rewrite producer to compute (queue, partition) at request time            |
| `docker stats` shows queen at 0 % CPU but test seems to be running | autocannon `DURATION` already elapsed; client done, server idle                                                                        | normal — wait for runner's metric-collection step                                                                                 |


## Files in `_runner/`


| File                    | Purpose                                                                                          |
| ----------------------- | ------------------------------------------------------------------------------------------------ |
| `bench-producer.js`     | autocannon producer with env-var config + cluster mode + multi-queue support                     |
| `bench-consumer.js`     | autocannon consumer with env-var config + cluster mode + multi-queue + `consumerGroup` parameter |
| `run_test_v3.sh`        | single-test runner (the canonical one — supports queue_count and cg_count)                       |
| `run_test_v2.sh`        | older runner (no queue_count or image_tag args) — kept for posterity                             |
| `run_test.sh`           | oldest runner — kept for posterity                                                               |
| `run_master.sh`         | 0.14.0.alpha.3 axis orchestrator (used for the partition + batch + queue tests)                  |
| `run_partition_axis.sh` | older partition-only orchestrator                                                                |
| `run_v12_master.sh`     | 0.12.19 axis orchestrator                                                                        |
| `run_cg_master.sh`      | consumer-group axis orchestrator                                                                 |


To reproduce **any** test in this session, the only script you need is `run_test_v3.sh` plus the producer/consumer pair. The orchestrators are just bash arrays calling `run_test_v3.sh` in sequence.
