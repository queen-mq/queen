# 2026-07-17 — Hot-path spike + storage-v2 (segments) soak — VM artifacts

VM (142.93.168.236, 32 vCPU / 62 GiB) was deleted after this. Everything needed to
continue is here. All runs: co-located PG (April tuning, `max_wal_size=96GB`) +
broker + `goload` on one box; `goload` bp-100 workload (100 msg/push, pop-batch 500,
100 partitions), fresh PG volume per run.

## Folders

- `outputs/`   — per-run stdout (trajectory tables: push/s, pop/s, CPU, table size).
- `scripts/`   — exact bench scripts used on the VM (also mirrored in `../../server-rust/`).
- `goload-logs/` — goload client-side final push/pop totals (`/tmp/*.log`).
- `build-logs/` — C++ segments broker build logs (`v2build*.out`).
- `Dockerfile.broker-prod.patched` — see "Rebuilding" below.

## Key results (segments = storage-v2, C++ broker built from branch `storage-v2-slice`)

Throughput = push/s + pop/s combined; hot table = `queen.messages` (rows) or `q2.segments` (segments).


| config                          | throughput            | hot table  | pgCPU  | notes                                                         |
| ------------------------------- | --------------------- | ---------- | ------ | ------------------------------------------------------------- |
| rows (C++ 0.16, bp-100)         | ~200k (100k+100k)     | 5.9 GB     | ~1500% | baseline                                                      |
| segments, DEFAULT tuning        | ~83k (declining)      | ~90 MB     | ~100%  | bottlenecked (see below)                                      |
| **segments, tuned, 1x load**    | **~312k (156k+156k)** | ~340 MB    | ~450%  | stable                                                        |
| **segments, tuned, 2x load**    | **~291k (146k+146k)** | ~315 MB    | ~500%  | stable — holds 2x (not loader-bound; broker/PG ceiling ~300k) |
| segments, tuned, dedup=120s, 2x | 118k→55k              | dedup→2 GB | —      | UNSTABLE (dedup issue, investigate)                           |


Files: `outputs/soakv2b.out` (tuned 1x), `outputs/soakv2d.out` (tuned 2x, dedup off),
`outputs/soakv2c.out` (dedup=120s, unstable), `outputs/soaklong7.out` (rows engine w/ parallel retention plateau).

## Root cause found today (segments 45k → 312k)

The ~45k ceiling was NOT zstd threading (compression is parallel per uWS worker).
Real cause: all segment SQL (push AND pop) goes through `JobType::CUSTOM`, which has
`max_concurrent = 1` in `lib/queen/batch_policy.hpp:131`, and the rest_engine had only
~5 DB slots. Fixes (env only, no recompile):

- `QUEEN_CUSTOM_MAX_CONCURRENT=64` (was hard-capped at 1)
- `QUEEN_REST_SLOTS=88`, `DB_POOL_SIZE=100`, `NUM_WORKERS=16`
- `QUEEN_CONCURRENCY_MODE=static`
- queue `dedupWindowSeconds=0` (default 3600s inflated q2.dedup to 2.8 GB + dragged pushes)

## OPEN ITEM for tomorrow — dedup window scalability

With `dedupWindowSeconds=120` at 2x load, `q2.dedup` grew to ~2 GB in 2 min, then the
retention dedup-purge (`DELETE ... q2.dedup` in 50k batches) + autovacuum churn
DESTABILIZED the push path (throughput crashed 118k→55k, empty pops spiked). Investigate:

- dedup purge batching / index bloat on `q2.dedup (partition_id, txn_hash)`;
- whether purge should run on a dedicated connection / off the retention advisory lock;
- autovacuum settings for `q2.dedup`.
See `outputs/soakv2c.out`.

## Suggestions to storage-v2 team

1. `JobType::CUSTOM max_concurrent=1` is a bad default for the segments engine (whole
  hot-path rides CUSTOM). Give segments a dedicated lane or raise the default.
2. Branch didn't build on Linux — `libzstd-dev` missing from the broker Dockerfiles
  (built on macOS/brew only). See patched Dockerfile.

## Rebuilding the segments broker (fresh box tomorrow)

```bash
cd <repo>
git worktree add /tmp/queen-v2 storage-v2-slice
# apply the one-line fix (add libzstd-dev to apt):
cp benchmark-queen/2026-07-17-storagev2-soak/Dockerfile.broker-prod.patched \
   /tmp/queen-v2/benchmark-queen/simd-vs-0155/Dockerfile.broker-prod
cd /tmp/queen-v2
DOCKER_BUILDKIT=1 docker build -f benchmark-queen/simd-vs-0155/Dockerfile.broker-prod \
   -t queen-mq:segments .
# then run scripts/soakv2.sh  (DEDUP=0 by default; DEDUP=120 to repro the instability)
```

`goload` source: `benchmark-queen/2026-06-04/goload/` (build: `GOWORK=off go build`).