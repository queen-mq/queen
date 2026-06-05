# Queen 0.16 — 2026-06-05 Go-loader soak & ceiling findings

Data collected during the 2026-06-05 session on the 32-core VM (broker + Postgres
co-located; clients on a separate 16-core loader VM). **Build under test: `0.16.0.beta.1-ui`
(pre observability-fix).** Companion docs: `METRICS-OBSERVABILITY-FIX-PLAN.md` (the fix),
`THROUGHPUT-CAMPAIGN.md` (prior campaign).

Chart: `soak-2026-06-05-oldversion.png` (push/delete/pop + broker/PG CPU over the run).

---

## 1. Setup of the running soak

- **Broker** (`start-broker.sh`, then `restart-queen.sh`): `NUM_WORKERS=10`, `DB_POOL_SIZE=50`,
`SIDECAR_POOL_SIZE=250`, `QUEEN_PUSH_MAX_HOLD_MS=40`, `QUEEN_CONCURRENCY_MODE=vegas`,
`RETENTION_PARALLELISM=8`. Postgres: `shared_buffers=24GB`, `max_wal_size=96GB`,
`synchronous_commit=on`, aggressive autovacuum (see `start-broker.sh`).
- **Load** (`goload`, the new Go load generator): `benchq`, **300 partitions**,
**1000 producers**, **300 consumers (pinned 1:1)**, push-batch 10, pop-batch 200, 256 B
payload. Pop uses **server-side autoAck** (`?autoAck=true`).
- **Queue config:** `retentionEnabled=true`, `completedRetentionSeconds=300`,
`retentionSeconds=600` (pending safety net).
- Fresh broker started **05:07 UTC**; data below is the clean run **05:07 → 08:44 UTC**
(07:07 → 10:44 CEST, ~3 h 25 m).

## 2. Data collected (from `long-mon.log`, 30 s samples)


| Time (CEST) | push+pop | broker mem | PG mem (incl. cache) | msgs table | disk free |
| ----------- | -------- | ---------- | -------------------- | ---------- | --------- |
| 08:20       | ~110k    | 537 MB     | 41.0 GB              | 28 GB      | 253 G     |
| 09:24       | ~110k    | 562 MB     | 48.4 GB              | 29 GB      | 248 G     |
| 10:02       | ~110k    | 585 MB     | 51.3 GB              | 29 GB      | 245 G     |
| 10:32       | ~110k    | 598 MB     | 52.7 GB              | 29 GB      | 242 G     |


Rate summary over the windowed run (direct PG counters; pop from `worker_metrics`):


| Metric                           | avg            | peak       |
| -------------------------------- | -------------- | ---------- |
| push (`n_tup_ins`)               | **112,982 /s** | 117,669 /s |
| delete / retention (`n_tup_del`) | **112,146 /s** | 119,628 /s |
| pop (`worker_metrics`)           | **109,219 /s** | 116,005 /s |
| Queen broker CPU                 | **6.2 vCPU**   | —          |
| Postgres CPU                     | **17.1 vCPU**  | 20.7 vCPU  |


Cumulative at 08:32 UTC: `n_tup_ins ≈ 1.38 B`, `n_tup_del ≈ 1.35 B` (retention kept pace),
`live ≈ 35 M`, `dead ≈ 2–5 M`.

## 3. Verdict: stable, no degradation after ~3.4 h

- **Throughput flat ~110k push+pop**, 0 client errors, **1.38 B messages** processed.
push ≈ delete ≈ pop the entire time → retention held the table bounded.
- **Broker memory flat ~600 MB → no leak.** (Contrast: the 2026-06-04 crash run grew the
broker 4 → 13.6 GB.) The difference: **nothing was polling the analytics/dashboard
endpoints**, so the single-concurrency `CUSTOM` lane never piled up.
- **Table bounded ~29 GB**; **disk plateauing** (~242 G free; the slow decline is WAL ramping
to its 96 GB `max_wal_size` ceiling, then recycling — not table growth).
- **CPU stable** (broker 6 vCPU, PG 17/32) — no creep over the run.
- PG's reported memory climbed 41 → 53 GB, but that is reclaimable **page cache** (cgroup
counts `shared_buffers` + cache), not committed memory → not an OOM risk on its own.

**Conclusion:** the Queen data path is rock-solid for hours at ~110k balanced push+pop with
retention on. The 2026-06-04 OOM crash was **not** a data-path problem — it was
observability amplification (see §4 and the fix plan).

## 4. Key findings from the session

1. **The throughput ceiling is Postgres, not the broker.** Across every broker knob tried
  (producers 2000→4000, workers 10→16, sidecar 250→500, partitions 100→500, static vs Vegas
   concurrency, `BATCH_INFLIGHT_THRESHOLD`), balanced throughput capped at **~110k push /
   ~220k combined** while **Postgres never CPU-saturated (12–19 of 32 cores)**. The limit is
   PG push-path serialization (per-call queue/partition upserts in `push_messages_v3`,
   3-index maintenance on `queen.messages`, `partition_lookup` hot rows / WAL), not anything
   tunable in libqueen.
2. `**QUEEN_PUSH_MAX_HOLD_MS=40` (vs 20) roughly halved PG CPU/msg** via better fusion
  (~112–144 inserts per commit, ~800–1000 commits/s for ~110k push). Forcing more
   concurrency (lower hold / inflight-threshold) *lost* — more PG CPU for the same throughput.
   Fusion (Vegas + hold=40) is the efficient operating point.
3. `**worker_metrics` lags badly *only* under observability contention.** During the crash run
  it lagged 13–28 min (causing a fake "end dip" in charts); during this clean run it stayed
   current (last bucket = now). Root cause is the same shared-lane contention.
4. **Go client = excellent load generator** once given a tuned HTTP transport
  (`MaxIdleConnsPerHost`); ~110k from a single process, 0 errors, beats autocannon and
   exercises the real client path. **Consumer pinning** (vs round-robin per pop) is essential —
   round-robin churns partition leases and throttles pop; pinned drained backlog at 194k/s.
5. **Retention must be explicitly enabled.** `POST /configure` is a full upsert; setting
  `completedRetentionSeconds` without `retentionEnabled=true` silently leaves retention OFF
   → unbounded table growth (hit 171 GB before we caught it via rising `blks_read`). Fixed in
   the `goload` configure call.

## 5. Reproduction

```
# broker (32-core VM)
QUEEN_PUSH_MAX_HOLD_MS=40 bash start-broker.sh
nohup bash long-mon.sh > long-mon.log 2>&1 &
# loader (separate VM), retention-fixed binary
./goload-linux-amd64 -url http://BROKER:6632 -queue benchq -partitions 300 \
  -producers 1000 -consumers 300 -push-batch 10 -pop-batch 200 \
  -completed-retention 300 -pending-retention 600
# chart: pull long-mon.log + export worker_metrics -> wm.tsv, then  python3 plot-soak.py
```

