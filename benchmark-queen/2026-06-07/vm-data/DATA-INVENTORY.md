# VM data inventory — new-arch (pushser) benchmark, 2026-06-07

All raw data pulled locally for building the "super report". Three boxes:


| Folder             | Box                               | Role                                                  |
| ------------------ | --------------------------------- | ----------------------------------------------------- |
| `soak-broker-165/` | 165.232.78.92 (32 vCPU / 62 GiB)  | 24h soak: Queen broker + Postgres (docker)            |
| `soak-loader-167/` | 167.99.246.68                     | 24h soak: `goload` load generator                     |
| `april-206/`       | 206.189.51.203 (32 vCPU / 62 GiB) | April matrix reruns on pushser (hold0 + default-hold) |


Pulled: 2026-06-07 ~13:06Z. Soak was **still running** at pull time (no data destroyed on VMs).

---

## 1. The 24h soak (headline run)

- **Broker:** 165.232.78.92, image `smartnessai/queen-mq:pushser`, started ~2026-06-06 13:04Z (broker elapsed 23:58:54 at capture).
- **Loader:** 167.99.246.68, `goload-new` → `http://165.232.78.92:6632`.

### Final numbers (as of ~2026-06-07 13:06Z, ~24h02m)


| Metric                | Value                                                                           |
| --------------------- | ------------------------------------------------------------------------------- |
| Avg throughput        | **~118.8k/s push AND pop** (10.279B / 86,520s)                                  |
| Instantaneous         | push ~108–121k/s, pop ~107–120k/s (balanced)                                    |
| Total pushed / popped | **10,278,596,240 / 10,278,496,280**                                             |
| Push–pop gap          | 99,960 (**0.001%**) — instantaneous in-flight, effectively zero loss            |
| Errors                | push **0**, consumer 172, empty pops 128 (over 10.28B)                          |
| Broker CPU / mem      | ~5 cores (450–560%) / **403 MiB RSS — flat all 24h**                            |
| Postgres CPU / mem    | ~20–23 cores (2000–2300%) / 33.8 GiB RSS                                        |
| `messages` table      | 15 GB / ~14M live — **stable** (retention keeps up)                             |
| `messages_consumed`   | **47 GB / 244.6M rows / 0 dead — slow leak (~2 GB/hr), reaper lagging**         |
| `retention_history`   | 710 MB / 4.47M rows                                                             |
| DB total / WAL        | 62 GB / 96 GB (WAL = configured `max_wal_size` ceiling)                         |
| Disk                  | 170 GB used / 218 GB free (44%) → ~4-day runway, bounded by `messages_consumed` |
| `pg_stat_database`    | xact_commit 93.9M, tup_inserted 10.50B, tup_deleted 10.24B, tup_fetched 71.86B  |
| Host                  | load avg ~40 (32 vCPU), 28 GiB mem available, 0 swap                            |


### Broker config (from `_report-capture/queen-inspect.json`)

```
NUM_WORKERS=10              DB_POOL_SIZE=50         SIDECAR_POOL_SIZE=250
QUEEN_CONCURRENCY_MODE=static   QUEEN_POP_MAX_CONCURRENT=16
QUEEN_PUSH_MAX_HOLD_MS=20   QUEEN_PUSH_PREFERRED_BATCH_SIZE=50
QUEEN_PUSH_MAX_BATCH_SIZE=500   QUEEN_PUSH_MAX_CONCURRENT=24
RETENTION_PARALLELISM=8     RETENTION_INTERVAL=5000 (ms)   RETENTION_BATCH_SIZE=50000
```

(Full PG tuning is in `start-broker.sh`; full env in `*-inspect.json`.)

### Loader config (`goload-new` argv)

```
-queue benchq -partitions 300 -producers 650 -consumers 200
-push-batch 10 -pop-batch 300 -pop-partitions 10 -pop-wait -pop-timeout 2000
-payload 256 -completed-retention 120 -pending-retention 600 -idle-conns 1600 -report 10 -retries 2
```

---

## 2. File-by-file

### `soak-broker-165/`

- `**long-mon.log**` — primary broker timeseries, **30s cadence**, 5411 lines. Format:
`TS | queen=CPU%/Mem postgres=CPU%/Mem | xc_ins_del_size_dead_live= <xact_commit> <n_tup_ins> <n_tup_del> <size> <dead> <live> | disk=<free>`
⚠️ **Spans two runs** (monitor_start 2026-06-05 13:57Z). For the 24h soak, **slice from ~2026-06-06 13:04Z** (broker restart) to end.
- `long-mon2.log` — older monitor (earlier run, context only).
- `long-mon.sh` — monitor script = authoritative column definitions.
- `start-broker.sh` — PG + broker launch (full Postgres tuning + broker env).
- `restart-queen.sh` — fast broker-only restart.
- `_report-capture/` (snapshot at ~13:02Z):
  - `queen-broker.log.gz` — full 24h broker stdout (`gunzip -c`). Fusion ratio / req-rate / batch logs live here.
  - `postgres-docker.log.gz` — full 24h PG stdout (checkpoints, autovacuum, any errors).
  - `queen-inspect.json` / `postgres-inspect.json` — exact container config/env/mounts.
  - `db-table-sizes.txt` — final per-table size + live/dead tuples.
  - `db-stats.txt` — `pg_stat_database` (xact_commit, tup_inserted/deleted/fetched).
  - `run-meta.txt` — capture time, broker elapsed, host uptime/mem/df.
- `results/` — **earlier (Jun 4) on-box matrix**, NOT the 24h soak. Includes version compares (`*-0.15.5`, `*-simd`) and `bp-100-sustained`. Context for the version-progression story.
- `wm.tsv`, `april016.log`, `sustained.log`, `subset.log`, `cal/`, `*.py`, `*.sh` — earlier-session calibration/analysis artifacts.

### `soak-loader-167/`

- `**goload.log`** — THE 24h loader timeseries, **10s cadence**, 8655 lines. Format:
`[HH:MM:SS] push=N/s pop=N/s | tot push=N pop=N | errs p=N c=N empty=N` (HH:MM:SS wraps daily; run starts 13:04:19).
- `goload-N{10..250}.log`, `gl_*.log`, `ceil_goload_*.log`, `goload-retries{0,2}*.log`, `goload-prev-*.log` — calibration/sweep runs (producer-conn sweep, Vegas-vs-static, pop-ceiling probes, retry A/B). Context for the tuning narrative.

### `april-206/`

- `results/` — **new-arch hold=0 matrix** (12 cells): bp-{1,10,100}, q-{1,10,100}, hi-part-{10,100,1000,10000}, bp-10-cg{5,10}.
- `results-defaulthold/` — same cells with **default push hold** (the hold=0 vs default A/B), plus `bp-10-h0`/`bp-10-h2` probes.
- Per cell: `producer.log`, `consumer*.log`, `queen.log`, `system-metrics.json`, `postgres-stats.json`, `docker-stats.log` (per-min pg+queen CPU/mem), `docker-stats-final.txt`, `status.json`, `queue-ops.json`, `metadata.json`, `start_time.txt`/`end_time.txt`.
- `master.log`, `cg-master.log` (+`.defaulthold`) — harness driver logs (per-cell msg/s summary lines).
- `run_test_v3.sh` (patched: pushser + static pop + hold), `run_master.sh`, `run_cg_master.sh`, `patch-runner.sh`, `patch-hold.sh` — exact methodology.
- `run_{rabbitmq,kafka,test,test_v2,partition_axis,v12_master}.sh` — April originals (reference; unused this round).

---

## 3. Caveats for the report

1. `**long-mon.log` covers >24h across two runs** — slice to the soak window (broker restart ~2026-06-06 13:04Z) before plotting.
2. `**messages_consumed` leak** is real and the only non-flat metric: 47 GB / 244.6M rows, 0 dead, growing ~2 GB/hr. Completed-retention reaper runs but trails completion by ~2–3k/s. Bounds long runs to ~4 days on this disk. Flag as the one open issue (missing/under-provisioned reaper in the pushser rework).
3. WAL at 96 GB is the configured ceiling, **not** a leak.
4. April matrix `q-100` cell exists in raw data but was excluded from the headline comparison (document if included).

