# 1 Hz metric samplers (QueenMQ final benchmark)

Two self-contained bash samplers that append one CSV line per second. They are
deployed on the two benchmark hosts:

| script              | host                        | writes about                    |
|---------------------|-----------------------------|---------------------------------|
| `bench-sampler.sh`  | bench (broker `r6682` + PG `qbench-pg`) | broker + Postgres CPU/mem + PG WAL/txn stats |
| `loader-sampler.sh` | loader                      | `goload` CPU/mem + host idle + eth1 net + TCP |

Both take `<outfile> <duration_s>`, sample at true 1 s cadence (each line is
scheduled off a fixed `start + i*1000 ms` grid so the interval never drifts),
write a header line when the output file is new, and print a one-line self
overhead report to **stderr** on exit. Every field is emitted empty (never a
crash) if its source is unreadable.

```
./bench-sampler.sh  /root/bench.csv   600
./loader-sampler.sh /root/loader.csv  600
```

`CPU%` in both files is **percent of one core** (docker-stats / `top`
semantics: `100%` = one full core). The bench box has 32 cores (ceiling
`3200%`); the loader has 48 (ceiling `4800%`).

---

## bench-sampler.sh columns

`epoch_ms,pg_cpu_pct,pg_mem_mb,queen_cpu_pct,queen_mem_mb,xact_commit_cum,wal_records_cum,wal_bytes_cum,wal_fsyncs_cum,wal_fsync_time_ms_cum,db_size_bytes,active_backends,top_wait`

| # | column                  | unit / type        | inst/cum | source |
|---|-------------------------|--------------------|----------|--------|
| 1 | `epoch_ms`              | ms since epoch     | instant  | `date +%s%3N` |
| 2 | `pg_cpu_pct`            | % of one core      | **rate** (1 s delta) | PG cgroup `cpu.stat` `usage_usec` delta |
| 3 | `pg_mem_mb`             | MiB                | instant  | PG cgroup `memory.current` |
| 4 | `queen_cpu_pct`         | % of one core      | **rate** (1 s delta) | broker cgroup `cpu.stat` `usage_usec` delta |
| 5 | `queen_mem_mb`          | MiB                | instant  | broker cgroup `memory.current` |
| 6 | `xact_commit_cum`       | count              | **cumulative** | `pg_stat_database` (`datname='postgres'`) |
| 7 | `wal_records_cum`       | count              | **cumulative** | `pg_stat_wal.wal_records` |
| 8 | `wal_bytes_cum`         | bytes              | **cumulative** | `pg_stat_wal.wal_bytes` |
| 9 | `wal_fsyncs_cum`        | count              | **cumulative** | `sum(pg_stat_io.fsyncs)` where `object='wal'` |
| 10| `wal_fsync_time_ms_cum` | milliseconds       | **cumulative** | `sum(pg_stat_io.fsync_time)` where `object='wal'` |
| 11| `db_size_bytes`         | bytes              | instant  | `pg_database_size('postgres')` |
| 12| `active_backends`       | count              | instant  | `pg_stat_activity` `state='active'` (excludes the sampler's own backend) |
| 13| `top_wait`              | text (may be empty)| instant  | most-common non-null `wait_event` among active backends |

Columns 6–10 are **monotonic cumulative counters** — compute per-second rates by
differencing consecutive rows (e.g. commits/s = Δcol6, WAL bytes/s = Δcol8,
mean fsync latency = Δcol10 / Δcol9). Reset only on a PG stats reset / restart.

The 8 PG fields (cols 6–13) come from **one** `SELECT` (scalar sub-selects, one
round trip) over a **persistent** `psql` session held open with
`docker exec -i` as a coprocess. The CPU/mem cgroup paths
(`/sys/fs/cgroup/system.slice/docker-<id>.scope`) are resolved once at start.

### PG 18 column-name adaptations
- **`pg_stat_wal` lost the write/sync counters in PG 18.** It now exposes only
  `wal_records, wal_fpi, wal_bytes, wal_buffers_full, stats_reset` — the old
  `wal_write / wal_sync / wal_write_time / wal_sync_time` are gone.
- **WAL fsync stats moved to `pg_stat_io`.** `wal_fsyncs_cum` /
  `wal_fsync_time_ms_cum` are `sum(fsyncs)` / `sum(fsync_time)` over the rows
  with `object='wal'`. `fsync_time` is already in **ms** (double precision).
  On this box `track_wal_io_timing=on`, so `fsync_time` is populated
  (`wal_sync_method=fdatasync`, so `fsyncs` counts fdatasync calls).

---

## loader-sampler.sh columns

`epoch_ms,goload_cpu_pct,goload_mem_mb,total_cpu_idle_pct,net_rx_mbps,net_tx_mbps,tcp_established,tcp_timewait`

| # | column               | unit / type   | inst/cum | source |
|---|----------------------|---------------|----------|--------|
| 1 | `epoch_ms`           | ms since epoch| instant  | `date +%s%3N` |
| 2 | `goload_cpu_pct`     | % of one core | **rate** (1 s delta) | Σ over `goload*` pids of Δ(utime+stime) from `/proc/<pid>/stat` |
| 3 | `goload_mem_mb`      | MiB           | instant  | Σ RSS (pages×page_size) over `goload*` pids |
| 4 | `total_cpu_idle_pct` | % (all cores) | **rate** (1 s delta) | `/proc/stat` idle-jiffy delta ÷ total-jiffy delta |
| 5 | `net_rx_mbps`        | **megabits/s**| **rate** (1 s delta) | eth1 rx_bytes delta ×8 |
| 6 | `net_tx_mbps`        | **megabits/s**| **rate** (1 s delta) | eth1 tx_bytes delta ×8 |
| 7 | `tcp_established`    | count         | instant  | `ss -s` `estab` |
| 8 | `tcp_timewait`      | count         | instant  | `ss -s` `timewait` |

Notes:
- `goload_cpu_pct` / `goload_mem_mb` are **empty** when no `goload*` process is
  running (e.g. before/after a run) — they are not `0`.
- CPU is computed from `/proc/<pid>/stat` **utime+stime deltas**, never `ps
  %cpu` (which is a lifetime average). New pids contribute `0%` on their first
  observed second (no prior sample to difference), full rate thereafter.
- `net_*` are **megabits per second** (bytes×8/1e6), not MB/s.
- goload pids are re-discovered by a full `/proc` scan every 3 s; in between only
  the cached pids are `stat`'d (keeps overhead low). A goload that starts mid-run
  is therefore detected within ~3 s of launch.

---

## Overhead (measured)

Each script self-reports on stderr; cross-checked with `/usr/bin/time -v`.
Both stay under 2% of one core, at idle and across a 10 s load window.

| script              | idle (15 s) | full 35 s run incl. 10 s load |
|---------------------|-------------|-------------------------------|
| `bench-sampler.sh`  | 1.67% /core | 1.63% /core                   |
| `loader-sampler.sh` | 1.20% /core | 1.34% /core                   |

The persistent-psql design also minimises observer effect: it does **not**
re-spawn a psql inside the PG container every second (which would otherwise add
its own startup CPU to `pg_cpu_pct`).
