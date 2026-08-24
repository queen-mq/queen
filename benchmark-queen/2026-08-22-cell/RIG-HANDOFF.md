# Bench rig handoff — pop autopilot iteration

## Machines

| role | public IP | VPC | spec |
|---|---|---|---|
| cell | `165.232.116.39` | 10.114.0.2 | 8 vCPU / 15.6 GiB / 484 GB |
| loader | `134.122.67.71` | 10.114.0.3 | 4 vCPU |

`ssh root@<ip>` — keys are already installed both ways.

The cell runs everything in Docker on network `cell`: `cell-pg`, `cell-broker-a`
(127.0.0.1:6632), `cell-broker-b` (127.0.0.1:6642), `cell-lb` (127.0.0.1:6630,
HAProxy active/passive), `cell-proxy` (10.114.0.2:6711, VPC only).

Postgres is UNPUBLISHED (no host port) on internal port 54987, user `queenadm`,
password in `/root/soak/pgpass`, databases `queen` and `queen_proxy`. Reach it
only via `docker exec cell-pg psql -U queenadm -p 54987 -d queen`.

## Current state — DO NOT WIPE IT

A soak has been running since 11:11 UTC: 35 full-quota tenants (17 free / 11 dev
/ 7 pro), 1 060 msg/s, **827 000 partitions and ~35M segments**. That state took
9 hours to accumulate and is the whole value of this rig — it is the regime
where static pop settings are wrong, which is the premise of autopilot.

**`soak-reset.sh` and `soak-cell.sh` WIPE POSTGRES. Do not run them.** To change
broker code or config, use `roll-broker.sh`, which preserves the database.

## Iterating on the broker

From the Mac (repo at `/Users/alice/Work/queen`):

```
cd benchmark-queen/2026-08-22-cell
./build-broker.sh 165.232.116.39          # rsyncs source, builds queen:soak ON the VM (~4 min)
```

Then on the cell, one broker at a time — passive first:

```
/root/roll-broker.sh cell-broker-b QUEEN_POP_AUTOPILOT=on
/root/roll-broker.sh cell-broker-a QUEEN_POP_AUTOPILOT=on
```

`roll-broker.sh` carries the existing container env forward verbatim (generated
PG password, mesh peer wiring, tenancy flags) and only swaps the image plus any
env you name. Rolling one at a time also exercises a mixed old/new fleet, which
is the compatibility path nothing else tests.

`QUEEN_POP_AUTOPILOT` = `on` | `shadow` | `off`. Shadow serves the pre-autopilot
parameters while the controller learns and logs — use it to park the cell in
baseline behaviour between experiments without losing the data.

Note: the image TAG stays `queen:soak` across rebuilds, so verify by image ID,
not tag: `docker inspect -f '{{.Image}}' cell-broker-a`.

## Iterating on the loader

Cross-compile from the Mac (the `replace` in go.mod points at the local
client-go, so SDK changes are picked up automatically):

```
cd benchmark-queen/2026-07-29-vm-campaign/goload
GOWORK=off GOOS=linux GOARCH=amd64 go build -o /tmp/goload-linux .
scp /tmp/goload-linux root@134.122.67.71:/root/goload
```

`GOWORK=off` is required — the module is not in `go.work` and the build fails
without it.

Restart the load on the loader (this does NOT touch Postgres):

```
pkill -9 -x goload
mv /root/soak/soak /root/soak/soak-$(date +%H%M)      # keep the old series
ulimit -n 200000
CONSUMERS=2 DRAIN=60 POP_PARTS=1 POP_BATCH=0 SKIP_CONFIGURE=1 \
  nohup setsid /root/soak-run.sh soak 2592000 17 11 7 > /root/soak.log 2>&1 </dev/null &
```

* `POP_PARTS=1` skips the `Partitions()` call -> broker sizes the width.
* `POP_BATCH=0` -> `Batch(0)`, which the SDK treats as never called -> broker
  sizes the batch.
* Any other value PINS that dimension. `SKIP_CONFIGURE=1` is essential: the
  queues and 827k partitions already exist; without it the run spends its life
  re-provisioning.
* Escape hatches: `QUEEN_SDK_POP_AUTOPILOT=off` in the loader env, or
  `Autopilot(false)` on the builder.

## Reading the result

On the cell:

```
/root/soakstat.sh          # one status line: rates, ready_age, pool, partitions, segments, disk
docker exec -i cell-pg psql -U queenadm -p 54987 -d queen -f - < /root/pgtop.sql
docker logs cell-broker-a --since 10m 2>&1 | grep -i autopilot
```

On the loader:

```
/root/lattrend.sh /root/soak/soak     # p50/p99 per interval, averaged across all 27 queues
/root/e2etop.sh   /root/soak/soak     # per-queue snapshot
python3 /root/verdict.py /root/soak/soak   # loss/duplication from the JSON artifacts
```

Collectors already running on the cell, one row/minute or /2min, independent of
any monitoring: `/root/samples/soak.csv` (CPU+RSS per container),
`/root/samples/soak-db.csv` (storage), `/root/samples/spstrend.csv` (per-statement
cost — differencing `calls`/`total_exec_time` gives windowed means).

## Baseline to beat

Same cell, same PG, same 35M segments, hand-tuned `W=10 B=100`:

| | |
|---|---|
| e2e p50 | **13.5 ms** |
| e2e p99 | **112 ms** (inter-checkpoint ~90-100, checkpoint-window ~160-220) |
| throughput | 1 060 msg/s |
| cell CPU | ~78% of 800 |

Autopilot as first deployed (controller converged to `W=1 B=200`):
p50 15 ms, **p99 380 ms**, throughput 1 060, CPU ~65%. A 3.4x worse tail for
~17% CPU. Archived raw data: `raw/2026-08-23-autopilot-before/`.

**Success = tail at or below 112 ms with the CPU saving retained.** Throughput
regression at equal latency is a failure, not a trade. Separate inter-checkpoint
p99 from checkpoint-window p99 — the checkpoint tail is not this feature's to fix
(checkpoints run every 30 min and flush ~72% of shared_buffers at this DB size).

## Traps that have cost time on this rig

1. **`pkill -f <pattern>` kills your own ssh session** when the pattern appears
   in the command line. Cost three dropped sessions. Use `pkill -9 -x goload`
   (exact name) or `p=oak-run; pkill -f "s${p}"`.
2. **`rsync` from the Mac resets the exec bit.** `soak-reset.sh` now chmods its
   dependencies; if you add a script, chmod it on arrival.
3. **`soak-run.sh` truncates `/root/samples/soak-db.csv` on launch**, so the
   storage collector shows only a header for the first minute.
4. **`soakstat.sh` reads the NEWEST `/root/samples/*-db.csv`.** It used to
   hardcode one label and silently reported a frozen 9 MB cell for a database
   that was at 343 MB.
5. **`lattrend.sh` output is `t_sec utc p50 p50_max p99 p99_max push/s`.** A
   monitor extracting a positional field breaks when the format changes — this
   already produced a "p99=19 ms" that was actually the median.
6. **`rates:` log lines come in two scopes.** `scope="global"` is the cell; the
   others are PER QUEUE. Reading a per-queue line as global makes a healthy cell
   look 100x slower.
7. **`pool=N/M` is connections OPENED, not busy.** `pool_waiting` is the real
   signal and has been 0 in every sample of every run. Raising the pool fixes
   nothing.
8. **Never generate a collector script through a nested heredoc over ssh** — the
   escaping silently produces header-only output. Write the file locally and
   rsync it.
