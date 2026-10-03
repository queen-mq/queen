# Kafka 4.3.1 + Pulsar 4.2.4 on Queen's 3-node rig

Same droplets, same workload, same report as Queen's 09-29 grid, with each broker tuned the way its experts run
it. `SPEC.md` is the contract (workload, flags, output format, every tuned setting and why). This file is the
runbook.

## What is here

| path | what |
|---|---|
| `mqload/` | Go loaders `kload` (franz-go) and `pload` (pulsar-client-go) on one shared core: goload's open-loop pacer, histograms and JSON payload, so the three systems are measured the same way |
| `kafka/` | install, per-node config render, node control (`kc.sh`), cluster reset/health (`cluster.sh`), one point (`run.sh`), the matrix (`grid.sh`) |
| `pulsar/` | the same for ZooKeeper + BookKeeper + broker on each node (`pc.sh`, `cluster.sh reset/health/balance`) |
| `common/` | `deploy.sh` (ship + tune + install), `os-tune.sh` (same kernel settings for all three systems), `sampler.sh` (per-node 2 s sampler), `dexec.sh` (ssh shim for the local rehearsal) |
| `report/report.py` | one row per run, same columns for Kafka, Pulsar and Queen (goload logs parse too) |
| `localtest/` | the Ubuntu 24.04 + JDK 21 image that stands in for a droplet in the rehearsal |
| `runs/` | per point: `summary.txt`, `run.log`, `verify.log`, loader logs, `samples/`, stats and tuning overrides — everything `report/report.py` and `charts/` read |
| `charts/` | the published charts and the scripts that draw them from `runs/` |

Not in git (2.1 GB, on Alice's Mac in `~/Work/queen-bench-archive/2026-09-30-kafka-pulsar/`, same paths): broker logs
(`*.gz`, `logs.tgz`, GC logs), loader `*.json` histograms, txn `*.ids`, rendered per-node configs, generated launch
scripts, smoke and `.partial-*` runs, `deploy-logs/`, and the `mqload/bin/` binaries (`mqload/build.sh` rebuilds them).
`run.env` files are kept on disk but `.gitignore` drops `*.env`; `report.py` reads them for the shape and durability
columns, so add them with `git add -f` when committing.

## Durability class per system (print it next to every number)

| system | copies | ack after | fsync before ack |
|---|---|---|---|
| Queen (raft) | 3 | 2 (quorum) | yes |
| Pulsar (E3 Qw3 Qa2) | 3 | 2 bookies | yes (journalSyncData) |
| Kafka (RF3, min.insync 2, acks=all) | 3 | all in-sync replicas (3) | no, page cache (Kafka's production model); profile `fsync` for one parity point |

## VM day

1. Create 6 droplets like 09-29 (16 vCPU / 32 GB, Ubuntu 24.04, one VPC): 3 brokers, 3 loaders. Run `lscpu` on the
   3 brokers: all three must be the same CPU model (on 09-29 n3 was an older Xeon and capped Queen's followers).
2. `cp hosts.env.example hosts.env` and fill in the IPs.
3. `(cd mqload && ./build.sh)`
4. `common/deploy.sh harness tune kafka pulsar clock check` (≈5 min; installs both, runs neither)
5. Kafka: `kafka/grid.sh` (≈2 h), then `report/report.py runs/kafka/* --loaders`
6. Pulsar: `pulsar/grid.sh` (≈2.5 h), then `report/report.py runs/pulsar/* --loaders`
7. Queen after the fix, same droplets, same `os-tune.sh`: the 09-29 harness in
   `../2026-09-29-1m-3node/harness-sz-latest/` (`sz9.sh`, now self-locating: runs land in its own `runs/`;
   `cluster.env` there still holds the 09-29 IPs — rewrite it with `setup.sh` for the new droplets).
8. One table: `report/report.py runs/kafka/* runs/pulsar/* <queen run dirs>`

Only one system runs at a time: every start script refuses while another system's ports listen on the node.

## Matrix (SPEC §7)

A rate at 200 partitions · B partitions 200→100k · C topics 10/100/1000 · D 1M and 10M keys (Pulsar Key_Shared,
Kafka keys on 1k partitions) · E the real shape (optional) · F 15-min soak.
