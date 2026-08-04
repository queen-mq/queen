# Scale campaign — 32-core rig, regime detector derivation

Broker `bench-01` 104.248.245.59, loader `bench-02` 159.89.104.168, both **32 vCPU /
62 GB**, Xeon Platinum 8358 @ 2.60GHz, KVM, kernel 6.8.0-136, Docker 29.7.1.
Loader reaches the broker on the eth1 VPC at **10.114.0.2** (the 10.19.0.0/16
addresses that appeared after the resize are eth0 and are NOT routable between
the two hosts for this purpose).

Provenance, resource-parity env and the resized `postgres.conf` are in `phase0/`.
Every run directory carries its own `invocation.txt`.

## Phase 0 — what had to be fixed before measuring

**Resource parity (would have invalidated the whole campaign).** The compose
files for pgmq, Kafka and RabbitMQ carry `deploy.resources.limits` with
`cpus: ${CM_CPUS:-8}` and `memory: ${CM_MEM:-16g}`. The Queen compose carries **no
limits at all**. On the old 8-core box this was a no-op; on 32 cores it would
have given Queen four times the machine of every competitor. Normalised to
`CM_CPUS=32 CM_MEM=56g` for all systems, with `KAFKA_HEAP_OPTS=-Xmx16g -Xms16g`
scaled in proportion (`phase0/resource-parity-env.sh`).

**Postgres was sized for 16 GB / 8 cores.** Scaled to the 62 GB box:
`shared_buffers` 4→16 GB, `effective_cache_size` 10→45 GB,
`maintenance_work_mem` 512 MB→2 GB, `wal_buffers` 64→128 MB,
`min/max_wal_size` 2/8→4/32 GB, `max_connections` 400→600,
`autovacuum_max_workers` 4→8. **`synchronous_commit=on` and `fsync=on` are
unchanged** — the durability tier is the anchor of the comparison and must not
move. Old file kept on the VM as `postgres.conf.8core.bak`.

**Loader-saturation gate.** Checked live during the largest cells: loader load
average 4.31 of 32 cores (~13%), CPU 21% busy. The loader is not the constraint
in any cell reported here.

## Calibration

Anchor shape 2k ev/s / 1000 lanes, with the config derived on the 8-core box
(`POP_FUSION=0`, `ADMISSION_MIN=INIT=96`, `V2_FUSION_HOLD_MS=3`, pp40, 4
workers/stage): **p50 65.5 ms, p95 92.7** against 170.0 on 8 cores.

## Derivation — the 2x2, and the factorial that names the actuator

Density `d` = messages per second per partition. All cells 60 s, ramp 5, drain
30, pp40, `ADMISSION_MIN=INIT=96`.

Set **A** = `POP_FUSION=0`, `HOLD_MS=3`, 4 workers/stage.
Set **B** = `POP_FUSION=1` (C4/MJ8/S4), `HOLD_MS=15`, 8 workers/stage.

| cell | rate | d | A p50 | B p50 |
|---|---:|---:|---:|---:|
| N=100 sparse | 200 | 2 | **60.1** | 60.1 |
| N=1000 sparse | 2 000 | 2 | **65.5** | 65.5 |
| N=100 dense | 2 500 | 25 | 85.0 | **71.5** |
| N=1000 dense | 12 000 | 12 | 679.9 | **340.0** |

The state variable is **density, not offered rate**: N=100 dense (2 500 ev/s)
and N=1000 sparse (2 000 ev/s) sit at nearly the same absolute rate and give
opposite verdicts.

Factorial at N=1000 dense (12 000 ev/s), one variable at a time:

| fusion | hold | workers | p50 | p95 |
|---|---:|---:|---:|---:|
| OFF | 3 | 4 | 679.9 | 1763.5 |
| OFF | 3 | 8 | 679.9 | 1482.9 |
| OFF | 15 | 8 | **370.7** | 2965.8 |
| ON | 15 | 8 | **340.0** | 623.5 |

**The push hold is the actuator** (45% of the effect); pop fusion adds 8% on p50
but is worth much more on the tail (p95 2965.8 → 623.5); **worker count does
nothing** (679.9 either way). Confirmed on the sparse side: hold 3 vs 15 gives
65.5 vs 71.5 at N=1000 and 60.1 vs 60.1 at N=100 — so the sign genuinely flips
with density rather than one setting simply dominating.

## The law

Detector: **messages served per visit**, which the broker already logs as
`cands_visit` in the `broker rates` line — density is observable in-process and
needs no user configuration.

- `d` low (≲4) → hold 3, pop fusion off — *low-latency set*
- `d` high (≳10) → hold 15, pop fusion on — *throughput set*

Workers drop out of the law entirely, which matters for usability: they are the
only client-side parameter of the three.

## Feasibility bound (structural)

25 000 ev/s at N=1000 (150 000 deliveries/s) is **above this box's ceiling**
under both sets — set A served 71.5% of the offered rate, set B ~80%, both with
multi-second latency. Maximum sustained is ≈120 000 deliveries/s ≈ 20 000 ev/s.

Therefore `N x d <= ceiling`: **the high-cardinality/high-density corner of the
grid does not exist on a single machine**, and those cells must be measured as
ceiling sweeps rather than at fixed rate.

## Caveats that bound these numbers

- All cells are 60 s. Within a run the instantaneous p50 **drifts upward**
  (measured on the 8-core box: 143 → 170 over 180 s) because the tables grow and
  `completedRetentionSeconds` is longer than the run, so nothing is ever pruned.
  Relative comparisons between sets hold; absolute values do not. The comparison
  matrix must be run at 180 s.
- Broker CPU at the largest cells was ~13.5 of 32 cores (Queen 4.2 + PG 9.3).
  The invariants `ADMISSION_MIN=INIT=96`, `DB_POOL_SIZE=160` and
  `ADMISSION_MAX=128` were derived on the 8-core box and are very likely leaving
  half of this machine idle. Re-deriving the ceiling is the next step and will
  move the absolute numbers.
- Correctness: **0 gaps / 0 order violations / 0 duplicates in every run**,
  including the overloaded 25 000 ev/s cells.
