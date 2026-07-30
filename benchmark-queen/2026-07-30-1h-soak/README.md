# 1-hour multi-tenant soak — free-tier 2-core cell (2026-07-30)

Bench VM `206.81.20.237` (8 vCPU / 15 GiB / NVMe). Cell = PG18 + pxdb + broker +
proxy in one CPU-capped systemd slice (`--cell-cpus 2 --cell-mem 8`), enforcement
ON. Broker binary carries the re-arm cap + ownership cache; proxy carries the
tokio-pool sizing (commits `ccfb844`, `1ad3d5e`, `301c9bc`).

## Scenario

12 tenants, **all sharing queue `orders` and consumer group `workers`** (the
shared-cell shape that stresses the tenant-keyed hot-list ring), driven through
the proxy for **3600 s** at 840 msg/s offered (70/tenant), push-batch 1, 3
consumers/tenant, manual ack, leaseTime 30 s, completedRetention 300 s. All three
enforcement mechanisms active:

- **Limiter**: `soak-0000`, `soak-0001` overridden to the free plan (5 req/s, 20
  msg/s) → clamped, steady 429s all hour.
- **Quota**: `soak-0002` given `max_retained_bytes` 768 KiB → trips and oscillates
  (block → 300 s retention frees bytes → release → re-block, ~10 cycles).
- **Meter**: automatic; `usage_minutes` push vs delivery compared at the end.

## Result

Broker healthy for the full hour — **RSS flat at 46 MB** (drops to 34 MB when load
stops), **server-side p99 3–10 ms**, **cell CPU ~1.9 c** under the 2-core cap,
hot-list wheel bounded (630–720, re-probed ≤1 s), **0 cross-tenant** across all 12,
meter exact (push msgs == delivery msgs to 2.4 M).

The loader's aggregate verdict is FAIL, but decomposed it is clean:

| class | tenants | missing | dup | note |
|---|---|--:|--:|---|
| healthy | soak-0003…0011 (9) | **0** | 52 total (0.002%) | at-least-once redelivery, negligible |
| rate-limited | soak-0000/0001 | ~25 k | ~49 k | undrained backlog + ack-blocked redelivery — **enforcement artifacts** |
| quota | soak-0002 | 0 | 3 | all admitted messages delivered |

The 9 healthy tenants each delivered ~250 k messages with **0 loss**, untouched by
the 3 deliberately-abused tenants (noisy-neighbour containment). The miss/dup is
confined to the 2 tenants throttled to 1/14 of their offered rate — a rate-limited
tenant *must* leave undrained backlog and redeliver when its acks are 429'd. The
broker behaved correctly; the loader's checker does not model enforcement-induced
redelivery/backlog.

## Files

- `png/1_dashboard.png` — throughput, enforcement, latency, CPU, memory, hot-list.
- `png/2_meter_pg.png` — meter accumulation, PG commits/s, connections & parked pops.
- `png/3_per_tenant.png` — per-tenant delivered + refused, verdict decomposition.
- `loader-interval.csv` — goload 15 s interval time series (throughput, latency, errors).
- `vm-sampler.csv` — cell sampler (CPU, PG commits/s, connections, usage_minutes, RSS).
- `broker-rates-sizes.log` — broker `rates`/`sizes` blocks (server-side latency, pool, hot-list).
- `soak1h.json` — goload final run record incl. per-tenant delivery verdict.
- `plot_soak.py` — regenerates the PNGs: `python3 plot_soak.py .`
