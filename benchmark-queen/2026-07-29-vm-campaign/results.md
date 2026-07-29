# Queen broker+proxy campaign — 8c/16G bench VM (2026-07-29/30)

Bench VM `206.81.20.237` (8 vCPU / 15 GiB / NVMe, Ubuntu 24.04). Disk calibration:
fdatasync avg **95.5 µs**, p99 ~396 µs (the July free-tier VM was ~130 µs, so this
box syncs faster — commit-bound numbers are not throttled by the disk).

Cell = PG18 + pxdb + broker + proxy sharing one systemd slice (`queencell.slice`),
CPU-capped together so the load generator (same box, outside the slice) can never
borrow the cell's cores. Free-tier shape = `--cell-cpus 2 --cell-mem 8`.

All load is driven by the real Go SDK through the proxy (per-tenant Host + api key),
with per-(tenant,seq) delivery accounting (missing / duplicate / cross-tenant) on
every run. A number without PASS delivery is not reported as a throughput result.

## TL;DR — what changed and what it bought

Three broker/proxy fixes landed this session (all committed on `rustproxy`):

| # | fix | commit | headline effect (verified on this VM) |
|---|-----|--------|----------------------------------------|
| 1 | hot-list **re-arm** lease-revisit cap | `ccfb844` | single-partition stall **gone**: p99 e2e 20 578 ms → ≤160 ms at the same config; no loss either way |
| 2 | **ownership-gate** positive cache | `1ad3d5e` | commits/delivered-msg **3.9 → 2.9** (~¼ of the txn budget) at the free-tier ceiling |
| 3 | proxy **tokio pool = cell CPU budget** + TCP_NODELAY | `301c9bc` | proxy CPU ~10% lower (uncapped), oversubscription removed on a capped cell |

Net product result: **the 2-core free-tier ceiling went from ~500 msg/s (July,
broker-only) to >2400 msg/s through the proxy, with zero loss and no stalls.**

## 1. Re-arm stall — root-caused and fixed

Repro (2800 msg/s, 4 partitions, manual ack, leaseTime 20 s, direct-to-broker):

| | before (`c147a7d`) | after (`ccfb844`) |
|---|---|---|
| e2e p99 | 19 529 ms | 276–506 ms |
| e2e max | 20 407 ms | 508–1002 ms (bounded by the 1 s revisit cap) |
| commits/delivered msg | ~1.9 | 1.90–1.96 (unchanged — NOT the +58% of the kill-switch) |
| delivery | PASS (0 loss) | PASS (0 loss) |

Mechanism (from the hot-list trace): a pop's SQL snapshot saw a lease still live and
its `Verdict::Leased(now+lease)` checkin parked the partition in the wheel at the
**full lease expiry** (up to 300 s by default). If the lease had already been
released by an ack whose `promote_ack` fired first, no future ack referenced the
partition and it sat dark until expiry. Capping the wheel park at
`MAX_LEASE_REVISIT_MS` (1 s) re-probes a stale lease within ~1 s. Disabling the ring
also removes the stall but costs +58% commits/msg; this keeps the ring's win.

## 2. Ownership gate — commits/msg dropped

At the free-tier ceiling shape (2c/8G, 8 partitions, manual ack), commits per
delivered message (PG `xact_commit` delta ÷ delivered):

| rate | campaign (un-cached gate) | after cache (`1ad3d5e`) |
|---|---|---|
| 700 msg/s | 3.896 | **2.887** |
| 800 msg/s | 3.980 | **2.927** |

The gate's per-ack `SELECT EXISTS(...)` (its own txn + pooled connection) is now a
memory lookup after the first ack of each partition (positives are immutable and
cached; forged/foreign pids still re-check the DB, so the map can't be grown by an
attacker).

## 3. Free-tier 2-core ceiling — proxy vs direct, all fixes on

40 s load + 15 s drain per point, 4 tenants sharing queue `orders` / group `workers`,
8 partitions, manual ack. Cell CPU = `queencell.slice` `usage_usec` delta (cell only).

| target | offered | achieved push/pop | cell CPU | commits/msg | e2e p99 | loss | verdict |
|---|--:|--:|--:|--:|--:|--:|:--|
| proxy | 500 | 500 / 500 | 1.22 c | 3.03 | 210 ms* | 0 | PASS |
| proxy | 700 | 700 / 700 | 1.71 c | 3.03 | 25 ms | 0 | PASS |
| proxy | 900 | 900 / 900 | 2.01 c | 2.75 | 69 ms | 0 | PASS |
| proxy | 1100 | 1100 / 1099 | 2.01 c | 2.15 | 86 ms | 0 | PASS |
| proxy | 1300 | 1300 / 1300 | 2.01 c | 1.73 | 159 ms | 0 | PASS |
| proxy | 1500 | 1500 / 1499 | 2.02 c | — | 216 ms | 0 | PASS |
| proxy | 2000 | 2000 / 1996 | 2.02 c | — | 461 ms | 0 | PASS |
| proxy | 2400 | 2400 / 2395 | 2.02 c | — | 741 ms | 0 | PASS |
| broker | 900 | 900 / 900 | 1.81 c | 2.98 | 43 ms | 0 | PASS |
| broker | 1100 | 1100 / 1100 | 2.02 c | 2.70 | 61 ms | 0 | PASS |

\* the 500-point p99 is a first-run warm-up outlier (ring/cursor cold seed).

Reading:
- **Ceiling > 2400 msg/s** at 2 cores (vs July 480–510 broker-only, and the campaign's
  ~700 proxied / ~900 direct with the un-fixed broker). The cell pins at 2.0 c from
  ~900 up but throughput keeps tracking offered because commits/msg falls under
  backlog (3.03 → 1.73). Latency is the pressure valve; **loss stays 0 and there is no
  stall** (p99 bounded, the 20 s stalls are gone).
- **Proxy CPU delta over direct**: +0.16 c at 500, +0.20 c at 900 — i.e. still a real
  per-request tax (~100–160 µs/req), but no longer the difference between fitting and
  not: the whole cell is cheaper per message now, so the proxy path absorbs 2400 msg/s
  where before it couldn't hold 900.

Failure mode past the knee (2400 msg/s, cell pinned): graceful — bounded latency
growth, 0 loss, 0 duplicate, no 429 (bench plan), no stall. Honest backpressure.

## 4. Proxy overhead — where it actually goes

perf on the proxy under 3000 msg/s: **no single hotspot.** `perf stat` = 0.915 core,
IPC 0.41 (memory/syscall-stall bound). Flat self-time: malloc/free ~3.5%, sha256 auth
0.73%, the rest is tokio scheduling + hyper server/client + the 4 socket syscalls per
request (recv client / send broker / recv broker / send client). Connection reuse is
healthy (**~0 new outbound conns/s** under load — pooling works; the earlier
`tcp_close`-heavy glimpse was an idle-window artifact).

So the ~100 µs/req is the genuine cost of terminating and re-originating HTTP, not
waste. The two structural wins: sizing the tokio pool to the cgroup quota (8→2 workers
on a 2-core cell removes 4× oversubscription; ~10% less proxy CPU uncapped, more when
capped) and TCP_NODELAY upstream (tail-latency). "Near-zero" is not achievable for an
HTTP-terminating proxy; these take the avoidable part off.

## 5. Correctness under sustained load

4-minute soak, 4 tenants **all sharing queue `orders` and group `workers`**, 900 msg/s,
manual ack, through the proxy (enforcing):

- **Delivery: PASS — 216 003 received, 0 missing, 0 duplicate, 0 cross-tenant.**
  Per-tenant: 54001 / 54001 / 54000 / 54001 (each tenant saw only its own).
- Latency steady across the whole 4 min: p50 6.2 ms, p99 84 ms, max 483 ms — no stall.
- **Broker RSS flat: 46 712 kB → 46 712 kB** (no growth under churning shared-name load).
- Hot-list at end: `4 rings / 0 ready / 0 wheel` — one ring per (tenant, queue), none
  stuck in the wheel (the re-arm fix keeps it drained), confirming the per-tenant ring
  keying.
- **Meter integrity**: `usage_minutes` push-msgs == delivery-msgs exactly (700 016 ==
  700 016 cumulative across the campaign) — every pushed message metered as delivered
  once; reads carry 0 msgs (correct).

## 6. OSS invariant

`test/run.sh --suite js --topo single,tenanted` → **TENANCY PARITY: OK**. The only
red is the pre-existing flaky **streams window** bucket (`tumblingBasicWindowSum`,
`tumblingIdleFlushClosesQuietPartitions` — a window over-emit, same class as the
2026-07-23 cross-language finding, tracked separately); core messaging
(push/pop/ack/dedup/isolation) is green on both lanes. The broker changes are on the
tenancy path (ownership cache is behind the flag) or timing bounds (re-arm cap), and
do not alter flag-OFF delivery semantics.

## What is still unmeasured / open

- **HA + tenancy at scale**: single-broker only here; the mesh residuals (frames now
  carry tenant, but multi-broker shared-cell throughput under the re-arm fix is
  untested at load).
- **Longer soak** (24–48 h): this run is 4 min; the decay-class lesson wants hours with
  limiter+meter+quota active before a real pilot.
- **Streams window bucket**: flaky, needs its own fix (spawned task).
- Proxy per-request cost is ~100 µs; further cuts (allocation trimming, splice for
  pass-through bodies) are diminishing returns and were deliberately not pursued.
