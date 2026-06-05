# Throughput campaign — separated loader VM (2026-06-04)

Goal: max push throughput, then balanced push+pop ≥100k (ideally ~150k),
sustainable with retention. Clients moved to a **dedicated loader VM**
(167.99.246.68, 16 vCPU) over the private VPC, so broker+PG (32 vCPU) get the
whole box. Broker image `0.16.0.beta.1-ui`, `max_wal_size=96GB`,
`RETENTION_PARALLELISM=8`, PG `max_connections=800`.

## Phase 1 — push-only ceiling

| config | conns | push/s | Queen vCPU | PG vCPU | notes |
|---|--:|--:|--:|--:|---|
| W4 (4 workers) | 3,500 | ~147k | 3.6/4 (90%) | 7.9 | **worker-thread-bound** |
| W8, pool96, default policy | 3,600 | **~152–179k** | 4.9/8 (61%) | 10.6 | not thread-bound; fusion 245 msgs/commit |

- **8 workers + high fan-in** is the push win: ~152–179k at ~5 broker vCPU, PG only ~11/32.
- **Coalescing ("fusion") works**: batch=10 client pushes get fused to ~245 msgs per PG commit.
- **Aggressive fusion tuning** (`MAX_CONCURRENT=6, MAX_HOLD_MS=50, PREFERRED=200, MAX_BATCH=2000`)
  *tripled* fusion (→730 msgs/commit) and halved CPU, but **throttled throughput**
  (concurrency cap starved the pipe). Verdict: the hold/batch knobs are an
  **efficiency** lever (lower PG load per msg); `MAX_CONCURRENT` is the throughput
  lever. Default policy + more workers wins for peak throughput.

## Phase 2 — balanced push+pop

- **Balanced ceiling ≈ 110–120k push (pop matching), PG-contention-bound** — *not*
  CPU (broker 17–30%, PG 48–53%, loader idle). Pushing harder either contends
  (98k) or starves pop. Pop is never the bottleneck (it out-paces push; long-poll
  empties the queue → client timeouts).
- **Partition count is decisive:** 1001 partitions → ~74k balanced *with active
  retention* (contention on partition_lookup/consumers + pop wildcard scan);
  **100 partitions → ~95–104k** (much less contention). Matches the earlier
  validation soak that hit 130k on 100 partitions with `batch=100`.
- **Client batch size matters:** `batch=100` ≈ 130k (validation); `batch=10`
  (this campaign, harder/more requests) ≈ 95–104k.

## Phase 3 — long-running balanced soak (LIVE)

Config: `bench-q100`, **100 partitions**, producer 12×300 (batch=10) +
consumer 7×250 (batch=200, autoAck), `RETENTION_PARALLELISM=8`,
`completed_retention=300s`. ~10h cap.

Steady state (verified through the retention inflection):
- **push ~95–104k/s, pop matching**, 0 server errors.
- **`messages` table plateaued flat at ~13 GB** (live ~30M stable, dead bounded
  0–3M → autovacuum keeping up). Retention deletes ~74–90k/s ≈ push.
- Queen ~2–4 vCPU (16–35% of 12), PG ~20–24 vCPU (62–75%), loader idle.
- Disk stable. **Sustainable** — no death spiral.

## Bugs / gotchas found

- **`completed_retention_seconds` keeps getting coerced to 1800** even when 300 is
  configured: the producer client's startup `configure()` re-applies it after any
  SQL fix. Worked around with an **enforce-loop** (`enforce-retention.sh`,
  re-applies 300 every 5s for 2.5 min until the one-time startup configure is
  past). **Real fix needed:** stop `configure` from coercing `completedRetentionSeconds`
  (route passes it through, so the 1800 default is applied at push-time queue
  auto-creation or a server default) — same bug seen in the sustained-soak work.
- **Self-`pkill` foot-gun** in ops: `pkill -f <script>.sh` issued over SSH matches
  the SSH shell's own command line → kills itself (exit 255). Kill by PID or use
  `pkill -x node`.

## How to push higher (next)

- **`batch=100`** instead of 10 → ~130k balanced (validation showed it).
- Reduce partition contention further / shard partition_lookup.
- The headroom is PG (lock contention, not CPU); 150k balanced needs less
  contention or more PG, not more broker workers.

Scripts: `start-broker.sh`, `restart-queen.sh`, `run-long.sh`, `bal-run.sh`,
`prod-burst.sh`, `fusion-sweep.sh`, `fusion-sample.sh`, `long-mon.sh`,
`enforce-retention.sh`. Live monitor on broker: `/root/bench-runs/long-mon.log`.
