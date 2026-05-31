# Queen vs pgmq — benchmark results

A like-for-like comparison of **Queen MQ** and **pgmq** (PostgreSQL Message Queue),
run on a single Mac, with a deliberate bias toward *fairness*: equal Docker
budget, identical Postgres config, native (non-emulated) Queen image, and a
**symmetric closed-loop client on both sides**.

> **TL;DR.** At the SQL-engine level Queen and pgmq are **equally fast** (~~1.4 ms/op).
> Where they differ is architecture: under **high concurrency** and **ordered
> fan-out** Queen does ~**1.75–2×** the throughput at **1× writes (vs ~10×)**,
> **zero churn (vs millions of UPDATE+DELETE)**, and **~~8× fewer active Postgres
> backends**. pgmq wins **single-op latency at low load** — but only because of
> Queen's optional broker hop, which Queen can skip entirely (`pg_qpubsub`). When
> messages do real work, throughput **converges** and the decision is about
> ordering / fan-out / DB-load, not msg/s.

---

## Methodology & fairness


| Aspect        | Setting                                                                                                                        |
| ------------- | ------------------------------------------------------------------------------------------------------------------------------ |
| Host          | Mac, 10 cores, OrbStack                                                                                                        |
| Docker budget | **identical per stack** — Postgres 3 CPU / 3 GiB on both; broker / PgBouncer 3 CPU                                             |
| Postgres      | **byte-identical config** both sides, `synchronous_commit=on` (WAL fsync — same durability tier)                               |
| Queen image   | **native arm64, built locally** (the published `amd64` image runs *emulated* on arm64 and badly understates Queen — see notes) |
| Queen broker  | `NUM_WORKERS=2` (≈ cores), sidecar pool sized per test                                                                         |
| Clients       | symmetric closed-loop: `pgmq-bench.js` (SQL) vs `queen-bench.js` (HTTP); `ENGINE=queen` calls Queen's stored procs directly    |
| pgmq access   | via **PgBouncer** (transaction pooling) for broker comparisons; **direct libpq** for latency/engine tests (its best case)      |
| pgmq ordering | `x-pgmq-group` + `read_grouped_head` (FIFO per group). Fan-out via topic `#` → one queue per consumer group                    |


**Read this caveat:** the Mac is **directional**, not absolute. Queen's broker was
capped at 3 (real) cores; the headline throughput numbers will be higher on the
32-core Linux host. The **architectural** metrics — write amplification, churn,
active-backend count — are exact and hardware-independent.

---

## Results

### 1) Balanced throughput — 100 conns/role, 1000 ordered partitions, 120s


| metric                   | **Queen**  | **pgmq** |
| ------------------------ | ---------- | -------- |
| push msg/s               | **14,616** | 7,882    |
| pop msg/s                | **14,271** | 6,650    |
| PG active backends (avg) | **7.0**    | 57.3     |
| hot-table UPDATE+DELETE  | **0**      | ~1.6M    |
| peak dead tuples         | **0**      | 259,884  |


→ ~1.85× push, ~2.1× pop, **8× fewer active backends**, **zero churn**. Queen's
broker coalesces the 100 connections onto ~7 Postgres backends; pgmq drives ~57.

### 2) Ordered fan-out — 10 consumer groups, 1000 partitions, FIFO **both sides**, 120s

The faithful Smartchat shape: every event delivered to all 10 groups, **ordered per partition**.


| metric                              | **Queen**  | **pgmq**  |
| ----------------------------------- | ---------- | --------- |
| logical push msg/s                  | **7,634**  | 4,330     |
| **delivered msg/s** (sum of groups) | **70,298** | 40,272    |
| delivered / logical                 | 9.2×       | 9.3×      |
| write amplification                 | **1×**     | **9.8×**  |
| hot-table UPDATE+DELETE             | **0**      | 9,665,218 |
| PG active backends (avg)            | **7.0**    | 60.7      |


→ Queen **~1.75× more delivered** at **1× writes** and **zero churn**, Postgres
near-idle (7 vs 61 backends). Note: pgmq *unordered* (competing consumers, no
ordering) reached 63k — but forcing the **per-partition FIFO guarantee** dropped
it to 40k. Queen's number is unchanged because for Queen, ordering *is* the
cursor model, not an added cost.

### 3) Single-message latency — concurrency = 1


| p50 / p99 (ms) | **Queen (via HTTP broker)** | **pgmq (direct libpq)** |
| -------------- | --------------------------- | ----------------------- |
| push           | 7.9 / 15.2                  | **1.4 / 4.5**           |
| pop            | 7.9 / 15.7                  | **1.4 / 4.7**           |


→ pgmq ~**5.6× lower** at low load. This is the honest cost of Queen's **broker
hop** at concurrency=1 (nothing to coalesce). See #4 for what it actually is.

### 4) Engine latency — Queen stored procs called **directly** (no broker), concurrency = 1


| p50 / p99 (ms) | **Queen SP (direct / `pg_qpubsub`)** | **pgmq (direct)** |
| -------------- | ------------------------------------ | ----------------- |
| push           | **1.4 / 4.7**                        | 1.5 / 5.2         |
| pop + consume  | **1.4 / 4.9**                        | 1.5 / 5.1         |


→ **The engines are equal** (Queen a hair faster). Decomposing #3:

```
Queen via HTTP broker:   7.9 ms
Queen stored-proc direct: 1.4 ms   ← the engine
pgmq direct:              1.5 ms
broker / HTTP hop ≈ 6.5 ms = 7.9 − 1.4
```

The #3 latency gap is **100% the broker hop, not the engine**. Deployed as
`pg_qpubsub` (extension, no broker), Queen gives **pgmq-class latency** *plus* the
cursor model (zero churn) and per-partition ordering. Queen offers both
deployments from one engine; pgmq only the in-DB one.

### 5) Worker-bound — 20 ms of real work per message, 100 consumer conns, 120s


| metric                   | **Queen** | **pgmq** |
| ------------------------ | --------- | -------- |
| delivered msg/s          | 4,310     | 4,605    |
| PG active backends (avg) | **2.7**   | 19.3     |


→ Throughput **converges** (within ~7%): once a message does real work, the
**broker stops being the bottleneck** — your worker fleet sets the rate. But Queen
reaches it at **7× less DB load** (2.7 vs 19.3 active), leaving Postgres free for
the rest of the app.

### Reference — vs Kafka / RabbitMQ (prior `2026-04-26` campaign, 32-core host)

- **Kafka**: ~39× Queen's raw throughput, but ~80× worse p99 under saturation,
60–140× the memory. Right tool above ~200k msg/s sustained.
- **RabbitMQ**: ≈ ties Queen on throughput, wins latency & CPU; Queen wins memory
and the architectural features (per-key ordering w/ parallel consumers, replay,
PG-transactional integration, dynamic partition cardinality).

---

## One-glance summary


| Dimension                     | Winner    | Margin                      |
| ----------------------------- | --------- | --------------------------- |
| SQL engine latency            | tie       | 1.4 vs 1.5 ms               |
| High-concurrency throughput   | **Queen** | ~1.9–2.1×                   |
| Ordered fan-out throughput    | **Queen** | ~1.75×                      |
| Write amplification (fan-out) | **Queen** | 1× vs 9.8×                  |
| Churn / bloat                 | **Queen** | 0 vs millions               |
| Active Postgres backends      | **Queen** | ~8× fewer                   |
| Single-op latency @ low load  | **pgmq**  | ~5.6× (it's the broker hop) |
| Worker-bound throughput       | tie       | within ~7%                  |


---

## Where each fits

- **Queen** — multi-consumer (fan-out), per-entity ordered, high cardinality,
high concurrency, polyglot, "one beefy Postgres." It pays the coalescing/ordering
cost natively, so it scales these with the DB near-idle. (For lowest latency,
deploy `pg_qpubsub` and skip the hop.)
- **pgmq** — SQS-style queue, simplest possible (just SQL functions, no broker),
excellent for lower-concurrency / unordered competing-consumer work and lowest
single-op latency in-DB. Pays 10× writes + churn to *simulate* ordered fan-out.

The honest one-liner: **Queen's engine equals pgmq; what you're really choosing is
whether you want a broker tier (coalescing, fan-out, polyglot) on top of it.**

---

## Reproduce

```bash
cd benchmark-queen/pgmq
# build native arm64 Queen image first (Mac):  docker build --platform linux/arm64 -t queen-mq:arm64-local ../..
QUEEN_IMAGE=queen-mq:arm64-local

# 1) balanced + 2) connection load
DURATION=120 CONNECTIONS=100 QUEEN_NUM_WORKERS=2 READ_QTY=200 ./compare.sh
# 2) ordered fan-out (faithful Smartchat: per-partition FIFO both sides)
PGMQ_ORDERED=1 GROUPS=10 QUEEN_NUM_WORKERS=2 QUEEN_SIDECAR_POOL_SIZE=150 READ_QTY=200 ./compare-fanout.sh
# 3) latency  &  5) worker-bound
./compare-extra.sh latency
./compare-extra.sh worker
# 4) engine latency (direct stored procs vs pgmq functions)
./run-sp-latency.sh
```

All raw artifacts are under `results/`. On the 32-core Linux host, drop the
`QUEEN_CPUS` cap and bump `PG_SHARED_BUFFERS` — relative results hold, absolute
throughput rises.