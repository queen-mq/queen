# pgmq on one Postgres — how far it goes, and how it compares to Queen

**Date:** 2026-06-08 · **Box:** DigitalOcean 32 vCPU / 62 GiB / 387 GB disk (`206.189.51.203`)
**Subject:** [pgmq](https://github.com/pgmq/pgmq) 1.11.1 on PostgreSQL 17, driven as a standalone Postgres-backed queue.

> **TL;DR.** Both pgmq and Queen are ultimately bounded by **one Postgres** (~~120–170k msg/s on this box). For **unordered, SQS-style** work pgmq is solid — *once you front it with a connection pooler and/or shard across many tables* (a single queue collapses under concurrency). For **ordered, per-entity FIFO** — the workload Queen is built for — pgmq runs **~~3–13k msg/s** vs Queen's **119k**, i.e. **~9–38× slower**, because ordered reads are an expensive per-group head-scan. The difference between the two systems is **architecture, not the database underneath.**

---

## Setup & methodology


| Aspect              | Value                                                                                                                                                                                                                                                  |
| ------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Host                | 32 vCPU / 62 GiB / 387 GB, Ubuntu, Docker                                                                                                                                                                                                              |
| Postgres            | `ghcr.io/pgmq/pg17-pgmq` · pgmq **1.11.1** · `shared_buffers=16GB` · `max_connections=2000` · `synchronous_commit=on` (WAL fsync on commit — strict durability) · aggressive autovacuum (`naptime=5s`, `scale_factor=0.02`, `cost_delay=0`, 6 workers) |
| Workload            | `MODE=plain` = SQS-style competing consumers (unordered); `MODE=fifo` = per-group FIFO via `read_grouped_head` / `read_grouped_rr`                                                                                                                     |
| Per op              | producer `pgmq.send_batch` of **10**; consumer `read`(qty **100**) + `delete` (unordered) or `read_grouped_*`(100 groups) + `delete` (ordered)                                                                                                         |
| Driver              | `pgmq-bench.js` — closed-loop, N concurrent virtual clients per role, records msg/s + latency histogram                                                                                                                                                |
| Connections         | **direct** = each client is its own PG backend; **pooled** = PgBouncer transaction pooling, `default_pool_size=64`                                                                                                                                     |
| Duration            | 60 s per sweep step; 45 min for the soak                                                                                                                                                                                                               |
| **Queen reference** | same droplet class: **24 h soak = ~119k balanced push+pop, *ordered* (300 partitions), zero loss**, broker ~5 cores / ~400 MB RSS, PG ~21/32 cores; matrix push peak ~171k                                                                             |


Harness, raw logs, per-step artifacts and the plot scripts are all in this folder.

---

## 1 · Unordered, single queue, direct — negative scaling

Each client = its own PG backend; one queue; ramp clients to saturation.

pgmq direct single-queue saturation


| clients/role | push | **pop**  | pop p99  | active backends |
| ------------ | ---- | -------- | -------- | --------------- |
| 25           | 128k | **60k**  | 0.1 s    | 43              |
| 100          | 106k | 31k      | 0.8 s    | 180             |
| 200          | 89k  | 20k      | 2.0 s    | 366             |
| 400          | 52k  | 13k      | 5.3 s    | 742             |
| 800          | 44k  | **8.5k** | **15 s** | 1442            |


**Finding.** A single pgmq queue **peaks at the *lowest* concurrency (~60k pop @ 25 clients) and degrades from there** — classic negative scaling. The consumer is the bottleneck: `read` (`FOR UPDATE SKIP LOCKED`) + `UPDATE` (visibility timeout) + `DELETE` all hammer one table, so more clients = more lock/buffer contention, not more throughput. Zero connection errors — it dies on **contention**, and the queue table bloated to 600–700 MB in 60 s as push outran pop.

---

## 2 · PgBouncer pooling — bounds the collapse

Same workload, but clients multiplex onto ~64 real PG backends.

pgmq direct vs pooled


| clients/role | pooled push/pop | pooled pop p99 | (direct pop)          |
| ------------ | --------------- | -------------- | --------------------- |
| 100          | 56k / 48k       | 64 ms          | (31k)                 |
| 200          | 50k / 44k       | 84 ms          | (20k)                 |
| 400          | 47k / 41k       | 143 ms         | (13k)                 |
| 800          | 25k / 18k       | —              | **errors**            |
| 1600         | 0               | —              | **collapse (errors)** |


**Finding.** Pooling caps active backends at ~~64 **and self-balances push≈pop** (slow pop transactions hold pool slots, throttling push). That holds pgmq at **~~41–48k with sub-150 ms p99 and a drained queue up to ~400 clients** — vs the direct collapse. But it's not infinite: past ~800 clients the slow reads exhaust the pool → `query_wait_timeout` errors → collapse at 1600. **A pooler is effectively mandatory for pgmq at concurrency.**

---

## 3 · 45-minute sustained soak (pooled, 200 clients) — stable

pgmq 45-minute pooled soak

**Finding.** At its stable operating point pgmq **held ~45k balanced push/pop for 45 minutes**, p99 68/91 ms, **0 errors**, queue table flat at ~150 MB, dead tuples bounded by autovacuum (sawtooth 0.4–1.3M, 360 runs), ~63 backends, ~16–18 PG cores. So pgmq is **not fragile over time** once pooled and client-bounded — it just has a modest single-queue ceiling.

---

## 4 · Sharding (unordered) — the intended scaling path

Spread the *same* 300-client load across N queues (= N tables), direct connections.

pgmq sharding scaling


| queues | push | **pop**  | pop p99 | PG cores |
| ------ | ---- | -------- | ------- | -------- |
| 1      | 70k  | **15k**  | 3.5 s   | 27       |
| 4      | 125k | 42k      | 1.7 s   | 27       |
| 8      | 145k | 66k      | 1.2 s   | 26       |
| 16     | 144k | 96k      | 0.9 s   | 25       |
| 32     | 153k | **125k** | 0.5 s   | 23       |


**Finding.** Sharding **lifts pop ~8× (15k → 125k)** as the single-table contention spreads across 32 tables; push plateaus ~~150k (PG's WAL/insert ceiling). The CPU panel is the tell: at 1 queue PG burned **~~27 cores to deliver 15k** (mostly spinning on locks); at 32 queues it used **~23 cores for 125k** — ~10× more useful work per core. So pgmq's single-queue wall was a **contention** limit, not a CPU limit, and sharding recovers it **up to** PG's real ceiling (it doesn't exceed it). Cost: 32 physical tables + 600 direct backends, and **no cross-queue ordering**.

---

## 5 · Ordered FIFO — the ordering tax

pgmq can preserve per-group FIFO via `read_grouped_head` (all consumers reach for the same group heads) or `read_grouped_rr` (per-group advisory locks → consumers get *disjoint* groups). "Groups" = ordered lanes = the analog of Queen partitions.

pgmq ordered FIFO — full comparison

Peak ordered pop (single queue, direct):


| consume function      | 300 lanes (matched to Queen) | 1000 lanes (pgmq best) |
| --------------------- | ---------------------------- | ---------------------- |
| `read_grouped_head`   | 1.5k                         | 4.3k                   |
| `**read_grouped_rr`** | **3.1k**                     | **13.4k**              |


**Findings.**

- **Ordering is 1–2 orders of magnitude slower than unordered.** Each `read_grouped_*` call computes the per-group head (`MIN(msg_id)` per group) over the whole table — expensive — and a lane can have only ~one message in flight at a time. All configs **peak then decline** as the backlog grows and head-scans get costlier.
- `**read_grouped_rr` is ~3× better than `read_grouped_head`** for competing consumers (advisory locks hand out disjoint groups, so it *scales* with consumers up to a point instead of colliding).
- **More lanes help pgmq** (300 → 3.1k, 1000 → 13.4k) — more parallel heads — the opposite of Queen, which is flat across partition count.

The ordering tax vs the unordered path, and the matched-cardinality comparison:

pgmq ordered vs unordered

pgmq ordered matched at 300 lanes

---

## 6 · Queen vs pgmq — what it all means

Same hardware, peak by workload:


| Workload                                         | pgmq           | Queen                           |
| ------------------------------------------------ | -------------- | ------------------------------- |
| Unordered, 1 queue (pooled, sustained)           | ~47k           | —                               |
| Unordered, sharded (32 tables)                   | ~125k          | —                               |
| **Ordered FIFO, matched 300 lanes**              | **~3k** (`rr`) | **119k**                        |
| **Ordered FIFO, pgmq's best (1000 lanes, `rr`)** | **~13k**       | ~119k (flat in partition count) |


1. **Both are one-Postgres-bound (~120–170k).** Neither beats it. pgmq's sharding *recovers to* that ceiling; Queen *already reaches it on one logical queue*. Splitting Queen into many queues would **share** the ceiling, not multiply it.
2. **pgmq's weakness is contention on a single queue table** → it needs a pooler (to cap backends) and sharding (to scale), and it gives up cheap ordering.
3. **Queen avoids the contention by architecture** — a broker coalesces thousands of clients onto a handful of PG backends (fusion ≈ 12k req/s → 1.1k commits/s) and uses per-partition serialization + a commit-ordered cursor — so it sustains **ordered** delivery at the full PG ceiling.
4. **Ordering is the decisive split.** Unordered, pgmq is in Queen's throughput tier. Ordered (per-entity FIFO, Queen's reason to exist), Queen is **~9–38× faster**.
5. **Latency:** pgmq wins single-op latency at low load (~~1.4 ms, no broker hop) — but Queen's *engine* is equal (~~1.4 ms direct; the gap is purely the HTTP hop, removable via `pg_qpubsub`). Under load pgmq's tail explodes to seconds; Queen stays bounded.

### When to use which

- **pgmq** — SQS-style, unordered or low-cardinality, competing consumers, "I already run Postgres and want zero extra moving parts." Front it with PgBouncer; shard to scale.
- **Queen** — per-entity ordering at high cardinality, fan-out to multiple consumer groups, replay, high concurrency, transactional integration — at full one-Postgres throughput, where pgmq would need many tables and still trail on ordered work.

**Net: same database, same ceiling — pgmq is the minimal in-DB queue; Queen is the broker tier that turns one Postgres into an ordered, fan-out, high-concurrency message queue.**

---

## Reproduce

```bash
cd benchmark-queen/pgmq           # harness (pgmq-bench.js, run.sh, sweep-*.sh)
cp docker-compose.206.yml docker-compose.yml && docker compose up -d   # 32-core tuned PG+pgmq+PgBouncer

# unordered: direct collapse, pooled ceiling, 45-min soak, sharding
QUEUES='25 50 100 200 400 600 800' bash sweep-direct.sh         # direct
QUEUES='25 50 100 200 400 800 1600' bash sweep-pooled.sh        # pooled (PgBouncer)
CONNECTIONS=200 DURATION=2700 bash soak-pooled.sh               # 45-min soak
QUEUES='1 2 4 8 16 32' TOTAL=300 bash sweep-shard.sh            # sharded

# ordered FIFO (per-group), head vs rr, 300 vs 1000 lanes
NUM_PARTITIONS=300  GROUPED_FN=read_grouped_head bash sweep-fifo.sh
NUM_PARTITIONS=1000 GROUPED_FN=read_grouped_rr   bash sweep-fifo.sh
```

Charts regenerate via the `plot-pgmq-*.py` scripts in `sweep-206/`. Raw per-step artifacts (producer/consumer JSON, `metrics.csv`, `*.dockerstats.csv`) are under `sweep-206/results/`.