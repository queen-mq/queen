# Engine-scaling discovery — "do we need multiple push/ack engines?"

**Question (from the libqueen-topology discussion):** is a *single* libqueen
push/ack engine serving all HTTP workers enough, or do we need multiple push
engines for throughput? Equivalently: at a fixed total DB concurrency, does the
**event-loop thread** (batching / firing / completion handling) saturate before
**Postgres** does?

This harness answers it empirically on the **current build** and isolates the
two candidate ceilings:

- **Engine-loop ceiling** → the libuv event loop can't drain fast enough.
Signal: `**evl=` lag** in `queen.log` rising above ~0–1 ms, broker pegging a
core, throughput dropping as engine count falls.
- **Postgres ceiling** → push-path serialization / WAL / index maintenance.
Signal: PG active backends + WAL rate flat-line while broker CPU and `evl`
stay low; throughput **unchanged** as engine count falls.

## What the existing data already tells us

From `../2026-06-04/THROUGHPUT-CAMPAIGN.md` and `SOAK-2026-06-05-FINDINGS.md`:

- **The push ceiling is Postgres, not the broker.** Across workers 10→16,
sidecar 250→500, static vs Vegas, balanced capped at **~110k push / ~220k
combined while PG used only 12–19 of 32 cores**. ("The limit is PG push-path
serialization … not anything tunable in libqueen.")
- `**bp-100`: 131k push/s at ~1.07 broker vCPU.** With good fusion the broker is
a near-thin pipe — one core is plenty.
- The broker only gets heavy (~5–6 vCPU) in the **low-fusion** regime
(`batch=1`, many partitions), where per-request HTTP + per-batch engine work
dominates. **That is the regime where a single engine loop is most at risk**,
so the sweep below stresses it explicitly.

So the prior is: **one push/ack engine should suffice** (PG-bound), and engine
count mostly matters only in the low-fusion regime. This harness confirms or
refutes that with `evl` lag + the engine-count sweep.

## How engine count is varied

On the current build there is **one Queen engine per HTTP worker**
(`acceptor_server.cpp`: "Queen instances: one per HTTP worker"), and the
connection budget is split `SIDECAR_POOL_SIZE / NUM_WORKERS`. So we use
`**NUM_WORKERS` as the engine-count knob** while holding **total slots
(`SIDECAR_POOL_SIZE`) constant** — i.e. same total DB concurrency, fewer/more
event loops. If `NUM_WORKERS=1` (one engine, all slots) sustains the same push/s
as `NUM_WORKERS=8`, multiple push engines add nothing.

> Caveat: `NUM_WORKERS` also sets the HTTP-thread count, so at `W=1` the single
> HTTP thread can bottleneck on JSON parse *before* the engine does. We
> disentangle with (a) the **fusion** regime (cheap parse: `batch=100`) — if
> `W=1` holds there, the engine loop is not the limit; and (b) `**evl` lag** +
> per-thread CPU, which are engine-loop-specific. A definitive "1 engine serving
> N HTTP threads" test needs the topology change; this pre-validates it cheaply.

## Files

- `run-engine-sweep.sh` — orchestrator. Sweeps `NUM_WORKERS × regime` (and an
optional slot sweep at `W=1`), restarting only the `queen` container each cell
(Postgres stays up). Truncates `queen.messages` between cells.
- `mon-engine.sh` — per-cell sampler → TSV: broker/PG CPU, PG active backends,
`xact_commit`, `n_tup_ins/del`, WAL bytes, and **max `evl` lag** parsed from
`docker logs queen`.
- `summarize-engine.py` — TSV → decision table per regime, with a verdict
heuristic (`engine-bound` vs `pg-bound`).

## Run (on the broker VM; Postgres + queen as docker containers)

```bash
# 1) PG + broker up once (reuses the 2026-06-04 broker bringup)
bash ../2026-06-04/start-broker.sh

# 2) sweep (goload can run locally pointing at the broker, or on the loader VM)
GOLOAD=../2026-06-04/goload/goload-linux-amd64 \
BROKER_URL=http://localhost:6632 \
WORKERS="1 2 4 8" SIDE=240 DURATION=90 \
bash run-engine-sweep.sh

# 3) read the verdict
python3 summarize-engine.py out
```

## Interpreting the output

For each regime, `summarize-engine.py` prints push/s, broker vCPU, PG vCPU, PG
active backends, WAL MB/s, and `evl` (max / p95) per `NUM_WORKERS`, then a
verdict:

- `**pg-bound**` — push/s ~flat as `W` falls and `evl` stays low ⇒ **one engine
is enough**; scale connection *slots*, not engines. (Expected for `fusion`.)
- `**engine-bound`** — push/s drops at low `W` while `evl` climbs and PG still
has CPU/connection headroom ⇒ multiple push engines (then partition-sharded)
are warranted. (Watch for this in `low-fusion`.)

The slot sweep at `W=1` (`PHASE=slots`) finds the DB-concurrency knee for a
single engine: the slot count past which more connections stop raising push/s
(the PG ceiling) — that's how many slots one push engine should own.