# KV + timers performance gates

The two performance rows of `PLAN_KV_TIMERS.md` §15, as scripts. They are **gates**, not a
benchmark session: they answer yes/no, they are meant to be re-run at F3 and F4, and they exit
non-zero when they fail. That is why they live here as tooling rather than under a dated result
folder — the dated folders in this directory are campaigns that happened once.

| script | plan row | phase | question |
|---|---|---|---|
| `perf-gate-hotpath.sh` | Perf gate | F4 | does a bundle carrying **neither** `kv` nor `timers` cost more CPU per message after the patch, and did any new statement start executing? |
| `perf-gate-sweeper.sh` | Perf gate sweeper | F3 | what does the sweeper cost an installation that never uses the feature, and does it tax the message path when it is awake with a non-empty table? |

Both run on their own compose project (`kvtgate`), their own volume and port 16644. Neither goes
anywhere near `:5432`, which is the live channel-ts stack.

---

## Why CPU per message, and why not latency

§15 says it in one line — "**CPU per messaggio entro l'1%**, che e' la metrica che ha catturato la
regressione push del seg v2 (la latenza e' troppo rumorosa a questi ordini di grandezza)" — and
everything in these scripts follows from it:

- **Cumulative counters, not samples.** CPU comes from each container's own cgroup
  (`cpu.stat: usage_usec`), read once before and once after the window. `docker stats` reports a
  percentage over a window it picks; two such percentages cannot be subtracted into an amount of
  work.
- **Fixed work, not fixed duration.** Every run sends exactly the same number of bundles. CPU per
  message is a point on a cost curve, so a run that happens to go faster is measured somewhere
  else on that curve, and the ratio stops meaning anything. The gate refuses to give a verdict
  when throughput between the two captures differs by more than 10%.
- **Byte-identical bodies.** The payload is a constant, the transaction ids are derived from
  (tag, worker, sequence), the queue and partition names are fixed. `loadgen.mjs` prints a
  checksum of every byte it sent and `compare` voids the run if the two captures disagree.
- **A fresh database per repetition.** Bloat, cache state and the `log_txns` window are all
  first-order effects at this resolution.
- **Both containers.** Most of the wire's cost is Postgres. A gate that measured only the broker
  would have missed the seg v2 regression entirely.

## What "UNRESOLVED" means, and why it is not a pass

The gate reports UNRESOLVED — and exits non-zero — when it cannot answer at 1%: fewer than three
repetitions, a within-capture spread larger than the tolerance, or a throughput difference
between captures. **This is the most important behaviour in the scripts.** A 1% question asked
of data that scatters by 5% has an answer that is a coin toss, and a green line that means "we
could not tell" is worse than no line at all.

Measured on a laptop that was also running docker builds, two captures **of the same image** came
out 35% apart. Run these on a quiesced rig — the PG :5455 rig of §15, never the live stack — and
keep `GATE_REPS >= 3`.

---

## `perf-gate-hotpath.sh` (F4)

```bash
# two images from the two commits
git switch <base>   && docker build -t queen:gate-before -f Dockerfile .
git switch kvtimer  && docker build -t queen:gate-after  -f Dockerfile .

cd benchmark-queen/kv-timers-gates
GATE_IMAGE=queen:gate-before ./perf-gate-hotpath.sh capture before
GATE_IMAGE=queen:gate-after  ./perf-gate-hotpath.sh capture after
./perf-gate-hotpath.sh compare before after
```

**Q1 — CPU per message, ±1%.** Median over repetitions of `(broker + postgres CPU µs) / messages`.

**Q2 — no new statement with `calls > 0`.** `pg_stat_statements` with
`pg_stat_statements.track=all`, so statements **inside** `log_transaction_wire_v1` and
`log_push_one_v1` are recorded individually — verified on this rig, the dump lists the wire's
provisioning query, its partition pre-lock and the dedup probe by name. This is not a weaker
restatement of Q1: §6.3 promises zero added statements, zero added plan nodes and zero added
locks when the arrays are absent, and the way that promise dies is a
`FROM jsonb_array_elements(COALESCE(p->'kv','[]'))` folded into a `UNION` with the pushes. One
extra Function Scan on a statement that runs on every bundle is far below 1% of CPU and
completely invisible to Q1 — and it is a permanent tax on everyone who never enables the feature.

New statements are reported in two classes. One naming `queen.kv`, `queen.log_timers`,
`kv_apply_v1`, `log_timers_*` and friends is **unwaivable**. Anything else is nearly always a
background loop (retention, the `log_txns` purge, stats reconcile) whose cadence straddled one
window and not the other; the capture already folds each label's idle-window statements into its
known set to absorb that, and `GATE_ALLOW_NEW='<extended regex>'` waives a culprit identified by
name.

Comparison is by normalized query **text**, never by `queryid`: `queryid` hashes relation OIDs
and every capture runs against a freshly created database.

## `perf-gate-sweeper.sh` (F3)

```bash
GATE_IMAGE=queen:gate-after ./perf-gate-sweeper.sh run
```

One image, three conditions, back to back:

| | flags | timers table |
|---|---|---|
| A | both OFF — the task is not spawned at all (§7.1) | — |
| B | both ON | empty |
| C | both ON | seeded (200k rows, due in ~30 days) |

- **G1 — the bill for a feature nobody uses.** Idle CPU of B minus A, in milli-CPU, against
  `GATE_IDLE_BUDGET_MCPU` (default 20 = 2% of one core = 1% of the 2-core free tier whose
  measured ceiling is ~480 msg/s). §7.1 is explicit that a per-second probe, a `Lane::Maint` slot
  and a pool connection on two empty tables is "rumore misurabile che nessun cliente ha chiesto".
- **G2 — the empty-table backoff engages.** Counts `log_timers_due_v1` calls during the idle
  window; more than 12 in 60 s means the sleep is not climbing toward the 30 s of §7.1. Without
  this, G1 passes on a fast host and fails on the customer's small one.
- **G3 — hot path unchanged.** CPU per message under C vs A, same ±1% machinery. This is what
  catches a fire loop competing with producers for the same partition serializer and the same
  fsync — the reason §7.3 keeps `QUEEN_SWEEPER_PARALLELISM` at 1.

Condition C seeds `queen.log_timers` with a direct `INSERT` rather than 200k HTTP schedules: the
condition needs a table with rows in it, not a test of the schedule path. It is the one place in
the repo outside the SQL that knows that table's shape — if the columns move, the `INSERT` in
`seed_timers()` moves with them. The gate reports C's probe rate separately and does **not**
budget it: a non-empty table is due-driven and does not back off, by design (§1.7), and that
number is the honest answer to "what does the feature cost at rest".

---

## Knobs

Defaults are in the script headers; everything is env.

| variable | default | notes |
|---|---|---|
| `GATE_IMAGE` | `queen:test` | the broker image under measurement |
| `GATE_REPS` | 3 | **floor for a verdict**; below 3 the gate reports UNRESOLVED |
| `GATE_BUNDLES` / `GATE_ITEMS` | 20000 / 20 | 400k messages per repetition |
| `GATE_WORKERS` | 8 | concurrent closed loops |
| `GATE_PARTITIONS` | 16 | destination partitions |
| `GATE_BROKER_CPUS` / `GATE_PG_CPUS` | 4 / 4 | container CPU limits |
| `GATE_SYNC_COMMIT` | `on` | full durability; measuring with it off measures a system nobody runs |
| `GATE_TOLERANCE_PCT` | 1.0 | the §15 number |
| `GATE_RATE_TOLERANCE_PCT` | 10 | throughput divergence that voids the comparison |
| `GATE_ALLOW_NEW` | — | extended regex waiving an identified background statement in Q2 |
| `GATE_SEED_TIMERS` | 200000 | condition C |
| `GATE_IDLE_S` | 15 (hotpath) / 60 (sweeper) | idle window |
| `GATE_IDLE_BUDGET_MCPU` | 20 | G1 |
| `GATE_MAX_IDLE_PROBES` | 12 | G2 |

`results/` is git-ignored: captures are local evidence, and a committed one would be read as a
published measurement.

## Requirements

Docker with cgroup CPU counters readable inside the container (cgroup v2 `cpu.stat`, or v1
`cpuacct.usage`; on Docker Desktop both live in the Linux VM and work), Node 18+ for the load
generator, `curl`, `awk`. No repo build step: the scripts drive images you build yourself, which
is what makes a before/after possible at all.
