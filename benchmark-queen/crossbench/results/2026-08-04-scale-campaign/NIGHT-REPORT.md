# Overnight campaign — 2026-08-03/04, 32-core rig

Broker `bench-01` 104.248.245.59 and loader `bench-02` 159.89.104.168, both
**32 vCPU / 62 GB**, Xeon Platinum 8358, NVMe. Resource parity for every system
(`CM_CPUS=32 CM_MEM=56g`, Kafka heap 16 GB), Postgres resized for the box,
`synchronous_commit=on` / `fsync=on` untouched. Loader sampled throughout and
never above ~13% of 32 cores. Full provenance in `phase0/`.

**Headline: the single most important result of the night is negative.** On
dense workloads Queen is not reproducible — the same configuration under the
same load produced p50 between 340 ms and 3 234 ms. Three conclusions reported
earlier in the session rest on single runs in that regime and are withdrawn
below.

---

## 1. Reproducibility — the finding that governs everything else

Same cell (P=1000, rate 12 000 ev/s = 72 000 deliveries/s), fresh stack each
time, broker env read back from `docker inspect` and recorded in each
`invocation.txt` so the labelling is proven, not remembered. Raw in
`repeatability/`.

| run | set A (`POP_FUSION=0`, `HOLD_MS=3`, 4 wk) | set B (`POP_FUSION=1`, `HOLD_MS=15`, 8 wk) |
|---|---:|---:|
| 1 | 679.9 | 623.5 |
| 2 | 1048.6 | 440.9 |
| 3 | 741.5 | 2965.8 |
| 4 | 340.0 | 3234.3 |
| **median** | **711** | **1795** |
| **spread** | **3.1x** | **7.3x** |

Pooling every measurement of this cell taken during the session: set A median
~680 (340–1049), set B median ~650 (340–3234). **The medians are the same; B has
the fat tail.**

It is not the machine degrading: A-4 produced the best value of the whole series
(340.0) at 22:36, between B-3 (2965.8 at 22:32) and B-4 (3234.3 at 22:38). Disk
7% used, 51 GB free, no kernel errors. The variance is intrinsic to the system in
this regime.

It is regime-specific: sparse cells, the tenant scenarios and the soak were all
stable across repeated measurements.

**It is NOT simply "dense".** The soak is a far denser shape — 200 partitions at
600 000 msg/s is 3 000 msg/s *per partition*, against 72/s per partition in the
unstable cmbench cell — and it held p50 118–125 ms with flat lag for an hour
(2.09 billion messages, zero errors). Forty times the density, perfectly
repeatable.

So the instability is not caused by load level. The differences between the two
shapes are elsewhere, and they are the places to look:

| | soak (stable) | cmbench dense cell (7x spread) |
|---|---|---|
| topology | one queue, one hop | 12 stages, 2 chained consumer hops |
| `pop-partitions` | 1 | 40 |
| ramp | 60 s | 5 s |
| consumers | 600 closed-loop drainers | 8/stage with simulated work |

The suspicion therefore points at the multi-stage chain and the visit width, not
at the offered rate.

### What this withdraws

- **The two-set "law"** derived earlier. Its entire signal was a 2x A/B gap in
  the dense cell; that gap is noise. The factorial that "isolated the push hold
  as the actuator carrying 45% of the effect" was measuring variance.
- **Queen vs Kafka on dense.** Reported as 340.0 vs 2287.0 (6.7x). Queen's range
  on that cell overlaps Kafka's single observation. Queen vs pgmq (11 863) and
  vs RabbitMQ (19 952) survives even at Queen's worst.
- **"Set B wins on dense."** Medians are equal; B is riskier. If a default had to
  be chosen tonight it would be A — the opposite of the earlier conclusion.

---

## 2. Head-of-line blocking — the differentiator, as a derivative

P = 20 000 properties, 6 000 ev/s (36 000 deliveries/s), 120 s. Raw in `hol/`.

| ordered lanes | Queen p50 | Queen p95 | Kafka p50 | Kafka p95 |
|---|---:|---:|---:|---:|
| 800 (200 lanes) | 131.1 | 3527.0 | 142.9 | 4987.9 |
| 4 000 (1 000 lanes) | 110.2 | 1923.1 | 155.9 | 12937.0 |
| 80 000 (dynamic, one per property) | **101.1** | 2493.9 | — | — |

**Queen improves as ordered lanes are added; Kafka degrades.** For Queen more
lanes means fewer properties contending for one lane — less blocking — and the
ring cost grows more slowly than the benefit. For Kafka every lane is another
physical partition, and its p95 grows 2.6x between 200 and 1 000 lanes.

The differentiator is therefore not "Queen is faster" but **"Queen is the only
one of the two whose cost falls when you add ordered lanes"**.

Note the level at a single point does not show this: at 20 properties per lane
(rho ~0.54) Queen dynamic and Queen constrained are within one bucket of each
other. The advantage is in the slope, and it needs the sweep to be visible.

### Cardinality ceiling

Queen creates and holds **400 000 ordered lanes** (100 000 properties x 4 queues)
with zero order violations, but the cardinality carries a rate ceiling:

| rate | deliveries/s | lag | verdict |
|---|---:|---|---|
| 1 500 | 9 000 | 113–369, flat | holds |
| 3 000 | 18 000 | ~700, flat | **holds** |
| 10 000 | 60 000 | 7 437 -> 129 695, monotone | **fails** |

So "Queen handles 100 000 partitions" is true, with "up to ~3 000 ev/s on this
box" attached.

### Kafka cardinality ladder — where it actually breaks

P = 20 000 properties, 6 000 ev/s, 120 s. `lanes` is partitions **per topic**;
the harness creates 4 topics. Raw in `hol/KLAD-*`.

| lanes | partitions/topic | total | result |
|---:|---:|---:|---|
| 200 | 200 | 800 | p50 142.9, **p95 4 987.9** |
| 1 000 | 1 000 | 4 000 | p50 155.9, **p95 12 937.0** |
| 2 000 | 2 000 | 8 000 | p50 155.9, **p95 23 726.6**, p99 28 215.8 |
| 5 000 | 5 000 | 20 000 | **POLICY_VIOLATION** — topic creation refused |
| 10 000 | 10 000 | 40 000 | **POLICY_VIOLATION** |
| 20 000 | 20 000 | 80 000 | **POLICY_VIOLATION** |

Two separate findings, and both matter:

**A hard wall between 2 000 and 5 000 partitions per topic.** On default
configuration the broker refuses `CreateTopics` outright. This is a
*configuration* refusal, not a proven physical limit — the compose sets no
`create.topic.policy`, so it is KRaft-internal validation, and a differently
tuned Kafka might go higher. That caveat should be stated whenever this is
quoted.

**The tail is already gone long before the wall.** p95 grows roughly linearly
with partition count — 4 988 -> 12 937 -> 23 727 ms as partitions go
800 -> 4 000 -> 8 000 — so even if the policy limit were raised, the operating
point beyond a few thousand partitions is not usable. Raising the limit would
not buy a usable system.

**Against Queen at 80 000 ordered lanes: p50 101.1, p95 2 493.9.** Queen's p95 at
ten times Kafka's maximum working cardinality is **9.5x better** than Kafka's p95
at the largest cardinality Kafka will accept.

---

## 3. Methodological trap: warmup contamination at high cardinality

At P = 100 000 the harness warmup delivers 1.2 M messages while a 60 s rated
window delivers ~1.08 M. The reported p50 is then majority warmup.

The diagnostic signature: **the reported p50 falls monotonically through the run
(51 748 -> 47 453 -> 39 903 -> 30 770 -> 23 727 -> 18 296 ms) while lag stays
flat**. It is the warmup ageing out of the histogram, not the system improving.
Confirmation: at a *lower* rate the reported p50 is *worse* (43% warmup share ->
43 515 ms; 35% share -> 18 296 ms), because a smaller rated window makes the
fixed warmup a larger fraction. A system that gets worse when you reduce its load
does not exist.

Any high-cardinality cell needs a rated window long enough to swamp the warmup —
300 s minimum at P >= 20 000 — or the run.log time series read instead of the
summary.

---

## 4. Tenants — the clean win

`goload -mode tenants`. Raw in `tenants/`.

| scenario | today | 2026-07-24 |
|---|---|---|
| SMALL — 10 x 10 = 100 queues | p50 7.5–10.2 ms, p99 ~39, 14 390 msgs, **0 errors, 0 tail** | p50 6–7 ms |
| BIG — 1000 x 10 = 10 000 queues, ~6 000 msg/s | **p50 80–121 ms**, p99 280–358, lag 60–709, 3.6 M msgs, **0 errors** | p50 334 ms, p99 610 |

The BIG scenario is **3–4x better than July** on the same rig shape, with 10 000
parked consumers and zero push/pop/ack errors. 100 queues provisioned in 0.1 s.

---

## 5. Soak — 600 000 msg/s sustained

`goload -mode openloop`, production semantics: 200 partitions, 600 consumers,
pop-batch 500, lease + explicit async acks (inflight 256), dedup 60 s, retention
completed 300 s / pending 3600 s, payload 256 B, push-batch 100.

First attempt ran with the default `-pop-batch 200` instead of July's 500 and was
**not in balance**: push 600 k/s against pop 400 k/s, lag growing 200 k/s.
Restarted with `-pop-batch 500`, after which:

```
push=89 951 100  pop=89 897 200  lag=53 900 (flat, 52k–67k band)
p50=123 ms  p99=309  p999=412
shed=0  errs push=0 pop=0  ackErr=0
```

**Final, 3 h 10 m:**

```
offered=6 822 018 300   achieved=6 821 967 400   shed=0
pushed =6 821 967 400   popped  =6 821 920 800   acked=6 821 901 600
pushErr=0   popErr=0   ackErr=0
lag final=46 600 (band 44 600-80 000 across the run, no trend)
overall p50=120.32   p99=296.96   p999=374.78 ms   ackAvg=81.26 ms
```

**6.82 billion messages, zero errors of any kind.** p50 sampled every ~15 min:
124, 107, 119, 115, 119, 120, 121, 116, 117 — **+/-8% over three hours**.

Against July's 24 h soak (51.8 billion msgs, p50 87.6 / p99 272 / p999 473,
63 368 errors = 0.00012%):

| | tonight (3 h) | July (24 h) |
|---|---:|---:|
| p50 | 120.3 | 87.6 |
| p99 | 297.0 | 272 |
| p999 | **374.8** | 473 |
| errors | **0** | 63 368 |

Median is 37% worse, p99 9% worse, **p999 21% better, and the error record is
perfect**. Tighter tail and clean run, higher median.

---

## 5b. pgmq — the mechanism, and its ceiling

`read_grouped_head` dumped from the running extension:

```sql
WITH fifo_groups AS (
    SELECT COALESCE(headers->>'x-pgmq-group','_default_fifo_group') AS fifo_key,
           MIN(msg_id) AS head_msg_id
    FROM pgmq.q_<queue>
    GROUP BY COALESCE(headers->>'x-pgmq-group','_default_fifo_group')
), selected_messages AS (
    SELECT g.head_msg_id FROM fifo_groups g
    JOIN pgmq.q_<queue> q ON q.msg_id = g.head_msg_id
    WHERE q.vt <= clock_timestamp()
    ORDER BY q.msg_id LIMIT $1 FOR UPDATE SKIP LOCKED
)
UPDATE ... SET vt = clock_timestamp() + interval, read_ct = read_ct + 1 ...
```

The group is a string inside the per-row JSONB `headers`. `create_fifo_index`
builds a **GIN index on `headers`** — and measured `EXPLAIN (ANALYZE)` on
200 000 rows / 50 000 groups shows **it is not used**:

```
HashAggregate (rows=50000)  Memory: 5649kB
  Group Key: COALESCE(headers->>'x-pgmq-group', '_d')
  -> Seq Scan on q_expq (rows=200000)   48.7 ms
```

A GIN on JSONB serves containment (`@>`, `?`), not `GROUP BY expr`. So the cost
of one read is **a sequential scan of the whole queue table**.

**Cost is O(standing rows), not O(groups).** The group count only sizes the hash
table (5.6 MB for 50 000 groups); the row count drives the scan. That single fact
explains both observed behaviours:

- **Flat in cardinality** — 12 000 vs 240 000 ordered lanes gave byte-identical
  p50 (71.4675) and p99 (5439.3393). Same rows, same cost.
- **Collapses under density** — when arrival exceeds service the table grows, and
  every read re-scans the whole backlog: slower service, bigger backlog, positive
  feedback.

### Ceiling, channel-manager shape, 1000 groups

| rate | deliveries/s | served | p50 |
|---:|---:|---:|---:|
| 3 000 | 18 000 | 95% | 55.1 |
| 4 500 | 27 000 | 95% | 60.1 |
| 6 000 | **36 000** | **95%** | **77.9** — last healthy |
| 9 000 | 54 000 | 83% | **7 054** — collapse |

Ceiling between 36 000 and 54 000 deliveries/s, and the collapse is a cliff:
**90x latency for a 1.5x rate increase**. Queen's ceiling on the same shape is
~120 000 deliveries/s, i.e. **2.5–3x higher**.

### The degenerate case: one group

| groups | deliveries/s demanded | served | p50 |
|---:|---:|---:|---:|
| 1 000 | 12 000 | 100% | 55.1 |
| **1** | 12 000 | **6%** (665/s) | **39 903** |

With one group per queue each `read_grouped_head` returns **one message** after
scanning the whole table, so throughput collapses to reads/s. **27x worse purely
by removing groups**, at identical message rate.

### The two cost functions are inverses

- **pgmq**: throughput = reads/s x groups with a visible head; cost per read =
  full table scan. **Gains per group, pays per standing row.**
- **Queen**: latency = ring lap = partitions / visit rate; each visit yields
  rate/partitions messages. **Pays per partition, gains per message in
  partition.**

Few dense lanes is Queen's best and pgmq's worst; many sparse lanes is the
reverse. The curves cross rather than one dominating — this is why the campaign
kept producing contradictory verdicts depending on which cell was measured.

### Feature consequence

The `UPDATE` mutates `vt`, `read_ct`, `last_read_at` **on the message row
itself**. There is no per-(queue, consumer-group) cursor anywhere. That is the
structural reason pgmq has no reconsume, no multiple consumer groups per queue,
no per-group lag and no natural DLQ bookkeeping: reading consumes the row's
availability and delete destroys it. Queen keeps `log_consumers` rows per
(partition, group) — an object per lane, which is exactly what costs it the
cardinality tail (p99 15 385 vs 5 439) and exactly what buys replay, DLQ,
multiple groups and per-group lag.

## 6. What should change in the campaign method

1. **Every dense cell needs >= 3 repetitions**, reported as median and spread. A
   single run in that regime is an unlabelled draw from a wide distribution.
   This applies retroactively to the comparison matrix's dense column, where
   every competitor is also a single run.
2. **High-cardinality cells need >= 300 s rated windows**, or the summary p50 is
   warmup.
3. **Record the broker env read back from the container** in every
   `invocation.txt` — it is what let the labelling question be settled on
   evidence instead of memory.
4. The loader calibration parser in the campaign script is broken (it produced
   "76 497 900 msg/s" by concatenating digits). Harmless here because the loader
   genuinely sustains 600 k/s, but it must be fixed before it silently picks a
   wrong soak rate.

## 7. Correctness

**Zero gaps, zero order violations, zero duplicates in every run of the campaign**
— all four systems, every configuration, including the collapsed cells at
25 000 ev/s with 1.3 M messages in flight and the runs that shed hundreds of
thousands of messages.
