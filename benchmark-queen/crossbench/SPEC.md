# CM-BENCH — channel-manager workload specification

Rev 1.0 — 2026-08-02. Branch `benchcomp`.

A **broker-independent** specification of the channel-manager workload, so the same
application shape can be run on QueenMQ, Apache Kafka, RabbitMQ and pgmq and the
results compared without arguing about what was measured.

This document is the contract. It is deliberately written so that a reviewer who
likes none of the four systems can check whether a given run honoured it.

---

## 0. Why this workload and not raw throughput

Raw push/pop throughput measures an engine. This measures an *application*: a hotel
channel manager that Smartpricing runs in production, whose defining traits are
**high-cardinality per-key ordering** and **fan-out to independent consumer groups**,
with consumers that do real work (10-30 ms per message) rather than counting.

The comparison is therefore **not** "who reaches N events/s". It is:

> At a fixed workload and a fixed correctness bar, on identical hardware,
> what does each system cost to serve — in CPU, in memory, in physical writes,
> in lanes and connections you must provision, and in semantics you must build
> yourself?

Peak throughput is reported too (§6.2), but as a second, separate table.

---

## 1. Topology

Two independent flows, each three hops deep, each fanning out to 5 consumer groups
at the final hop.

```
Flow A (availability, ~200 B payload)
  producer --(key=property)--> cm-avail
      group cm-db     : work 10-20 ms, then re-publish (key=property) --> cm-ota-sync
          group ota-1 : work 30 ms   (terminal)
          group ota-2 : work 30 ms   (terminal)
          group ota-3 : work 30 ms   (terminal)
          group ota-4 : work 30 ms   (terminal)
          group ota-5 : work 30 ms   (terminal)

Flow B (prices, ~2 KB payload)
  producer --(key=property)--> cm-prices
      group cm-cal    : work 10 ms,    then re-publish (key=property) --> cm-ota-prices
          group otap-1: work 30 ms   (terminal)
          ...
          group otap-5: work 30 ms   (terminal)
```

- 4 logical topics, 12 consumer streams (2 intermediate + 10 terminal).
- **P properties** (default 1000). The ordering key is the property.
- The offered rate **R** (total events/s) is split 50/50 between the two flows.
- Terminal groups do not re-publish; they only record.

### 1.1 Event stamp

Every event carries `{prop, flow, seq, ts}`. `seq` is monotonic per `(property, flow)`
and is assigned by the producer. **Derived publishes carry the stamp forward
unchanged** — the same `seq` traverses all three hops. `ts` is UnixMicro of the
producer's *scheduled* instant (not its send instant), so end-to-end latency is
coordinated-omission corrected.

Flow B additionally carries a `rates` array padded to the target payload size.

### 1.2 Per-property publish order

Each property is routed to a single ordered publisher (a fixed property→shard map),
so publish order equals `seq` order. Any system that reorders between publisher and
consumer is producing a real defect, not a harness artefact.

---

## 2. Derived invariants

These follow from §1 alone and hold for **every** system. They are the fairest
possible anchor: nobody gets to redefine the amount of work.

With `R` = total offered events/s, `F` = 5 (fan-out width), and per-stage work times
`Wa = 15 ms` (avg of 10-20), `Wb = 10 ms`, `Wota = 30 ms`:

| Quantity | Formula | R = 5 000 | R = 25 000 (July ref) |
|---|---|---|---|
| Deliveries/s | `R × (1 + F)` = `6R` | 30 000 | 150 000 |
| **Ordered concurrent lanes** | `R/2 × (Wa + F·Wota) + R/2 × (Wb + F·Wota)` = `0.1625 × R` | **813** | **4 063** |
| Physical publishes/s, native fan-out | `2R` | 10 000 | 50 000 |
| Physical publishes/s, materialised fan-out | `R × (1 + F)` = `6R` | 30 000 | 150 000 |

The lane count is Little's law on the work sleeps alone (broker RTT excluded). It is
**the** number the comparison turns on: the workload demands ~813 units of work in
flight at 5k ev/s, each of which must stay ordered within its property.

The last two rows are the fan-out cost. A system with native consumer groups
publishes each derived event **once**; a system without them must materialise one
copy per group, giving **3× the physical writes** for the same application.

---

## 3. Correctness contract

Every consumer stage appends `"<prop>,<seq>\n"` to `<logdir>/<topic>_<group>.log`
at the moment it finishes processing a message. A single shared verifier
(`internal/verify`, extracted verbatim from the July `cm.go`) then proves, per
`(file, property)`:

| Check | Definition | Verdict |
|---|---|---|
| **gaps** | any `seq` in `[baseSeq, maxSeen]` never delivered — a *higher* seq arrived, so the missing one was not merely in flight | **always FAIL** |
| **viols** | first occurrence of a lower `seq` after a higher one | **FAIL**, unless acks failed during the run (a redelivery may legitimately reorder), in which case reported not fatal |
| **dups** | the same `seq` delivered more than once to the same stream | reported; not fatal by itself, but see §3.1 |
| **inflight** | `producedMax − maxSeen`, i.e. the tail still in flight at cutoff | reported, never a failure |

The verifier is deliberately conservative: it dedups by first occurrence before
checking order, and it clamps per-property shortfall at zero so a negative term can
never cancel a genuine gap elsewhere.

### 3.1 Duplicates are a first-class result

At-least-once systems are permitted to duplicate. But **the duplicate count is part
of the result**, not a footnote: an application that must be idempotent downstream
is paying a cost the benchmark should show. Runs report dups per stream and in total.
A system that needs exactly-once configuration to reach 0 dups must be measured
**both** ways (§5.3).

### 3.2 Ground truth

`produced.meta` records, per `(flow, property)`, the highest `seq` the producer
actually assigned, plus the run's `ackErr` count and `baseSeq`. The verifier can be
re-run offline against a log directory (`-verify-only`), so results are auditable
after the fact by someone who did not run them.

---

## 4. What is measured

Sampled at 1 Hz on **both** VMs for the whole run, plus a per-second application report.

**Application (harness):** offered/s, published/s, delivered/s per stream, acked/s,
shed/s, errors by class, end-to-end latency p50/p95/p99 per flow (CO-corrected from
the scheduled instant), per-hop latency.

**Broker VM:** CPU per cgroup (broker, database, other), RSS per cgroup, disk read/write
bytes and IOPS, network both directions, plus per-system internals — for Postgres-backed
systems commits/s, WAL bytes/s, dead tuples, autovacuum activity, DB size; for Kafka
log size and flush stats; for RabbitMQ queue depth and memory alarms.

**Loader VM:** CPU, RSS, network, and the harness's own goroutine/connection counts —
present to prove the loader was never the bottleneck.

**Derived cost metrics** (the headline of §6.1):
- mCPU per 1 000 events/s, broker VM total.
- Bytes written to disk per ingress event.
- Physical publishes per ingress event.
- Peak RSS per 1 000 ordered lanes.
- Connections / consumer members / queues / partitions provisioned.

---

## 5. Fairness rules

These are the rules a reviewer should check first. Any run that breaks one is void.

### 5.1 Identical hardware and budget

Both VMs identical in size, disk class and region. The **whole** broker VM budget is
available to whatever the system under test needs there. For QueenMQ that budget is
shared by broker **and** Postgres; for pgmq the same budget is all Postgres; for Kafka
and RabbitMQ it is all theirs. This deliberately handicaps Queen and is to be stated
in every report.

The loader VM must stay under 70% CPU. A run where the loader saturates is void.

### 5.2 Equal tuning budget

Each system gets the same wall-clock tuning budget, recorded in the run log. Every
non-default setting must be listed with a one-line reason. Queen's July run had days
of tuning behind it; a comparison where only Queen is tuned is worthless. Where a
system has a domain expert available, their configuration is preferred over ours and
credited.

### 5.3 Durability is an axis, not an assumption

The four systems do **not** have the same default durability, and pretending otherwise
is the most common way these comparisons lie.

- Queen / pgmq: Postgres `synchronous_commit=on` — fsync per commit.
- Kafka: `acks=all` on a single broker is **not** fsync-per-batch; Kafka defaults to
  relying on replication, flushing lazily. Kafka is therefore run in **two** configs:
  `default` (as people actually deploy it) and `fsync` (`flush.messages=1`), and both
  are reported.
- RabbitMQ: persistent messages + publisher confirms; classic and quorum queue modes
  both run.

No headline number may compare across durability tiers without saying so in the same
sentence.

### 5.4 Dedup is an axis, not a freebie

Queen dedups by key server-side over a 300 s window. Kafka's idempotent producer
dedups only per `(producer-id, partition)` within a session — not a generic key over
a wide window. RabbitMQ and pgmq have nothing.

The **headline comparison runs with dedup off everywhere** so the core shape is
comparable.

The cost of the feature is then measured **where the feature exists**: Queen is run
dedup-on against Queen dedup-off, same hardware, same rate, and the delta is what
broker-side dedup costs.

The other three are **not** given a bolt-on dedup store to even things up. Building
a Redis or side-table deduper for Kafka would measure *our* implementation of dedup,
not Kafka's — an artefact we control, and the first thing a reviewer would attack.
Their lack of it is a **capability** fact, and it belongs in the "semantics you must
build" row of §6.1, not inside a throughput number.

So the dedup axis yields two things, and no others: a Queen on/off delta, and a
capability column stating what each system would make the application build.

### 5.5 Each system is implemented idiomatically

The adapter interface (§7) is stated at the level of the *application's* need —
ordered batch delivery per key, explicit batch ack, keyed ordered publish. How a
system satisfies it is that system's business, and the best-known idiom must be used:
Kafka gets per-partition dispatch with pause/resume (not one consumer member per lane),
pgmq gets `read_grouped_head`/`read_grouped_rr`, RabbitMQ gets a consistent-hash
exchange. Extra application code a system requires to meet the contract is itself a
result (§6.1, "semantics you must build").

**Consumption strategies must be charged symmetrically.** Several systems offer a
fast mode that buys server cost by pushing machinery into the application. If one
system's fast mode is used and charged, every system's must be — and if it is used
and *not* charged, the cost table lies.

The case that forced this rule is Queen's targeted pops. Targeted pops with static
partition ownership are far cheaper than wildcard pops (July: ~0.15 ms against
12-35 ms per pop), but they get there by making the application own a static
partition-to-worker map, which means knowing the key space up front and re-sharding
when it changes. That is Queen's counterpart of Kafka's static partition assignment,
and it trades away exactly the property Queen is claimed to have: dynamic lanes,
elastic consumers, no advance knowledge of cardinality.

Running Queen targeted against Kafka's parallel consumer would therefore compare
Queen's rigid mode with Kafka's flexible one, while flattering Queen on server cost.
So **both Queen modes are run and published as a pair**, the way Kafka's two
durability tiers and RabbitMQ's two queue types are:

| mode | what it is | charged with |
|---|---|---|
| `wildcard` (default) | the broker assigns lanes dynamically | the server-side candidate scan — a real cost of the claim, not to be hidden behind the other mode |
| `targeted` | the application owns a static partition map | the ownership map, the re-shard when the key space changes, the loss of consumer elasticity, the sweep pacing |

Neither is "the" Queen number. A single Queen figure quoted without its mode is not
a result.

### 5.6 Publish everything

Raw samples, configs, container images with digests, harness commit hash and the full
verifier output ship with any published result. Losses on any axis are published in
the same document as wins.

---

## 6. The two result tables

### 6.1 Cost to serve (primary)

Fixed `R`, fixed `P`, all systems PASS the §3 correctness contract. If a system cannot
PASS at that rate, that is its result for this table and the ceiling table tells the
rest.

| | Queen | Kafka | RabbitMQ | pgmq |
|---|---|---|---|---|
| broker-VM cores @ R | | | | |
| peak RSS | | | | |
| disk bytes written / ingress event | | | | |
| physical publishes / ingress event | | | | |
| ordered lanes provisioned (partitions / queues / groups) | | | | |
| consumer members / connections | | | | |
| e2e p50 / p99 | | | | |
| dups / gaps / order violations | | | | |
| semantics built in the app (dedup, cross-hop order, retry) | | | | |

### 6.2 Ceiling (secondary)

Highest `R` each system sustains on the same hardware **while still PASSing §3**, plus
the resource that bound it and the first symptom of breakage.

### 6.3 Cardinality sweep

`P ∈ {100, 1000, 5000}` at fixed `R`. This separates cost-per-message from
cost-per-lane and yields, per system, a two-term cost model
`mCPU ≈ a·(msg/s) + b·(lanes) + c`. For Queen this directly prices the
"1 property = 1 partition" bet.

---

## 7. Adapter contract

An adapter implements, for one system:

```
Setup(topology)                      create topics/queues/partitions/groups
Publish(topic, key, payload)         single keyed publish, order-preserving per key
PublishBatch(topic, key, payloads)   ordered batch to ONE key, atomic if supported
Consume(topic, group, handler)       deliver batches grouped by key; handler returns
                                     when the batch is processed; adapter then acks
                                     the whole batch
Stats()                              adapter-native counters for §4
Close()
```

Ordering requirement on `Consume`: within a key, batches must be delivered in `seq`
order and a key must never be concurrently in flight in two handlers of the same group.
Across keys the adapter is free — and *is expected* — to run as concurrently as the
system allows. Meeting the §2 lane count is the adapter's job; how many members,
threads, connections or queues that takes is the measurement.

---

## 8. Out of scope

Single-node only. This says nothing about HA, failover, rebalance under node loss,
multi-region, operability, ecosystem (connectors, stream processing, schema registry)
or client-library maturity — all of which are legitimate reasons to choose a broker
and none of which this measures. It is one workload shape: it does not speak for
large-payload streaming, very low cardinality at very high rate, or fan-in topologies.

---

## 9. Reference point

The July 2026 Queen run of this exact shape (32-core broker VM, 48-core loader):
R = 25 000 ev/s for 600 s, P = 1000, 88 503 408 deliveries across 12 streams,
**0 dups, 0 gaps, 0 order violations**. It is a reference for the shape, not a
baseline for this campaign — the 8-core campaign re-measures Queen from scratch on
the same harness as everyone else (§5.1).
