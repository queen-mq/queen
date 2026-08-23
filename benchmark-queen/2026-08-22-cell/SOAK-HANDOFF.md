# Soak handoff — 2026-08-22, machines destroyed, resume tomorrow

The soak never reached steady state. What follows is what was learned anyway,
what to do differently, and the exact rig to recreate.

Both VMs are gone. Scripts here are current and correct; the two in
`~/Work/queen-cloud/` (PLANS.md, plans.sql) already carry the corrections.

---

## Rig to recreate

| role | spec | notes |
|---|---|---|
| cell | 8 vCPU / 15 GiB / 484 GB | `soak-cell.sh` builds everything |
| loader | 4 vCPU / 7 GiB | 27 goload processes; marginal but adequate |

Both need the same VPC. `soak-cell.sh` handles Postgres (unpublished, generated
password, non-default port), two brokers (loopback), HAProxy active/passive
(loopback), proxy (VPC address only). `plans.sql` sets the limits. Then
`soak-run.sh <label> <seconds> <free> <dev> <pro>` from the loader.

Order that works:
1. `soak-cell.sh up` on the cell
2. apply `plans.sql`
3. `goload -mode provision` x3 (free/dev/pro), copy the JSON to the loader
4. start `/root/dbtrend.sh` (a SHIPPED file — see trap 4)
5. `CONSUMERS=2 soak-run.sh soak3d 259200 17 11 7` on the loader

---

## What was established

### The pop shape decides everything

`pop-partitions` was set to the full per-queue partition count, so one consumer
asked the broker to sweep 5 000 partitions to collect the ~5 msg/s its queue
carried. Changing it to 10, with 3 consumers and batch 100:

| | before | after |
|---|---:|---:|
| push/s | 610 (58% of target) | **1 059 (100%)** |
| ready_age p95 | 2 376 ms | **5 ms** |

**475x on rotation latency from three client flags.** The crossbench rule is
`pop-partitions ~ lanes/25`; 5 000 was 25x too wide. This is the SECOND time in
one day a client-side setting decided whether a cell looked healthy — the first
was the balancer hash key. The SDK must derive both, never default them.

### Active/passive beats hash affinity at small tenant counts

Tenant-hash over 2 brokers with 35 tenants split 28/72; the loaded broker hit
`ready_age_p95` 3 273 ms while the other sat at 85 ms.

| | total push/s | worst ready_age |
|---|---:|---:|
| 2 brokers, tenant-hash | 507 | 3 273 ms |
| **1 active + standby** | **1 058** | **56 ms** |

Hash affinity needs enough keys to balance. It worked well at 5 000 tenants in
the earlier campaign; at 35 it is 1:2 and the cell's ceiling becomes whichever
broker drew the long straw.

### The 8-core ceiling is ~35 full-quota tenants

At 1 060 msg/s over 827 000 partitions the box ran at **91% CPU** with push p99
at **193 ms** against a 200 ms SLO. That is the ceiling, not a comfortable
operating point — soak at ~25 tenants and quote 35 as the measured limit.

Postgres is **5,3–6,4 of the 8 cores** — 64-66% of everything, consistent with
every campaign this month.

### Storage constant: ~410 bytes per message

Database grew ~26-29 MB/min at 1 060 msg/s = ~37 GB/day over 91,6M messages.
PLANS.md assumed 512 B, so its storage column is ~25% pessimistic.

Projected plateaus at full quota (35 tenants): free 3,0 GB, dev 29,2 GB, pro
105 GB (byte-capped from 173 GB) = **~137 GB of 484 GB**.

**pro's 15 GB byte cap binds at ~4,3 days.** A 3-day soak never sees it. If the
point is to test that guard — and it is the one plan mechanism never exercised —
the run must be **5+ days**.

### Two product defects worth fixing

**The proxy writes to Postgres on nearly every request.** From
`pg_stat_statements`: 743 149 `INSERT INTO queen_proxy.queues` and 103 726
`UPDATE queen_proxy.api_keys SET last_used_at` against ~1 000 000 pops. That is
~850 000 control-plane writes per million operations, on the same Postgres the
data path is saturating. It is why the client saw **4 178 ms** push RTT while the
broker reported **5 ms**. `last_used_at` wants async batching; the queue upsert
wants a cache.

> **Correction (2026-08-23, from the proxy code).** The two counts are right;
> the sentence above them is not, and the fix is different from the one it
> suggests.
> * 743 149 queue upserts = `registry.admit` writing ONE synchronous row per
>   NEW (queue, partition) pair, awaited before the push is forwarded. That is
>   a partition-creation-ramp cost (827k partitions created lazily on push);
>   at steady state the fast path is a HashSet hit and the write vanishes.
> * 103 726 `last_used_at` = a thundering herd at the 30 s key-TTL expiry, not
>   "per request": the lookup had no single-flight, so every request arriving
>   while a refresh was in flight ran its own SELECT + UPDATE, the UPDATEs
>   serialised on the same `api_keys` row behind each other's fsync. 35 keys x
>   ~34 expiries in ~17 min = ~1.2k events -> ~87 writes per event.
> * Both are fixed in `proxy/` (working tree, 2026-08-23): admit is DB-free on
>   the request path (coalesced async persist, O(1) partition count), cache
>   lookups are single-flight + stale-while-revalidate, `last_used_at` is
>   batched, the pxdb pool has connect/wait timeouts (`PXDB_TIMEOUT_MS`).
> * NOT measured yet and worth checking FIRST on the rebuilt rig: with
>   `QUEEN_PROXY_RECONCILE_MS=10000` the reconciler calls
>   `GET /api/v1/resources/queues` once per cluster every 10 s, and
>   `log_queue_stats_all_v1` behind it aggregates ALL of `queen.log_segments`
>   per call (the `seg_counts` CTE has no tenant filter) -- O(clusters x
>   segments) per interval, steady-state, on the data path's Postgres. Look for
>   it in `pg_stat_statements`; set the interval back to 60 s+ for the soak.

**`log_refresh_all_stats_v1` takes 3,7 seconds per call** at 733 000 partitions
(it was ~1 s at 54 k). 187 calls = 5% of all database time, and it is a periodic
full sweep — a plausible source of the recurring multi-second `ready_age` spikes,
and it only gets worse with cardinality.

### Data path itself is fine

pop 6,93 ms, push 4,63 ms, ack 0,81 ms mean. `ack_hit_pct` 100%,
`pop_empty_pct` 0,1%. Queen's engine is not the problem anywhere in this work.

---

## Traps paid for today — do not repeat

1. **`pool="N/M"` is connections OPENED, not in use.** At `pool=208/300` only
   **4** queries were actually active (210 idle). `pool_waiting` was 0 the entire
   campaign, including when it read 160/160. Raising the pool 160 -> 300 fixed
   nothing. **Read `pool_waiting`, not the ratio.**

2. **`rates:` lines come in two kinds.** `scope="global"` is the cell; the others
   are PER QUEUE. Reading a per-queue line as the global rate made a healthy cell
   look 100x slower than it was, twice.

3. **Field names contain digits.** `grep -oE 'ready_age_p95="[0-9]+"' | grep -oE '[0-9]+'`
   returns `95` AND the value. Same for `eph_push_s` matching `push_s`. Extract
   with `sed 's/.*=//'`.

4. **Never generate a collector script through a nested heredoc.** The escaping
   turned `$(...)` into a literal and produced a header with no rows, silently,
   three times. `/root/dbtrend.sh` is now a shipped file — keep it that way, and
   note `soak-run.sh` used to overwrite it on every launch.

5. **`pkill -f <pattern>` kills your own ssh session** when the pattern appears in
   the command line. Cost two dropped sessions today. Use `p=btrend; pkill -f "d${p}"`
   or PID files. This trap is already documented in the RIG-RUNBOOK and was still
   walked into twice.

6. **Loader connection fan-out.** goload auto-sizes idle connections per tenant
   client from `max-inflight`: 414 each x ~325 clients = ~134k sockets against a
   ~55k ephemeral port range, giving "cannot assign requested address" and load
   average 229 on 4 cores. Looks exactly like a broken cell. Set `-idle-conns 8`.

7. **`-max-inflight` converts latency into shed load.** At 64 with a 4 s RTT the
   pacer shed half the offered rate, so the cell appeared to be serving 570/s of
   a 1 060/s target when it was never asked for the rest. Watch `shed=` and
   `inflight=` before concluding anything about capacity.

8. **Partitions are created lazily on push**, ~25 000/min, so 827 000 takes ~20
   minutes during which the cell is provisioning AND serving. Nothing measured in
   that window is steady state. `SKIP_CONFIGURE=1` does not help — it skips queue
   config, not partition creation.

---

## What tomorrow should do

1. **Rebuild, then wait out partition creation** before reading any number.
2. **Soak at ~25 full-quota tenants**, not 35 — leave headroom for the storage
   growth and the stats-refresh spikes.
3. **Run 5+ days** if the pro byte cap is to be tested; 3 days only reaches free
   and dev retention.
4. **Consider fixing the two proxy writes first** — they are ~850k writes per
   million operations and would change the capacity number materially. Measuring
   a cell that does avoidable work per request measures the work, not the cell.
5. Still never measured: **bloat and autovacuum at steady state**, aged dormant
   partitions, and any failure injection.
