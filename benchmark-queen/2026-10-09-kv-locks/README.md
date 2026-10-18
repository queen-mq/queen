# KV and Locks under load, 2026-10-09

The first load test of KV and of Locks on Queen 2. KV had been measured once, on 1.0.4 over
PostgreSQL (2026-08-20, `git show 5f9ca967c^:benchmark-queen/2026-08-20-kv/results.md`). Locks, new
in 2.1.0, had only been through Jepsen, which checks safety at 200 to 400 operations a second.

All of it ran on ONE virtual machine, loader included, against ONE node. The numbers are what a
16-core node does when it serves nothing else. A cluster was not measured.

## What ran

- **Build:** the released image `ghcr.io/queen-mq/queen:2.1.0` (amd64 manifest `sha256:244efb35…`).
  The binary was copied out of the image and run on the host, not in Docker; its checksum is in
  `binary.md5`.
- **Machine:** one DigitalOcean droplet in fra1 (`<host-40>`): 16 vCPU Intel Xeon Gold 6548N,
  31 GiB, a 200 GB disk (ext4), Ubuntu 24.04.5, kernel 6.8.0-142. A 4 KiB write with its fsync takes
  0.57 ms on that disk, 1.24 ms at p99 (`runs/fio-fsync.json`).
- **Broker:** code defaults, with two exceptions. It listens on 127.0.0.1 only, because it has no
  authentication. And the four KV rate limits are raised out of the way, because at their defaults
  the test measures the limiter ([below](#found-on-the-way)).
- **Loader:** `kvlockload/`, on the same machine over loopback: plain HTTP, no SDK, one keep-alive
  connection per worker. Closed loop: a connection sends its next call when the last one is
  answered. The points called paced are open loop: calls start on a fixed schedule and latency
  counts from the scheduled time.
- **A point:** a fresh broker on an empty directory, 5 seconds that are not measured, then 30
  seconds, or the length its table says. Where a table says the broker was kept, its points share
  one broker.
- **Read per point:** the loader's rates, a latency histogram of every call and every error by
  kind; and over the same window the broker's CPU by thread, its own metrics and the disk's writes
  (`broker.txt` in each run).
- **Topology:** one node, which is a single voter that fsyncs every write before it answers.

<!-- T:totals -->
94 points, the first started at 09:46Z and the last at 10:57Z. Calls that failed, over all of them: 0. Acquisitions under contention: 2,630,831; with two holders at once: 0; with a token lower than one seen before: 0.
<!-- /T -->

In the tables, "broker cores" is the CPU the broker used and "idle cores" what the machine had left
of its 16, the loader's share taken out. Latency is of one call, whatever it carries.

## KV

The mix is the one the 1.0.4 campaign used: 60% `incr` (a rate limiter's decision), 30% `get`, 10%
`putIfAbsent` (an idempotency marker), over 100 namespaces of 20,000 keys, every write with a 60
second lifetime.

**One operation per call, each point on a fresh broker, 30 s**

<!-- T:kv-fresh -->
|  | connections | ops/s | p50 ms | p99 ms | p99.9 ms | broker cores | idle cores |
|---|--:|--:|--:|--:|--:|--:|--:|
| mix | 16 | 29,456 | 0.48 | 2.88 | 12.6 | 3.4 | 10.7 |
| mix | 64 | 49,955 | 1.04 | 10.9 | 21.3 | 4.8 | 8.9 |
| mix | 256 | 66,043 | 3.32 | 18.2 | 39.9 | 6.3 | 7.4 |
| mix | 1,024 | 78,336 | 14.0 | 40.2 | 70.8 | 7.9 | 5.8 |
| mix | 4,096 | 92,644 | 50.7 | 99.4 | 122 | 9.6 | 4.0 |
| `incr` only | 64 | 37,400 | 1.22 | 11.9 | 25.1 | 3.9 | 10.1 |
| `incr` only | 256 | 49,392 | 3.93 | 20.2 | 51.2 | 5.1 | 8.9 |
| `incr` only | 1,024 | 57,396 | 16.6 | 44.2 | 71.0 | 6.3 | 7.7 |
| `putIfAbsent` only | 64 | 47,834 | 1.04 | 8.86 | 19.7 | 4.7 | 9.2 |
| `putIfAbsent` only | 256 | 67,118 | 3.18 | 13.6 | 29.9 | 6.3 | 7.5 |
| `putIfAbsent` only | 1,024 | 76,198 | 12.6 | 28.8 | 57.0 | 7.5 | 6.4 |
<!-- /T -->

**The broker kept: the mix for 120 s first, then 60 s a point**

<!-- T:kv-steady -->
|  | connections | ops/s | p50 ms | p99 ms | p99.9 ms | broker cores | idle cores |
|---|--:|--:|--:|--:|--:|--:|--:|
| mix, 1 per call | 16 | 20,528 | 0.52 | 11.0 | 19.2 | 2.8 | 11.4 |
| mix, 1 per call | 64 | 32,233 | 1.23 | 16.0 | 27.0 | 3.7 | 10.3 |
| mix, 1 per call | 256 | 44,088 | 4.27 | 23.7 | 47.6 | 4.9 | 9.0 |
| mix, 1 per call | 1,024 | 64,423 | 17.2 | 55.5 | 85.4 | 7.2 | 6.6 |
| mix, 25 per call | 64 | 99,897 | 13.5 | 50.9 | 81.2 | 2.7 | 12.0 |
| mix, 25 per call | 256 | 126,875 | 49.7 | 70.7 | 121 | 3.1 | 11.7 |
| `incr` only | 64 | 21,928 | 1.52 | 17.8 | 28.9 | 2.9 | 11.2 |
| `incr` only | 256 | 31,863 | 5.18 | 30.9 | 55.3 | 4.0 | 10.1 |
<!-- /T -->

A fresh broker flatters KV writes. Its store starts empty, and while the tree is small every write
is cheap: the 256 connections that average 66,000 operations a second over their first 30 seconds
do 44,000 once every key exists and the first ones have begun to expire. Read the kept broker as
what a node sustains and the fresh one as its best case.

With one operation per call a node sustains 20,500 operations a second on 16 connections and 64,400
on 1,024. The median follows the connections (0.5 ms, 1.2 ms, 4.3 ms, 17 ms) and p99 never goes
under 11 ms.

**At a fixed pace, on that kept broker, 60 s**

<!-- T:kv-paced -->
|  | ops/s carried | p50 ms | p90 ms | p99 ms | p99.9 ms | furthest behind schedule, ms | broker cores |
|---|--:|--:|--:|--:|--:|--:|--:|
| mix offered at 5,000/s | 5,000 | 0.51 | 8.55 | 35.7 | 47.8 | 0.00 | 0.9 |
| mix offered at 20,000/s | 20,000 | 0.56 | 14.5 | 24.9 | 42.3 | 25.7 | 2.9 |
| mix offered at 40,000/s | 40,000 | 1.99 | 40.2 | 123 | 157 | 155 | 4.8 |
<!-- /T -->

Closed-loop latency hides stalls, because a stalled broker is sent nothing. Paced calls show them.
At 5,000 operations a second, a quarter of what 16 connections carry, the median is half a
millisecond, one call in ten takes over 8.5 ms and p99 is 36 ms. At 40,000 a second the node still
carries the load, with p99 at 123 ms and calls up to 155 ms behind their schedule.
[The disk](#the-disk) has where that tail probably comes from.

**Several operations per call, fresh broker, 30 s**

<!-- T:kv-batch -->
|  | connections | ops/s | p50 ms | p99 ms | p99.9 ms | broker cores | idle cores |
|---|--:|--:|--:|--:|--:|--:|--:|
| mix, 5 per call | 64 | 84,878 | 2.23 | 19.7 | 51.4 | 3.0 | 11.4 |
| mix, 25 per call | 16 | 97,664 | 2.34 | 18.4 | 51.9 | 2.1 | 12.4 |
| mix, 25 per call | 64 | 129,957 | 10.2 | 45.5 | 91.8 | 2.8 | 12.0 |
| mix, 25 per call | 256 | 157,035 | 39.0 | 112 | 147 | 3.2 | 11.5 |
| mix, 25 per call | 1,024 | 159,856 | 158 | 213 | 239 | 3.4 | 11.4 |
| mix, 100 per call | 64 | 158,663 | 38.5 | 106 | 162 | 2.9 | 11.9 |
| `incr`, 100 per call, no expiry | 64 | 216,662 | 26.1 | 88.5 | 158 | 3.3 | 11.7 |
<!-- /T -->

A call's operations are planned as one command and logged as one entry, so the call size is the
lever. On 64 connections, 5 per call gives 85,000 operations a second where 1 per call gives
50,000, and 25 per call gives 130,000. The mix stops near 160,000 a second whatever the call size
and the connections, with 11 cores idle ([what bounds it](#what-bounds-it)). A call's latency
grows with what it carries: 10 ms at the median for 25 operations on 64 connections.

**Reads, of 2 million counters written first**

<!-- T:kv-reads -->
|  | connections | ops/s | p50 ms | p99 ms | p99.9 ms | broker cores | idle cores |
|---|--:|--:|--:|--:|--:|--:|--:|
| `get`, 1 per call | 64 | 153,320 | 0.39 | 0.98 | 1.48 | 9.3 | 3.4 |
| `get`, 1 per call | 256 | 146,154 | 1.67 | 3.84 | 5.01 | 8.9 | 3.8 |
| `get`, 1 per call | 1,024 | 136,940 | 6.60 | 20.9 | 27.2 | 8.9 | 3.9 |
| `get`, 25 per call | 256 | 1,323,317 | 4.62 | 10.4 | 13.1 | 12.9 | 1.5 |
| `get`, 1 per call, from 7 million rows | 256 | 143,060 | 1.70 | 4.03 | 5.80 | 9.0 | 3.8 |
<!-- /T -->

A call that only reads is not logged and never meets the planner: it waits until the node has
applied what the cluster had committed, then reads the node's own store. One node answers about
150,000 single `get`s a second in 0.4 ms, and 1.3 million a second with 25 per call. The size of
the store does not show: 143,000 a second from 7 million rows.

The single `get`s stay near 150,000 a second however many connections ask, with 9 broker cores
busy and 3 to 4 of the machine's 16 still idle. What holds them there was not looked into.

**Writes by value size and by key space, 256 connections unless said, fresh broker, 30 s**

<!-- T:kv-shapes -->
|  | connections | ops/s | p50 ms | p99 ms | p99.9 ms | broker cores | idle cores |
|---|--:|--:|--:|--:|--:|--:|--:|
| `put` of a 16 B value | 256 | 47,289 | 3.98 | 23.7 | 47.0 | 5.0 | 9.0 |
| `put` of a 1 KiB value | 256 | 27,462 | 6.40 | 31.7 | 90.8 | 3.5 | 10.5 |
| `put` of a 16 KiB value | 256 | 11,385 | 17.3 | 203 | 261 | 3.4 | 10.9 |
| mix over 10 million keys, 7 million there | 256 | 54,376 | 3.62 | 21.9 | 32.1 | 5.5 | 8.3 |
| mix over 2.7 million expired keys | 256 | 55,559 | 3.75 | 19.7 | 29.6 | 5.7 | 8.1 |
| mix over 1,000 keys | 64 | 66,767 | 0.93 | 3.34 | 8.34 | 5.9 | 7.7 |
| mix over 1,000 keys | 256 | 96,147 | 2.77 | 6.68 | 14.2 | 8.3 | 5.3 |
| mix over 1,000 keys | 1,024 | 100,088 | 11.2 | 22.2 | 41.3 | 8.7 | 4.8 |
| `incr` of ONE key | 16 | 23,746 | 0.52 | 3.03 | 7.11 | 2.8 | 11.4 |
| `incr` of ONE key | 64 | 56,008 | 0.98 | 3.45 | 9.48 | 5.3 | 8.6 |
| `incr` of ONE key | 256 | 82,306 | 2.88 | 7.01 | 16.0 | 7.4 | 6.3 |
<!-- /T -->

- **The key space moves KV more than anything else here.** Over 1,000 keys the mix does 96,000
  operations a second at 256 connections with p99 at 6.7 ms. Over 2 million keys it does 66,000 on
  a fresh broker and 44,000 on a kept one, with p99 at 18 and 24 ms.
- **One hot key is not slower.** One counter that every caller increments does 82,000 a second:
  the planner is serial anyway, so sharing a key costs nothing more.
- **Value size.** 47,300 puts a second of 16 bytes, 27,500 of 1 KiB and 11,400 of 16 KiB, which is
  187 MB of values a second with p99 at 203 ms.
- **Rows already there.** A store that holds 7 million rows, or 2.7 million expired ones, does
  54,400 and 55,600 a second in its next 30 seconds, against 66,000 on an empty one.

### Against 1.0.4

All at 64 connections, except where said.

| | 1.0.4 on PostgreSQL, 2026-08-20 | 2.1.0, fresh broker | 2.1.0, broker kept |
|---|---|---|---|
| mix, 1 per call | 37,415 ops/s, p50 1.69 ms, p99 3.51 ms | 49,955, 1.04 ms, 10.9 ms | 32,233, 1.23 ms, 16.0 ms |
| `incr` only | 32,983, 1.82 ms, 3.42 ms | 37,400, 1.22 ms, 11.9 ms | 21,928, 1.52 ms, 17.8 ms |
| mix, 25 per call | 153,930, 4.93 ms, 9.27 ms (32 connections) | 129,957, 10.2 ms, 45.5 ms | 99,897, 13.5 ms, 50.9 ms |
| CPU | PostgreSQL 12.5 to 13.2 cores, Queen 1.5 to 3.2 | 2.8 to 4.8 cores | 2.7 to 3.7 cores |

On a kept broker, 2.1.0 carries less than 1.0.4 did at the same connections, with a p99 four to
five times longer, on about a fifth of the CPU. The machines differ: 1.0.4 ran on a 32-core VM with
PostgreSQL beside it and the loader on a second VM, for 120 seconds from an empty table. Read this
as the same order of magnitude, not as percentages. The 1.0.4 protocol was not repeated on this
machine.

## Locks

A lock is one KV row, so `acquire` and `release` are one KV write each and `renew` is a read and a
write.

**Acquire then release, nobody else on the lock**

<!-- T:lock-cycle -->
|  | connections | locks per call | acquire + release pairs/s | acquire p50 ms | p99 ms | release p50 ms | p99 ms | broker cores | idle cores |
|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|
|  | 16 | 1 | 12,043 | 0.54 | 2.75 | 0.53 | 2.72 | 3.0 | 11.1 |
|  | 64 | 1 | 26,130 | 1.08 | 3.47 | 1.07 | 3.49 | 5.3 | 8.5 |
|  | 256 | 1 | 37,185 | 3.23 | 7.38 | 3.22 | 7.35 | 7.1 | 6.5 |
|  | 1,024 | 1 | 37,573 | 13.1 | 24.1 | 13.1 | 24.1 | 7.3 | 6.4 |
|  | 64 | 16 | 140,410 | 3.27 | 9.10 | 3.20 | 9.00 | 4.8 | 9.4 |
|  | 64 | 64 | 205,643 | 9.67 | 22.3 | 9.38 | 23.4 | 5.0 | 9.1 |
<!-- /T -->

An acquire or a release takes half a millisecond on a quiet node and one millisecond at 26,000
pairs a second. One node tops out near 37,000 pairs a second, which is 75,000 lock operations, with
one lock per call. A call may carry up to 64 locks, and the same node then does 206,000 pairs a
second.

**Renewals**

<!-- T:lock-renew -->
|  | connections | ops/s | p50 ms | p99 ms | p99.9 ms | broker cores | idle cores |
|---|--:|--:|--:|--:|--:|--:|--:|
| renew, 1 lock per call | 64 | 41,175 | 1.37 | 4.58 | 11.8 | 6.2 | 7.7 |
| renew, 1 lock per call | 256 | 53,731 | 4.54 | 10.1 | 20.0 | 8.1 | 5.8 |
| renew, 1 lock per call | 1,024 | 54,807 | 18.1 | 32.6 | 42.6 | 8.1 | 5.9 |
| renew, 64 locks per call | 64 | 210,812 | 18.2 | 53.5 | 70.0 | 5.1 | 9.4 |
<!-- /T -->

**A fleet of holders, each lock renewed every 10 s (a third of a 30 s lifetime), 60 s, paced**

<!-- T:lock-fleet -->
|  | ops/s carried | p50 ms | p90 ms | p99 ms | p99.9 ms | furthest behind schedule, ms | broker cores |
|---|--:|--:|--:|--:|--:|--:|--:|
| 10,000 locks held | 1,000 | 0.94 | 1.44 | 3.40 | 8.68 | 0.00 | 0.2 |
| 100,000 locks held | 10,000 | 0.54 | 1.65 | 14.7 | 32.5 | 1.51 | 2.2 |
<!-- /T -->

The SDKs renew each lock by itself, every third of its lifetime. With 30 second locks, 100,000
held locks are 10,000 renewals a second: 2.2 cores of the broker, half a millisecond at the median,
15 ms at p99, none lost. The ceiling of 54,000 renewals a second is 540,000 such locks.

**One name fought over**

<!-- T:lock-contend -->
|  | acquired/s | wait p50 ms | wait p99 ms | longest wait ms | acquire calls/s | answered `held`/s | answered `contended`/s | call p50 ms | broker cores |
|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| 1 lock, 2 workers, retry at once | 2,347 | 0.49 | 1.93 | 44.0 | 5,031 | 2,685 | 0 | 0.15 | 1.1 |
| 1 lock, 16 workers, retry at once | 1,743 | 5.97 | 39.2 | 99.1 | 36,848 | 35,105 | 0 | 0.41 | 3.5 |
| 1 lock, 64 workers, retry at once | 971 | 44.7 | 299 | 695 | 79,218 | 78,245 | 0 | 0.77 | 6.6 |
| 100 locks, 16 workers each, retry at once | 5,463 | 187 | 1,254 | 3,614 | 88,281 | 82,771 | 0 | 16.4 | 8.3 |
| 1 lock, 8 workers, the SDKs' wait, held 20 ms | 47 | 0.37 | 1.55 | 32,556 | 56 | 9 | 0 | 0.36 | 0.0 |
| 100 locks, 8 workers each, the SDKs' wait, held 20 ms | 4,547 | 0.64 | 6.32 | 56,193 | 5,510 | 951 | 0 | 0.60 | 1.3 |
<!-- /T -->

"Retry at once" is a waiter that asks again the moment it is refused. No SDK does that; it shows
what one lock can do and what a crowd costs. Two workers pass one lock back and forth 2,347 times a
second. More waiters make it slower: with 64 of them the lock changes hands 971 times a second
while the broker answers 78,000 refusals a second. A refused acquire is planned and not logged, so
it costs CPU and no fsync.

The SDKs wait 100 ms after a refusal and half as long again each time, up to one second. With that
wait, and a holder that takes the lock again as soon as it has released it, the lock stayed with
whoever had it. Of the 2,828 acquisitions of the one lock in 60 seconds, 99% waited under 2 ms,
which is the same worker coming back; the seven others shared the rest, and one of them waited
32.6 seconds. With 100 locks and 8 workers on each, one worker waited 56 of the 60 seconds. The
lock is free for one round trip, about 0.4 ms, between a release and its holder's next acquire,
and a waiter looks once a second. The locks page says "no fairness and no wake-up"; this is how
much.

**Semaphores**

<!-- T:sem -->
|  | acquired/s | wait p50 ms | wait p99 ms | longest wait ms | acquire calls/s | answered `held`/s | answered `contended`/s | call p50 ms | broker cores |
|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| 64 semaphores of 8, 4 workers each: never full | 31,438 | 4.08 | 10.8 | 69.1 | 31,439 | 0 | 2 | 4.08 | 8.4 |
| 8 semaphores of 64, 32 workers each: never full | 22,197 | 5.79 | 17.2 | 69.3 | 22,210 | 0 | 13 | 5.78 | 8.9 |
| 1 semaphore of 1,024, 256 workers: never full | 5,487 | 24.5 | 58.7 | 124 | 5,487 | 0 | 0 | 24.5 | 10.0 |
| 1 semaphore of 8, 32 workers, held 2 ms | 2,382 | 0.84 | 88.9 | 302 | 6,959 | 4,427 | 149 | 0.51 | 1.9 |
| 1 semaphore of 64, 256 workers, held 2 ms | 4,655 | 32.7 | 205 | 463 | 9,218 | 5 | 4,555 | 23.5 | 10.2 |
<!-- /T -->

A semaphore's acquire reads the slots, picks a free one and writes it, so it costs more than a
lock's, and more the larger the limit: 31,400 acquires a second on semaphores of 8, 22,200 on
semaphores of 64 and 5,500 on one of 1,024, where the broker spends 10 cores reading 1,024 rows
for every acquire.

A crowded semaphore does poorly. With 256 workers on 64 permits, each held 2 ms, half of the
acquire calls answered `contended` (the call lost four slots in a row to other workers) and a call
took 23.5 ms at the median. 4,655 permits a second were granted, with about 38 of the 64 held at
any moment, on 10 broker cores.

In every run of the last two tables the loader counted the holders of each permit itself. No
permit ever had two holders, and no token was lower than one seen before on the same permit (the
totals are under [What ran](#what-ran)).

## What a guard costs

A guard is a `check` of the lock's row, carried beside the write it protects.

<!-- T:guard -->
|  | 64 connections: per s | p50 ms | planner thread, cores | 256 connections: per s | p50 ms | planner thread, cores | 1,024 connections: per s | p50 ms | planner thread, cores |
|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| KV put | 49,438 | 1.12 | 0.33 | 65,927 | 3.47 | 0.35 | 64,783 | 14.9 | 0.36 |
| KV put behind a guard | 48,055 | 1.17 | 0.37 | 61,962 | 3.70 | 0.39 | 60,248 | 16.1 | 0.40 |
| transaction: 1 push | 44,309 | 1.26 | 0.20 | 70,695 | 3.23 | 0.16 | 87,284 | 11.0 | 0.15 |
| transaction: 1 push + guard | 44,677 | 1.21 | 0.51 | 55,601 | 4.23 | 0.83 | 36,691 | 28.6 | 0.93 |
| transaction: 1 push + KV put | 38,431 | 1.41 | 0.57 | 34,120 | 7.16 | 0.89 | 25,855 | 37.9 | 0.92 |
| transaction: 1 push + KV put + guard | 36,723 | 1.48 | 0.57 | 32,752 | 7.43 | 0.89 | 25,051 | 41.4 | 0.92 |
<!-- /T -->

- **On a KV call** a guard costs 3 to 7%: 61,962 puts a second against 65,927 at 256 connections.
  Two runs of one configuration differ by about 5% here, so this is near what the runs resolve.
- **On a transaction that only pushes**, the guard changes who plans it. Pushes alone are planned
  on the lanes, in parallel. A transaction with a guard, or with any KV operation, is planned by
  the control step, one at a time. At 64 connections that does not show: 44,700 a second against
  44,300. At 256 the guarded transaction does 55,600 a second against 70,700, with the planner
  thread at 0.83 of a core. At 1,024 it falls to 36,700 with the planner at 0.93, while the plain
  one reaches 87,300.
- **A KV put in the transaction** costs more than a guard: 34,100 a second at 256 connections,
  25,900 at 1,024.

So a step guarded by a lock tops out near 55,000 a second on this node, and an overloaded control
step does less, not the same.

## What bounds it

A write passes three stages, each a single loop or thread. The planning loop drains the waiting
commands, plans them and proposes one log entry. The log writer appends the entry and fsyncs it.
Apply writes its effects into the store. Once a second the store's checkpoint makes what was
applied durable. "Busy" is the share of each second a stage spent working, from the broker's own
histograms (`queen_raft_drain_to_propose_seconds`, `_log_fsync_`, `_apply_entry_`,
`_checkpoint_write_`).

<!-- T:bounds -->
|  | connections | ops/s | planning loop busy | of which planner thread CPU, cores | log writer busy | apply thread busy | store checkpoint busy | disk MB/s | idle cores |
|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| mix, 1 per call | 16 | 29,456 | 52% | 0.30 | 94% | 21% | 22% | 99 | 10.7 |
| mix, 1 per call | 256 | 66,043 | 57% | 0.32 | 64% | 52% | 62% | 186 | 7.4 |
| mix, 1 per call | 1,024 | 78,336 | 73% | 0.35 | 50% | 56% | 73% | 204 | 5.8 |
| mix, 1 per call | 4,096 | 92,644 | 94% | 0.43 | 44% | 63% | 76% | 208 | 4.0 |
| mix, 25 per call | 256 | 157,035 | 49% | 0.46 | 61% | 92% | 89% | 289 | 11.5 |
| mix, 25 per call | 1,024 | 159,856 | 54% | 0.52 | 43% | 98% | 84% | 295 | 11.4 |
| lock pairs, 1 per call | 1,024 | 75,146 | 85% | 0.27 | 21% | 36% | 11% | 37 | 6.4 |
| lock pairs, 64 per call | 64 | 411,285 | 73% | 0.65 | 89% | 81% | 8% | 88 | 9.1 |
| transaction: 1 push | 1,024 | 87,284 | 83% | 0.15 | 78% | 42% | 24% | 78 | 6.7 |
| transaction: 1 push + guard | 1,024 | 36,691 | 98% | 0.93 | 28% | 8% | 10% | 36 | 9.9 |
| transaction: 1 push + KV put | 1,024 | 25,855 | 98% | 0.92 | 29% | 23% | 14% | 48 | 10.9 |
<!-- /T -->

- **One operation per call: the planning loop.** At 4,096 connections it is busy 94% of the time,
  about 14 µs a command, under half of that CPU of the planner thread. That makes 65,000 to 90,000
  commands a second on this CPU whatever the cores, and 4 to 7 of the 16 were idle. More
  connections only pack more commands into each entry, which is why the rate climbs slowly and the
  latency fast. On a quiet node the log writer is the busy stage: 94% at 16 connections, with
  6,200 fsyncs a second.
- **Several operations per call: apply.** With 25 per call the apply thread is busy 92 to 98% of
  the time and the mix stops near 160,000 operations a second.
- **A transaction with a guard or a KV operation: the control step**, which is the planner thread
  itself: 0.93 of a core and a loop 98% busy at 36,700 guarded transactions a second.
- **A read: none of them.**

KV and lock commands are cheap for that loop. Before this run we expected them near the 5,000 to
9,000 a second of the [transaction benchmark](https://queenmq.com/benchmarks/transactions/), which
is bound by the same thread. They cost it about 14 µs each; the ack-and-push transaction measured
there cost about 70 µs.

### The disk

<!-- T:disk -->
|  | connections | ops/s | planning loop busy | of which planner thread CPU, cores | log writer busy | apply thread busy | store checkpoint busy | disk MB/s | idle cores |
|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| mix over 2,000,000 keys, fresh broker | 256 | 66,043 | 57% | 0.32 | 64% | 52% | 62% | 186 | 7.4 |
| the same, broker kept | 256 | 44,088 | 42% | 0.26 | 75% | 46% | 97% | 200 | 9.0 |
| the same, broker kept, paced at 5,000/s | 512 | 5,000 | 14% | 0.09 | 44% | 9% | 19% | 54 | 14.1 |
| mix over 1,000 keys, fresh broker | 256 | 96,147 | 77% | 0.32 | 50% | 53% | 10% | 38 | 5.3 |
<!-- /T -->

The store is a copy-on-write tree: an update rewrites its 4 KiB leaf and the pages above it, and
the checkpoint writes those pages once a second. The mix over 2 million keys puts about 200 MB a
second on the disk for 31,000 to 46,000 writes of some hundred bytes each, and still 54 MB a second
for 3,500 writes a second: 4 to 15 KiB a write. On the kept broker the checkpoint is writing 97%
of the time. Over 1,000 keys the same mix writes 38 MB a second, the checkpoint is busy 10% of the
time, and the node does twice the operations with a p99 a third as long.

The log's fsync shares that disk. Its median is 0.13 ms and its p99 is in the 4 to 8 ms bucket of
the broker's own histogram (`log_fsync_us_p99` in the `rsm timing` log line), and a slow fsync
holds every entry behind it. That is the likely source of a p99 that never goes under 11 ms on the
kept broker, and of the paced tail. It was not isolated: the run with the data directory in RAM,
which would show it, was not done.

## Found on the way

1. **At the default limits a broker without tenants does 100 KV writes a second.** Such a broker
   is one tenant, and a tenant's KV calls are limited to 200 reads and 100 writes a second on each
   node (`QUEEN_KV_READ_RATE`, `QUEEN_KV_WRITE_RATE`; the limits page lists them). In a 10 second
   run at the defaults before the campaign, which was not archived, the mix did 300 operations a
   second, 100 of them writes, and 14,784 calls were answered 429 `rate_limited`. The locks page
   says a lock counts against the same write rate, which makes 100 lock operations a second; that
   was not measured. Every number above was taken with the four limits raised.
2. **Expired keys leave at 513 a second.** After 2.88 million markers with a 5 second lifetime,
   written in 60 seconds, 2.78 million were still in the store a minute later, and they left at
   513 a second (`runs/expiry-namespace-counts.txt`). That is 512 rows a step and one step a
   second, the defaults of `QUEEN_RAFT_KV_SWEEP_LIMIT` and `QUEEN_RAFT_KV_SWEEP_MS`. This backlog
   takes 89 minutes to clear, and a workload that keeps writing more than 512 keys with a lifetime
   a second never clears its own. Reads stay right, since an expired key reads as absent; what
   grows is the store. The two settings are not in the configuration reference, and raising them
   was not tried.
3. **The gauges of that backlog read zero.** `queen_kv_expired_not_pruned`,
   `queen_kv_expired_not_pruned_capped` and `queen_kv_expiry_lag_seconds` stayed at 0 with 2.7
   million expired rows in the store. Nothing sets them in 2.x: `set_kv_expiry` in
   `server/src/metrics.rs` is called by one test.
4. **A holder that comes straight back keeps its lock.** Waiters on the SDKs' wait got it after
   seconds to tens of seconds ([one name fought over](#locks)).
5. **Waiters that do not wait are paid by the planning loop.** With 100 locks and 16 retrying
   workers on each, the loop is 81% busy answering 83,000 refusals a second and a refusal takes
   16 ms.
6. **A crowded semaphore answers `contended`** on half of its acquire calls
   ([semaphores](#locks)).

## What this does not show

- **A cluster.** A three-voter phase was started and stopped before its first point finished.
  Replication adds a network round trip and a second fsync to every write, and none of that is in
  these numbers.
- **A broker with other work.** KV and locks share the planning loop with pushes, acks and pops.
- **A day.** A point lasts 30 to 120 seconds and the whole campaign 73 minutes.
- **The cause of the tail.** No run with the data directory in RAM, and no profile.
- **A network.** Everything went over loopback: add the round trip.
- **The SDKs, tenants, the proxy, JWT.** The loader speaks plain HTTP to an open broker.
- **Three checks that were planned and not run:** locks at the default limits, the expiry sweep
  with its limit raised, and the 1.0.4 protocol repeated on this machine.

## Files

| Path | Contents |
|---|---|
| `kvlockload/` | the loader (Go, standard library only) |
| `harness/` | the scripts that ran on the VM, and `report.py`, which fills every table above from `runs/` |
| `runs/<topology>/<point>/` | `load.txt` (the loader's output and its `[json]` summary), `broker.txt` (the broker's CPU by thread, its metrics and the disk over the measured window), `env.txt`, `broker-warnings.txt` |
| `runs/matrix-<phase>.log` | the points of each phase as they ran |
| `runs/fio-fsync.json` | the disk's fsync, measured before the first point |
| `runs/expiry-namespace-counts.txt` | the expired keys left on the broker, counted three times |
| `binary.md5` | checksum of the tested binary |

The two counter snapshots behind each `broker.txt` (`before.json` and `after.json`, 50 KB each)
were left on the VM.
