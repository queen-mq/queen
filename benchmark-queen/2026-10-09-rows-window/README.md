# Memory that follows the dedup window, 2026-10-09 and 10

What 2.2.0 changes in the broker's memory and log files, measured against 2.1.0 on one VM while
the Jepsen campaign for the same build ran on eleven others (`../2026-10-09-jepsen-220/`). The
runs are in `runs/`, the scripts that made them in `scripts/`.

## The machine

One DigitalOcean droplet: 16 vCPU, 31 GiB, a 200 GB volume, Ubuntu 24.04 (kernel 6.8). It was
also the Jepsen control node from 21:22Z on 2026-10-09, so from then on the broker and the load
generators of every run here were pinned to cores 6 to 15 (`taskset -c 6-15`) and the two Jepsen
controls to cores 0 to 5. The first three runs (`base-210`, `base2`, `new3`) were made before
that, on all 16 cores. One node, no replication, unless a run says otherwise: these runs compare
two builds on the same machine, they are not a capacity figure for a cluster.

The load generator is `qload`, the Queen loader of `../2026-09-30-kafka-pulsar/mqload/` (open
loop, HDR histograms); `mqgen.py` pushes to many queues, which `qload` does not do.

## The builds

| Name | Commit | What it is |
|---|---|---|
| 2.1.0 | `e90e5d4c8` | the release |
| rc0 | `e8efda8cb` | rows leave the store at the dedup window |
| rc1 | `6f18bd40f` | rc0 and the two start-up fixes Jepsen found |
| rc2 | (between) | rc1 and the read-ahead for walks over old messages |
| rc3 | `5f24bf9bb` | rc2 and small indexes held in memory |
| rc4 | `4cc97a187` | rc3 and: an aged file is sealed only for retention or its size, one map per sealed file, a refused map falls back to memory, the directory keeps merging |
| rc5 | `c51b8fcbb` | rc4 and a roll that changes nothing until the sealed index is open |
| rc6 | `3b1a6d9bb` | rc5 and a log file sealed right after a log cut created for the seq that comes next. The code of 2.2.0 |

`binary.md5` has the checksum of each. The memory and in-sync tables are of rc6. The others were
measured on rc4 or rc5, and rc6 differs from those only in what a node does when its log is cut
back, which none of these runs does.

## Memory: a queue that keeps everything

`big.sh`, rc6. One queue, 2,000 partitions, retention off, a dedup window of 60 s, pushes of ten
1 KB messages at 150,000 messages a second for 1,000 s.

| | |
|---|---|
| Written | 149,250,000 messages in 14,925,000 pushes: 99.3 GB of log in 1,478 files (push p50 8.0 ms, p99 40.7 ms) |
| Broker memory (anonymous) while pushing | 1.4 to 1.6 GB, flat: about 900,000 rows, the pushes of the last 60 s |
| 80 s after the last push | no row, 365 MB |
| Memory maps | 2,059 for the process, 1,476 of them sealed-file indexes: one per file |
| Stop, drop the page cache, start | 3.5 s to `listening`, nearly all of it opening and verifying the 1,477 indexes (about 700 MB, 48 bytes a push) |
| Memory after that start | 15 MB |
| The backlog read back from the cold disk, 16 consumers, 60 s | 21,659,000 messages: 361,000 a second, 617 MB of broker memory |
| Retention switched on | 1,476 files, 99 GB, gone in under 30 s |
| WARN or ERROR lines | 0 |

`big-rc5` is the same run on rc5: 146.6 million messages, 502 MB afterwards, a start in 3.4 s, 362,000
messages a second from the cold disk.

2.1.0 keeps one row per push for as long as the message: for this queue 14.9 million rows, about
5 GB by the figure below (346 bytes a row of ten messages), on every node, loaded again at every
start.

`cold.sh` is the same with a third of the data, on both builds, so that 2.1.0 fits (48.75 million
messages, 32.4 GB, page cache dropped before the read):

| | 2.1.0 | rc1 | rc2 | rc4 |
|---|---|---|---|---|
| Rows 80 s after the last push | 4,875,003 | 0 | 0 | 0 |
| Broker memory then | 2,014 MB | 328 MB | 320 MB | 397 MB |
| Backlog read from the cold disk, alone (msg/s, 2 s windows) | 377,000 | 312,000 | 363,000 | 356,000 |
| In-sync traffic beside that read: push p50 / p99 | 35.6 / 124 ms | | 32.6 / 96.8 ms | 31.1 / 102.9 ms |
| ...and how far its consumers were behind (e2e p50) | 10.7 s | | 6.5 s | 7.2 s |
| Backlog read in those 60 s | 8,477,000 | | 10,061,000 | 9,285,000 |
| Retention: 482 files, 32 GB | under 20 s | under 20 s | under 20 s | under 10 s |

rc1 read a record's ids and then its payload, two waits for the disk a record; from rc2 on a walk
that has waited once asks the kernel for the next 128 records whole. The in-sync figures are of a
machine that is saturated on purpose (the broker and two load generators on ten cores): they say
that the old readers do not starve the new ones more than they did.

## Consumers at the tail

`ab3.sh`, each build alone on the machine: 2.1.0 twice (`ab3-210`, `ab3-210b`) and the candidate
three times (`ab3-rc5`, `ab3-rc6`, `ab3-rc6b`), interleaved over two and a half hours. First 5.95
million single pushes into 2,000 partitions at 20,000 a second with retention off and a window of
60 s, so that there is a backlog without rows; then the phases below, with 16 consumers and pushes
of 100 messages at 300,000 messages a second. The ranges are the runs.

| | 2.1.0 | 2.2.0 (rc5, rc6) |
|---|---|---|
| The single pushes: p50 / p99 | 1.82 to 1.86 / 34.6 to 38.1 ms | 1.83 to 1.88 / 35.6 to 45.8 ms |
| Rows when they stop, and 75 s later | 5,950,000, 5,950,000 | 1,200,000 to 1,220,000, 0 |
| Broker memory at those two moments | 1.77 to 1.85 GB, 1.36 to 1.37 GB | 0.96 to 1.12 GB, 0.28 to 0.31 GB |
| The backlog read back alone, 8 s (warm page cache) | 512,000 to 535,000 msg/s | 484,000 to 506,000 msg/s |
| In-sync traffic alone, 70 s, 19.5 million messages: push p50 | 8.77 to 9.02 ms | 8.51 to 9.41 ms |
| ...push p99 / p99.9 | 41.7 / 54.0 to 57.6 ms | 38.7 to 41.2 / 49.9 to 54.0 ms |
| ...end to end p50 / p99 | 23.4 to 24.2 / 72.2 ms | 22.9 to 25.2 / 72.2 to 78.3 ms |
| In-sync traffic while the rest of the backlog is read beside it: push p50 / p99 | 9.02 to 9.79 / 64.8 to 71.2 ms | 9.28 to 10.30 / 66.1 to 74.2 ms |
| ...end to end p50 | 24.2 to 26.2 ms | 25.0 to 27.5 ms |
| Broker memory at the end | 2.98 GB | 1.47 to 1.54 GB |

Consumers at the tail are served from the rows in memory on both builds, and their numbers are
inside each other's spread from one run to the next. Reading a backlog that has no rows any more
costs about 5% while the page cache still holds it. `base2` and `new3` are the same script on
2.1.0 and rc0 before the Jepsen controls shared the machine (16 cores): push p50 6.43 and 6.82 ms
in-sync, and the backlog read at about 590,000 and 500,000 messages a second. The work on the read
path between rc0 and rc5 closed most of that gap.

## Many partitions, single pushes

`sparse.sh`. One queue, 1,000,000 partitions, single pushes round-robin over them, retention off,
a window of 60 s: every push leaves a row that a watermark of its own partition takes away a
minute later, so row expiry works at the push rate.

At 50,000 pushes a second the machine is saturated on both builds (rc4 against 2.1.0, 240 s):

| | 2.1.0 | rc4 |
|---|---|---|
| Pushed of 11.75 million offered | 11,429,254 | 11,226,635 |
| Push p50 / p99 | 39.7 / 167 ms | 53.5 / 167 ms |
| Rows when the pushes stop | 11,429,254 | 2,956,299 |
| Broker memory then, and 100 s later | 6.3 GB, 3.3 GB | 5.5 GB, 1.9 GB |
| The 11 million messages read back, 16 consumers, 100 partitions a pop | about 320,000 msg/s | about 318,000 msg/s |

At 20,000 a second it is not (rc5 against 2.1.0, 200 s, the 10 s windows after every partition
exists):

| | 2.1.0 | rc5 | rc5, scanner at 5,000 partitions/s |
|---|---|---|---|
| Push p50 | 1.85 to 2.06 ms | 1.94 to 2.35 ms | 1.90 to 2.13 ms |
| Push p99 | 27 to 49 ms, once 202 | 40 to 64 ms | 34 to 49 ms, once 71 |
| Rows at the end | 3,899,518, growing | 1,328,320, steady | 3,090,702, still rising |
| CPU | 2.6 cores | 2.6 cores | 2.5 cores |

The gap in the middle column is row expiry. With one row to expire per push the leader plans as
many watermarks as pushes, up to 1,024 of them in one planning cycle beside the clients' commands.
The last column slows the scanner that proposes them (`QUEEN_RAFT_RETENTION_SCAN_PER_S`, 50,000
partitions a second by default): the pushes are back where 2.1.0 has them, and a row then stays
up to one round of the scanner past its window, 200 s for a million partitions at 5,000 a second
(the run ended before the row count had settled). Queues with few partitions, or pushes of many
messages, have nothing of this: there is one row per push and one watermark per partition a
round.

On both builds the first pop of each of the 16 consumers answered `503 timeout`: a new group's
first pop over a million partitions takes more than its two seconds. That is not new and not
part of this work.

## Many quiet queues

`manyq2.sh` with `mqgen.py`. 500 queues, one message into each every 5 s for five minutes, the
seal age at 30 s, a twentieth of its default: a minute here is twenty minutes of such queues.

| | 2.1.0 | rc4 |
|---|---|---|
| Retention off: log files at the end | 3,630 | 501, the number it started with |
| ...memory maps of the process | 6,780 | 537 |
| ...disk used by the logs (15 MB of messages) | 33 MB | 21 MB |
| ...restart | 0.26 s | 0.15 s |
| Retention of 20 s: log files while pushing | 629 to 750 | 629 to 757 |
| ...memory maps | 791 to 1,019 | 538 |
| ...files 45 s after the last message | 506 | 501 |
| Every queue deleted: its log drains and its directory goes | yes | yes |

On 2.1.0 a queue that gets a message at least every ten minutes makes a file every ten minutes,
whether or not retention has anything to free in it, and each sealed file costs two memory maps.
`manyq.sh` shows the second half alone (`mf-210`, `mf-rc3`: one log sealed every 4 KiB): 33,834
sealed files were 67,139 maps on 2.1.0 and 1,140 on rc3. The kernel allows a process 65,530
unless `vm.max_map_count` is raised (this VM has 1,048,576, so nothing failed here).

## An exactly-once pipeline over messages without rows

`pipe.sh`, three nodes on this machine. The transactional pipeline of the Kafka comparison
(`qload -txn`): a feeder writes `vx-in` at 30,000 messages a second for 300 s, 16 workers move ten
messages a transaction to `vx-out` (one transaction acks its inputs and pushes its outputs), and
rows leave the store 20 s after a push. The workers are slower than the feeder on this machine, so
within a minute they are behind the rows and claim their input from the queue log, inside
transactions; when the feeder stops they go on until the input is empty. The verifier then reads
all of `vx-out` and the rest of `vx-in`, none of it with a row by then, and accounts for every id
the feeder wrote down.

| | 2.1.0 | rc5 |
|---|---|---|
| Produced, and found once in `vx-out` | 8,850,030 | 8,850,010 |
| Duplicates, missing, extra | 0, 0, 0 | 0, 0, 0 |
| Verdict | PASS | PASS (and PASS again on rc6: 8,849,990 produced and found once, `pipe-rc6`) |
| Claims served from the queue log, on the leader | | 653,161 |
| Workers while the feeder runs (msg/s) | 15,200 to 15,800 | 14,200 to 15,700 |
| ...transaction commit p50 / p99 | 3.8 to 4.0 / 13 to 16 ms | 3.7 to 4.3 / 12 to 22 ms |
| Workers alone, after it (msg/s) | 21,000 | 19,500 |
| ...transaction commit p50 / p99 | 2.90 / 8.9 ms | 2.93 / 10.0 ms |
| Rows on each node at the end | 1,770,406 | 0 |
| Broker memory on each node at the end | 730 to 860 MB | 110 to 250 MB |

A worker that is behind the window reads its input's ids from the log and no longer from memory,
which costs it about 7% here. One that keeps up is not on that path.

`pipe-rc5-overrun` is the first attempt, kept because its verdict reads FAIL: the feeder ran for
600 s and the workers got 60 s to finish, so 7.8 million inputs were still unprocessed. The
verifier reads that rest without acking it, one batch a partition, and so found 148,000 of them
and counted the others missing. Nothing was: no duplicate, nothing extra, and every output the
workers had made was there once. `pipe.sh` now lets the workers finish.

## A rolling upgrade

`roll.sh`, three nodes on this machine, traffic through all three for the whole roll, and a queue
with 280,000 messages written by 2.1.0 and never consumed. Run on rc3, rc4, rc5 and rc6; `roll-rc6`:

1. Two nodes move to the new build: the cluster version stays 5, the rows stay.
2. One of them goes back to 2.1.0 and starts on the directory the new build had written.
3. The last node moves: the version rises to 6 by itself, and the rows are gone on every node
   within the window.
4. The 280,000 messages are read back from the queue log: 280,000 popped, no error.
5. 2.1.0 refuses to start on the upgraded data, naming the version it cannot read.

The pushes and pops that fail during the roll (a few hundred of 74,500 pushes) are those sent to a
node while it restarts: the load generator talks to each node directly and does not retry.

## What these runs do not show

- More than one machine: every figure is one node, or three on one disk.
- The Kafka facade and the S3 sink reading messages that have no row. Both read the queue log by
  offset and never read rows (`fetch_records` and `fetch_log` in `facade/real/phase2/reads.rs`),
  but neither was run here.
- A terabyte. The largest run is 99.3 GB; files, maps and the start scale with it (about 15,000
  files and maps and half a minute of index to open per terabyte at ten 1 KB messages a push, more
  for smaller pushes: an index entry is 48 bytes a push).
- Memory at the default window. These runs use a window of 60 s to reach a steady state in
  minutes. A row lives for the queue's dedup window (3,600 s by default) or
  `QUEEN_RAFT_TXN_WINDOW_MIN_S` (900 s) if that is longer, and costs about 200 bytes a push plus
  16 bytes for each further message in it: at 20,000 single pushes a second and the default
  window that is about 14 GB.
