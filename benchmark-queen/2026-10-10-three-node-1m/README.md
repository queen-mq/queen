# 2.2.0 against 2.1.0 on three machines, at a million messages a second, 2026-10-10

2.2.0 removes a push's row from memory when the queue's dedup window has passed, whatever the
retention ([`../2026-10-09-rows-window/`](../2026-10-09-rows-window/) measured it on one VM). Each
removed row is one more small entry that every node applies, and in the three-machine runs of
September the apply thread of the followers was the limit. These runs ask whether that new work
costs anything on three machines at the rate of the published headline.

It does not cost throughput: every run below kept 1,000,000 messages a second in and out for five
minutes with nothing shed and no error. With retention off, the default, 2.2.0 spends about eight
more points of the apply thread on each node and keeps its memory flat, where 2.1.0 grows by about
1.7 GB a minute.

## The machines

Six DigitalOcean droplets in one private network, each 16 vCPU and 31 GB, Ubuntu 24.04 (kernel
6.8), 191 GB free on an ext4 root disk. Three run a broker each and three run the load.

| Role | CPU |
|---|---|
| Broker n1, the leader in every run | Xeon Gold 6548N |
| Brokers n2 and n3 | Xeon Platinum 8358 |
| The three loaders | Xeon Platinum 8358 |

The kernel settings are `scripts/os-tune.sh`, the one of
[`../2026-09-30-kafka-pulsar/`](../2026-09-30-kafka-pulsar/), and chrony keeps the six clocks
together, because end-to-end latency is the consumer's clock minus the producer's.

## The builds

| Build | Binary (`binary.md5`) | What it is |
|---|---|---|
| 2.1.0 | `e67a2c1e...` | the released binary |
| 2.2.0 | `30b710d2...` | built from the server tree of `8f5342d29`, the code of `2.2.0-beta.3` |

Both run with the code's defaults and one setting, `QUEEN_RAFT_TXN_WINDOW_MIN_S=60`, so that a
five-minute run is past the window after its first minute. The default is 900 s.

## The load

One queue, `bench`, with 500,000 partitions that exist before the run (`qload -create-only
-warm`), and `dedupWindowSeconds` 60. Nine `qload` processes, three per loader, each offering
111,111 messages a second in an open loop: a unit is 100 messages of 256 bytes for one partition,
and the partitions are taken round robin. Each process also runs 33 consumers (297 in all) that
pop up to 1,000 messages from up to ten partitions, with a long poll of 2 s and asynchronous acks.
Process j talks to broker j modulo three, so two thirds of the requests reach a follower and are
forwarded. The nine start on one instant. A run is 300 s with a 10 s ramp, so the steady window is
290 s.

A unit is one push, so it leaves one row: 10,000 rows a second. A row of 100 messages weighed
about 1.5 KB here (the 2.7 million rows 2.1.0 holds over 2.2.0 at the end of the retention-off
runs are 4.0 GB of memory on the leader and 4.1 GB on a follower).

`scripts/run.sh` does one run: a fresh cluster, the leader put on n1, the queue configured, the
partitions made, the load, a sample of each broker every 10 s (`scripts/sampler.py`: memory, CPU
ticks per thread, rows), and everything copied into `runs/<tag>/`. `scripts/report.py` merges the
nine processes' histograms and prints the numbers below (`runs/report-*.txt`).

## Both builds cleaning at the same pace

The queue removes a consumed message after 60 s (`completedRetentionSeconds` 60), so both builds
remove rows and messages from the first minute on. Two runs each, the second pair in the other
order.

| | 2.1.0 | 2.1.0 again | 2.2.0 | 2.2.0 again |
|---|---|---|---|---|
| Run | `a-210-completed` | `a2-210-completed` | `b-rc7-completed` | `b2-rc7-completed` |
| Pushed per second | 1,000,005 | 999,982 | 999,986 | 999,960 |
| Consumed per second | 999,856 | 999,984 | 999,976 | 999,917 |
| Shed, push, pop and ack errors | 0 | 0 | 0 | 0 |
| Push p50 / p99 / p999 (ms) | 11.1 / 57.3 / 90.1 | 10.8 / 62.5 / 94.2 | 10.6 / 60.9 / 91.1 | 11.3 / 56.3 / 88.1 |
| End to end p50 / p99 / p999 (ms) | 32.5 / 133 / 176 | 32.0 / 182 / 248 | 31.2 / 160 / 203 | 33.3 / 191 / 256 |
| Leader: cores, apply thread | 8.5, 63% | 8.7, 64% | 8.6, 64% | 8.6, 65% |
| Followers: cores, apply thread | 4.5, 55 to 56% | 4.5, 55 to 56% | 4.5, 57% | 4.5, 56 to 58% |
| Memory at its highest, leader / followers (GB) | 7.2 / 4.7 | 7.3 / 4.6 | 7.0 / 4.6 | 7.1 / 4.7 |
| Rows at their highest | 743,126 | 743,093 | 743,344 | 743,076 |

The builds cannot be told apart here. The end-to-end p99 moves more between two runs of one build
(133 and 182 ms on 2.1.0) than between the builds.

## Retention off

The default queue. 2.1.0 removes nothing, and 2.2.0 removes each row a minute after its push
while the messages stay.

| | 2.1.0 (`d-210-off`) | 2.2.0 (`c-rc7-off`) |
|---|---|---|
| Pushed / consumed per second | 999,979 / 999,975 | 999,989 / 1,000,011 |
| Shed, push, pop and ack errors | 0 | 0 |
| Push p50 / p99 / p999 (ms) | 10.0 / 61.4 / 97.3 | 10.8 / 65.5 / 98.3 |
| End to end p50 / p99 / p999 (ms) | 27.9 / 114 / 160 | 29.7 / 118 / 156 |
| Leader: cores, apply thread | 8.3, 47% | 8.4, 55% |
| Followers: cores, apply thread | 4.4, 39 to 41% | 4.4 to 4.5, 47 to 48% |
| Rows | 3,449,999 at the end, one more per push | 743,051 at the highest, 459,123 at the end |
| Leader memory (GB) | 2.3 at the start, 11.1 at the highest | 2.4 at the start, 7.1 at the highest, 5.1 at the end |
| Follower memory (GB) | 0.8 at the start, 8.7 at the highest | 0.8 at the start, 4.6 at the highest, 3.7 at the end |
| Free memory at its lowest, leader (GB) | 8.8 | 17.5 |

The row cleanup is the eight points of the apply thread, on the leader and on the followers
alike, and 4 to 7% on the p99s. In five minutes 2.1.0's leader grew by 8.8 GB, about 1.8 GB a
minute: at that pace its own memory reaches the machine's 31 GB some eleven minutes later.

## The highest rate, on 2.2.0

One cluster kept up and the offered rate raised in steps of 75 s (a 10 s ramp, then a steady
window of 60 s), with consumed messages removed after 60 s (`scripts/ramp.sh`, `runs/ramp-rc7/`).
A step is carried when nothing is shed, nothing errs and the consumers keep up.

| Offered | Pushed / consumed per second | Shed | Push p50 / p99 (ms) | End to end p50 / p99 (ms) | Leader, followers (cores) |
|---|---|---|---|---|---|
| 1,200,000 | 1,199,936 / 1,199,934 | 0 | 14.5 / 467 | 38.9 / 1,049 | 9.1, 4.8 |
| 1,400,000 | 1,399,757 / 1,401,009 | 0 | 33.8 / 114 | 91.1 / 266 | 10.3, 5.5 |
| 1,600,000 | 1,497,648 / 1,497,965 | 3,251,700 messages | 1,524 / 3,277 | 1,819 / 3,506 | 12.3, 6.6 |

2.2.0 carries 1,400,000 messages a second on these machines, and offered 1,600,000 it pushes and
delivers about 1,500,000 and sheds the rest. The steps between were not run. The leader is what
gives way: its raft thread is at a whole core at every rate, and the followers use about half of
what it does. The loaders were at 30.5 of their 48 cores in the last step.

The p99s of the first step are one event. In the fifth 10 s window of the cluster's first
minute under load, the consumers fell to 1,105,000 a second, and they caught up in the sixth; the
1,400,000 step, later on the same cluster, has no such window. The 1,000,000 runs above show the
same dip, much smaller, 30 to 40 s into their load and on both builds (consumed 989,000 in
`b-rc7-completed`, 988,600 in `a2-210-completed`). Its cause was not looked for. 2.1.0 was not
run in the ramp.

## One message a request, on 2.2.0

The same ramp with one message in each push request instead of 100 (`BATCH=1`), and pops that
claim up to 100 partitions so that a pop still returns many messages (`POPW=100`). Every message
is then its own request, its own planned command and its own row. `runs/ramp-rc7-single/` and
`runs/ramp-rc7-single-fine/`, a fresh cluster for each.

| Offered | Pushed / consumed per second | Shed | Push p50 / p99 (ms) | End to end p50 / p99 (ms) | Leader, followers (cores) |
|---|---|---|---|---|---|
| 50,000 | 49,992 / 49,956 | 0 | 6.8 / 56 | 18.7 / 557 | 8.5, 4.7 |
| 60,000 | 59,998 / 59,904 | 0 | 9.1 / 71 | 26.9 / 1,008 | 9.2, 5.2 to 5.4 |
| 70,000 | 69,997 / 70,411 | 0 | 62 / 533 | 1,344 / 2,851 | 9.9, 5.7 to 5.9 |
| 80,000 | 72,966 / 72,985 | 419,651 messages | 393 / 885 | 3,015 / 4,096 | 10.6, 5.7 to 5.9 |
| 100,000 | 74,400 / 74,448 | 1,638,894 messages | 549 / 983 | 3,244 / 4,129 | 10.9, 5.4 to 5.5 |

With one message a request the cluster carries 60,000 messages a second at its usual push
latencies and 70,000 with nothing shed but more than a second from push to delivery; offered
more, it stays at about 73,000 and sheds the rest. That is a twentieth of the rate it carries with
100 messages a request. The threads that fill are the apply threads, 155 to 180% of a core
together on the leader and 128 to 162% on a follower, where the batched runs keep them near 60%;
the checkpoint thread follows, at 41 to 77%.

The end-to-end p99s of the 50,000 and 60,000 steps are again one 10 s window in the cluster's
first minute, in which the consumers fell behind by a tenth and caught up.

## The threads

In September the followers' apply thread ran at 93 to 97% at this rate and was what gave way
first. In these runs it is at 39 to 58%, and the leader's at 47 to 65%. The thread nearest to a
whole core is the leader's raft thread, at 98 to 103% in every run, on both builds.

## What these runs do not show

- More than five minutes. The window here is 60 s, where the default is 900 s and a queue's dedup
  window is an hour by default: at this rate and those windows 2.2.0 holds 15 or 60 times the
  rows of these runs, about 13 GB or 54 GB of 100-message rows. That is what remembering every
  `transactionId` of a million messages a second for that long weighs.
- The highest rate to the nearest 100,000 (10,000 with one message a request), and the highest
  rate of 2.1.0. The ramps are on 2.2.0 only.
- A consumer that is behind. Every consumer here reads at the tail, from memory. Reads of
  messages whose rows are gone were measured on one VM, in `../2026-10-09-rows-window/`.
- A fault. No node was stopped during a run.
- More than one run at a point, except the pair above.

## Run it

```bash
cp scripts/hosts.env.example scripts/hosts.env     # the six machines
scripts/deploy.sh <a directory with queen-210, queen-rc7 and qload>
scripts/chain1.sh                                   # the four runs, about 30 minutes
scripts/chain2.sh                                   # the pair again
scripts/ramp.sh ramp-rc7 queen-rc7 completed 1200000 1400000 1600000   # the highest rate
BATCH=1 POPW=100 scripts/ramp.sh ramp-rc7-single queen-rc7 completed 50000 100000   # one message a request
```

The warning lines a run's `end-n*.txt` counts are almost all from the start of the cluster, when
`run.sh` stops whichever node leads until n1 does: the second pair counts them before the load
too (`warn-before-n*.txt`), and the load itself added 2 lines on the leader and none on the
followers, on both builds.
