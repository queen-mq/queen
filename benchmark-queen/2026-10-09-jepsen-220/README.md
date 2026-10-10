# Jepsen, the campaign of 2.2.0, 2026-10-09 and 10

Copied from the Jepsen control node before the VMs were released. One control node (16 vCPU) ran
the harness for two sets of nodes at once: set A, nodes `n1` to `n5`, and set B, nodes `m1` to
`m5` with `m6` for the failover tests. The eleven nodes are DigitalOcean droplets with 2 vCPU and
4 GB. The harness is in git (`test/jepsen`). This folder holds what git does not.

## What ran

- **The first pass**, 2026-10-09 from 15:57Z to 19:46Z, on set A: 41 tests
  (`first-pass/matrix.txt`, in git as `test/jepsen/matrix-rows.txt`) on `e8efda8cb`, the first
  build of the rows window. 37 valid, 2 invalid, 2 unknown: the four found two ways a node could
  refuse to start. See "The first pass".
- **The campaign**, from 21:23Z on 2026-10-09 to 13:44Z on 2026-10-10, on both sets: 244 tests in
  352 runs. It ran on five builds, because fixes and changes landed while it ran, and the tests
  that had run on an older build ran again on the newer one. Every one of the 244 ran on the
  last, `3b1a6d9bb`.
- **Result:** 351 runs valid. The other is a crash of the harness on a node that had no lazyfs
  binary, and that test was valid when it ran again. One run that the checkers judged valid, on
  the first of the five builds, had two nodes that refused to start: see "The node logs".
- **The final code**, 2026-10-10 from 13:42Z, on both sets: 11 tests on `8f5342d29`, the server
  code of 2.2.0, which adds three commits to the campaign's last build. All 11 valid. See "The
  final code".

## The tests

The 244 are `test/jepsen/matrix-220.txt` (213) and the failover tests of
`test/jepsen/matrix-220b2.txt` (31). `set-a/matrix.txt` and `set-b/matrix.txt` are the lists as
each set ran them, with the runs added on the way.

| Tests | What they are |
|---|---|
| 153 | the suite 2.1.0 passed, unchanged: P11's 125 (W1 to W10) and the 28 lock and semaphore tests (W11, W11b) |
| 40 | the first pass again (`rw-`, `d-`, `s-`): rows that live two seconds, on 256 KiB log files sealed after 10 s, so that consumers read messages that have no row after every fault and in the final drain |
| 12 | `sf-`: the default 24 h dedup window on the same small files, so that sealing, reclaiming and the removal of empty logs run under faults with rows that stay |
| 8 | `sh-`: the shared-log layout (`QUEEN_QLOG_SHARDS=4`, eight queues), which no campaign had run, with the default settings and with rows that live two seconds |
| 27 | `fo-`: W12, a standby cluster's failover, on two clusters of three nodes |
| 4 | `rw-fo-`: the failover with rows that live two seconds and small log files on both clusters |

## The builds

Each was built on the control node with `cargo build --release` from the tree of its commit,
the first two from the working tree that became it. `binaries.md5` has the checksums.

| Build | Commit | What it added | Runs |
|---|---|---|---|
| `queen-rows` | `e8efda8cb` | the rows window | the first pass, 41 |
| `queen-220rc1` | `6f18bd40f` | the fixes for the two refusals the first pass found | 22 |
| `queen-220rc3` | `5f24bf9bb` | old messages read from a cold disk a chunk at a time, small log files without a memory map | 15 |
| `queen-220rc4` | `4cc97a187` | a quiet queue no longer makes a log file every ten minutes | 8 |
| `queen-220rc5` | `c51b8fcbb` | a log file that could not be sealed keeps its messages | 54 |
| `queen-220rc6` | `3b1a6d9bb` | a log file sealed right after a log cut is created for the seq that comes next | 253 |
| `queen-220rc7` | `8f5342d29` | completed retention holds no row, `/health` needs a majority | the final code, 11 |

The campaign read its binary from one path, and each newer build replaced the file between two
tests. `set-a/binary.md5` and `set-b/binary.md5` have the time of each change. A test that had
started before a change ran again at the end of its set's list with the label of the build that
replaced its own (`-r4` 37 runs, `-r5` 8, `-r6` 54), and the reruns that had not started when a
newer build came ran on that one. `scripts/summarize.py .` counts the runs by set, build and
verdict from the summaries.

## Files

| Path | Contents |
|---|---|
| `first-pass/`, `set-a/`, `set-b/` | one folder per run of `run-campaign.sh` |
| `*/summary.txt` | one `start` and one `done` line per test |
| `*/results/<test>.edn` | each test's `results.edn` |
| `*/matrix.txt` | the tests, as that set ran them |
| `*/binary.md5` | the build the set started on and the time of each change |
| `*/probe.txt` | every 20 s and per node: claims served from the queue log (`cold`), rows in the store, runs of the cross-file directory |
| `first-pass/not-valid.txt` | the lines with which the nodes of the four tests refused to start |
| `first-pass/TREE.txt` | how the first pass's tree was put on the control node |
| `scan-fatal.txt` | the node logs of both sets that hold a refusal to start or a panic: two, of one run |
| `binaries.md5` | the checksum of every build |
| `final-code/` | the 11 tests on the final code: the two summaries, the two lists and every `results.edn` |
| `scripts/` | `campaign-a.sh` and `campaign-b.sh` (how each set ran), `rc7-a.sh` and `rc7-b.sh` (the final code), `scan-fatal.sh` and `scan-new.sh` (the node logs), `collect-jepsen.sh` (this folder), `summarize.py` |

The Jepsen stores (every history, the Jepsen logs and each node's `queen.log`) were not copied.

## The first pass

The first pass ran the consumption workloads with rows that live two seconds
(`--dedup-window 2`, `QUEEN_RAFT_TXN_WINDOW_MIN_S=2`, a retention scan every second) on log files
of 256 KiB that are sealed after 10 s, where the default is ten minutes. A five-minute test then
seals, reclaims and removes log files many times, which no earlier campaign had reached. Four of
its 41 tests were not valid, and in each of them nodes had refused to start
(`first-pass/not-valid.txt`):

- `rw-w2-lz-kill` and `rw-w2-slow-lz-kill` (invalid) and `rw-w8-lz-kill` (unknown), all three
  with power losses: an empty queue log was deleted in place. On the test's filesystem the
  removal stopped half-way (`Directory not empty`), the directory stayed with no file in it, and
  the next start took it for a log that had lost its records:
  `rsm qlog corrupt (queue log q...): it ends at seq 0, below seq 5`. Every node refused, the
  cluster had no quorum, and the messages still to deliver were judged lost.
- `rw-w8-kill` (unknown), with kills: a follower's raft log was cut back, the next entry went
  into an empty file that had been created for a later seq, a search by seq skipped that file,
  and the node refused to start: `raft log entries 10215..10216 are not all in the queue logs`.

`6f18bd40f` fixes both, with a unit test for each. Both are in 2.1.0 as well: there a log file
is sealed after ten minutes, so a five-minute test does not meet them. The four tests were the
first of the campaign and were valid on the fixed build.

## The node logs

A valid verdict is not a healthy cluster. `rw-w2-kill` ran on the first build of the campaign at
21:47Z and its checkers found nothing: 3 nodes of 5 carried the history. Two nodes, `n4` and
`n5`, had refused to start four minutes into it, with the second refusal of the first pass:
`raft log entries 25213..25214 are not all in the queue logs (0 found)`. The first fix covered
the file a cut empties, and not the file sealed right after a cut: the log's count of what it
had written only ever rose, so after a cut the idle pass sealed a file for the old tail plus
one, and the new leader's entries, with lower seqs, went into a file whose header said it held
nothing that low. `3b1a6d9bb` is the fix, and it is why the campaign moved to that build and ran
every earlier test again.

It was found by reading the node logs, in the night of the campaign. Since then:

- `scripts/scan-fatal.sh` reads every node's `queen.log` of both sets for a refusal to start, a
  panic or a render gap, the tests that damage a node's files on purpose left out. Run after the
  last test, it prints two lines, the two nodes of that run (`scan-fatal.txt`). No other run has
  one, on any build.
- `rw-w2-kill` ran eight more times on the last build (`-x1` to `-x8`): valid, and no refusal.
- `test/jepsen/run-campaign.sh` now reads the node logs after each test and writes `REFUSED`
  instead of the checkers' verdict when one holds such a line. The script that ran this campaign
  did not have it yet.

## The harness crash

`fo-crash-lz-kill` ended `CRASHED(rc=255)` at 02:40Z on set B. It is the first failover test
with power losses, the first to need lazyfs on six nodes, and the sixth node had never built it:
the build took longer than the 60 s the harness gives a node's setup. It was built by hand, and
the test ran again at the end of the failover list as `fo-crash-lz-kill-again`: valid.

## The final code

2.2.0's server code is `8f5342d29`. It adds three commits to `3b1a6d9bb`: one line of the rows
window's rule (a queue's completed retention no longer keeps a push's row in memory), `/health`
answering ready only while the leader has its majority, and tests. No Jepsen test sets a
completed retention above its queue's dedup window, so no test here tells the old rule from the
new one: that change is covered by unit tests and by a run on a staging cluster, not by Jepsen.

Eleven tests ran on that code after the campaign, six on set A and five on set B
(`final-code/`): the queue under kills, partitions, pauses, clock faults and restarts, the log
under membership changes with and without kills, the counter under power loss, the queue and
retention with rows that live two seconds under kills, and one failover under kills.

All eleven are valid. The seven queue and retention tests acknowledged 135,234 messages and lost
none. The failover's source crashed with 4 acknowledged writes its standby had not read yet,
which is what a standby that follows asynchronously may lose; the standby had replayed 9,693
records, was promoted, and took its first write 8 ms later. `scripts/scan-fatal.sh` ran once
more after them, over every node log of both sets, these runs included: `scan-fatal.txt` is
that run, and it has the same two lines.
