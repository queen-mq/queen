# Jepsen, the full campaign, 2026-10-08 and 09

Copied from the two Jepsen control nodes before the VMs were released: `178.128.199.140` (the
first set, nodes `n1` to `n5`) and `164.90.162.218` (the second set, its nodes `n1` to `n5`; the
sixth was idle). All nodes are DigitalOcean droplets with 2 vCPU and 4 GB. The harness is in git
(`test/jepsen`). This folder holds what git does not.

## What ran

- **Build:** commit `b69470031`: 2.0.4 with the standby cluster and locks (`e2f7236bd`) and the
  four fixes of the night of 2026-10-08. Built on the first control node with
  `cargo build --locked --profile fastrel --bin queen`; its checksum is in `binary.md5`. The
  server code of 2.1.0 differs from it in the version number and in four lines of a unit test.
- **Tests:** 153, in `matrix.txt`: P11's 125 (W1 to W10) and the 28 lock and semaphore tests
  (W11, W11b). The first set ran tests 1 to 102 in the matrix's order, from 20:36Z on
  2026-10-08 to 05:59Z on 2026-10-09; the second set ran tests 153 to 103, from the end, from
  00:46Z to 05:48Z, after it had finished the failover campaign
  (`../2026-10-08-jepsen-failover/`).
- **Result:** 152 valid, 1 invalid. The invalid one was the harness, not the broker: the test
  nodes' clocks had drifted apart. With the harness fixed (`c4525140e`, the same binary) the
  test ran again and was valid. See "The clocks".

## Files

| Path | Contents |
|---|---|
| `summary-first.txt`, `summary-second.txt` | one `start` and one `done` line per test, as `run-campaign.sh` wrote them on each set |
| `gate-first.txt`, `gate-second.txt` | per test: how often a leader's entries waited for its quorum, see "The lease" |
| `results/<test>.edn` | each test's `results.edn`; `x-sem-leader-deaf-again.edn` is the test that ran again |
| `matrix.txt` | the 153 tests |
| `binary.md5` | checksum of the tested binary |
| `lease-demo.sh`, `lease-demo.txt` | a leader cut off for 2.4 s, on the build before the fix and on this one, see "The lease" |
| `before-e2f7236bd/` | the run on the build before the fixes, stopped after 48 tests: its `summary.txt` and every `results.edn` |
| `before-e2f7236bd/p11-w2-slow-lz-kill.tar.zst` | the store of the test that found the bug: its history, the Jepsen log and each node's `queen.log`, 245 MB once unpacked (`zstd -dc --long=27 <file> \| tar -xf -`) |

The other Jepsen stores (every history, the Jepsen logs and each node's `queen.log`: 16 GB on
the first control node and 8.5 GB on the second, the runs made on the way included) were not
copied.

## The run before the fixes

The campaign first ran on `e2f7236bd`, the build as it was merged, on the first set, from 15:49Z
to 20:12Z on 2026-10-08. It was stopped after 48 tests, when the fix for what its one invalid
test had found was ready: 47 valid, 1 invalid. `before-e2f7236bd/` has its `summary.txt` and
every `results.edn`.

The invalid one is `p11-w2-slow-lz-kill` (W2 `queue` on five nodes, two of them with a 300 ms
fsync, single-node power losses): seven acknowledged messages were never delivered. They are in
the log. A follower lost power at 18:55:37.4 and the leader, `n1`, planned its last entry in
that same second. It led on in term 7 for the 2 min 18 s the test had left, its heartbeats
answered, every node's log ending at index 27870, and every pop ended "the request deadline
elapsed". With one fast follower gone, the leader's quorum needed one of the two slow nodes,
whose appends took up to 2.2 s to come back. openraft takes a write only from a leader that a
quorum has acknowledged within 2 s, refused one, and the driver took the refusal for a lost
leadership: it stopped planning and waited for a change of role that never came, because the
node still led. `5d48aef94` is the fix, and its commit message has the whole of it. The same
test is valid on the fixed build, as is `p11-w5-slow-lz-kill`.

## The lease

`gate-first.txt` and `gate-second.txt` count, per test, the lines that fix logs. In 59 of the
153 tests a leader was handed an entry while its last quorum acknowledgement was 1.5 s old or
more: 152 times in all, and 116 of them, in 51 tests, with one of 2 s or more, which is where
openraft refuses a write. Each time the entry waited, 46.9 s at the longest (`x-lock-all`): 41
went to the log when a quorum answered again or were handed over when the node no longer led,
and 111 were dropped with the pipeline of a leadership that had ended. Before the fix openraft
refused such an entry and the driver stopped for as long as the node led. The 59 are tests under
kills, power losses, partitions, pauses and a deaf leader. No driver had to start its pipeline
again while it led (`restarts`), and no apply was refused anywhere (`refusals`). The test that
ran again had 8 such waits of its own, which the counts above leave out.

`lease-demo.sh` is the bug without Jepsen, run on the second set at 06:00Z on 2026-10-09 once
its tests had ended. The leader of five nodes is cut off from the other four for 2.4 s while a
client keeps writing to it, and the network heals. Three seconds later the script asks whether
the same node still leads in the same term, and if it does, whether it takes a write.
`lease-demo.txt` has two runs, of 8 and of 20 trials, for each build:

| Build | Trials | The cut ended in an election | Same leader, takes the next write | Same leader, takes none |
|---|---|---|---|---|
| `e2f7236bd`, before the fix (`queen-r21`) | 28 | 16 | 1 | 11 |
| `b69470031` (`queen-r21-final`) | 28 | 18 | 10 | 0 |

"Takes none" is two writes of 5 s each with no answer, from a node that reports itself the
leader of the term it led before the cut; the script then restarts it. On the fixed build the
nodes logged 28 waits of the kind counted above in those trials.

## The clocks

`x-sem-leader-deaf` (W11b under a deaf leader) was judged invalid on the second set at 01:16Z:
a permit renewed for 4 s went to another owner 3.45 s later, 543 ms before its lease could have
ended and 43 ms past the checker's margin. The broker did what it says. A lease survives a
leader change while the nodes' clocks agree within `QUEEN_RAFT_MAX_CLOCK_SKEW_MS` (500 ms), and
those nodes' did not: leadership had just moved from `n3` to `n2`, and `n2`'s log has its own
vote for itself 0.55 s after the votes the others gave it.

The harness turns the nodes' time daemon off for every test, because the clock faults need the
clocks left alone, and only a clock test set them again. In between they ran free: forty
minutes after a clock test the first set's five nodes were between -93 ms and +31 ms of true
time, and on the second set the last clock test was three hours old. `c4525140e` makes every
test start with its nodes' clocks set; it was put on both control nodes at 01:24Z, and the
tests that started after that ran with it. The test ran again at the end of the second set's
list as `x-sem-leader-deaf-again`, from 05:48Z to 05:54Z: valid.
