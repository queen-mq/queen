# Jepsen, the failover campaign, 2026-10-08 and 09

Copied from the second Jepsen control node `164.90.162.218` (six nodes `n1` to `n6` and the
control, DigitalOcean, 2 vCPU and 4 GB each) before the VMs were released. The harness is in git
(`test/jepsen`; the workload is W12 `failover`, in `src/jepsen/queen/workload/failover.clj`, and
the two matrices are `matrix-failover-repro.txt` and `matrix-failover.txt`). This folder holds
what git does not.

## What ran

- **Build:** commit `b69470031`: 2.0.4 with the standby cluster and locks (`e2f7236bd`) and the
  four fixes this campaign led to, listed below. Built on the first control node with
  `cargo build --locked --profile fastrel --bin queen`; its checksum is in `binary.md5`.
- **Clusters:** `n1` to `n3` are the source, `n4` to `n6` the standby, started empty and
  following from the first entry. Every test has the standby follow for half its time while the
  faults run on both clusters, then the failover, then the promoted cluster serves under the
  same faults.
- **Tests:** 37, from 20:37Z to 23:53Z on 2026-10-08. `matrix-failover-repro.txt` has 10 (a
  failover while a partition isolates the leaders every 3 s); `matrix-failover.txt` has 27:
  crashes of the source, planned switches, transactions over several partitions, and crashes
  that find the standby 10 s behind, under kills, power loss, pauses, partitions, clock faults,
  restarts and all of them at once.
- **Result:** 36 valid, 1 unknown. The unknown was the harness, not the broker. With the
  harness fixed (`7bf42d5ac`, the same binary) the ten planned tests ran again, from 23:53Z to
  00:46Z: 10 of 10 valid. See "The one unknown".

## Files

| Path | Contents |
|---|---|
| `summary.txt` | one `start` and one `done` line per test, as `run-campaign.sh` wrote them |
| `table.txt` | per test: the verdict and what the failover checker counted (columns below) |
| `gate.txt` | per test: how often a leader's entries waited for its quorum, see "The lease" |
| `results/<test>.edn` | each test's `results.edn` |
| `planned-again/` | `summary.txt`, `table.txt`, `gate.txt` and `results/` of the ten planned tests run again |
| `before/<run>/` | the runs made on the way, on the builds without one fix or another |
| `binary.md5` | checksum of the tested binary |

The Jepsen stores (every history, the Jepsen logs and each node's `queen.log`) were not copied.

`table.txt`, left to right: `acked`, the writes the source acknowledged; `replayed`, the records
of the promoted log that came from the source; `lost` and `lost-ms`, the acknowledged writes the
promoted cluster does not hold and the time from the first of them to the crash; `older`, writes
missing although a later one survived (the checker rejects any); `pl-lost`, writes a planned
switch lost (any is rejected); `diverg`, records where the two logs differ (any is rejected);
`own`, the writes the promoted cluster acknowledged; `refused`, the writes the standby refused
while it was one; `late-ref` and `late-ms`, `503 standby` answers after the promotion and when
the last one came; `1st-own`, milliseconds from the promotion's answer to the first write the
promoted cluster acknowledged.

## What the tests counted

Over the 37 tests (`table.txt`):

| | |
|---|---|
| writes the source acknowledged | 446,016 |
| of them replayed into the promoted logs | 440,741 |
| writes a standby refused before its promotion | 393,215 |
| writes the promoted clusters acknowledged | 337,621 |
| writes a standby took before its promotion | 0 |
| records where a promoted log differs from its source's | 0 |
| acknowledged writes missing while a later one survived | 0 |
| writes a planned switch lost (10 tests, 111,813 acknowledged) | 0 |
| transactions present in part | 0 |

All 37 standbys were promoted. The promotion's answer took 9 ms at the median and 10.5 s at the
slowest (`fo-planned-txn-part`, three requests: a partition isolated the node asked first), and
the promoted cluster acknowledged its first write 10 ms after it at the median, 47 ms at the
latest.

## What a crash lost

Replication is asynchronous, so a crash of the source loses the acknowledged writes the standby
had not read. The checker allows that and rejects anything else: a write missing while a later
one is there, or a transaction there in part.

| Tests | Lost |
|---|---|
| 19 of the 23 crashes without a link cut | nothing |
| `fo-repro-primaries-2` | 24 writes, acknowledged in the 7.8 s before the crash |
| `fo-repro-one-1` | 461 writes, 3.4 s |
| `fo-crash-lz-kill`, `fo-crash-txn-lz-kill` | 3 writes each, 4.8 s and 19.8 s |
| the 4 crashes with the link cut for 10 s before (`fo-behind*`) | 616 to 2,115 writes, 10.0 to 11.9 s |

In the four of the first group that lost something, the standby had stopped hearing its source
before the crash. Under partitions its link had had no answer for 9.2 s
(`fo-repro-primaries-2`) and 3.4 s (`fo-repro-one-1`); in the two power-loss tests the standby
had no leader at that moment. Only the standby's leader reads the source, so a standby falls
behind while that node is cut off, and until another one leads.

## Answers after the promotion

A promoted cluster answered `503 standby` after its promotion in three tests, all with
partitions: 78 times over 3.0 s (`fo-repro-planned-1`), 24 over 0.9 s (`fo-planned-txn-part`)
and once (`fo-behind-txn-part`). A node cut off from the others when the promotion commits
still believes it leads a standby, and says so to its own clients until it hears the new term.
None of those writes was applied. In the other 34 tests there was none.

## The four bugs

The campaign began on `e2f7236bd` and found three bugs in the broker; a fourth came from the
link's own tests the same evening. `before/` has the runs that showed each, with the
`results.edn` of every test that did not end valid.

| Fixed in | What went wrong | Seen in |
|---|---|---|
| `963f7846b` | A standby's new leader planned a client's command that had waited out its election, stamped with its own clock. Every node refused the next entry it replayed (I5) and stopped; after a power loss none opened its store | `before/runs-failover` (`fo-crash-lz-kill`), `before/runs-failover-repro` (`fo-repro-primaries-1`, `fo-repro-planned-1`) |
| `5d48aef94` | A leader that no quorum had acknowledged for 2 s had a write refused by openraft, stopped planning and never started again while it led. A standby in that state could not be promoted: 120 s of `503 timeout` | `before/runs-failover` (`fo-crash-part`), `before/runs-fix1` (`fo-repro-primaries-1` and `-3`), and an ordinary cluster in the other campaign of that night (`../2026-10-08-jepsen-full/`) |
| `63a4805ed` | A node that had led the standby and was elected again after the promotion answered `503 standby`, also to the retry of a write the cluster had taken: `refused-but-present` | `before/runs-fix3` (`fo-repro-primaries-3`), `before/runs-fix4` (`fo-repro-primaries-6`), `before/runs-fix6` (`fo-repro-primaries-2`) |
| `3965c7dc8` | A seeded standby left no hold on its source's log until its first read, and halted with "needs a new seed" when the source had purged past the seed by then | the link's unit tests under load; the Jepsen standby starts empty |

Which build each of those runs used is in its `binary.txt`: `queen-r21` is `e2f7236bd`,
`queen-r21-fix` and `-fix2` add the first fix (uncommitted then), `-fix3` and `-fix4` the second,
`-fix6pre` the third. The first test of `before/runs-fix3` is not to be read: the Jepsen process
of the run before it had not been killed and went on driving the nodes under it. In
`before/runs-failover-repro`, `fo-repro-primaries-2` crashed in its setup ("the standby never
follows"); it was not explained and did not come back in the 60 or so tests run since.

## The lease

`gate.txt` counts, per test, the lines the second fix logs. In 21 of the 37 tests a leader was
handed an entry while its last quorum acknowledgement was 1.5 s old or more: 88 times in all,
and 57 of them, in 20 tests, with one of 2 s or more, which is where openraft refuses a write.
Each time the entry waited, 20.6 s at the longest (`fo-planned-pause`): 53 went to the log when
a quorum answered again or were handed over when the node no longer led, and 35 were dropped
with the pipeline of a leadership that had ended. Before the fix openraft refused such an entry
and the driver stopped for as long as the node led. No apply was refused anywhere (`refusals`).

`restarts` is a driver that started its pipeline again while it still led. The one of this
campaign (`fo-repro-planned-2`, node `n4`, 21:17:48Z) shows something that is not new and was
left as it is: a leader with no quorum acknowledgement for 3 s hands its leadership to another
voter, and from then on openraft takes no write from it and sends no heartbeat. `n4` named a
node that did not take the leadership, got its quorum back 0.6 s later, and led for 9.4 s in
all, refusing writes, under partitions that came every 3 s, before the others elected.

## The one unknown

`fo-planned-txn-kill` ended unknown: the checker does not judge "a planned switch loses nothing"
when the harness never saw the standby caught up, and here it waited 120 s without an answer.
The standby had read everything (position 8899, the source's applied index), was promoted in
0.8 s and lost nothing. What went wrong was the test. The failover's steps are separate nemesis
ops, the other faults go on between them, and the nemesis runs one op at a time: a
`:kill :majority` came between `:quiesce` and `:await-standby` and killed two nodes of each
cluster, and the `:start` that follows a kill waited behind `:await-standby` for its whole
120 s. The standby had one node and no leader to ask.

`7bf42d5ac` makes `:await-standby` heal every node before it waits (a planned switch has faults
before and after it, and the wait for the standby is the switch), and has `:promote` heal the
standby again every 20 s it goes unanswered. `planned-again/` is the ten planned tests on that
harness and the same binary: 116,032 writes acknowledged by the sources, none lost, no `standby`
answer after a promotion, and `fo-planned-txn-kill` valid.
