# Jepsen and a rolling upgrade for 2.7.0, 2026-10-30 and 31

Branch `next-release`. **226 tests, all 226 valid at their first run.** No reruns, no second checks.
**Rolling upgrade 2.6.0 -> 2.7.0 and back: passed** (section below).

The campaign ran 2.6.0's 257 tests minus the 31 standby failovers. It ran again in full because
2.7.0 changes what the broker accepts and how it answers (request bounds, a memory guard, bounded
dead letters and client-grown state, KV reads served as their stored text, transaction KV writes at
their own rate) and adds stream claims. The cluster version stays 7.

One control node and five test nodes, `n1` to `n5`: GCP VMs in one VPC (`smartness-stage`,
europe-west1-b), the nodes `e2-standard-2` (2 vCPU / 8 GB), the control `e2-standard-8`
(8 vCPU / 32 GB), heap `-Xmx12g`. The harness is `test/jepsen` of `19f3d492c` (W9 closes quiet
partitions under a claim, as the runners do). This folder holds what git does not.

## The builds

| Tests | Build | md5 |
|-------|-------|-----|
| 1 to 19 | `19f3d492c`: the hardening, the long-poll fix and stream claims sent to the leader | `c919be7b252b7c90e7de2f48f8117607` |
| 20 to 226 | `f9c9a37f2`: the same, plus KV reads as stored text (`7fe23f4ed`), a lock per Kafka transactional id (`3d24969b3`), the facade's commit row outside the KV rate (`d1c81376e`), and a transaction's KV writes at their own rate (`f9c9a37f2`) | `03df1a5db8cfc30d3015cfa3a3078c57` |

Tests 1 to 19 are the ten streams tests (`p11-w9-*`) and the first nine queue tests. The KV
workloads ran on `f9c9a37f2`: elle reads and writes keys in one KV batch, the path `7fe23f4ed`
changed, and checks it at strict serializability; register checks compare-and-set for
linearizability.

The swap (`binary-swap.txt`, `scripts/swap-bin.sh`): the new build was moved into place on the
control right after test 19 ended, at 2026-10-30 13:28 UTC. The harness uploads the binary to every
node at each test's setup, so test 20 on ran `f9c9a37f2`; all five nodes logged its md5.

After `f9c9a37f2`, the broker changed only by: rustfmt (`645be744e`, `2523c5591`), the version lines
(`bc784619c` beta.1, `b014a6d2d` beta.2), two disk-space fields in each node's minute sample for the
dashboard (`107aaaf9b`), and one join branch in the Kafka facade's group code (`f94c649cf`). The proxy
fix (`b0e4a6109`) is outside the tests' path: the tests talk to the brokers.

## The list

`matrix.txt` (= `test/jepsen/matrix-prio-2026-10-30.txt`), run top to bottom:

- **125 tests**, the list `matrix-p11`, ordered by what 2.7.0 changed: streams (10), queue (13),
  claim (9), elle (8), log (15), dedup (8), pipeline (14), retention (9), DLQ (9), timers (9),
  register (14), counter (7); within each, kill, partition, pause and leader-deaf first. 125 valid.
- **13 tests**, batched pushes (`matrix-24-batch`). 13 valid.
- **28 tests**, locks and semaphores (from `matrix-220`). 28 valid.
- **60 tests**, the rest of `matrix-220`: the rows matrix (40: rows that live two seconds in
  256 KiB log files sealed after 10 s, and 8 at default settings), the same small files with rows
  that stay (12), and the shared-log layout, four shards for eight queues (8). 60 valid.

The last three groups were appended to the running list at 2026-10-30 16:50 UTC.

| When (UTC) | What |
|---|---|
| 10-30 11:39 | test 1, `p11-w9-kill` |
| 10-30 13:28 | the binary swap, after test 19 |
| 10-30 16:50 | 101 tests appended |
| 10-31 08:54 | test 226, `sh-rw-w8-kill`, ends: 226 valid |

About 5.6 minutes a test.

## Files

`summary.txt` (one line per test start and end), `results/<test>.edn` (each test's `results.edn`;
every one has `:valid? true` at its top level), `binary.md5`, `scripts/` (how the VMs were set up,
the controller's key left out). The histories and node logs were deleted with the VMs.

## Rolling upgrade 2.6.0 -> 2.7.0 and back

`upgrade/roll27.sh`, 2026-10-31 09:19 to 09:29 UTC, run from a control VM (`e2-standard-4`) against
three brokers on `n1` to `n3` (`e2-standard-2`, `upgrade/node.sh` on each), after the Jepsen VMs were
gone. It is 2.6.0's `roll26.sh` with the versions moved up, plus checks for what 2.7.0 adds. The old
build is the released 2.6.0, the new one 2.7.0-beta.2, the image stage runs (`upgrade/binaries.md5`);
next-release's broker differs from beta.2 only by `f94c649cf`, the Kafka facade's clippy change.

The queues: `hist` (192,000 messages written by 2.6.0, read back at the end), `live` (three `qload`
loaders, 1,500 msg/s each, each through its own node), `dup` (`upgrade/dup.py`: sets of 2,000 messages
with fixed `transactionId`s, pushed once and then again; every second answer must be `duplicate`
naming the first push's offset). And for 2.7.0 (`upgrade/extra27.py`): KV keys whose answers are
compared byte for byte (`upgrade/kv/`), a stream query and its claim, and a queue `big` for a push of
12,000 messages, which 2.6.0 takes and 2.7.0 refuses. `upgrade/roll.log` is the run.

| Step | What | Result |
|------|------|--------|
| 0 | a fresh 2.7.0 cluster | cluster version 7, `kinds` 7 on all three |
| 1 | a fresh 2.6.0 cluster | version 7; history written (192,000, 0 errors), set A queued; KV keys `k1`, `k2` and a stream query written; a stream claim answers 404, as 2.6.0 has no claims |
| 2 | `n3` (the leader), then `n2`, on 2.7.0; `n1`, on 2.6.0, leads | version 7 with the mix; set A all duplicates with their offsets, set B queued. The push of 12,000: 201 through the 2.6.0 leader, 400 `too_many_items` through a 2.7.0 node, which refuses it before forwarding. A claim through a 2.7.0 node: 404 from the 2.6.0 leader, the answer on which runners turn claims off |
| 3 | `n3` back to 2.6.0 on the directory 2.7.0 wrote, then forward again | starts and follows; set B all duplicates |
| 4 | `n1`, the last 2.6.0 member, on 2.7.0 | every node on 2.7.0, version 7 |
| 5 | after the roll | set C queued, sets A, B, C all duplicates; the KV GET and `getMany` answers are the bytes 2.6.0 gave; the claim answers 200; the 12,000 messages the 2.6.0 leader took pop back as 10,000 and 2,000 |
| 6 | the whole cluster stopped and started on 2.7.0 | sets A, B, C all duplicates; the history read back whole: 192,000 popped, 0 errors |
| 7 | `kill -9` of the leader (`n2`) under load | `n3` leads; sets A, B, C all duplicates; set D queued, then all duplicates |
| 8 | `n3` back on 2.6.0 on its own directory, after 2.7.0 wrote everything | serves, lag 0; sets A to D all duplicates |
| 8b | `n3`, on 2.6.0, made the leader: the 2.7.0 leader stopped until `n3` won an election (3 tries) | a 2.6.0 leader with two 2.7.0 followers: sets A to D all duplicates, set E queued then all duplicates; the KV answers are the bytes of 2.6.0's first read, and of 2.7.0's read of a key 2.7.0 wrote; then forward again, set E duplicates |

Traffic over the roll (steps 1 to 5, 360 s, 4,500 msg/s offered): 1,606,860 messages pushed,
1,607,620 popped. The 760 more are the redeliveries of the 760 acks that failed while a node
restarted. 431 pushes failed, each at the loader whose own node was restarting: `qload` talks to one
node and never retries. In step 7, 389,460 pushed and 389,460 popped; the loader of the killed
leader lost 553 pushes, and 20 acks failed across the leader change.

Node logs over all starts: no FATAL, panic, `poisoned`, unknown-version or `APPLY FAILED` line on any
node. Every WARN is openraft's `Unreachable node` or a failed heartbeat towards a peer that was
restarting; `a member restarted with an empty data directory` appears only at 09:19:36 to 09:19:47,
after the wipe between steps 0 and 1.

## Not covered

- **The 31 standby failovers** (`matrix-220b2`): they need a second cluster of three nodes.
- **The Kafka facade**: the workloads use Queen's own HTTP API; the `log` workload applies the Kafka
  checker to it, never the facade. The facade's own tests are in `protocols/queen-kafka/`.
- **In the upgrade**: five nodes; a roll under the Jepsen nemeses; a member that comes back with a
  delta snapshot during the roll; 2.6.1-rc.1, production's build, as the old version (stage rolled
  from it to 2.7.0-beta.1 and beta.2 on 2026-10-30, without these checks).
