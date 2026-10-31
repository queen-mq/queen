# Jepsen for 2.7.0, 2026-10-30 and 31

Branch `next-release`. **226 tests, all 226 valid at their first run.** No reruns, no second checks.

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

## Not covered

- **The 31 standby failovers** (`matrix-220b2`): they need a second cluster of three nodes.
- **The Kafka facade**: the workloads use Queen's own HTTP API; the `log` workload applies the Kafka
  checker to it, never the facade. The facade's own tests are in `protocols/queen-kafka/`.
- **A rolling upgrade from 2.6.0**: not run under Jepsen this time. 2.7.0 keeps cluster version 7.
