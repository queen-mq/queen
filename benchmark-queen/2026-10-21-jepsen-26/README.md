# Jepsen and a rolling upgrade for 2.6.0, 2026-10-21 and 22

Branch `next-release`. The campaign ran two builds of 2.6.0, swapped between two tests (below): its
first beta, `d3809f2e5` (2.6.0-beta.1), and `229a4c3ea` (2.6.0-beta.6), the server code of 2.6.0
without its version line (`ae4924cf6` "server 2.6.0" changes nothing else in the broker).

What changed since 2.5.0, and why the whole campaign ran again:

- a member that comes back recovers from the leader's log or from a delta snapshot, not from the
  whole state: member holds, the question to the member about where its log stands, the indexes
  carried in a snapshot;
- a boot refills the dedup rows from a hash sidecar per sealed queue-log file;
- three raft fixes: the raft port bound at the start of a boot and listened on late, the 404 to the
  snapshot question, a replication read the purge passed;
- `PARTITION_CLEANUP_SECONDS`, `retryDelay` enforced, the ephemeral pop's lost wake-up;
- the Kafka facade rewritten as a translation layer (outside these tests: below).

The cluster version stays 7.

One control node and six test nodes, `n1` to `n6`: GCP VMs in one VPC (`smartness-stage`,
europe-west1-b), Ubuntu 24.04.5, the nodes `e2-standard-2` (2 vCPU / 8 GB), the control
`e2-standard-4` (4 vCPU / 16 GB) while the tests ran. The harness is in git (`test/jepsen` of
`68f83bd73`, unchanged since). This folder holds what git does not.

## Result

**Jepsen: 257 tests, 255 valid on the first run.** The other two:

- `p11-w5-slow-lz-kill` failed its checker once, on the first beta, because its final drain ran out
  of time with inputs still queued, and was valid in four reruns on beta.6 (below);
- `p11-w3-lead-deaf` came back UNKNOWN because the linearizability checker ran out of memory on two
  keys of its history; checked again with knossos' WGL search, the same history is linearizable on
  all 117 keys (below).

So all 257 are valid: 255 at their first run, one in four reruns, one once its history was checked
again.

- **Main list**, 213 tests (`test/jepsen/matrix-220.txt`, the list 2.2.0 to 2.5.0 passed), on `n1`
  to `n5`: 211 valid, `p11-w5-slow-lz-kill` INVALID, `p11-w3-lead-deaf` UNKNOWN.
- **Failover**, 31 tests (`test/jepsen/matrix-220b2.txt`, a standby cluster's failover, the source on
  `n1` to `n3`, the standby on `n4` to `n6`): 31 valid.
- **Batched pushes**, 13 tests (`test/jepsen/matrix-24-batch.txt`, pushes of up to 8 messages to one
  partition): 13 valid.
- **`p11-w5-slow-lz-kill` again**, four times on beta.6 after the main list (`rerun/`), the last one
  with a ten-minute final drain: 4 valid.
- **`p11-w3-lead-deaf` checked again** on a 64 GB control with a 56 GB heap (`reanalyze/`): the
  linear search stalled at 49 GB and was stopped after 13 minutes; the WGL search found the stored
  history linearizable, all 117 keys, in 2 seconds.

**Rolling upgrade 2.5.0 -> 2.6.0 and back: passed** (section below). The cluster version stays 7
throughout, every duplicate is found across the mix, a full restart and a `kill -9` of the leader,
and 2.5.0 starts on, and serves, a directory 2.6.0 wrote.

## The builds

`binaries.md5` holds the last two:

| File | md5 | What |
|------|-----|------|
| `queen-26`, first | `ff3e7789d2e087de31b48e65f5e72b2c` | 2.6.0-beta.1: `next-release` at `d3809f2e5`. The setup check and tests 1 to 91 of the main list. |
| `queen-26`, from test 92 | `ffd1460b8a503bb05b69592208ab7133` | 2.6.0-beta.6: `229a4c3ea`, the binary of `ghcr.io/queen-mq/queen:2.6.0-beta.6`. Tests 92 to 213 of the main list, the failover and batched lists, the reruns and the rolling upgrade. |
| `queen-250` | `627f18341a7aabba7648772e0191f118` | the released 2.5.0 amd64, for the upgrade test |

The swap (`main/binary-swap.txt`): at 2026-10-21 16:33 UTC, while test 91 (`w11-lock-pause`) ran, the
beta.6 binary was renamed into place on the control. The harness uploads it to every node at each
test's setup, so the test running kept its binary and test 92 on ran beta.6. `/health` says
2.6.0-beta.1 and 2.6.0-beta.6.

## `p11-w5-slow-lz-kill`: the final drain, not a lost message

The test runs the pipeline workload (read an input, write its output and ack the input in one
transaction) under kills, with `n4` and `n5` fsyncing 300 ms late through lazyfs, which drops what a
kill catches unsynced. Its checker counts an acknowledged input as lost when no output for it is in
the last final read. The final reads come after the drain, and the harness caps the drain at
120 seconds (`--final-time-limit`) and ends each drain thread after eight empty pops; a pop is also
empty when every one of the four partitions is leased by another worker.

In the run of 2026-10-21 16:19 UTC, on beta.1 (`main/results/p11-w5-slow-lz-kill.edn`), 1,323 of
3,798 acknowledged inputs had no output when the final reads were taken, right after the last kill,
and 1,633 transactions committed after them, to the end of the test: the inputs were still being
processed when they were counted. Nothing else was wrong: no output written twice, none without its
input, every committed transaction's output present. The run's throughput had collapsed under the
kills, 3,798 inputs where the reruns took 15,500 to 21,000, which left more backlog than the drain
had time for.

The four reruns (`rerun/`), on beta.6, from 05:05 to 05:31 UTC on 10-22: valid, valid, valid, and
valid with `--final-time-limit 600`. The harness still owes a drain that checks the input queue's
depth before its final read.

## `p11-w3-lead-deaf`: the checker's memory

The register workload (W3: a compare-and-set register per key, checked for linearizability by knossos'
linear analyzer) under a leader that stops answering. Its run of 2026-10-21 22:58 UTC, on beta.6,
came back `:valid? :unknown` with `:cause :out-of-memory` on keys 0 and 1, the other 115 keys valid,
with the control's 12 GB heap. A history the checker cannot finish is not a violation; it is no
evidence either.

After the archive, the control was resized to `e2-highmem-8` (64 GB) and the stored history checked
again with `lein run analyze -t <run>` from a copy of the harness with a 56 GB heap
(`reanalyze/`; the campaign's verdict is `main/results/p11-w3-lead-deaf.edn`):

- **knossos' linear search** (the campaign's): the search of key 0 put its cost at 3.9e26
  configurations, the heap reached 49 GB within three minutes, and for the next 13 minutes there was
  neither a progress line nor a garbage collection; it was stopped by hand
  (`reanalyze/reanalyze-linear-56g.log`).
- **knossos' WGL search** (`:algorithm :wgl`, `reanalyze/register-wgl.diff`, the only change): the
  analysis took 2 seconds and every one of the 117 keys is `:valid? true`, keys 0 and 1 included
  (`reanalyze/p11-w3-lead-deaf.wgl.edn`, `reanalyze/reanalyze-wgl.log`).

Both searches decide the same thing, whether a linearization of the history against the
compare-and-set register exists; WGL found one for every key. The harness itself still runs the
linear search.

## Rolling upgrade 2.5.0 -> 2.6.0 and back

`upgrade/roll26.sh`, run from the control against three brokers on `n1` to `n3` (`upgrade/node.sh` on
each), after the Jepsen lists, with the queues of the 2.5 test: `hist` (192,000 messages written by
2.5.0, read back at the end), `live` (three `qload` loaders, 1,500 msg/s each, each through its own
node) and `dup` (`upgrade/dup.py`: sets of 2,000 messages with fixed `transactionId`s, pushed once and
then again; every second answer must be `duplicate` naming the first push's offset).
`upgrade/roll.log` is the run.

| Step | What | Result |
|------|------|--------|
| 0 | a fresh 2.6.0 cluster | cluster version 7, `kinds` 7 on all three |
| 1 | a fresh 2.5.0 cluster | version 7; history written (192,000, 0 errors), set A queued |
| 2 | `n3`, then `n2`, on 2.6.0; `n1` (the leader) still on 2.5.0 | version 7 with the mix; set A all duplicates with their offsets, set B queued |
| 3 | `n3` back to 2.5.0 on the directory 2.6.0 wrote, then forward again | starts and follows; set B all duplicates |
| 4 | `n1`, the last 2.5.0 member, on 2.6.0 | every node on 2.6.0, version 7 |
| 5 | after the roll | set C queued, sets A, B, C all duplicates |
| 6 | the whole cluster stopped and started on 2.6.0 | sets A, B, C all duplicates; the history read back whole: 192,000 popped, 0 errors |
| 7 | `kill -9` of the leader (`n2`) under load | `n3` leads; sets A, B, C all duplicates; set D queued, then all duplicates |
| 8 | `n3` back on 2.5.0 on its own directory, after 2.6.0 wrote everything | serves, lag 0; sets A to D all duplicates; then 2.6.0 again |

Traffic over the roll (steps 1 to 5, 360 s, 4,500 msg/s offered): 1,605,700 messages pushed,
1,606,680 popped. The 980 more are redeliveries of the 980 acks that failed while a node restarted.
491 pushes failed, each a transport error or a 503 at the loader whose own node was restarting:
`qload` talks to one node and never retries. In step 7, the loader of the killed leader lost 546
pushes the same way, and 40 acks answered `outcome_unknown` across the leader change; those 40 came
back and were popped again.

Node logs over all starts: no FATAL, panic, `poisoned` or unknown-version line on any node. Every WARN
is openraft's `Unreachable node` or a failed heartbeat towards a peer that was restarting.

Not covered: five nodes; a roll under the Jepsen nemeses; a member that comes back with a delta
snapshot during the roll (no node was away long enough for the leader to purge past it).

## Jepsen

The harness is `test/jepsen` of `68f83bd73` (`/root/jepsen-26`, heap `-Xmx12g` on the 16 GB control,
`scripts/project.clj.as-run`). lazyfs 0.3.0 was built on all six nodes before the first test.

| When (UTC) | What |
|---|---|
| 10-21 07:46 to 07:48 | the setup check `s-w8-none`: valid |
| 10-21 07:48 | the main list starts (`scripts/go-26.sh`, `scripts/campaign-26.sh`) |
| 10-21 16:33 | the binary swap, during test 91 |
| 10-22 04:58 | the main list ends: 211 valid, 1 INVALID, 1 UNKNOWN |
| 10-22 04:58 | the failover list starts (`scripts/campaign-26-fo.sh`) |
| 10-22 05:05 to 05:31 | the failover list paused, `p11-w5-slow-lz-kill` four times (`scripts/rerun-26.sh`); the interrupted failover test ran again on resume |
| 10-22 08:25 | the failover list ends: 31 valid |
| 10-22 10:00 | the batched-push list ends: 13 valid (`scripts/campaign-26-batch.sh`) |
| 10-22 10:01 to 10:11 | the rolling upgrade (above), then the archive (`scripts/after-26.sh`, `scripts/collect-jepsen-26.sh`) |
| 10-22 10:15 to 10:29 | `p11-w3-lead-deaf` checked again on the resized control: the linear search stopped, the WGL search valid (`reanalyze/reanalyze-wgl.sh`) |

About 6 minutes a test. Production has run 2.6.0-beta.5, the code of beta.6 without the purge-race fix
(`ea823f809`), since 10-21 16:02 UTC.

Each list's folder holds its `matrix.txt`, `summary.txt` (one line per test start and end),
`binary.md5` and `results/<test>.edn` (each test's `results.edn`). The histories and node logs stayed
on the control.

**Not covered: the Kafka facade.** The Jepsen workloads use Queen's own HTTP API; the `log` workload
applies the Kafka checker to it, never the facade, which 2.6.0 rewrote. The facade's own tests and the
client suites run against it are in `protocols/queen-kafka/`.
