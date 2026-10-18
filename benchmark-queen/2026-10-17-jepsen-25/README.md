# Jepsen and a rolling upgrade for 2.5.0, 2026-10-17 and 18

Branch `next-release`, broker 2.5.0: commit `46b2d2eb1` "server 2.5.0", plus docs in `3646d217f`.

What changed since 2.4.0, and why the whole campaign ran again:

- openraft moves from a git revision to the 0.10.0-alpha.37 crate;
- the batcher gains a "proposed" hook for the Kafka facade's ordered pushes;
- a windowBuffer hold ends one window after the oldest waiting message;
- the facade's fetch wake, pipelined and relayed produce, and catalog mutex.

The cluster version stays 7.

One control node and six test nodes, `n1` to `n6`: DigitalOcean droplets in one VPC (fra1), Ubuntu
24.04, the nodes 2 vCPU / 4 GB, the control 4 vCPU / 16 GB. The harness is in git (`test/jepsen` of
`46b2d2eb1`, unchanged for the campaign). This folder holds what git does not.

## Result

**Jepsen: 257 tests, 256 valid on the first run.** The 257th, `p11-w1-corrupt-all`, failed only the
harness's panic checker, on its own deliberate damage, in both of its runs; the fixed checker passes
both (below).

- **Main list**, 213 tests (`test/jepsen/matrix-220.txt`, the list 2.2.0, 2.3.0 and 2.4.0 passed),
  on `n1` to `n5`: 212 valid, `p11-w1-corrupt-all` INVALID.
- **Failover**, 31 tests (`test/jepsen/matrix-220b2.txt`, a standby cluster's failover, the source on
  `n1` to `n3`, the standby on `n4` to `n6`): 31 valid.
- **Batched pushes**, 13 tests (`test/jepsen/matrix-24-batch.txt`, pushes of up to 8 messages to one
  partition, the path the batcher's new hook sits on): 13 valid.
- **`p11-w1-corrupt-all` again**, once, after the batched list (`rerun/`): INVALID for the same
  reason.

**Rolling upgrade 2.4.0 -> 2.5.0 and back: passed** (section below). The cluster version stays 7
throughout. No push is refused during the mix, and every duplicate is found across the mix, a full
restart and a `kill -9` of the leader. 2.4.0 starts on, and serves, a directory 2.5.0 wrote.

## The builds

`binaries.md5`:

| File | md5 | What |
|------|-----|------|
| `queen-25` | `485eb2180541ec943ae3802d38540e17` | 2.5.0: `next-release` at `3646d217f` (server code of `46b2d2eb1`), the release Dockerfile, linux/amd64 (`./build.sh --amd64 --registry queen-jepsen25 --tag 2.5.0-3646d217f`; local image only). The same binary as `ghcr.io/queen-mq/queen:2.5.0-beta.1` on stage. |
| `queen-240` | `b096e5c2622d0fb3071d0b3fb1465f93` | the released 2.4.0 amd64 (`ghcr.io/queen-mq/queen@sha256:1a3e1cf8...`, what stage ran before and prod runs), for the upgrade test |

The broker has no `--version` flag (`queen-25 --version` starts a broker). `/health` says 2.5.0 on
`queen-25` and 2.4.0 on `queen-240`.

## `p11-w1-corrupt-all`: the panic checker, not the broker

The corrupt nemesis kills one node, damages one of its files on purpose (bitflip or truncation of a
queue log, the checkpoint `store/data.mdb` or `raft/state.json`), starts it, and watches. A node that
refuses to boot gets its copy back (remedy `restore`) or is wiped and rejoins (`rejoin`). Every other
checker passed in both runs: the workload (no anomaly), the faults (every trial back:
`corrupt-not-back []`), the members, lazyfs, the clocks.

| Run | Trials | Refusals (exit 1) | NA-QLOG-I1 lines | Old checker | Fixed checker |
|-----|--------|-------------------|------------------|-------------|---------------|
| 1, main list, 10-18 02:48Z | 21 | 18 | 3, all `n2` | INVALID | valid, 3 excused |
| 2, `rerun/`, 10-18 15:21Z | 25 | 20 | 1, `n5` | INVALID | valid, 1 excused |

Each NA-QLOG-I1 line is the victim's boot refusal within a second of the nemesis truncating one of
its queue logs, for example `queue logs' durable tail 19279 is BEHIND the store's qlog-durable index
19280: a committed record is missing (NA-QLOG-I1)`. That is the designed fail-stop: the boot's global
tail check finds a cut of the newest entries, which the per-queue check ("ends at seq N below seq M")
cannot see. The old checker, `log-file-pattern #"panicked at|NA-QLOG-I1|poison"`, failed any line that
named the code. 2.4.0's run of the same test passed because none of its 6 queue-log refusals went
through this check, not because the broker behaved differently.

The fix, `panic-checker` in `test/jepsen/src/jepsen/queen/core.clj`, excuses an NA-QLOG-I1 line only
when the nemesis recorded that exact line as the reason a trial of its victim was `:refused`. An
NA-QLOG-I1 at runtime, after a restore or on another node still fails the test, and so does a panic
or a poisoned lock on any node. `rerun/panic-checker-fixed.txt` is the fixed checker over both stored
runs (`rerun/reanalyze-panic.clj`, run read-only on the control). With the nemesis's
`:corrupt-file` records taken out of the history, both runs fail again on the same lines.

## Rolling upgrade 2.4.0 -> 2.5.0 and back

`upgrade/roll25.sh`, run from the control against three brokers on `n1` to `n3` (`upgrade/node.sh` on
each), after the Jepsen lists, with the queues of the 2.4 test: `hist` (192,000 messages written by
2.4.0, read back at the end), `live` (three `qload` loaders, 1,500 msg/s each, each through its own
node) and `dup` (`upgrade/dup.py`: sets of 2,000 messages with fixed `transactionId`s, pushed once and
then again; every second answer must be `duplicate` naming the first push's offset).
`upgrade/roll.log` is the run.

| Step | What | Result |
|------|------|--------|
| 0 | a fresh 2.5.0 cluster | cluster version 7, `kinds` 7 on all three |
| 1 | a fresh 2.4.0 cluster | version 7; history written (192,000, 0 errors), set A queued |
| 2 | `n3`, then `n2`, on 2.5.0; `n1` (the leader) still on 2.4.0 | version 7 with the mix; set A all duplicates with their offsets, set B queued |
| 3 | `n3` back to 2.4.0 on the directory 2.5.0 wrote, then forward again | starts and follows; set B all duplicates |
| 4 | `n1`, the last 2.4.0 member, on 2.5.0 | every node on 2.5.0, version 7 |
| 5 | after the roll | set C queued, sets A, B, C all duplicates |
| 6 | the whole cluster stopped and started on 2.5.0 | sets A, B, C all duplicates; the history read back whole: 192,000 popped, 0 errors |
| 7 | `kill -9` of the leader (`n2`) under load | `n1` leads; sets A, B, C all duplicates; set D queued, then all duplicates |
| 8 | `n3` back on 2.4.0 on its own directory, after 2.5.0 wrote everything | serves, lag 0; sets A to D all duplicates; then 2.5.0 again |

Traffic over the roll (steps 1 to 5, 360 s, 4,500 msg/s offered): 1,607,660 messages pushed, 1,608,740
popped. The 1,080 more are redeliveries of the 1,080 acks that failed while a node restarted. 391
pushes failed, each one a transport error of the loader whose own node was restarting: `qload` talks
to one node and never retries. In step 7, the loader of the killed leader lost 515 pushes the same
way.

Node logs over all starts: no FATAL, panic, `poisoned` or unknown-version line on any node. Every WARN
is openraft's `Unreachable node` or a failed replication towards a peer that was restarting.

Not covered: five nodes; a roll under the Jepsen nemeses.

## Jepsen

The harness is `test/jepsen` of `46b2d2eb1` (`/root/jepsen-25`, heap `-Xmx12g` on the 16 GB control,
`scripts/project.clj.as-run`). Every test ran `queen-25`, md5 `485eb218`. lazyfs 0.3.0 was built on
all six nodes before the first test.

| When (UTC) | What |
|---|---|
| 10-17 15:05 to 15:08 | the setup check `s-w8-none`: valid; every `/health` sample read cluster version 7 (`setup-check/cv.txt`) |
| 10-17 15:08 | the main list starts (`scripts/go-25.sh`, `scripts/campaign-25.sh`) |
| 10-18 11:07 | the main list ends: 212 valid, 1 INVALID |
| 10-18 13:58 | the failover list ends: 31 valid (`scripts/campaign-25-fo.sh`) |
| 10-18 15:20 | the batched-push list ends: 13 valid (`scripts/campaign-25-batch.sh`) |
| 10-18 15:21 to 15:27 | `p11-w1-corrupt-all` again (`scripts/after-25.sh`): INVALID, same reason |
| 10-18 15:27 to 15:37 | the rolling upgrade (above), then the archive (`scripts/collect-jepsen-25.sh`) |

About 5.7 minutes a test. Stage ran `2.5.0-beta.1` (this binary) from 10-17 16:50.

Each list's folder holds its `matrix.txt`, `summary.txt` (one line per test start and end),
`binary.md5` and `results/<test>.edn` (each test's `results.edn`). The histories and node logs stayed
on the control.

**Not covered: the Kafka facade.** The Jepsen `log` workload uses the Kafka checker over Queen's own
HTTP API, never the facade, yet the facade's produce path changed the most. The facade across a
leader change was tested on the OpenMessaging Benchmark rig instead (`../2026-10-16-omb/RESULTS.md`,
section 4): a `kill -9` and a graceful stop of the leader under 200k msg/s of idempotent producers,
0 records lost, 0 reordered. That is one fault at a time, not Jepsen's nemeses.

## Watching from the Mac

`JEPSEN_CTL=<control address> ./watch-campaign.sh watch-state.txt` polls the control every 5 minutes
and exits when a list ends, the archive is collected, or a test ends other than valid
(`watch-state.txt` lists what it already reported).
