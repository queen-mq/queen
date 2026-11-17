# Jepsen for 2.9.0, 2026-11-16 and 17

Branch `next-release`. **568 tests, all 568 valid**: 561 at their first run, 5 again after the
fixes their first run led to, and 2 whose only failures were wrong answers under clock jumps far
past the 500 ms the cluster allows, which their checkers now count and do not judge.

Four rigs in one VPC (`smartness-stage`, europe-west1-b), each a control node `e2-standard-8`
(8 vCPU / 32 GB, heap `-Xmx12g`) and six test nodes `e2-standard-2` (2 vCPU / 8 GB): `n1` to `n5`
for the five-node tests, all six for the standby failovers. The list was cut into four shares, one
per rig. This folder holds what git does not.

## The builds

| Tests | Build | md5 |
|-------|-------|-----|
| 568, the campaign | the broker built from `4b641a866` on rig 1 and copied to rigs 2 to 4: its server code is that of 2.9.0-beta.3 (`df952a5da`). The harness is `test/jepsen` of `4b641a866`. | `94dd10fc4fd8d222abbc64a88f0655fb` |
| 5, the reruns | the broker built from `72d5e6360` on rig 1: the server code of 2.9.0, which is beta.3's plus the ephemeral hand-over fix (`5543add64`). The harness of `72d5e6360`, with the seek test of `fc0b6f978`. | `73d4cb26439c834680e4688edcfa2ce4` |

Between beta.3 and 2.9.0 the server changed only in `server/src/ephemeral.rs` and
`server/src/handlers/ephemeral.rs` (the fix, with its tests) and in its version lines.

## The list

`matrix/rig<k>-gate.txt`, `rig<k>-rest5.txt` and `rig<k>-six.txt`, run top to bottom on rig `k`:

- **59 tests, the gates**, first on every rig: what changed most since 2.8, reading queues (13),
  Elle over `/api/v1/transaction` (13), the PostgreSQL source (6) and sink (6), KV ttl (5), queue
  options (4), scoped keys (4), tenants (3), and one each of DLQ operations, Elle, the log, the
  queue and seek. 59 valid.
- **451 tests on five nodes**, every other workload of the harness.
- **58 tests on six nodes**, the standby failovers.

By workload, as the harness names them: queue 66, failover 58, log 49, pipeline 45, kafka 30,
retention 20, duppush 19, feed 18, locks 17, streams 16, register 16, dedup 16, txnmix 13,
txnelle 13, timers 13, reading 13, filtered 13, semaphore 11, dlq 11, tenants 10, link 10, elle 10,
queueopts 9, claim 9, s3sink 8, feedstream 8, counter 7, pgsource 6, pgsink 6, dlqops 6, kvttl 5,
ephemeral 5, seek 4, scoped 4, lifecycle 4. By fault: kill 181, partition 87, pause 55, restart 50,
none 45, leader-deaf 37, membership 34, clock 31, packet 15, bridge 14, upgrade 9, corrupt 6,
runner 5, all 5, disk-full 3, pause-kill 2, part-kill 2, snap-kill 1; 60 of them on lazyfs, with
power loss.

## Not valid at their first run, and what became of them

| Test | Rig | What the checker found | Now |
|------|-----|------------------------|-----|
| `eph-restart`, `eph-kill`, `eph-part`, `eph-pause` | 1, 3, 4, 4 | an ephemeral partition that moved to another node lost messages (a new ring knew only the groups that had popped there, and freed what the first of them took) and handed some out twice (a ring that landed in a busy one dropped every group's place) | the broker fix `5543add64`; all four valid on the 2.9.0 build (`results-rerun/`) |
| `seek-kill` | 2 | the test's only two seeks both met killed nodes and failed, so nothing about seek was checked | the harness sends a seek again until one is answered (`fc0b6f978`); valid (`results-rerun/`) |
| `long-pipeline` | 4 | one step answered rolled back (`rejected_ack`) though its output exists: the node that relayed it sent it again to a new leader whose clock was 202 s ahead, which had retired the answers kept for such retries, so the step ran again | counted under clock faults (`3120e4729`); its stored history checked again: valid, 1 answer counted (`recheck/`) |
| `x-lock-all` | 2 | two steps answered rolled back (`duplicate`) though their records exist: the same retry, after n5 took over with its clock 155 s ahead | counted under clock faults (`fbf602761`); its stored history checked again: valid, 2 answers counted (`recheck/`) |

In the last two nothing was lost or committed twice: the retry was refused, and only its answer
was wrong. A cluster whose clocks agree within `QUEEN_RAFT_MAX_CLOCK_SKEW_MS` (500 ms) keeps
those answers for 60 s (`QUEEN_RAFT_REQUEST_ID_WINDOW_S`), give or take that skew.

The recheck ran `lein run analyze` on each stored test with the harness of `72d5e6360` and the lock
checker of `fbf602761` (`scripts/recheck-c4.sh`): it rebuilds the test from its stored command line
and checks the stored history again.

## Timeline

| When (UTC) | What |
|---|---|
| 11-16 10:02 | the four rigs start; rig 1 builds the broker |
| 11-16 11:15 | the broker copied to rigs 2 to 4. The build had stalled an hour: our script logged the binary's `--help`, and the broker has no flags, so that started a broker and waited on it |
| 11-16 11:17 | a first short test on each rig, valid |
| 11-16 12:40 to 12:54 | the four gates done: 59 valid of 59 |
| 11-17 01:15 to 01:57 | the four shares done: 568 tests, 561 valid; each rig powers off |
| 11-17 06:08 to 06:50 | the five reruns on rig 1, on the 2.9.0 build: 5 valid |
| 11-17 07:15 | the two stored histories checked again: both valid |

About 6 minutes a test on each rig.

## Files

- `matrix/`: each rig's share, and `reruns.txt`.
- `results/<test>.edn`: each test's results as its first run left them. `results-rerun/`: the five
  reruns. `recheck/`: the new results of the two stored histories (their first are in `results/`).
- `summary.txt`: every test's start and end line, rig by rig, then the reruns.
- `logs/`: each rig's orchestrator log, and the reruns'.
- `scripts/`: what ran the campaign (`orchestrate-6.sh`, `start-6.sh`, `fanout-6.sh`), read its
  progress (`news-rig6.sh`), ran the reruns (`orchestrate-7.sh`, `start-7.sh`), the recheck
  (`recheck-c4.sh`) and made this copy (`fetch-results.sh`).
- `binary.md5`: the two builds.
