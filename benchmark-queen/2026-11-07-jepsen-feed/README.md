# Jepsen for the tenant feed (W13), 2026-11-07

Branch `srf-sdk`. **W13 found two bugs, both in the feed's resolvedTs, and both are fixed in
`a6e85381d`.** On the fixed code, **21 W13 tests, all valid at their first run**, three of them with
every read relayed by the leader. The first campaign's 24 regression tests (log, queue, pipeline,
streams) were all valid.

One control node and five test nodes, `n1` to `n5`: GCP VMs in one VPC (`smartness-stage`,
europe-west1-b), the nodes `e2-standard-2` (2 vCPU / 8 GB), the control `e2-standard-8`
(8 vCPU / 32 GB), set up with `../2026-10-30-jepsen-27/scripts/`. The harness is `test/jepsen` of
`a6e85381d`: W13 is `src/jepsen/queen/workload/feed.clj`. This folder holds what git does not.

## The workload

Writers push to 48 partitions of the test queues: one message, 2 to 4 messages of one partition, or
one transaction of 2 to 4 pushes to different partitions. Readers read the tenant feed
(`POST /api/v1/feed`, queues `jepsen-*`) from the start, on every node, with pages of 1 to 256
records and short long polls: five read it whole, five read one shard each. A reader starts on its
own node and moves to the next one when a read fails. At the end one transaction covers every shard,
and each reader reads until it has it.

The checker rejects:

- an acknowledged message that a reader selecting it did not read exactly once, a failed push that
  was read, or a message no push sent;
- two orders, a hole, part of a transaction, or an answer with part of a position;
- an unsafe resolvedTs (a message at or after `next` stamped at or before it), or one that goes back
  between two answers of one node with no restart in between;
- stamps that do not rise with position;
- a shard reader that reads another shard's partitions.

The checker ran first on crafted histories: one clean, 18 with a planted bug each, and 3 for the
relay. `synthetic.txt`: 13 tests, 213 assertions, 0 failures.

## The bugs

| Found by | What went wrong | Fixed in `a6e85381d` |
|---|---|---|
| `w13-smoke`, no faults, on the build before the campaign | a caught-up answer could claim a resolvedTs at or past the stamp of the entry at its `next`. The node read its applied index before its clock, and an entry that applied in between gave it the next entry's clock | `feed.rs` reads the clock first. Test `a_caught_up_answer_claims_no_stamp_past_its_next_while_entries_apply` |
| `w13-kill`, `w13-part`, `w13-idx-kill` | resolvedTs went back by 1 µs on one node. An answer cut short claimed the stamp of its newest entry. A new leader's empty entry moves the applied index but not the clock, so the next caught-up answer claimed 1 µs less | a partial answer claims at most what a caught-up one would. Test `resolved_ts_does_not_go_back_over_a_no_op_entry` fails on the old line 3 times in 3 |

No run of either campaign lost a message, read one twice, left a hole or served an unsafe
resolvedTs.

## The runs

| Run | When (UTC) | Build | Tests | Result |
|---|---|---|---|---|
| First campaign | 11:51 to 15:57 | the srf-sdk tree with the first fix | 43: W13 without faults (60 s); 18 W13 under faults; 24 regression | 40 valid; the other 3 were the second bug |
| Rerun | 15:57 to 17:56 | the srf-sdk tree of 13:21 with both fixes and the feed relay | 21: the 3 that failed; 3 with `--feed-relay`; the other 15 W13 under faults | 21 valid |

The 18 W13 tests under faults: kill, power loss (lazyfs), partition, pause, clock, deaf leader,
restart, membership, partition with kills, pause with kills, bridge; kill, power loss and
partition on 256 KiB log files sealed after 10 s; kill and partition on shared logs (four shards);
partition and deaf leader with the leader serving feed reads (`QUEEN_FEED_ON_LEADER=true`). The 24
regression tests: W1 `log`, W2 `queue`, W5 `pipeline` and W9 `streams`, each under kill, partition,
pause, clock, deaf leader and restart. Lists: `test/jepsen/matrix-w13-smoke.txt` and
`test/jepsen/matrix-w13-2026-11-07.txt`, then `matrix-rerun.txt`.

The rerun's broker files are those of `a6e85381d`, checked file by file, except three files of the
embedded proxy (`proxy/src/config.rs`, `gateway.rs`, `routes.rs`), which no test runs.

A W13 test under faults acknowledged 12,867 to 44,336 messages, and its readers took 15,491 to
39,064 answers. Over the 40 W13 runs: 1,314,843 messages acknowledged.

## The relay runs

With `--feed-relay` every read sends `x-queen-feed-relay: 1`, the header the proxy sends, so a
leader passes the read to a follower instead of answering 421.

- **Without the header**, the leader answered 421 2 to 58 times per test. It was 2 in the quietest
  tests (no faults, clock, bridge): the two readers that start on the leader.
- **With it**, 0, 0 and 1. The one, under a deaf leader, came back through a relay, most likely
  from a follower that had just become leader. The reader moved on, and nothing wrong was served.

A relayed answer does not say which follower gave it, so in these runs a resolvedTs step back is
listed, not judged: 2, 7 and 3.

## Files

- `summary.txt`: the first campaign, one line per test start and end.
- `summary-rerun.txt`: the rerun, the same way.
- `results/<test>.edn`, `results-rerun/<test>.edn`: each test's `results.edn`.
- `matrix-rerun.txt`: the rerun's list.
- `synthetic.txt`: the checker's own tests.

The histories and node logs were deleted with the VMs.

## Not covered

- **The proxy**: the relay runs send its header straight to the nodes.
- **The stream runners on the feed** (`Stream.fromFeed` and its ports): W13 reads the HTTP feed.
  The runners have their own tests and the conformance kit, `clients/conformance/feed`.
- **Several tenants, standby failovers**: W13 reads one tenant's feed on one cluster.
- **A refused start between tests**: `run-matrix.sh` does not read the nodes' logs the way
  `run-campaign.sh` does. Each test's panic checker read every node's log. Its member check found
  all five nodes answering as voters at the end.
