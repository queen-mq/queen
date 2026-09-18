# difffuzz regression fixtures

A **failing seed is the bug report** (PLAN_RAFT.md §13.4). When `campaign.sh`
finds a divergence it writes the run's report here as `seed-<N>.txt`. Each report
carries the seed, the run-id, the operation mix and the exact replay command,
plus the first divergence (operation index, JSON path, the two sides' values, and
the request that produced it).

## Replaying a fixture

The last line of every report is the replay command. It targets a fresh pair of
brokers (side A a postgres broker, side B a raft broker); because it pins the
`-run-id`, the queue/group/transaction names are the same, so against two fresh
brokers the sequence reproduces exactly:

```sh
GOWORK=off go run . -a http://127.0.0.1:6632 -b http://127.0.0.1:7632 \
    -seed <N> -ops <N> -mix <mix> -run-id <run-id> -queues 3 -partitions 4 -groups 3 -dup-rate 15
```

Add `-v` to print every operation, and drop `-stop-on-diff` (it defaults on) to
collect every divergence in the run rather than only the first.

## What is NOT a fixture here

The **classified phase-1 parity gaps** between the postgres oracle and the raft
facade are catalogued in `../PARITY-NOTES.md`, not here: the fuzzer absorbs each
one (with the reason, at the absorbing site) so the campaign can reach the
message-path semantics underneath, and each is a finding for WP-1.7/WP-2.6, not
an open regression. A file in THIS directory is a divergence the fuzzer did NOT
expect — a genuine bug to run down, fix (if small and inside `rsm/`) or report
with its seed.
