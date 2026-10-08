# Jepsen, the locks campaign, 2026-10-08

Copied from the Jepsen control node `178.128.199.140` (nodes `n1` to `n5`, DigitalOcean) before
the VMs were released. The harness is in git (`test/jepsen`; the workloads are W11 `locks` and W11b
`semaphore`, in `src/jepsen/queen/workload/locks.clj`). This folder holds what git does not.

## What ran

- **Build:** commit `2287397a3`, which is 2.0.3 with locks, the KV `check` op and cluster version
  5. Built on the control node with `cargo build --locked --profile fastrel --bin queen`; its
  checksum is in `binary.md5`. It is the locks branch before its merge with 2.0.4 and the standby
  cluster: the merged tree was not run.
- **Tests:** 41, from 11:57Z to 15:39Z. `matrix-main.txt` has 3 smokes without faults, 14 lock and
  semaphore tests and 10 KV and transaction tests. `matrix-extra.txt` has 12 more lock and
  semaphore tests, the pipeline under restarts and Elle under a deaf leader.
- **Result:** 41 of 41 valid.

## Files

| Path | Contents |
|---|---|
| `summary.txt` | one `start` and one `done` line per test, as `run-campaign.sh` wrote them |
| `table.txt` | per test: the verdict and, for the lock workloads, what the checker counted |
| `results/<test>.edn` | each test's `results.edn` |
| `matrix-main.txt`, `matrix-extra.txt` | the tests as they ran |
| `two-slots-x-sem-clock.txt` | the one thing the checker reports and does not judge, below |
| `binary.md5` | checksum of the tested binary |

The Jepsen stores (every history, the Jepsen logs and each node's `queen.log`, 4.7 GB) were not
copied.

## What the lock tests counted

Over the 29 lock and semaphore tests (`table.txt`, last line):

| | |
|---|---|
| guarded transactions that committed | 373,312 |
| transactions the guard refused, their token no longer the permit's | 3,245 |
| transactions with an unknown outcome | 816 |
| permits that changed hands | 18,456 |
| permits handed over before the lease could have ended (not judged under clock faults) | 0 |
| tokens that went back along a guarded partition | 0 |
| records of a transaction that rolled back | 0 |

## An owner in two slots, under clock strobes

`x-sem-clock` (a semaphore of 3 under clock jumps and strobes) is the only test where a `get`
showed one owner in two slots: 51 reads, 12 pairs of permits, all after the first strobe began.
The checker reports this and does not judge it.

`two-slots-x-sem-clock.txt` lists them and traces the first. The owner held slot 1 with a 4 second
lease, taken at 187.40 s. At 189.33 s, with a strobe running, its next `acquire` was answered with
slot 2 and a new token, and not with `already`: the permit in slot 1 looked expired at that
moment. The acquires right after answered `already` for slot 1 again, and the reads showed both
slots for about a second. Three of the twelve pairs disappear and come back between reads, which
is what a strobe, moving a clock back and forth, would do to a lease.

A lease is timed on a clock, so a clock that jumps changes how long it lasts. No semaphore gave
out more permits than its limit (the slots are fixed), and no token went back.
