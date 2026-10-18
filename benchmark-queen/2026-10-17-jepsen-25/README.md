# Jepsen and a rolling upgrade for 2.5.0: PREPARED, NOT STARTED

Branch `next-release`, broker 2.5.0: commit `46b2d2eb1` "server 2.5.0", plus docs in `3646d217f`.

What changed since 2.4.0, and why the whole campaign runs again:

- openraft moves from a git revision to the 0.10.0-alpha.37 crate;
- the batcher gains a "proposed" hook for the Kafka facade's ordered pushes;
- a windowBuffer hold ends one window after the oldest waiting message;
- the facade's fetch wake, pipelined and relayed produce, and catalog mutex.

The cluster version stays 7.

## What runs

| Step | Where | Duration | Pass means |
|------|-------|----------|------------|
| Rolling upgrade 2.4.0 -> 2.5.0 and back, `upgrade/roll25.sh` | n1..n3 from the control | ~10 min | version 7 throughout; no push refused; every duplicate found across the mix, a full restart and a `kill -9` of the leader; 2.4.0 starts on data 2.5.0 wrote |
| Setup check `s-w8-none` (`smoke-25.sh`) | n1..n5 | 2 min | valid, cluster version 7 on every node |
| Main list, `test/jepsen/matrix-220.txt` (213) | n1..n5 | ~20 h | 213 valid (2.2.0, 2.3.0 and 2.4.0 passed it) |
| Failover, `matrix-220b2.txt` (31) | n1..n6 | ~3 h | 31 valid |
| Batched pushes, `matrix-24-batch.txt` (13) | n1..n5 | ~1.3 h | 13 valid |

`scripts/go-25.sh` runs the setup check, then the three lists one after the other (the 2.4
campaign's scripts, with paths moved to `-25`). The total is about 25 hours after setup. For
reference, the 2.4 campaign ran from 10-15 13:20Z to 10-16 13:32Z.

**Not covered: the Kafka facade.** The Jepsen `log` workload uses the Kafka checker over Queen's own
HTTP API, never the facade, yet the facade's produce path changed the most. Two options:

- **Cheap (~1 h of the same droplets):** after the batch list, run `kfail`
  (`../2026-10-16-omb/failover/`) on n1..n3 with the facade on. It covers `kill -9`, a graceful
  stop, a network partition and a pause of the leader, a few rounds each.
- **Proper (about a day of work):** a Jepsen workload that drives the facade with a Kafka client.

## Binaries

Not in git; md5 of each below.

| File | md5 | From |
|------|-----|------|
| `queen-25` | to build | `./build.sh --amd64 --registry queen-jepsen25 --tag 2.5.0-46b2d2eb1` from a clean worktree at `46b2d2eb1` (8 to 18 min; local image only, nothing pushed) |
| `queen-240` | `b096e5c2622d0fb3071d0b3fb1465f93` | `ghcr.io/queen-mq/queen@sha256:1a3e1cf8...` (the released 2.4.0 amd64, what stage and prod run), `/app/bin/queen` |
| `qload` | `e54d7bc04d742d4938b6ad08fa5e6dee` | `../2026-09-30-kafka-pulsar/mqload/cmd/qload`, linux/amd64 |

The broker has no `--version` flag: `queen-25 --version` starts a broker. Tell the builds apart by
md5, or by the version `/health` reports.

## Machines: 7 DigitalOcean droplets, needs Alice's go

Same shape as the 2.4 campaign. There are no droplets now; the 2.4 ones were deleted.

- Region fra1, one VPC, Ubuntu 24.04.
- n1..n6: `s-2vcpu-4gb`, $0.0357/h each.
- Control `jepsen-ctl`: `s-4vcpu-16gb-amd`, $0.125/h, with the JVM heap at `-Xmx12g`.
- Together $0.34/h: about **$10** for 30 hours including setup and collection.

Setup, as for 2.4 (`test/jepsen/RESEARCH.md`, "setup"):

1. Map n1..n6 to their private addresses in `/etc/hosts` on every machine.
2. Put the control's key in every node's `authorized_keys`.
3. Install a JDK and Leiningen 2.11 or later on the control.
4. Unpack `git archive 46b2d2eb1 test/jepsen` into `/root/jepsen-25`, and put `scripts/*` beside it.
5. Copy the binaries to `/root/bin` and `/root/roll/bin`, and `upgrade/*` to `/root/roll` on the
   control and on n1..n3.
6. Build lazyfs on all six nodes before the first test. The harness has its own installer. A node
   that never built it made a test fail in 2.2.0 and in 2.3.0.

## After

1. Run `scripts/collect-jepsen-25.sh` on the control, and copy the archive into this folder.
2. Fill in the result here.
3. Delete the droplets, on her word.
4. Then the 2.5.0 release, with the steps and traps of 2.4.0 (memory: the 2.4.0 release note).
