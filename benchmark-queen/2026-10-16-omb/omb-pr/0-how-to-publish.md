# Publishing the OMB driver: NOT done, needs Alice's go

Everything is local in `~/Work/openmessaging-benchmark`. Nothing has been pushed, forked or opened.

| Branch             | Commits                        | Check                                                     |
|--------------------|--------------------------------|-----------------------------------------------------------|
| `fix-spotless-443` | 288cfbc (the master fix)       | `mvn --no-transfer-progress --batch-mode verify`: SUCCESS |
| `queen-driver`     | 288cfbc + 1b56e95 (the driver) | same command: SUCCESS; 3 local smoke runs on Queen 2.4.0  |

Two PRs, because master fails `verify` since #443 (upstream run 38648660840 failed on it, 2026-10-14).
PR 1 is that fix alone. PR 2 carries the same commit, so its CI passes either way.

The files 1-fix-verify.md and 2-queen-driver.md are the PR bodies as they are. Before opening PR 2:
- Its text says "four of the five families": many topics has run since, so it can say all five.
- The last smoke run (2026-10-18: the new `queen.yaml` with `pollers: 8`, Queen 2.4.0 in Docker) stalled
  3 of 10 partitions for the rest of the 1-minute test. During a network hiccup between the containers,
  8 pops answered 503 and their claims, made on the leader, waited out the queue's 300 s lease.
  Everything delivered was exact. That is the designed fallback for a lost answer
  (`../../2026-10-18-stuck-lease/README.md`); a shorter lease in the profiles would bound it.

The fork goes to the personal account, not the queen-mq org: GitHub lets upstream maintainers push
fixes onto a PR's branch only when the fork belongs to a person.

```bash
gh repo fork openmessaging/benchmark --clone=false
git -C ~/Work/openmessaging-benchmark push git@github.com:alice-viola/benchmark.git fix-spotless-443 queen-driver
gh pr create --repo openmessaging/benchmark --base master --head alice-viola:fix-spotless-443 --title "Fix mvn verify on master: spotless and spotbugs in the files #443 added" --body-file ~/Work/queen/benchmark-queen/2026-10-16-omb/omb-pr/1-fix-verify.md
gh pr create --repo openmessaging/benchmark --base master --head alice-viola:queen-driver --title "Add a Queen MQ driver" --body-file ~/Work/queen/benchmark-queen/2026-10-16-omb/omb-pr/2-queen-driver.md
```
