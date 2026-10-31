# Publishing the OMB driver: NOT done, needs Alice's go

Everything is local in `~/Work/openmessaging-benchmark`. Nothing has been pushed, forked or opened.

| Branch             | Commits                        | Check                                                     |
|--------------------|--------------------------------|-----------------------------------------------------------|
| `fix-spotless-443` | 288cfbc (the master fix)       | `mvn --no-transfer-progress --batch-mode verify`: SUCCESS |
| `queen-driver`     | 288cfbc + e131747 (the driver) | same command: SUCCESS (10-19); 2 local smoke runs on 2.5.0 |

Two PRs, because master fails `verify` since #443 (upstream run 38648660840 failed on it, 2026-10-14).
PR 1 is that fix alone. PR 2 carries the same commit, so its CI passes either way.

The files 1-fix-verify.md and 2-queen-driver.md are the PR bodies as they are. On 10-19, at Alice's
word, PR 2 says all five families, PR 1 cites #463 (it formats the same files but not the spotbugs
finding), the profiles pop with `leaseSeconds: 30` and the driver runs Queen 2.5.0. The smoke run of
10-18 that stalled 3 partitions for 300 s (`../../2026-10-18-stuck-lease/README.md`) is why the lease
is 30 s.

The fork goes to the personal account, not the queen-mq org: GitHub lets upstream maintainers push
fixes onto a PR's branch only when the fork belongs to a person.

```bash
gh repo fork openmessaging/benchmark --clone=false
git -C ~/Work/openmessaging-benchmark push git@github.com:alice-viola/benchmark.git fix-spotless-443 queen-driver
gh pr create --repo openmessaging/benchmark --base master --head alice-viola:fix-spotless-443 --title "Fix mvn verify on master: spotless and spotbugs in the files #443 added" --body-file ~/Work/queen/benchmark-queen/2026-10-16-omb/omb-pr/1-fix-verify.md
gh pr create --repo openmessaging/benchmark --base master --head alice-viola:queen-driver --title "Add a Queen MQ driver" --body-file ~/Work/queen/benchmark-queen/2026-10-16-omb/omb-pr/2-queen-driver.md
```
