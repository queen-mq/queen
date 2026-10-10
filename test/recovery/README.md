# Recovery rehearsals

The scripts behind `helm_v2/RECOVERY.md`: a 3-node Queen cluster in Docker built like the prod
StatefulSet pods (same env, pod names, headless DNS names, uid 65532, read-only root, restart policy
"always" like the kubelet), a tool that writes a known data set and proves what survived, one script
per failure, and a k3s variant that runs the real `helm_v2` chart for the kubectl steps.

## Prerequisites

- Docker (OrbStack on the Mac), `python3` with `requests`, `openssl`, `perl`.
- For the Kubernetes part: `kubectl`, `helm`, and the local `helm_v2/` chart (gitignored).
- An image of the tree under test, from the repo root:

  ```bash
  QUEEN_REGISTRY=local ./build.sh --tag recovery      # -> local/queen:recovery, host arch
  ```

## The Docker cluster

```bash
cd test/recovery
./qr.sh up                 # queen-mq-v2-0/1/2 = nodes 1/2/3, broker on localhost:16630-16632
./qr.sh health
python3 wl.py write --tag A          # 900 pushes over 3 queues x 4 partitions + 40 KV keys, ledgered
python3 wl.py consume --group g1     # pop + ack 100 per queue, ledgered
python3 wl.py verify                 # every acknowledged write present exactly once, KV values, group cursors
./qr.sh down
```

`health-quorum.py` needs none of this: three processes of a local build on loopback, and what
`/health` answers when the majority is lost, when it returns, through a leader failover and for a
voter that comes back empty (F8 and F2 in `FINDINGS.md`).

`wl.py` records each write as `ok` (the broker said it stored it: it must survive) or `unknown`
(timeout, 5xx: may or may not exist). `verify --nodes 1` reads through one node only, which is how a
node that boots on damaged files is caught serving holes. `verify --allow-loss` reports instead of
failing (for the procedures that lose data by design).

`qr.sh` also does: `crash i` (kill -9, restarted by the policy), `kill i` (stays down), `stop i`,
`restart i` (SIGTERM, like `kubectl delete pod`), `wipe i` (container + volume: the disk is lost),
`start i K=V…` (recreate with extra env), `sh i 'cmd'` (a shell on node i's volume while it is down).

## Scenarios (results of 2026-10-01, image from 22773001 + the test knob; sc01, sc03, sc03c, sc11, sc14 re-run on beta.2 d82e785f: same results)

| Script | Failure | Procedure | Result |
|---|---|---|---|
| `sc01-crash.sh` | kill -9 leader / follower, SIGTERM leader, under load | P1 | automatic; new leader ≤ 4 s; 0/3,300 lost |
| `sc02-snapshot.sh` | follower away past the purge hold | P2 | snapshot, exit 75, caught up in 5 s; 0/1,210 lost |
| `sc03-replace.sh` (`VICTIM=leader JOIN=1`) | one disk lost | P3 | 0/3,840 lost, follower and leader cases |
| `sc03c-empty-voter.sh` | disk wiped, restarted WITHOUT removal | P3 | stuck at applied 0 while healthy (F1, F2); P3 fixes it |
| `sc04-corrupt.sh [case…]` | 16 kinds of file damage on one node | P3 | see the table in `FINDINGS.md`; replace fixes every case |
| `probe-sealed.sh` | bit flip in an old sealed queue-log file | P3 | node boots, pops via it 500, auto-ack loses batches (F3) |
| `probe-store-purged.sh` | store deleted, log purged | P3 | refused at boot with a clear FATAL |
| `sc05-diskfull.sh` | one node's disk fills (300 MB loop volume) | P4 | gate 507 at 85 %, ENOSPC stop at 100 %, grow + restart: 0/6,900 lost |
| `sc06-stale-copy.sh` | one node rolled back to its old copy | P3 | stuck (F1); replace: 0/1,500 lost |
| `sc08-config.sh` | wrong token, wrong peers, token rotation | P11 | clear errors; rolling rotation 0/1,598 lost |
| `sc09-quorum-loss.sh` | 2 of 3 down 30 s; all 3 killed at once | P5 | leader 3 s after quorum returns; 0/4,054 lost; survivor says healthy throughout (F8) |
| `sc11-force-recover.sh` | 2 of 3 disks lost (leader's included) | P6 | 0/1,770 lost (survivor was current) |
| `sc12-restore-backup.sh` (`BACKUP=pause\|naive`) | all disks lost | P7 | everything acked before the copy back; after it lost (expected) |
| `sc13-addresses.sh` | every peer address changed | P10 | force-recover + rebuild under new names: 0/900 lost |
| `sc14-apply-skip.sh` | poisoned entry stops every node | P8 | `QUEEN_RAFT_APPLY_SKIP` on all: back, 0 other writes lost |
| `sc15-joint.sh` | leader killed during a voter change | P13 | not reproduced in 6 attempts |
| `sc16-split-brain.sh` | force-recover run on two nodes | P9 | two clusters (different clusterId); converged, other side's writes lost |
| `sc17-partition-key.sh` | leader isolated; encryption key changed | P1, P12 | re-election ~4 s, 0 lost; wrong key = ciphertext served silently (F10) |

## The Kubernetes part (k3s in Docker, real chart)

Never touches `~/.kube/config`: `k3s.sh up` writes its own kubeconfig and two wrappers, `./kk`
(kubectl) and `./hh` (helm), pinned to it.

```bash
./qr.sh down                         # the port-forwards reuse 16630-16632
./k3s.sh up && ./k3s.sh load local/queen:recovery
./k8s-reset.sh                       # secrets, helm install with prod.yaml + k3s-values.yaml
./pf.sh &                            # localhost:1663i -> pod i (for wl.py)
./k8s-K1-replace.sh                  # P3 with kubectl
./k8s-K2b-lost2.sh                   # two PVCs deleted while running (F11)
SURVIVOR=1 ./k8s-K2c-ondelete.sh     # P6 with updateStrategy OnDelete
./k8s-K3-skip.sh                     # P8 with kubectl
./k8s-K4-restore.sh                  # P7 with a PVC created before scale-up
./k3s.sh down
```

| Script | Result |
|---|---|
| `k8s-K1-replace.sh` | 20 s under load, 0/3,060 lost, rebuilt pod alone serves all |
| `k8s-K2b-lost2.sh` | cluster cannot commit, every pod Ready (F11) |
| `k8s-K2c-ondelete.sh` | from that state to 3 voters in 73 s, 0/900 lost |
| `k8s-K3-skip.sh` | back in service 14 s after setting the skip, 0 lost |
| `k8s-K4-restore.sh` | restored in 66 s; 1,200/1,200 writes from before the copy |

`QUEEN_TEST_REFUSE_APPLY_KV=<ns>/<key>` (server/src/rsm/faults.rs) is the test-only knob that makes
apply refuse any entry writing that KV key on every node started with it. Never set it on a cluster
that serves anyone.
