#!/usr/bin/env bash
# K4 (Kubernetes) — every PVC lost; restore from a backup of ONE pod's volume.
# The backup is a crash-consistent copy (process frozen, files copied: what a
# VolumeSnapshot gives) of queen-mq-v2-2's volume, restored as queen-mq-v2-0's
# PVC (pod 0 first: no placement race), QUEEN_RAFT_FORCE_RECOVER=1, then 1 and 2
# rebuilt empty.
set -uo pipefail
. "$(dirname "$0")/k8s-lib.sh"
cd "$REC"; rm -rf state state-at-backup
hr "K4 setup"
$WL write --tag A --msgs 400 >/dev/null; $WL consume --group g1 --max 60
sleep 3
SRC=2
NODE=$(k get pod queen-mq-v2-$SRC -o jsonpath='{.spec.nodeName}')
PV=$(k get pvc data-queen-mq-v2-$SRC -o jsonpath='{.spec.volumeName}')
DIR=$(docker exec "$NODE" sh -c "ls -d /var/lib/rancher/k3s/storage/${PV}_*")
PID=$(docker exec "$NODE" sh -c 'for p in $(pgrep -f /app/bin/queen); do grep -q queen-mq-v2-'$SRC' /proc/$p/environ 2>/dev/null && echo $p; done' | head -1)
say "backup source: pod $SRC on $NODE, $DIR, queen pid $PID"
hr "K4 backup = freeze the process, copy the volume, thaw (a VolumeSnapshot)"
docker exec "$NODE" kill -STOP "$PID"
cp -r state state-at-backup
docker exec "$NODE" tar -C "$DIR" -cf - . > runs/k4-backup.tar
docker exec "$NODE" kill -CONT "$PID"
ls -la runs/k4-backup.tar | awk '{print "backup size", $5}'
$WL write --tag AFTER --msgs 100 >/dev/null
hr "K4 disaster: scale to 0, every PVC deleted"
k scale sts queen-mq-v2 --replicas=0; k wait --for=delete pod -l run=queen-mq-v2 --timeout=180s >/dev/null 2>&1
k delete pvc --all
t0=$(date +%s)
hr "K4 restore step 1: a PVC named like pod 0's, filled from the backup (GKE: dataSource VolumeSnapshot)"
cat <<YAML | k apply -f -
apiVersion: v1
kind: PersistentVolumeClaim
metadata: {name: data-queen-mq-v2-0}
spec:
  accessModes: [ReadWriteOnce]
  storageClassName: local-path
  resources: {requests: {storage: 2Gi}}
---
apiVersion: v1
kind: Pod
metadata: {name: restore-helper}
spec:
  securityContext: {runAsUser: 65532, runAsGroup: 65532, fsGroup: 65532}
  containers:
  - name: h
    image: "$IMAGE"
    imagePullPolicy: IfNotPresent
    command: ["/bin/sh","-c","sleep 3600"]
    volumeMounts: [{name: d, mountPath: /data}]
  volumes: [{name: d, persistentVolumeClaim: {claimName: data-queen-mq-v2-0}}]
YAML
k wait --for=condition=Ready pod/restore-helper --timeout=120s
k exec -i restore-helper -- tar -C /data -xf - < runs/k4-backup.tar
k exec restore-helper -- sh -c 'ls /data; du -sh /data'
k delete pod restore-helper --wait=true
hr "K4 step 2: pod 0 alone, with QUEEN_RAFT_FORCE_RECOVER=1"
k set env sts/queen-mq-v2 QUEEN_RAFT_FORCE_RECOVER=1
k scale sts queen-mq-v2 --replicas=1
wait_ready queen-mq-v2-0 240; kmem queen-mq-v2-0
hr "K4 step 3: drop the variable (pod 0 restarts), then scale to 3 and add 2 and 3"
k set env sts/queen-mq-v2 QUEEN_RAFT_FORCE_RECOVER-
k rollout status sts/queen-mq-v2 --timeout=240s | tail -1
k scale sts queen-mq-v2 --replicas=3
for i in 1 2; do
  for t in $(seq 1 60); do sleep 2; [ "$(k get pod "queen-mq-v2-$i" -o jsonpath='{.status.phase}' 2>/dev/null)" = Running ] && break; done
  q queen-mq-v2-0 /api/v1/system/raft/membership/learners -X POST -H 'Content-Type: application/json' \
    -d "{\"id\":$((i+1)),\"raft\":\"$(FQK "$i"):7400\",\"http\":\"$(FQK "$i"):6632\"}" | grep -o '"ok":true\|"code":"[a-z_]*"'
done
for t in $(seq 1 30); do r=$(q queen-mq-v2-0 /api/v1/system/raft/membership/promote -X POST -H 'Content-Type: application/json' -d '{"ids":[2,3]}'); echo "$r" | grep -q '"ok":true' && break; sleep 3; done
for i in 1 2; do wait_ready "queen-mq-v2-$i" 120; done
pods; kmem queen-mq-v2-0
say "restore took $(( $(date +%s) - t0 ))s"
hr "K4 verify against the ledger at backup time"
$WL verify --state "$REC/state-at-backup" --allow-loss | cut -c1-300
$WL verify --allow-loss | cut -c1-200
