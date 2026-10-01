#!/usr/bin/env bash
# S12 — EVERY disk is lost. The only way back is a backup of ONE node's data
# directory: restore it on that node, start it with QUEEN_RAFT_FORCE_RECOVER,
# rebuild the other two empty. Everything written after the backup is gone.
#   BACKUP=pause  frozen copy (docker pause + copy = what a volume snapshot gives)
#   BACKUP=naive  cp -a of a RUNNING node's directory (files copied at different times)
set -uo pipefail
. "$(dirname "$0")/lib.sh"
cd "$REC"; rm -rf state state-at-backup
B="${BACKUP:-pause}"
hr "S12 setup (backup method: $B)"
$QR down >/dev/null; $QR up 3 >/dev/null; wait_all_healthy 3 120
$WL write --tag A --msgs 400 >/dev/null; $WL consume --group g1 --max 60
$WL load --seconds 30 --rate 40 --tag L12 > runs/s12-load.log 2>&1 &
LOAD=$!; sleep 10
hr "S12 backup of node 1's disk while it serves ($B)"
docker volume rm qr-backup >/dev/null 2>&1; docker volume create qr-backup >/dev/null
if [ "$B" = pause ]; then
  docker pause queen-mq-v2-0 >/dev/null
  cp -r state state-at-backup
  docker run --rm -v qr-data-0:/src -v qr-backup:/dst --entrypoint /bin/sh "$IMAGE" -c 'cp -a /src/. /dst/'
  docker unpause queen-mq-v2-0 >/dev/null
else
  cp -r state state-at-backup
  docker run --rm -v qr-data-0:/src -v qr-backup:/dst --entrypoint /bin/sh "$IMAGE" -c 'cp -a /src/. /dst/'
fi
say "backup taken"
wait $LOAD; tail -1 runs/s12-load.log
$WL write --tag AFTER --msgs 100 >/dev/null
hr "S12 disaster: all three disks are gone"
for i in 0 1 2; do $QR wipe "$i"; done
hr "S12 restore: node 1's volume from the backup, the others empty"
docker volume create qr-data-0 >/dev/null
docker run --rm -v qr-backup:/src -v qr-data-0:/dst --entrypoint /bin/sh "$IMAGE" -c 'cp -a /src/. /dst/'
hr "S12 start all with QUEEN_RAFT_FORCE_RECOVER=1 (k8s: env on the sts, scale to 3)"
for i in 0 1 2; do $QR start "$i" QUEEN_RAFT_FORCE_RECOVER=1 >/dev/null; done
wait_healthy 0 120; sleep 3; hs
for i in 0 1 2; do say "index $i: $(cstate "$i")"; done
docker logs queen-mq-v2-0 2>&1 | grep -E 'FATAL|FORCE_RECOVER' | head -2 | cut -c1-300
hr "S12 unset it, restart, add + promote the two empty nodes"
for i in 0 1 2; do $QR start "$i" >/dev/null; done
wait_healthy 0 60; sleep 4
for i in 1 2; do api 0 POST /api/v1/system/raft/membership/learners "{\"id\":$((i+1)),\"raft\":\"$(FQ "$i"):7400\",\"http\":\"$(FQ "$i"):6632\"}" | cut -c1-120; done
for t in $(seq 1 30); do r=$(api 0 POST /api/v1/system/raft/membership/promote '{"ids":[2,3]}'); echo "$r" | grep -q '"ok":true' && break; sleep 2; done
echo "$r" | cut -c1-150
wait_caught_up 1 120; wait_caught_up 2 120; hs; mem
hr "S12 verify against the ledger as it was at the backup"
$WL verify --state "$REC/state-at-backup" --allow-loss | cut -c1-500
hr "S12 what was written after the backup (expected lost)"
$WL verify --allow-loss | cut -c1-300
