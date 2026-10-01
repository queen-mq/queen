#!/usr/bin/env bash
# S3c — the MISTAKE: a voter's disk is wiped and it is started again under the
# same id WITHOUT being removed first (on Kubernetes: the PVC was deleted and
# the StatefulSet recreated the pod). What does the cluster do?
set -uo pipefail
. "$(dirname "$0")/lib.sh"
cd "$REC"; rm -rf state
hr "S3c setup"
$QR down >/dev/null; $QR up 3 >/dev/null; wait_all_healthy 3 120
$WL write --tag A --msgs 200; $WL consume --group g1 --max 60
$WL load --seconds 60 --rate 40 --tag L3c > runs/s3c-load.log 2>&1 &
LOAD=$!; sleep 3
L=$(leader); V=$(( (L + 1) % 3 )); ID=$((V + 1))
say "leader index $L; victim follower index $V (node $ID)"
hr "S3c wipe node $ID and start it straight away, still a voter"
$QR wipe "$V"; sleep 2; $QR start "$V"
for i in $(seq 1 30); do sleep 2; hs; [ "$(status "$V")" = healthy ] && break; done
docker logs "queen-mq-v2-$V" 2>&1 | grep -iE 'joins an existing|initializ|snapshot' | tail -4 | cut -c1-260
say "container: $(cstate "$V")"; mem
wait $LOAD; tail -1 runs/s3c-load.log
hr "S3c verify"
$WL verify; $WL verify --nodes "$V"; hs
