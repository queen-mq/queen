#!/usr/bin/env bash
# Probe: a damaged record in a SEALED queue-log file of one follower that the
# boot does not read. What does a pop through that node return?
set -uo pipefail
. "$(dirname "$0")/lib.sh"
cd "$REC"; rm -rf state
export QR_ENV="QUEEN_RAFT_SEGMENT_BYTES=262144"
$QR down >/dev/null; $QR up 3 >/dev/null; wait_all_healthy 3 120 >/dev/null
$WL write --tag base --msgs 1500 >/dev/null
say "waiting 40 s for store checkpoints, then more writes"; sleep 40
$WL write --tag later --msgs 300 --queues 3 >/dev/null; sleep 15
L=$(leader); V=$(( (L + 1) % 3 )); say "victim index $V"
$QR stop "$V" >/dev/null
docker volume rm qr-bak >/dev/null 2>&1; docker volume create qr-bak >/dev/null
docker run --rm -v qr-data-$V:/src -v qr-bak:/dst --entrypoint /bin/sh "$IMAGE" -c 'cp -a /src/. /dst/'
FILES=$($QR sh "$V" 'cd /var/lib/queen/raft && ls qlog/q*/r*.qidx | sed "s/qidx$/qlog/"')
say "sealed files: $(echo $FILES)"
for F in $FILES; do
  SZ=$($QR sh "$V" "stat -c %s /var/lib/queen/raft/$F"); frac=50
  off=$(( SZ * frac / 100 ))
  docker run --rm -v qr-bak:/src -v qr-data-$V:/dst --entrypoint /bin/sh "$IMAGE" -c 'rm -rf /dst/* && cp -a /src/. /dst/'
  $QR sh "$V" "cd /var/lib/queen/raft && perl -e 'my (\$f,\$off)=@ARGV; open(my \$h,\"+<\",\$f) or die; seek(\$h,\$off,0); read(\$h,my \$b,16); \$b ^= (\"\\xa5\" x 16); seek(\$h,\$off,0); print \$h \$b; close \$h;' $F $off"
  before=$(docker inspect -f '{{.RestartCount}}' "queen-mq-v2-$V")
  docker start "queen-mq-v2-$V" >/dev/null
  ok=""
  for t in $(seq 1 20); do
    sleep 1
    rc=$(docker inspect -f '{{.RestartCount}}' "queen-mq-v2-$V")
    [ "$rc" -gt "$before" ] && break
    [ "$(status "$V")" = healthy ] && { ok=1; break; }
  done
  if [ -n "$ok" ]; then say "$F offset $off ($frac%): BOOTED"; break; fi
  say "$F offset $off ($frac%): refused: $(docker logs --tail 1 queen-mq-v2-$V 2>&1 | grep -o 'rsm qlog corrupt[^"]*' | cut -c1-200)"
  docker stop -t 5 "queen-mq-v2-$V" >/dev/null
done
[ -z "$ok" ] && { say "no offset booted"; exit 1; }
wait_caught_up "$V" 60
hr "pop every queue through node index $V only, NO auto-ack, fresh group"
for q in rec.base0 rec.base1 rec.base2; do
  got=0; calls=0
  for i in $(seq 1 30); do
    r=$(curl -s -m 15 -o /tmp/pop.$$ -w '%{http_code}' "localhost:1663$V/api/v1/pop/queue/$q?consumerGroup=probe&subscriptionMode=all&batch=1000&partitions=64&wait=false")
    calls=$((calls+1))
    n=$(python3 -c 'import json,sys; d=json.load(open(sys.argv[1])); print(len(d.get("messages") or []))' /tmp/pop.$$ 2>/dev/null || echo 0)
    [ "$r" != 200 ] && { echo "  $q call $calls: HTTP $r $(head -c 300 /tmp/pop.$$)"; break; }
    got=$((got+n)); [ "$n" = 0 ] && break
  done
  say "$q: delivered $got of 1500 (via index $V)"
done
hr "the same through a healthy node"
for q in rec.base0 rec.base1 rec.base2; do
  r=$(curl -s -m 15 "localhost:1663$L/api/v1/pop/queue/$q?consumerGroup=probe2&subscriptionMode=all&batch=1000&partitions=64&wait=false&autoAck=true" | python3 -c 'import json,sys; print(len(json.load(sys.stdin).get("messages") or []))')
  say "$q via leader: first call delivered $r"
done
hr "node $V log: WARN/ERROR + stderr gap lines"
docker logs "queen-mq-v2-$V" 2>&1 | grep -E 'RENDER_GAP|not readable|"level":"(WARN|ERROR)"' | grep -v 'registry\|kv invalidation\|usage_days\|revocation\|monthly quota\|no leader yet\|Heartbeat' | tail -8 | cut -c1-400
hr "auto-ack verify through node $V"
$WL verify --nodes "$V" --allow-loss | cut -c1-500
