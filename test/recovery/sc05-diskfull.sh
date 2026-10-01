#!/usr/bin/env bash
# S5 — one node's disk fills up (smaller volume, or uneven usage). The disk gate
# only guards the writes a node's OWN clients send; writes taken by the other
# nodes keep replicating to it until the filesystem is full.
# Node index 2 gets a 300 MB ext4 volume (loop device in the Docker VM).
set -uo pipefail
. "$(dirname "$0")/lib.sh"
cd "$REC"; rm -rf state
IMG=/var/lib/docker/qr-small.img
vm() { docker run --rm --privileged --pid=host alpine nsenter -t 1 -m sh -c "$*"; }
cleanup_small() {
  docker rm -f queen-mq-v2-2 >/dev/null 2>&1
  docker volume rm qr-data-2 >/dev/null 2>&1
  vm "for d in \$(losetup -a | grep qr-small | cut -d: -f1); do losetup -d \$d; done; rm -f $IMG" >/dev/null 2>&1
}
hr "S5 setup: node 3 on a 300 MB disk, gate 85/80 (prod values)"
$QR down >/dev/null; cleanup_small
vm "truncate -s 300M $IMG"
docker run --rm --privileged -v /var/lib/docker:/h alpine sh -c "apk add -q e2fsprogs >/dev/null 2>&1 && mkfs.ext4 -q -F /h/qr-small.img"
DEV=$(vm "d=\$(losetup -f) && losetup \$d $IMG && echo \$d")
docker volume create --driver local --opt type=ext4 --opt device="$DEV" qr-data-2 >/dev/null
docker run --rm --user 0 -v qr-data-2:/d --entrypoint /bin/sh "$IMAGE" -c 'chown 65532:65532 /d; rm -rf /d/lost+found'
say "small volume on $DEV"
export QR_ENV="QUEEN_RAFT_SEGMENT_BYTES=8388608" QR_DISK_HIGH=85 QR_DISK_LOW=80
$QR up 3 >/dev/null; wait_all_healthy 3 120
if [ "$(leader)" = 2 ]; then $QR restart 2 >/dev/null; wait_healthy 2 60; fi   # keep node 3 a follower
say "leader index $(leader)"
$WL write --tag A --msgs 200 >/dev/null; $WL consume --group g1 --max 60
df3() { docker exec queen-mq-v2-2 df -h /var/lib/queen/raft 2>/dev/null | tail -1 || echo "(node 3 not running)"; }
say "node 3 disk: $(df3)"

hr "S5 fill through nodes 1 and 2 only (60 KB messages)"
for round in $(seq 1 22); do
  $WL write --tag "F$round" --queues 1 --partitions 4 --msgs 400 --kv 0 --pad 60000 --nodes 0,1 | cut -c1-200
  say "node 3 disk: $(df3) | role $(role 2) | $(cstate 2)"
  st=$(curl -s -m 5 -o /dev/null -w '%{http_code}' -X POST localhost:16632/api/v1/push -H 'Content-Type: application/json' -d '{"items":[{"queue":"rec.probe","partition":"p","payload":{"x":1}}]}')
  say "a push sent to node 3 itself answers HTTP $st"
  [ "$(role 2)" = stopped ] && break
  [ "$(role 2)" = down ] && break
done
hs; mem
say "node 3 log:"; docker logs queen-mq-v2-2 2>&1 | grep -E '"level":"(ERROR|WARN)"' | grep -iE 'space|poison|stops|storage pressure|No space|fsync|write' | tail -6 | cut -c1-400

hr "S5 the cluster still serves writes and reads"
$WL write --tag after --msgs 100 --kv 5 --nodes 0,1 | cut -c1-200
$WL verify --nodes 0,1 | cut -c1-300

hr "S5 remedy: stop node 3, grow its volume (= PVC expansion), start it"
$QR stop 2
vm "truncate -s 1200M $IMG && losetup -c $DEV"
docker run --rm --privileged --device "$DEV" alpine sh -c "apk add -q e2fsprogs e2fsprogs-extra >/dev/null 2>&1; e2fsck -fy $DEV >/dev/null 2>&1; resize2fs $DEV 2>&1 | tail -1"
docker start queen-mq-v2-2 >/dev/null; wait_healthy 2 180; wait_caught_up 2 180
say "node 3 disk: $(df3)"; hs; mem
hr "S5 verify (all nodes, then node 3 alone)"
$WL verify | cut -c1-400; $WL verify --nodes 2 | cut -c1-400
