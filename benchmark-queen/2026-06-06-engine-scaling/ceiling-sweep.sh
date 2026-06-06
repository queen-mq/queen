#!/usr/bin/env bash
# Ceiling sweep: drive the broker from a SEPARATE loader VM (no co-location
# confound) and sweep push concurrency C until broker or PG saturates. Run from
# a control host with SSH access to both VMs. Prints one summary line per C with
# push/s + the saturation signals; the plateau + which resource is maxed is the
# new ceiling.
set -u
BROKER="${BROKER:-root@165.232.78.92}"
LOADER="${LOADER:-root@167.99.246.68}"
BROKER_PRIV="${BROKER_PRIV:-10.114.0.2}"        # broker private VPC IP (loader -> broker)
TAG="${TAG:-pushser}"
CLIST="${CLIST:-16 32 48 64 96}"
DUR="${DUR:-50}"
SIDE="${SIDE:-250}"
PARTS="${PARTS:-1000}"; PROD="${PROD:-1500}"; PB="${PB:-10}"
MODE="${MODE:-static}"; MAXP="${MAXP:-8}"; NW="${NW:-12}"
GOLOAD="${GOLOAD:-/root/goload-linux-amd64}"
DIR="/root/queen/benchmark-queen/2026-06-06-engine-scaling"
log(){ echo "[$(date -u +%FT%TZ)] $*"; }

log "ceiling sweep: TAG=$TAG mode=$MODE C=[$CLIST] dur=${DUR}s parts=$PARTS prod=$PROD side=$SIDE maxp=$MAXP"
for C in $CLIST; do
  log "---- C=$C ----"
  # 1. restart broker for this cell (static concurrency C)
  ssh -o BatchMode=yes "$BROKER" "TAG=$TAG MODE=$MODE MAXP=$MAXP NUM_WORKERS=$NW bash $DIR/ceil-restart.sh $C $SIDE"
  # 2. launch goload on the loader (push-only) against the broker's private IP
  ssh -o BatchMode=yes "$LOADER" "nohup $GOLOAD -url http://$BROKER_PRIV:6632 -queue ceilq \
      -partitions $PARTS -producers $PROD -consumers 0 -push-batch $PB \
      -duration $DUR -report 20 -completed-retention 100000 -pending-retention 0 \
      > /root/ceil_goload_$C.log 2>&1 < /dev/null & echo loader-launched"
  sleep 3
  # 3. sample broker+PG for the run window (concurrent with goload)
  ssh -o BatchMode=yes "$BROKER" "bash $DIR/ceil-sample.sh $((DUR-5)) $C"
  # 4. ground-truth push count from goload
  sleep 4
  gl=$(ssh -o BatchMode=yes "$LOADER" "grep -E '^\[final\]' /root/ceil_goload_$C.log | tail -1")
  log "  goload: $gl"
done
log "sweep complete."
