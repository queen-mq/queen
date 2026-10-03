#!/usr/bin/env bash
# smoke.sh (Mac) [KEY=VAL ...] — prove a running cluster with Kafka's OWN tools, from loader 1 (no kload involved):
#   1. kafka-topics --create: PARTS partitions, RF 3, min.insync.replicas=2; --describe
#   2. kafka-consumer-perf-test (group smoke-g-<ts>) started first, then kafka-producer-perf-test with the SPEC producer
#      settings (acks=all, idempotent, lz4, linger 5 ms, 1 MiB batches, 5 in flight) at RATE msg/s x SECS s, 256 B records
#   3. FAILOVER=1: broker VICTIM (default 3) is stopped cleanly a third into the produce and restarted at two thirds; the
#      ISR must shrink (2 in sync, writes go on: min.insync 2) and recover to 3 (no under-replicated partitions) afterwards
# KEY=VAL: RATE=10000 SECS=30 PARTS=6 FAILOVER=0 VICTIM=3 RECOVER_TIMEOUT=120 TOOL_HEAP=-Xmx192m HOSTS_ENV=<harness>/hosts.env
# Loader 1 gets the Kafka dist via kafka/install.sh when it has none (CACHE from hosts.env).
set -u
main() {
  local a; for a in "$@"; do case $a in *=*) printf -v "${a%%=*}" '%s' "${a#*=}";; *) awk 'NR > 1 && /^#/ {print; next} NR > 1 {exit}' "$0"; return 2;; esac; done
  local H; H=$(cd "$(dirname "$0")/.." && pwd)
  HOSTS_ENV=${HOSTS_ENV:-$H/hosts.env}; . "$HOSTS_ENV"
  local R=${REMOTE_ROOT:-/root/bench} RATE=${RATE:-10000} SECS=${SECS:-30} PARTS=${PARTS:-6} FAILOVER=${FAILOVER:-0}
  local VICTIM=${VICTIM:-3} RT=${RECOVER_TIMEOUT:-120} TH=${TOOL_HEAP:--Xmx192m}
  local L=${L_PUB[0]} KB=$R/kafka/dist/bin BOOT="" b T G NREC=$((RATE * SECS))
  for b in "${B_PRIV[@]}"; do BOOT="$BOOT${BOOT:+,}$b:9092"; done
  T=smoke-$(date -u +%H%M%S); G=smoke-g-$(date -u +%H%M%S)
  log() { echo "[$(date -u +%FT%TZ)] smoke: $*"; }
  rx() { local h=$1; shift; $SSH root@"$h" "$*"; }
  kt() { rx "$L" "KAFKA_HEAP_OPTS=$TH LOG_DIR=/tmp/smoke-logs $*"; }
  rx "$L" "test -x $KB/kafka-topics.sh" || { log "installing the Kafka CLI on loader 1"; rx "$L" "CACHE=${CACHE:-} bash $R/kafka/install.sh" | tail -2; }

  log "1. topic $T: $PARTS partitions, RF 3, min.insync.replicas=2 (bootstrap $BOOT)"
  kt "$KB/kafka-topics.sh --bootstrap-server $BOOT --create --topic $T --partitions $PARTS --replication-factor 3 --config min.insync.replicas=2" || return 1
  kt "$KB/kafka-topics.sh --bootstrap-server $BOOT --describe --topic $T" | sed 's/^/    /'

  log "2. consumer-perf-test ($NREC records, group $G) + producer-perf-test ($RATE msg/s x ${SECS}s, 256 B, acks=all idempotent lz4 linger 5 ms)"
  rx "$L" "mkdir -p /tmp/smoke-logs; KAFKA_HEAP_OPTS=$TH LOG_DIR=/tmp/smoke-logs nohup $KB/kafka-consumer-perf-test.sh --bootstrap-server $BOOT --topic $T --group $G --num-records $NREC --timeout 60000 > /tmp/smoke-cons-$T.log 2>&1 < /dev/null &"
  local FJ=""
  if [ "$FAILOVER" = 1 ]; then
    ( sleep $((SECS / 3))
      log "   FAILOVER: stopping broker $VICTIM (${B_PRIV[$((VICTIM - 1))]}) cleanly"
      rx "${B_PUB[$((VICTIM - 1))]}" "bash $R/kafka/kc.sh stop" | sed 's/^/    /'
      sleep 3
      log "   ISR with broker $VICTIM down:"
      kt "$KB/kafka-topics.sh --bootstrap-server ${B_PRIV[0]}:9092 --describe --topic $T" | grep -E 'Partition:' | sed 's/^/    /'
      sleep $((SECS / 3 - 3))
      log "   FAILOVER: starting broker $VICTIM again"
      rx "${B_PUB[$((VICTIM - 1))]}" "bash $R/kafka/kc.sh start" | sed 's/^/    /' ) &
    FJ=$!
  fi
  kt "$KB/kafka-producer-perf-test.sh --bootstrap-server $BOOT --topic $T --num-records $NREC --record-size 256 --throughput $RATE --reporting-interval 5000 --command-property acks=all enable.idempotence=true compression.type=lz4 linger.ms=5 batch.size=1048576 max.in.flight.requests.per.connection=5" 2>&1 | sed 's/^/    /'
  [ -n "$FJ" ] && wait "$FJ"
  local t
  for t in $(seq 1 45); do rx "$L" "pgrep -f '[C]onsumerPerformance.*$T' > /dev/null" || break; sleep 2; done   # [C]: not our own shell
  log "   consumer-perf-test:"; rx "$L" "cat /tmp/smoke-cons-$T.log" | grep -vE '^\s*$' | tail -2 | sed 's/^/    /'

  if [ "$FAILOVER" = 1 ]; then
    log "3. waiting for full ISR again (no under-replicated partitions, timeout ${RT}s)"
    local ur="" t0; t0=$(date +%s)
    while :; do
      ur=$(kt "$KB/kafka-topics.sh --bootstrap-server $BOOT --describe --under-replicated-partitions" 2>/dev/null)
      [ -z "$ur" ] && { log "   full ISR on every partition after $(( $(date +%s) - t0 ))s"; break; }
      [ $(( $(date +%s) - t0 )) -ge "$RT" ] && { log "   STILL under-replicated after ${RT}s:"; echo "$ur" | sed 's/^/    /'; break; }
      sleep 3
    done
    kt "$KB/kafka-topics.sh --bootstrap-server $BOOT --describe --topic $T" | grep -E 'Partition:' | sed 's/^/    /'
    log "   ISR events in the brokers' server.log (Shrinking ISR / ISR updated to):"
    for b in "${B_PUB[@]}"; do rx "$b" "bash $R/kafka/kc.sh stats" | grep -oE 'node=[0-9]+|isr_shrinks=[0-9]+|isr_updates=[0-9]+|errors=[0-9]+' | tr '\n' ' ' | sed 's/^/    /'; echo; done
    [ -z "$ur" ] || return 1
  fi
}
main "$@"; exit $?
