#!/usr/bin/env bash
# omb.sh <cmd> ... — OpenMessaging Benchmark (OMB) on the 3 loaders against Kafka or Queen on the 3 brokers of ../hosts.env.
# bash 3.2-clean (the Mac's /bin/bash). Every step is logged with a UTC timestamp.
#
#   deploy <tarball>        unpack the OMB distribution on every loader at /root/bench/omb, plus ../drivers and ../workloads
#   workers                 (re)start 2 OMB workers per loader: :8080 (stats :8081) and :8082 (stats :8083), heap $WHEAP (8G).
#                           The workers list puts the :8080 three first. OMB (DistributedWorkersEnsemble) gives the LAST half
#                           of the list the consumers and the first half the producers (the leader, workers[0], which
#                           creates the topics, is a producer): every loader runs one producer (:8080) and one consumer (:8082).
#   kafka-reset <partitions> fresh 3-node Kafka 4.3.1 with the docs runs' settings (../../2026-09-30-kafka-pulsar/kafka/cluster.sh
#                           reset, KPROPS=num.io.threads=24,num.network.threads=12,num.replica.fetchers=12), and
#                           vm.dirty_background_bytes=1 GiB on every broker as before every docs point
#   queen-reset <bin>       fresh 3-node Queen (../../2026-10-12-three-node-new-forms/scripts/qc.sh: stop, wipe, start), the
#                           broker binary /root/p3/bin/<bin>/queen, the Kafka facade on in CLUSTER mode as the docs set it up
#                           (guides/kafka.mdx: QUEEN_KAFKA_EMBEDDED=true, QUEEN_KAFKA_NODE_ID=1..3, QUEEN_TOKEN, each node
#                           advertising its own VPC address :9092; without a node id every node coordinates its own copy of
#                           a group), QEXTRA_ENV added; waits for 3 healthy nodes
#   stop-all                Kafka and Queen stopped on every broker (nothing else may run while a system is measured)
#   run <tag> <driver.yaml> <workload.yaml>
#                           one point: workers restarted, samplers on brokers and loaders (common/sampler.sh, tag omb-<tag>),
#                           `bin/benchmark` on loader 1 against the 6 workers, then everything collected into ../runs/<tag>/:
#                           result.json, benchmark.log, samples/*, run.env; ../scripts/summary.py prints the row.
# pkill -f patterns are written [x]yz: a plain pattern also matches the remote shell running the pkill, which then
# kills itself and the rest of the command never runs.
# Env: WHEAP (8G), SYSTEM (kafka|queen, for run.env and the sampler roles), RUN_TIMEOUT (seconds, default 1800),
#      KPROPS_EXTRA (more k=v,k=v Kafka broker props for kafka-reset, e.g. log.retention.ms=-1 for the backlog family).
set -u
S=$(cd "$(dirname "$0")" && pwd); H=$(cd "$S/.." && pwd); . "${OMB_HOSTS:-$H/hosts.env}"
KP=$(cd "$H/../2026-09-30-kafka-pulsar" && pwd); QS=$(cd "$H/../2026-10-12-three-node-new-forms/scripts" && pwd)
R=$REMOTE_ROOT; O=$R/omb; WHEAP=${WHEAP:-8G}
log() { echo "[$(date -u +%H:%M:%S)] omb: $*"; }
rx() { local h=$1; shift; $SSH root@"$h" "$*"; }
workers_list() {
  local l w=""
  for l in "${L_PRIV[@]}"; do w="$w${w:+,}http://$l:8080"; done
  for l in "${L_PRIV[@]}"; do w="$w,http://$l:8082"; done
  echo "$w"
}

cmd=${1:-}; shift || true
case $cmd in
deploy)
  TB=${1:?the OMB -bin.tar.gz}
  for h in "${L_PUB[@]}"; do
    ( rx $h "rm -rf $O $R/omb-dist && mkdir -p $R/omb-dist" && $SCP "$TB" root@$h:$R/omb-dist/omb.tgz \
      && rx $h "cd $R/omb-dist && tar -xzf omb.tgz && mv openmessaging-benchmark-* $O && mkdir -p $O/drivers $O/workloads-run $R/omb-results" \
      && $SCP "$H"/drivers/*.yaml root@$h:$O/drivers/ && $SCP "$H"/workloads/*.yaml root@$h:$O/workloads-run/ \
      && echo "  $h: $(rx $h "ls $O/lib | wc -l") jars, $(rx $h "ls $O/drivers | tr '\n' ' '")" ) &
  done; wait ;;
sync-conf)  # drivers/ and workloads/ only (no new tarball)
  for h in "${L_PUB[@]}"; do ( $SCP "$H"/drivers/*.yaml root@$h:$O/drivers/ && $SCP "$H"/workloads/*.yaml root@$h:$O/workloads-run/ ) & done; wait ;;
workers)
  for h in "${L_PUB[@]}"; do
    rx $h "pkill -f '[i]o.openmessaging.benchmark.worker.BenchmarkWorker'; pkill -f '[i]o.openmessaging.benchmark.Benchmark '; sleep 1;
      cd $O && for p in 8080 8082; do HEAP_OPTS='-Xms$WHEAP -Xmx$WHEAP' setsid nohup bin/benchmark-worker --port \$p --stats-port \$((p+1)) > $R/omb-results/worker-\$p.log 2>&1 < /dev/null & done; true" &
  done; wait
  for t in $(seq 1 60); do
    ok=0; for l in "${L_PRIV[@]}"; do for p in 8080 8082; do rx ${L_PUB[0]} "curl -s -m 2 -o /dev/null -w '%{http_code}' http://$l:$p/counters-stats" 2>/dev/null | grep -q 200 && ok=$((ok+1)); done; done
    [ $ok = $((2*${#L_PRIV[@]})) ] && { log "$ok workers up"; exit 0; }; sleep 2
  done; log "workers not all up ($ok/$((2*${#L_PRIV[@]})))"; exit 1 ;;
kafka-reset)
  P=${1:?total partitions}
  for h in "${B_PUB[@]}"; do rx $h "/root/p3/qc.sh stop >/dev/null 2>&1; pkill -9 -x queen; sysctl -qw vm.dirty_background_bytes=1073741824; sync; echo 3 > /proc/sys/vm/drop_caches" & done; wait
  ( cd "$KP" && HOSTS_ENV="$H/hosts.env" PARTS=$P KPROPS=num.io.threads=24,num.network.threads=12,num.replica.fetchers=12${KPROPS_EXTRA:+,$KPROPS_EXTRA} bash kafka/cluster.sh reset default 2>&1 | tail -3 ) ;;
queen-reset)
  BIN=${1:?the broker binary name under /root/p3/bin}
  for h in "${B_PUB[@]}"; do rx $h "bash $R/kafka/kc.sh stop >/dev/null 2>&1; pkill -9 -f '[k]afka.Kafka'; /root/p3/qc.sh stop; pkill -9 -x queen; /root/p3/qc.sh wipe; sysctl -qw vm.dirty_background_bytes=1073741824; sync; echo 3 > /proc/sys/vm/drop_caches" & done; wait
  for i in 0 1 2; do
    rx ${B_PUB[$i]} "BIN=/root/p3/bin/$BIN/queen EXTRA='QUEEN_KAFKA_EMBEDDED=true QUEEN_KAFKA_NODE_ID=$((i+1)) QUEEN_TOKEN=kafka-facade QUEEN_KAFKA_ADVERTISED_ADDR=${B_PRIV[$i]}:9092 ${QEXTRA_ENV:-}' /root/p3/qc.sh start" &
  done; wait
  for t in $(seq 1 90); do
    ok=0; for i in 0 1 2; do rx ${B_PUB[$i]} "curl -s -m 2 http://${B_PRIV[$i]}:6632/health" 2>/dev/null | grep -q '"status":"healthy"' && ok=$((ok+1)); done
    [ $ok = 3 ] && { log "queen $BIN: 3 healthy ($(rx ${B_PUB[0]} "curl -s -m 2 http://${B_PRIV[0]}:6632/health" | grep -o '"role":"[a-z+]*"' | head -1))"; exit 0; }; sleep 2
  done; log "queen $BIN: not healthy after 180 s"; exit 1 ;;
stop-all)
  for h in "${B_PUB[@]}"; do rx $h "bash $R/kafka/kc.sh stop >/dev/null 2>&1; pkill -9 -f '[k]afka.Kafka'; /root/p3/qc.sh stop >/dev/null 2>&1; pkill -9 -x queen; true" & done; wait; log "all stopped" ;;
run)
  TAG=${1:?tag}; DRV=${2:?driver yaml}; WL=${3:?workload yaml}; SYS=${SYSTEM:-?}
  OUT=$H/runs/$TAG; rm -rf "$OUT"; mkdir -p "$OUT/samples"
  log "run $TAG: system $SYS, driver $DRV, workload $WL"
  bash "$S/omb.sh" workers || { log "workers failed"; exit 1; }
  for h in "${B_PUB[@]}" "${L_PUB[@]}"; do rx $h "bash $R/common/sampler.sh start omb-$TAG 2 >/dev/null 2>&1" & done; wait
  START=$(date -u +%s)
  rx ${L_PUB[0]} "cd $O && HEAP_OPTS='-Xms2G -Xmx2G' timeout ${RUN_TIMEOUT:-1800} bin/benchmark --drivers drivers/$DRV --workers $(workers_list) --output $R/omb-results/$TAG.json workloads-run/$WL > $R/omb-results/$TAG.log 2>&1; echo rc=\$? >> $R/omb-results/$TAG.log"
  END=$(date -u +%s)
  for h in "${B_PUB[@]}" "${L_PUB[@]}"; do rx $h "bash $R/common/sampler.sh stop omb-$TAG >/dev/null 2>&1" & done; wait
  $SCP root@${L_PUB[0]}:$R/omb-results/$TAG.json "$OUT/result.json" 2>/dev/null
  $SCP root@${L_PUB[0]}:$R/omb-results/$TAG.log "$OUT/benchmark.log" 2>/dev/null
  i=0; for h in "${B_PUB[@]}"; do i=$((i+1)); $SCP root@$h:$R/samples/omb-$TAG.txt "$OUT/samples/broker-$i.txt" 2>/dev/null & done
  i=0; for h in "${L_PUB[@]}"; do i=$((i+1)); $SCP root@$h:$R/samples/omb-$TAG.txt "$OUT/samples/loader-$i.txt" 2>/dev/null & done; wait
  # the native driver's own totals (pushed / delivered / acked, lease extensions, pop pool) from every worker's log
  for h in "${L_PUB[@]}"; do rx $h "grep -h -E 'Queen driver closed|Queen verify|Queen: [0-9]+ (consumers|producers)' $R/omb-results/worker-80*.log 2>/dev/null | sed 's/^/$h /'"; done > "$OUT/driver-totals.txt" 2>/dev/null
  { echo "TAG=$TAG"; echo "SYSTEM=$SYS"; echo "DRIVER=$DRV"; echo "WORKLOAD=$WL"; echo "START=$START"; echo "END=$END"
    echo "QEXTRA_ENV=\"${QEXTRA_ENV:-}\""; echo "BIN=${QBIN:-}"; } > "$OUT/run.env"
  cp "$H/drivers/$DRV" "$OUT/driver.yaml"; cp "$H/workloads/$WL" "$OUT/workload.yaml"
  python3 "$S/summary.py" "$OUT" ;;
*) sed -n '2,24p' "$0"; exit 2 ;;
esac
