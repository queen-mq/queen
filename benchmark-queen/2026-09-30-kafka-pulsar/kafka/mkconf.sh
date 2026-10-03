#!/usr/bin/env bash
# mkconf.sh (node) [default|fsync] — render THIS node's Kafka 4.3.1 config from hosts.env + PROFILE (SPEC.md §5):
#   $REMOTE_ROOT/kafka/conf/server.properties   every SPEC §5 value, each with a one-line "why" comment above it
#   $REMOTE_ROOT/kafka/conf/jvm.env             KAFKA_HEAP_OPTS + KAFKA_JVM_PERFORMANCE_OPTS, sourced by kc.sh start
# The node finds its index by matching its own addresses (hostname -I) against B_PRIV — never a NIC name (droplets: eth1,
# rehearsal containers: eth0). node.id = index + 1. The 3 nodes differ only in node.id and the listener IP; the two
# profiles differ only in the flush block. Env: PROFILE (vm|local, from hosts.env), PARTS (total partitions of the coming
# run: sizes the heap), HEAP (e.g. 8g: overrides the heap rule), KPROPS (opt-in extra k=v,k=v lines, NOT part of the SPEC;
# they are appended in their own marked block so a run.env always shows them), JVM_EXTRA (opt-in JVM flags appended to
# Kafka's own, e.g. -XX:+UseTransparentHugePages; NOT part of the SPEC, marked in jvm.env).
set -u
main() {
  local MODE=${1:-default}
  case $MODE in default|fsync) ;; *) echo "usage: mkconf.sh [default|fsync]" >&2; return 2;; esac
  local D; D=$(cd "$(dirname "$0")/.." && pwd)
  [ -f "$D/hosts.env" ] || { echo "mkconf: no $D/hosts.env" >&2; return 2; }
  . "$D/hosts.env"
  local PROF=${PROFILE:-vm} K=$D/kafka
  case $PROF in vm|local) ;; *) echo "mkconf: PROFILE must be vm|local (got $PROF)" >&2; return 2;; esac
  local MYIPS IDX="" i
  MYIPS=" $(hostname -I 2>/dev/null) $(ip -o -4 addr show 2>/dev/null | awk '{split($4, a, "/"); printf "%s ", a[1]}') "
  for i in "${!B_PRIV[@]}"; do case "$MYIPS" in *" ${B_PRIV[$i]} "*) IDX=$i;; esac; done
  [ -n "$IDX" ] || { echo "mkconf: none of this host's addresses ($MYIPS) is in B_PRIV (${B_PRIV[*]})" >&2; return 2; }
  local ID=$((IDX + 1)) IP=${B_PRIV[$IDX]} N=${#B_PRIV[@]}
  local VOTERS="" j
  for j in "${!B_PRIV[@]}"; do VOTERS="$VOTERS${VOTERS:+,}$((j + 1))@${B_PRIV[$j]}:9093"; done
  mkdir -p "$K/conf"
  local OUT=$K/conf/server.properties TMP=$K/conf/.server.properties.$$

  {
  cat <<EOF
# Kafka 4.3.1, KRaft, combined broker+controller: node $ID of $N ($IP), flush profile "$MODE", PROFILE=$PROF.
# Rendered by kafka/mkconf.sh on $(hostname) at $(date -u +%FT%TZ) from hosts.env. Do not edit: re-render (kc.sh format).
# Kafka vs Pulsar vs Queen harness, SPEC.md §5. Each value has its reason on the line above it; defaults are named.

############################# KRaft quorum and listeners
# 3-node cluster, no extra controller boxes: every node is a broker AND a KRaft voter.
process.roles=broker,controller
# this host's position in hosts.env B_PRIV, plus 1 (found by address, never by NIC name)
node.id=$ID
# static voters on the 3 private IPs (formatted once with one cluster id; no --standalone / dynamic quorum)
controller.quorum.voters=$VOTERS
# all traffic on the private VPC address; nothing listens on the public IP
listeners=PLAINTEXT://$IP:9092,CONTROLLER://$IP:9093
# clients and followers reach this node by its private IP
advertised.listeners=PLAINTEXT://$IP:9092,CONTROLLER://$IP:9093
# replication traffic on the same plaintext listener as the clients (one NIC; no TLS on any system in the harness)
inter.broker.listener.name=PLAINTEXT
# the KRaft quorum talks on its own listener/port 9093
controller.listener.names=CONTROLLER
# plaintext everywhere, as Queen and Pulsar run in this harness
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
# the one local ext4 data disk (metadata log lives here too); logs go to $D/logs/kafka (kc.sh sets LOG_DIR)
log.dirs=$D/data/kafka

############################# threads (16 vCPU / 31 GB droplets)
# default 3 is sized for 4-core boxes; ~1k connections per broker (9 loaders x 37 franz-go clients x produce/fetch/group conns)
num.network.threads=8
# = vCPU: request handler threads (default 8); acks=all produce and fetch handling run here
num.io.threads=16
# fetcher threads PER SOURCE broker (x2 sources = 16 per node = vCPU); default 1 cannot follow ~200 MB/s per broker, and
# 09-30: with 4 (8 per node, ~4k partitions per fetcher at 50k) acks=all produce p50 was ~500 ms at 50k partitions with
# Kafka at 3 cores/node: long follower fetch rounds, not CPU. 8 = the 09-24 per-fetcher load at 50k (Alice: "no sense to
# run under powered")
num.replica.fetchers=8
# KafkaScheduler threads (default 10): partition.metadata flushes, retention, checkpoints at 100k partitions
background.threads=16
# log loading at start and flushing at stop (default 2): restart/wipe speed at 100k partitions
num.recovery.threads.per.data.dir=8

############################# sockets
# -1 = do not set SO_SNDBUF: kernel autotuning (os-tune.sh raises net.core.wmem_max/tcp_wmem to 64 MiB; default 100 KiB caps it)
socket.send.buffer.bytes=-1
# -1 = do not set SO_RCVBUF: kernel autotuning (os-tune.sh raises rmem maxima; default 100 KiB)
socket.receive.buffer.bytes=-1
# -1 = autotuned follower fetch sockets too (default 64 KiB caps a replica fetcher's window)
replica.socket.receive.buffer.bytes=-1
# ~1k clients connect at once at READY (default 50); net.core.somaxconn=4096 from os-tune.sh
socket.listen.backlog.size=4096
# >= consumer fetch sessions + follower sessions per broker without evictions (default 1000)
max.incremental.fetch.session.cache.slots=4000

############################# durability class (SPEC §0.3): RF 3, min.insync 2, acks=all + idempotent producers
# 3 copies of every partition
default.replication.factor=3
# acks=all returns once 2 in-sync copies have it; a partition below 2 in sync refuses writes instead of losing them
min.insync.replicas=2
# consumer offsets: same class (Kafka default 3; the shipped config/server.properties lowers it to 1)
offsets.topic.replication.factor=3
# transaction state (idempotent producers only, but same class): Kafka default 3 (shipped file: 1)
transaction.state.log.replication.factor=3
# transaction state min ISR: Kafka default 2 (shipped file: 1)
transaction.state.log.min.isr=2
# share-group state topic: Kafka default 3 (shipped file: 1); unused by the loaders, same class anyway
share.coordinator.state.topic.replication.factor=3
# share-group state min ISR: Kafka default 2 (shipped file: 1)
share.coordinator.state.topic.min.isr=2
# never elect an out-of-sync replica: it would silently drop acked records (Kafka default false, stated)
unclean.leader.election.enable=false
# topics are created explicitly (kload -create-only: RF 3, min.insync 2); a typo must not create a topic
auto.create.topics.enable=false

############################# retention and segments (400 GB disk, bounded soaks)
# 10 min (default 7 days): a 15-min soak at 1M msg/s cannot fill the disk
log.retention.ms=600000
# look for deletable segments every minute (default 5 min) so deletion keeps pace inside a soak
log.retention.check.interval.ms=60000
# 256 MiB (default 1 GiB): segments roll, and retention can delete them, within a soak
log.segment.bytes=268435456
# keep the producer's lz4 batches as they arrive: never recompress on the broker (Kafka default, stated)
compression.type=producer
# 16 MiB (default 1 MiB + 12 B): the KIP-848 group coordinator writes a group's metadata for 100k partitions as one batch
# to __consumer_offsets; at 1 MiB every join failed with RecordTooLargeException (09-30; 09-24 raised it for 500k too).
# Producer batches stay <= 1 MiB (kload -batch-max-bytes), so data topics are unaffected
message.max.bytes=16777216

EOF
  if [ "$MODE" = fsync ]; then cat <<'EOF'
############################# flush profile "fsync" (parity point, NOT the main grid)
# fsync every append before it counts (leader, followers, internal topics): Queen/Pulsar-style fsync-before-ack
log.flush.interval.messages=1
EOF
  else cat <<'EOF'
############################# flush profile "default" (the main grid)
# no forced fsync (log.flush.interval.messages/ms unset = Kafka's production model): durability by replication,
# the page cache is flushed by the OS (vm.dirty_* from os-tune.sh)
EOF
  fi
  if [ "$PROF" = local ]; then cat <<'EOF'

############################# PROFILE=local sizing (rehearsal containers only; never on the droplets)
# the log cleaner allocates this on the heap at start (default 128 MiB = a third of the 384m rehearsal heap)
log.cleaner.dedupe.buffer.size=33554432
EOF
  fi
  if [ -n "${KPROPS:-}" ]; then
    echo; echo "############################# opt-in knobs (KPROPS): NOT part of SPEC §5, set for this run on purpose"
    local kv
    IFS=',' read -ra KVS <<< "$KPROPS"
    for kv in "${KVS[@]}"; do
      case $kv in *=*) echo "# opt-in via KPROPS"; echo "$kv" ;; *) echo "mkconf: bad KPROPS entry '$kv' (want k=v)" >&2; return 2;; esac
    done
  fi
  } > "$TMP" || { rm -f "$TMP"; return 1; }
  mv "$TMP" "$OUT"

  # heap by partition count (SPEC §5: 6g <= 10k partitions, 10g >= 50k; the rest of 31 GB is page cache = Kafka's read path)
  local H WHY
  if [ -n "${HEAP:-}" ]; then H=$HEAP; WHY="HEAP=$HEAP set explicitly"
  elif [ "$PROF" = local ]; then H=384m; WHY="PROFILE=local: rehearsal containers of 1.1 GB"
  elif [ "${PARTS:-0}" -le 10000 ] 2>/dev/null; then H=6g; WHY="PARTS=${PARTS:-unset} <= 10k partitions"
  else H=10g; WHY="PARTS=$PARTS > 10k partitions (SPEC: 10g at >= 50k; 10k-50k rounds up)"
  fi
  # Kafka's own KAFKA_JVM_PERFORMANCE_OPTS, read from this dist's kafka-run-class.sh so it is exactly what Kafka ships
  local RC=$K/dist/bin/kafka-run-class.sh PERF
  PERF=$(sed -nE 's/^[[:space:]]*KAFKA_JVM_PERFORMANCE_OPTS="([^"]*)"[[:space:]]*$/\1/p' "$RC" 2>/dev/null | head -1)
  [ -n "$PERF" ] || PERF="-server -XX:+UseG1GC -XX:MaxGCPauseMillis=20 -XX:InitiatingHeapOccupancyPercent=35 -XX:+ExplicitGCInvokesConcurrent -XX:MaxInlineLevel=15"
  cat > "$K/conf/.jvm.env.$$" <<EOF
# jvm.env for node $ID ($IP), rendered by mkconf.sh at $(date -u +%FT%TZ); sourced by kc.sh start.
# heap: $WHY. -Xms = -Xmx: the heap never resizes inside a window.
KAFKA_HEAP_OPTS="-Xms$H -Xmx$H"
# Kafka's own G1 options (bin/kafka-run-class.sh) + AlwaysPreTouch: the heap is pre-faulted at start, no first-touch stalls
# in the measured window. GC log stays on: kafka-server-start.sh's -loggc -> \$LOG_DIR/kafkaServer-gc.log (10 x 100 MB).
KAFKA_JVM_PERFORMANCE_OPTS="$PERF -XX:+AlwaysPreTouch${JVM_EXTRA:+ $JVM_EXTRA}"
EOF
  [ -n "${JVM_EXTRA:-}" ] && echo "# opt-in JVM_EXTRA (NOT part of SPEC §5): $JVM_EXTRA" >> "$K/conf/.jvm.env.$$"
  mv "$K/conf/.jvm.env.$$" "$K/conf/jvm.env"
  echo "mkconf: node $ID/$N ip=$IP profile=$MODE PROFILE=$PROF heap=$H ($WHY) -> $OUT$([ -n "${KPROPS:-}" ] && echo " +KPROPS=$KPROPS")$([ -n "${JVM_EXTRA:-}" ] && echo " +JVM_EXTRA=$JVM_EXTRA")"
}
main "$@"; exit $?
