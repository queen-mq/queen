#!/usr/bin/env bash
# kc.sh (node) — one node of the 3-node Kafka 4.3.1 KRaft cluster (combined broker+controller). Port of 09-24 k3/kc.sh.
#   kc.sh format <cluster-id> [default|fsync]  render config (mkconf.sh; env PARTS HEAP KPROPS JVM_EXTRA) + kafka-storage format
#   kc.sh start                                start (must be formatted); refuses while Queen (6632/7400) or Pulsar
#                                              (2181/3181/6650/8080) listens here; ulimit -n 1048576; checks vm.max_map_count
#   kc.sh stop                                 SIGTERM, wait STOP_WAIT s (default 60), SIGKILL; also any stray kafka.Kafka JVM
#   kc.sh wipe                                 delete data + logs (refuses while running)
#   kc.sh health                               "node= alive= leader= voters= unfenced= fenced= maxlag=" via Kafka's own CLI
#   kc.sh cpu                                  %CPU of the broker JVM over 3 s (top)
#   kc.sh stats                                RSS, threads, maps, fds, data MB, partition dirs, GC pauses, ERROR/WARN/ISR counts
#                                              (all rotated server.log* files; *_since_mark = after the last `kc.sh mark`)
#   kc.sh threads [secs]                       per-thread CPU over secs (default 3) from /proc, grouped by thread name; the
#                                              names come from the jstack taken at `mark` (jstack = a JVM safepoint: never
#                                              inside the measured window), else from a live jstack
#   kc.sh mark                                 the load is about to start: remember the instant (UTC) for stats + thread names
#   kc.sh conf | version | features | uuid     rendered config | versions + running JVM flags | feature levels | cluster id
# Layout (SPEC §1): dist $REMOTE_ROOT/kafka/dist, config $REMOTE_ROOT/kafka/conf, pid/mark $REMOTE_ROOT/kafka/state,
# data $REMOTE_ROOT/data/kafka, logs $REMOTE_ROOT/logs/kafka. Env: FORCE=1 (start despite other listeners / low limits),
# STOP_WAIT, TOOL_HEAP (CLI tools, default -Xmx256m; PROFILE=local -Xmx128m).
set -u
D=$(cd "$(dirname "$0")/.." && pwd)
[ -f "$D/hosts.env" ] && . "$D/hosts.env"
K=$D/kafka; KH=$K/dist; CONF=$K/conf/server.properties; JENV=$K/conf/jvm.env; ST=$K/state
DATA=$D/data/kafka; LOGS=$D/logs/kafka
PROF=${PROFILE:-vm}
MAPS_MIN=4194304; NOFILE=1048576

node_id() {   # sets ID IP from hosts.env B_PRIV by address (never by NIC name)
  local my i
  my=" $(hostname -I 2>/dev/null) $(ip -o -4 addr show 2>/dev/null | awk '{split($4, a, "/"); printf "%s ", a[1]}') "
  ID=""; IP=""
  for i in "${!B_PRIV[@]}"; do case "$my" in *" ${B_PRIV[$i]} "*) ID=$((i + 1)); IP=${B_PRIV[$i]};; esac; done
  [ -n "$ID" ] || { echo "kc.sh: this host ($my) is not a broker in hosts.env B_PRIV (${B_PRIV[*]:-})" >&2; return 2; }
}
gone() {   # gone <pid>: no such process, or a zombie (exited but not reaped: a container whose PID 1 does not reap)
  [ ! -e "/proc/$1" ] || [ "$(sed -E 's/^.*\) ([A-Za-z]).*$/\1/' "/proc/$1/stat" 2>/dev/null)" = Z ]
}
alive() {  # sets P; true only when the pid file points at a live (not zombie) kafka.Kafka JVM
  P=$(cat "$ST/kafka.pid" 2>/dev/null) && [ -n "$P" ] && ! gone "$P" \
    && tr '\0' ' ' < "/proc/$P/cmdline" 2>/dev/null | grep -q 'kafka\.Kafka '
}
tool() {   # a Kafka CLI tool: small serial-GC JVM, niced, its own log dir, bounded admin timeouts
  mkdir -p "$K/tool-logs" "$ST"
  printf 'request.timeout.ms=5000\ndefault.api.timeout.ms=10000\n' > "$ST/admin.properties"
  local th=${TOOL_HEAP:-}; [ -n "$th" ] || { [ "$PROF" = local ] && th=-Xmx128m || th=-Xmx256m; }
  KAFKA_HEAP_OPTS="$th" KAFKA_JVM_PERFORMANCE_OPTS="-XX:+UseSerialGC -XX:TieredStopAtLevel=1" LOG_DIR=$K/tool-logs \
    nice -n 10 "$@"
}
jnames() {  # jnames <pid>: "<nid> <thread name>" per JVM thread. JDK 21 prints nid= in DECIMAL (older JDKs: hex 0x...)
  jstack "$1" 2>/dev/null | grep -E '^"' | sed -nE 's/^"([^"]+)".* nid=(0x[0-9a-f]+|[0-9]+).*/\2 \1/p'
}
others() { # listeners of the other systems on this node (SPEC §0.1)
  ss -ltnH 2>/dev/null | awk '{print $4}' | grep -oE ':(6632|7400|2181|3181|6650|8080)$' | sort -u | tr '\n' ' '
}
log() { echo "[$(date -u +%FT%TZ)] kc.sh n${ID:-?}: $*"; }

main() {
  local cmd=${1:-}; [ $# -gt 0 ] && shift
  case $cmd in uuid|"") ;; *) node_id || return 2;; esac
  case $cmd in
  format)
    local cid=${1:-} mode=${2:-default}
    [[ $cid =~ ^[A-Za-z0-9_][A-Za-z0-9_-]{21}$ ]] || { echo "usage: kc.sh format <22-char base64url cluster id, not starting with '-'> [default|fsync]"; return 2; }
    alive && { log "running (pid $P): stop first"; return 1; }
    bash "$K/mkconf.sh" "$mode" || return 1
    mkdir -p "$ST"; echo "$mode" > "$ST/profile"
    tool "$KH/bin/kafka-storage.sh" format -t "$cid" -c "$CONF" ;;
  start)
    alive && { log "already running (pid $P)"; return 1; }
    [ -f "$CONF" ] && [ -f "$JENV" ] || { log "no rendered config: kc.sh format first"; return 1; }
    grep -qx "node.id=$ID" "$CONF" && grep -q "^listeners=PLAINTEXT://$IP:9092," "$CONF" || { log "$CONF is not node $ID ($IP)"; return 2; }
    [ -f "$DATA/meta.properties" ] || { log "not formatted (kc.sh format)"; return 1; }
    local o; o=$(others)
    if [ -n "$o" ] && [ "${FORCE:-0}" != 1 ]; then log "another system listens here ($o): refusing to start (FORCE=1 overrides)"; return 1; fi
    ss -ltnH 2>/dev/null | awk '{print $4}' | grep -qE ':(9092|9093)$' && { log "9092/9093 already bound here: refusing"; return 1; }
    ulimit -n $NOFILE 2>/dev/null || ulimit -n "$(ulimit -Hn)" 2>/dev/null
    if [ "$(ulimit -n)" != "$NOFILE" ] && [ "$(ulimit -n)" != unlimited ]; then
      if [ "$PROF" = vm ] && [ "${FORCE:-0}" != 1 ]; then log "nofile $(ulimit -n) < $NOFILE: refusing (FORCE=1 overrides)"; return 1; fi
      log "WARN nofile only $(ulimit -n)"
    fi
    local maps; maps=$(cat /proc/sys/vm/max_map_count)
    if [ "$maps" -lt $MAPS_MIN ]; then
      sysctl -qw vm.max_map_count=$MAPS_MIN 2>/dev/null; maps=$(cat /proc/sys/vm/max_map_count)
    fi
    if [ "$maps" -lt $MAPS_MIN ]; then
      if [ "$PROF" = vm ] && [ "${FORCE:-0}" != 1 ]; then log "vm.max_map_count $maps < $MAPS_MIN (run common/os-tune.sh broker): refusing (FORCE=1 overrides)"; return 1; fi
      log "WARN vm.max_map_count $maps < $MAPS_MIN (not settable here; fine for a few hundred partitions)"
    fi
    mkdir -p "$LOGS" "$ST"
    local KAFKA_HEAP_OPTS KAFKA_JVM_PERFORMANCE_OPTS
    . "$JENV"
    export KAFKA_HEAP_OPTS KAFKA_JVM_PERFORMANCE_OPTS LOG_DIR=$LOGS
    nohup "$KH/bin/kafka-server-start.sh" "$CONF" >> "$LOGS/kafkaServer.out" 2>&1 < /dev/null &
    echo $! > "$ST/kafka.pid.tmp" && mv "$ST/kafka.pid.tmp" "$ST/kafka.pid"
    sleep 2
    alive || { log "exited at once:"; tail -20 "$LOGS/kafkaServer.out"; return 1; }
    log "started pid $P ($IP, profile $(cat "$ST/profile" 2>/dev/null)) heap='$KAFKA_HEAP_OPTS' nofile=$(ulimit -n) max_map_count=$maps" ;;
  stop)
    local w=${STOP_WAIT:-60} i s
    if alive; then
      kill "$P"
      for i in $(seq 1 $((w * 10))); do gone "$P" && break; sleep 0.1; done
      if ! gone "$P"; then log "no exit after ${w}s: SIGKILL $P"; kill -9 "$P"; sleep 0.5; else log "stopped pid $P in $((i / 10)).$((i % 10))s"; fi
    fi
    for s in $(pgrep -f 'kafka\.Kafka ' 2>/dev/null); do
      tr '\0' ' ' < "/proc/$s/cmdline" 2>/dev/null | grep -q 'java' || continue
      log "stray kafka.Kafka JVM pid $s: SIGKILL"; kill -9 "$s" 2>/dev/null
    done
    rm -f "$ST/kafka.pid"; return 0 ;;
  wipe)
    alive && { log "running (pid $P): stop first"; return 1; }
    rm -rf "$DATA" "$LOGS" "$K/tool-logs" "$ST/mark.time" "$ST/threads.names"
    log "wiped $DATA $LOGS" ;;
  health)
    local A=0 L=- V=0 U=0 F=0 ML=- q e
    if alive; then
      A=1
      q=$(tool "$KH/bin/kafka-metadata-quorum.sh" --bootstrap-controller "$IP:9093" --command-config "$ST/admin.properties" describe --status 2>/dev/null)
      L=$(echo "$q" | awk -F: '/^LeaderId/{gsub(/[ \t]/, "", $2); print $2}')
      V=$(echo "$q" | grep '^CurrentVoters' | grep -oE '"id": *[0-9]+' | wc -l | tr -d ' ')
      ML=$(echo "$q" | awk -F: '/^MaxFollowerLag:/{gsub(/[ \t]/, "", $2); print $2}')
      e=$(tool "$KH/bin/kafka-cluster.sh" list-endpoints --bootstrap-server "$IP:9092" --command-config "$ST/admin.properties" --include-fenced-brokers 2>/dev/null)
      U=$(echo "$e" | awk '$NF == "broker" && $(NF-1) == "unfenced"' | wc -l | tr -d ' ')
      F=$(echo "$e" | awk '$NF == "broker" && $(NF-1) == "fenced"' | wc -l | tr -d ' ')
    fi
    echo "node=$ID alive=$A leader=${L:--} voters=$V unfenced=$U fenced=$F maxlag=${ML:--}" ;;
  cpu)
    alive || { echo "-1"; return 1; }
    top -b -n 2 -d 3 -p "$P" | awk -v p="$P" '$1 == p {c = $9} END {print c}' ;;
  stats)
    alive || { echo "node=$ID not running"; return 1; }
    local S=/proc/$P/status GCP M CNT
    GCP=$(cat "$LOGS"/kafkaServer-gc.log* 2>/dev/null | grep -E '\]\[gc *\] GC\([0-9]+\) Pause.*ms$' | grep -oE '[0-9.]+ms$' | tr -d 'ms')
    M=$(cat "$ST/mark.time" 2>/dev/null)
    # server.log rotates hourly (and at 100k partitions the volume is large): count over every rotated file, oldest first
    CNT=$(ls -tr "$LOGS"/server.log* 2>/dev/null | xargs -r cat | awk -v m="$M" '
      / ERROR / {e++; if (m != "" && substr($0, 2, 19) >= m) em++}
      / WARN /  {w++}
      /Shrinking ISR/  {s++; if (m != "" && substr($0, 2, 19) >= m) sm++}
      /ISR updated to/ {u++; if (m != "" && substr($0, 2, 19) >= m) um++}
      END {printf "errors=%d warns=%d isr_shrinks=%d isr_updates=%d errors_since_mark=%d isr_shrinks_since_mark=%d isr_updates_since_mark=%d", e, w, s, u, em, sm, um}')
    echo "node=$ID rss_MB=$(( $(awk '/^VmRSS/{print $2}' "$S") / 1024 )) threads=$(awk '/^Threads/{print $2}' "$S")" \
      "maps=$(wc -l < "/proc/$P/maps") fds=$(ls "/proc/$P/fd" | wc -l) data_MB=$(du -sm "$DATA" 2>/dev/null | cut -f1)" \
      "part_dirs=$(ls "$DATA" 2>/dev/null | grep -cE -- '-[0-9]+$') gc_pauses=$(echo "$GCP" | grep -c .)" \
      "gc_max_ms=$(echo "$GCP" | sort -n | tail -1) gc_sum_ms=$(echo "$GCP" | awk '{s += $1} END {printf "%.0f", s}')" \
      "$CNT mark='${M:-none}' uptime_s=$(( $(date +%s) - $(stat -c %Y "$ST/kafka.pid") ))" ;;
  mark)
    alive || { echo "node=$ID not running"; return 1; }
    date -u '+%Y-%m-%d %H:%M:%S' > "$ST/mark.time"
    { echo "pid=$P"; jnames "$P"; } > "$ST/threads.names.tmp" && mv "$ST/threads.names.tmp" "$ST/threads.names"
    echo "node=$ID marked at $(cat "$ST/mark.time") UTC ($(($(wc -l < "$ST/threads.names") - 1)) thread names)" ;;
  threads)  # per-thread CPU over [s] s, grouped by thread name (digits dropped); jstack maps names to native tids
    alive || { echo "node=$ID not running"; return 1; }
    local s=${1:-3} T; T=$(mktemp -d)
    for t in /proc/"$P"/task/*; do echo "${t##*/} $(awk '{print $14 + $15}' "$t/stat" 2>/dev/null)"; done > "$T/a"
    sleep "$s"
    for t in /proc/"$P"/task/*; do echo "${t##*/} $(awk '{print $14 + $15}' "$t/stat" 2>/dev/null)"; done > "$T/b"
    if [ "$(head -1 "$ST/threads.names" 2>/dev/null)" = "pid=$P" ]; then tail -n +2 "$ST/threads.names" > "$T/n"; fi
    [ -s "$T/n" ] || jnames "$P" > "$T/n"   # no usable names from `mark` (other JVM, or jstack failed then): live jstack
    python3 - "$T" "$s" "$ID" <<'PY'
import re, sys
t, s, node = sys.argv[1], float(sys.argv[2]), sys.argv[3]
hz = 100.0
rd = lambda f: {l.split()[0]: int(l.split()[1]) for l in open(f) if len(l.split()) == 2}
a, b = rd(t + "/a"), rd(t + "/b")
names = {}
for l in open(t + "/n"):
    nid, name = l.rstrip("\n").split(" ", 1)
    names[str(int(nid, 16) if nid.startswith("0x") else int(nid))] = name
g = {}
for tid, v in b.items():
    d = (v - a.get(tid, v)) / hz / s
    key = re.sub(r"-?\d+", "", names.get(tid, "?native"))
    c, n, mx = g.get(key, (0.0, 0, 0.0))
    g[key] = (c + d, n + 1, max(mx, d))
tot = sum(c for c, _, _ in g.values())
print(f"node={node} total={tot:.2f} cores over {s:.0f}s ({len(b)} threads, {len(names)} named by jstack)")
for k, (c, n, mx) in sorted(g.items(), key=lambda kv: -kv[1][0])[:15]:
    print(f"  {c:6.2f} cores  {n:4d} thr  max1={mx:4.2f}  {k[:70]}")
PY
    rm -rf "$T" ;;
  conf)
    echo "# $CONF"; grep -vE '^(#|$)' "$CONF"; echo "# $JENV"; grep -vE '^(#|$)' "$JENV" ;;
  version)  # versions + what the running JVM actually got (heap, GC, pre-touch, nofile) + the rendered config's hash
    local jv="" h
    h=$(grep -vE '^(#|$)|^node\.id=|^listeners=|^advertised\.listeners=' "$CONF" 2>/dev/null | md5sum | cut -c1-8)
    if alive; then
      jv=$(tr '\0' '\n' < "/proc/$P/cmdline" | grep -E '^-Xm[sx]|^-XX:(\+UseG1GC|MaxGCPauseMillis|InitiatingHeapOccupancyPercent|\+AlwaysPreTouch)' | tr '\n' ' ')
      jv="pid=$P jvm=[${jv% }] nofile=$(awk '/Max open files/{print $4}' "/proc/$P/limits")"
    fi
    echo "node=$ID kafka=$(sed -n 's/^version=//p' "$KH/.installed" 2>/dev/null) java=\"$(java -version 2>&1 | head -1)\"" \
      "profile=$(cat "$ST/profile" 2>/dev/null) PROFILE=$PROF conf_hash=$h ${jv:-not-running}" ;;
  features)  # finalized feature levels (metadata.version; group.version >= 1 = KIP-848 consumer groups available)
    alive || { echo "node=$ID not running"; return 1; }
    tool "$KH/bin/kafka-features.sh" --bootstrap-server "$IP:9092" --command-config "$ST/admin.properties" describe 2>/dev/null \
      | awk '{f = ""; l = ""; for (i = 1; i <= NF; i++) { if ($i == "Feature:") f = $(i+1); if ($i == "FinalizedVersionLevel:") l = $(i+1) } if (f != "") printf "%s=%s ", f, l} END {print ""}' ;;
  uuid)   # like Kafka's Uuid.randomUuid(): never starting with '-', which kafka-storage.sh would read as a flag
    python3 -c 'import uuid, base64
while True:
    c = base64.urlsafe_b64encode(uuid.uuid4().bytes).decode().rstrip("=")
    if not c.startswith("-"): break
print(c)' ;;
  *) awk 'NR > 1 && /^#/ {print; next} NR > 1 {exit}' "$0"; return 2 ;;
  esac
}
main "$@"; exit $?
