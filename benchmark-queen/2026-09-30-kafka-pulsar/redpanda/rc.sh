#!/usr/bin/env bash
# rc.sh (node) — one node of the 3-node Redpanda 26.2.3 cluster (the analogue of kafka/kc.sh).
#   rc.sh render                 mkconf.sh: redpanda.yaml + .bootstrap.yaml + overrides.txt (env PARTS FALLOC RPROPS START_FLAGS)
#   rc.sh tune                   `rpk redpanda tune all` with this node's production config. Before applying, the tuners' own
#                                plan (--output-script) is read and the current value of every path/sysctl it will write is
#                                saved ONCE in state/os-baseline.sh (the restore script; kept until untune); state/tune.txt =
#                                tuner table + every value before -> after. Idempotent.
#   rc.sh untune                 restore the pre-Redpanda OS state from state/os-baseline.sh (SPEC os-tune values: IRQ
#                                affinities, RPS/RFS, disk scheduler/nomerges, clocksource, aio-max-nr), drop the ballast
#   rc.sh iotune [duration]      `rpk iotune` on the data directory -> state/io-config.yaml (once per node; default 10m, the
#                                vendor default); refuses while Redpanda runs
#   rc.sh check                  `rpk redpanda check` (the production checks `rpk redpanda start --check=true` runs)
#   rc.sh start                  refuses while Queen (6632/7400), Kafka (9092/9093) or Pulsar (2181/3181/6650/8080) listens here
#                                (FORCE=1 overrides) or a Redpanda port is bound; ulimit -n 1048576 -l unlimited; nohup `rpk
#                                redpanda start --check=true` (rpk execs redpanda: same pid); oom_score_adj -950 (the unit's)
#   rc.sh stop                   SIGINT (Redpanda's clean shutdown), wait STOP_WAIT s (default 60), SIGKILL; also strays
#   rc.sh wipe                   delete data + logs (refuses while running); keeps state/ (baseline, io-config, install record)
#   rc.sh health                 "node= alive= ready= healthy= controller= nodes= down= leaderless= under_replicated="
#   rc.sh stats                  RSS, threads, fds, data MB, partition dirs, log ERROR/WARN counts (all / since mark),
#                                leadership changes since mark, reactor busy seconds, disk free
#   rc.sh threads [secs]         per-thread CPU over secs (default 3) from /proc grouped by thread name (reactor-N, syscall-N,
#                                ...), plus the shards' own busy time over the same interval (Seastar reactors poll: /proc CPU
#                                counts the polling, redpanda_cpu_busy_seconds_total does not)
#   rc.sh busy                   one line "t=<unix s> busy_s=<sum over shards of redpanda_cpu_busy_seconds_total>
#                                produce_hist=<le:count,...>" (broker-side produce latency, cumulative log2 buckets)
#   rc.sh transfers              leadership transfers started on this node (leader balancer) + unix time of the last one
#   rc.sh mark                   the load is about to start: remember the instant (UTC) for stats
#   rc.sh conf | version | ports rendered files | versions + running command line + limits | Redpanda ports bound here
# Layout (SPEC §1): config $REMOTE_ROOT/redpanda/conf, pid/mark/baseline $REMOTE_ROOT/redpanda/state, data
# $REMOTE_ROOT/data/redpanda, logs $REMOTE_ROOT/logs/redpanda/redpanda.log. Env: FORCE=1, STOP_WAIT.
set -u
D=$(cd "$(dirname "$0")/.." && pwd)
[ -f "$D/hosts.env" ] && . "$D/hosts.env"
RP=$D/redpanda; CONF=$RP/conf/redpanda.yaml; ST=$RP/state
DATA=$D/data/redpanda; LOGS=$D/logs/redpanda
NOFILE=1048576
OTHERS='6632|7400|9092|9093|2181|3181|6650|8080'   # Queen, Kafka (9092 is also ours: checked before our own start), Pulsar
OURS='9092|9644|33145|8081|8082'

node_id() {   # sets ID IP from hosts.env B_PRIV by address (never by NIC name)
  local my i
  my=" $(hostname -I 2>/dev/null) $(ip -o -4 addr show 2>/dev/null | awk '{split($4, a, "/"); printf "%s ", a[1]}') "
  ID=""; IP=""
  for i in "${!B_PRIV[@]}"; do case "$my" in *" ${B_PRIV[$i]} "*) ID=$((i + 1)); IP=${B_PRIV[$i]};; esac; done
  [ -n "$ID" ] || { echo "rc.sh: this host ($my) is not a broker in hosts.env B_PRIV (${B_PRIV[*]:-})" >&2; return 2; }
}
gone() { [ ! -e "/proc/$1" ] || [ "$(sed -E 's/^.*\) ([A-Za-z]).*$/\1/' "/proc/$1/stat" 2>/dev/null)" = Z ]; }
alive() {  # sets P; true only when the pid file points at a live redpanda process (rpk execs it: same pid)
  P=$(cat "$ST/redpanda.pid" 2>/dev/null) && [ -n "$P" ] && ! gone "$P" \
    && tr '\0' ' ' < "/proc/$P/cmdline" 2>/dev/null | grep -q 'redpanda --redpanda-cfg'
}
strays() { pgrep -f '[r]edpanda --redpanda-cfg' 2>/dev/null; }
listening() { ss -ltnH 2>/dev/null | awk '{print $4}' | grep -oE ":($1)\$" | sort -u | tr '\n' ' '; }
radm() { curl -sS -m "${2:-5}" "http://$IP:9644$1"; }                      # admin API of this node
rpkx() { rpk "$@" -X brokers="$IP:9092" -X admin.hosts="$IP:9644" 2>&1 | grep -v 'ec2imds\|IMDSv1'; }
busy_s() {  # sum over shards of the reactors' busy time; the name filter keeps the scrape at ~1 KB even at 100k partitions
  radm '/public_metrics?__name__=redpanda_cpu_busy_seconds_total' 10 2>/dev/null \
    | awk '/^redpanda_cpu_busy_seconds_total[{ ]/ {s += $NF; n++} END {if (n) printf "%.3f", s; else printf "NA"}'
}
produce_hist() {  # "le:count,..." of redpanda_kafka_request_latency_seconds{redpanda_request="produce"} (server side, cumulative)
  radm '/public_metrics?__name__=redpanda_kafka_request_latency_seconds*' 10 2>/dev/null \
    | awk '/^redpanda_kafka_request_latency_seconds_bucket/ && /redpanda_request="produce"/ {
        if (match($0, /le="[^"]+"/)) { le = substr($0, RSTART + 4, RLENGTH - 5); s = s (s == "" ? "" : ",") le ":" $NF } }
        END {print (s == "" ? "NA" : s)}'
}
log() { echo "[$(date -u +%FT%TZ)] rc.sh n${ID:-?}: $*"; }

tune_plan_parse() {  # tune_plan_parse <plan.sh>: "kind<TAB>target<TAB>value" per action of rpk's tuning script
  python3 - "$1" <<'PY'
import re, shlex, sys
for l in open(sys.argv[1]):
    l = l.strip()
    if not l or l.startswith("#"): continue
    m = re.match(r"^echo\s+(.+?)\s*>\s*(\S+)$", l)
    if m: print(f"file\t{m.group(2)}\t{shlex.split(m.group(1))[0]}"); continue
    m = re.match(r"^sysctl\s+-w\s+([^=\s]+)=(.*)$", l)
    if m: print(f"sysctl\t{m.group(1)}\t{m.group(2).strip()}"); continue
    m = re.match(r"^truncate\s+-s\s+(\d+)\s+(\S+)$", l)
    if m: print(f"ballast\t{m.group(2)}\t{m.group(1)}"); continue
    print(f"other\t{l}\t-")
PY
}
same() {  # same <target> <a> <b>: equal values; CPU masks (smp_affinity, rps/xps_cpus) compared as numbers (0001 = 00000001)
  case $1 in
    */smp_affinity|*/rps_cpus|*/xps_cpus) [ "$((16#$(echo "$2" | tr -d ',')))" = "$((16#$(echo "$3" | tr -d ',')))" ] 2>/dev/null ;;
    *) [ "$2" = "$3" ] ;;
  esac
}
cur_value() {  # cur_value <kind> <target>: the value in force now, in the form the tuner writes it
  case $1 in
    file) if [ ! -e "$2" ]; then echo "<absent>"
          elif [ "${2##*/}" = scheduler ]; then sed -nE 's/.*\[([^]]+)\].*/\1/p' "$2"
          else tr -d '\n' < "$2"; fi ;;
    sysctl) sysctl -n "$2" 2>/dev/null | tr -s '\t ' ' ' ;;
    ballast) [ -e "$2" ] && stat -c %s "$2" || echo "<absent>" ;;
    *) echo "?" ;;
  esac
}

main() {
  local cmd=${1:-}; [ $# -gt 0 ] && shift
  case $cmd in ""|-h|--help) ;; *) node_id || return 2;; esac
  case $cmd in
  render)
    bash "$RP/mkconf.sh" ;;
  tune)
    [ -f "$CONF" ] || { log "no rendered config: rc.sh render first"; return 1; }
    mkdir -p "$ST" "$DATA"
    local plan=$ST/tune-plan.sh base=$ST/os-baseline.sh kind tgt val old new rc added=0 TAB
    TAB=$(printf '\t')
    rm -f "$plan"
    rpk redpanda tune all --config "$CONF" --output-script "$plan" > "$ST/tune-plan.out" 2>&1 \
      || { log "rpk could not plan the tuning:"; cat "$ST/tune-plan.out"; return 1; }
    [ -f "$plan" ] || : > "$plan"   # nothing left to change
    tune_plan_parse "$plan" > "$ST/tune-plan.tsv"
    # OS baseline = the value of every target BEFORE any Redpanda tuner wrote it, taken the first time the target shows up
    # in a plan (rpk plans only what still differs, so a later pass can never record a tuned value as "before")
    if [ ! -s "$base" ]; then
      { echo "#!/usr/bin/env bash"; echo "# os-baseline.sh: restores what rpk redpanda tune changed on $(hostname) (first tune $(date -u +%FT%TZ))"; } > "$base"
      : > "$ST/os-baseline.tsv"; : > "$ST/os-baseline.values"
    fi
    while IFS="$TAB" read -r kind tgt val; do
      awk -F'\t' -v t="$tgt" '$2 == t {f = 1} END {exit !f}' "$ST/os-baseline.tsv" && continue
      old=$(cur_value "$kind" "$tgt")
      printf '%s\t%s\t%s\n' "$kind" "$tgt" "$val" >> "$ST/os-baseline.tsv"
      printf '%s\t%s\t%s\n' "$kind" "$tgt" "$old" >> "$ST/os-baseline.values"
      case $kind in
        file) [ "$old" = "<absent>" ] || echo "[ \"\$(cat $tgt 2>/dev/null)\" = '$old' ] || echo '$old' > $tgt 2>/dev/null || echo 'untune: cannot restore $tgt' >&2   # was: $old" ;;
        sysctl) echo "sysctl -qw $tgt='$old'   # was: $old" ;;
        ballast) [ "$old" = "<absent>" ] && echo "rm -f $tgt   # no ballast before" ;;
        *) echo "# not restorable automatically: $tgt" ;;
      esac >> "$base"
      added=$((added + 1))
    done < "$ST/tune-plan.tsv"
    [ "$added" -gt 0 ] && log "OS baseline: $added values recorded before their first tuning ($(grep -c . "$ST/os-baseline.tsv") in all, restore script $base)"
    rpk redpanda tune all --config "$CONF" > "$ST/tune-apply.out" 2>&1; rc=$?
    { echo "# rpk redpanda tune all on $(hostname) ($IP) at $(date -u +%FT%TZ), rc=$rc, config $CONF; this pass planned $(grep -c . "$ST/tune-plan.tsv") writes"
      grep -v 'ec2imds\|IMDSv1' "$ST/tune-apply.out"
      echo "# every value a Redpanda tuner has written on this box: OS baseline (before its first tune; restored by rc.sh untune) -> now"
      while IFS="$TAB" read -r kind tgt val; do
        old=$(awk -F'\t' -v t="$tgt" '$2 == t {print $3; exit}' "$ST/os-baseline.values")
        new=$(cur_value "$kind" "$tgt")
        printf '%-7s %-72s %s -> %s%s\n' "$kind" "$tgt" "${old:-?}" "$new" "$(same "$tgt" "$new" "$val" || [ "$kind" = ballast ] || echo "   (NOT APPLIED: wanted $val)")"
      done < "$ST/os-baseline.tsv"; } > "$ST/tune.txt"
    log "tuners applied (rc=$rc): $(grep -cE '^(file|sysctl|ballast) ' "$ST/tune.txt") values differ from the OS baseline, $(grep -c 'NOT APPLIED' "$ST/tune.txt") refused by the kernel (state/tune.txt)"
    return $rc ;;
  untune)
    local base=$ST/os-baseline.sh
    [ -s "$base" ] || { log "no OS baseline here: nothing was tuned (or already restored)"; return 0; }
    if alive && [ "${FORCE:-0}" != 1 ]; then log "Redpanda runs (pid $P): stop it first (FORCE=1 overrides)"; return 1; fi
    bash "$base"
    local kind tgt val now bad=0
    while IFS="$(printf '\t')" read -r kind tgt val; do
      now=$(cur_value "$kind" "$tgt")
      case $kind in ballast) [ "$val" = "<absent>" ] && [ "$now" != "<absent>" ] && bad=$((bad + 1));; file|sysctl) [ "$now" = "$val" ] || { echo "  still $tgt = $now (baseline $val)"; bad=$((bad + 1)); };; esac
    done < "$ST/os-baseline.values"
    mv "$base" "$ST/os-baseline.restored-$(date -u +%Y%m%dT%H%M%SZ).sh"
    log "OS restored to the pre-Redpanda baseline ($bad values could not be restored)"
    [ "$bad" = 0 ] ;;
  iotune)
    alive && { log "Redpanda runs (pid $P): iotune would measure a busy disk; stop first"; return 1; }
    [ -f "$CONF" ] || { log "no rendered config: rc.sh render first"; return 1; }
    mkdir -p "$DATA" "$ST"
    local dur=${1:-10m}
    log "rpk iotune --directories $DATA --duration $dur (the disk is saturated for that long)"
    rpk iotune --config "$CONF" --directories "$DATA" --duration "$dur" --out "$ST/io-config.yaml.tmp" --no-confirm > "$ST/iotune.out" 2>&1 \
      || { log "iotune failed:"; tail -20 "$ST/iotune.out"; return 1; }
    mv "$ST/io-config.yaml.tmp" "$ST/io-config.yaml"
    cp "$ST/io-config.yaml" "$RP/conf/io-config.yaml" 2>/dev/null
    log "io-config: $(grep -vE '^\s*#|^\s*$' "$ST/io-config.yaml" | tr -s ' \n' ' ')" ;;
  check)
    rpk redpanda check --config "$CONF" --timeout 10s 2>&1 | grep -v 'ec2imds\|IMDSv1' ;;
  start)
    alive && { log "already running (pid $P)"; return 1; }
    [ -f "$CONF" ] || { log "no rendered config: rc.sh render first"; return 1; }
    grep -qE "^ *node_id: $ID\$" "$CONF" && grep -q "address: $IP" "$CONF" || { log "$CONF is not node $ID ($IP)"; return 2; }
    [ -f "$RP/conf/.bootstrap.yaml" ] || { log "no $RP/conf/.bootstrap.yaml (rc.sh render)"; return 1; }
    local o s
    s=$(strays); [ -z "$s" ] || { log "a redpanda process already runs (pid $s) outside the pid file: refusing"; return 1; }
    o=$(listening "$OTHERS")   # 9092/9093 here can only be Kafka (or Queen's Kafka facade): no redpanda process runs
    if [ -n "$o" ] && [ "${FORCE:-0}" != 1 ]; then log "another system listens here ($o): refusing to start (FORCE=1 overrides)"; return 1; fi
    o=$(listening "$OURS"); [ -z "$o" ] || { log "Redpanda's ports are bound by something else ($o): refusing"; return 1; }
    [ -s "$RP/conf/io-config.yaml" ] || log "WARN no io-config.yaml (rc.sh iotune): Seastar runs without the measured disk model"
    ulimit -n $NOFILE 2>/dev/null || ulimit -n "$(ulimit -Hn)" 2>/dev/null
    ulimit -l unlimited 2>/dev/null
    if [ "$(ulimit -n)" != "$NOFILE" ] && [ "${FORCE:-0}" != 1 ]; then log "nofile $(ulimit -n) < $NOFILE: refusing (FORCE=1 overrides)"; return 1; fi
    mkdir -p "$LOGS" "$ST" "$DATA"
    nohup rpk redpanda start --config "$CONF" --check=true >> "$LOGS/redpanda.log" 2>&1 < /dev/null &
    echo $! > "$ST/redpanda.pid.tmp" && mv "$ST/redpanda.pid.tmp" "$ST/redpanda.pid"
    local i; for i in $(seq 1 100); do alive && break; gone "$(cat "$ST/redpanda.pid")" && break; sleep 0.1; done
    alive || { log "did not come up (rpk checks or start failed):"; tail -25 "$LOGS/redpanda.log" | grep -v 'ec2imds\|IMDSv1'; return 1; }
    echo -950 > "/proc/$P/oom_score_adj" 2>/dev/null
    log "started pid $P ($IP) nofile=$(awk '/Max open files/{print $4}' "/proc/$P/limits") memlock=$(awk '/Max locked memory/{print $4}' "/proc/$P/limits") oom_score_adj=$(cat "/proc/$P/oom_score_adj")" ;;
  stop)
    local w=${STOP_WAIT:-60} i s
    if alive; then
      kill -INT "$P"
      for i in $(seq 1 $((w * 10))); do gone "$P" && break; sleep 0.1; done
      if ! gone "$P"; then log "no exit after ${w}s: SIGKILL $P"; kill -9 "$P"; sleep 0.5; else log "stopped pid $P in $((i / 10)).$((i % 10))s"; fi
    fi
    for s in $(strays); do log "stray redpanda pid $s: SIGKILL"; kill -9 "$s" 2>/dev/null; done
    rm -f "$ST/redpanda.pid"; return 0 ;;
  wipe)
    alive && { log "running (pid $P): stop first"; return 1; }
    [ -z "$(strays)" ] || { log "a redpanda process still runs: stop first"; return 1; }
    rm -rf "$DATA" "$LOGS" "$ST/mark.time" "$ST/mark.busy"
    log "wiped $DATA $LOGS" ;;
  health)
    local A=0 R=- HY=- C=- NN=0 DN=- LL=- UR=- h
    if alive; then
      A=1
      R=$(radm /v1/status/ready 3 2>/dev/null | sed -nE 's/.*"status": *"([a-z_]+)".*/\1/p')
      h=$(rpkx cluster health)
      HY=$(echo "$h" | awk -F: '/^Healthy:/{gsub(/[ \t]/, "", $2); print $2}')
      C=$(echo "$h" | awk -F: '/^Controller ID:/{gsub(/[ \t]/, "", $2); print $2}')
      NN=$(echo "$h" | sed -nE 's/^All nodes: *\[(.*)\].*/\1/p' | wc -w | tr -d ' ')
      DN=$(echo "$h" | sed -nE 's/^Nodes down: *\[(.*)\].*/\1/p' | wc -w | tr -d ' ')
      LL=$(echo "$h" | sed -nE 's/^Leaderless partitions \(([0-9]+)\).*/\1/p')
      UR=$(echo "$h" | sed -nE 's/^Under-replicated partitions \(([0-9]+)\).*/\1/p')
    fi
    echo "node=$ID alive=$A ready=${R:--} healthy=${HY:--} controller=${C:--} nodes=$NN down=${DN:--} leaderless=${LL:--} under_replicated=${UR:--}" ;;
  stats)
    alive || { echo "node=$ID not running"; return 1; }
    local S=/proc/$P/status M CNT
    M=$(cat "$ST/mark.time" 2>/dev/null)
    # Redpanda log lines: "LEVEL YYYY-MM-DD HH:MM:SS,mmm [shard N:grp] ..." (UTC on the droplets)
    # transfers = leadership transfers started by this node (the leader balancer moving leaders); elections = "became the
    # leader" on this node (creation, transfers landing here, failovers)
    CNT=$(awk -v m="$M" '
      { since = (m != "" && $2 " " substr($3, 1, 8) >= m) }
      /^ERROR / {e++; if (since) em++}
      /^WARN /  {w++; if (since) wm++}
      /Starting leadership transfer/ {t++; if (since) tm++}
      /became the leader/ {l++; if (since) lm++}
      END {printf "errors=%d warns=%d errors_since_mark=%d warns_since_mark=%d transfers=%d transfers_since_mark=%d elections_won=%d elections_won_since_mark=%d", e, w, em, wm, t, tm, l, lm}' "$LOGS/redpanda.log" 2>/dev/null)
    echo "node=$ID rss_MB=$(( $(awk '/^VmRSS/{print $2}' "$S") / 1024 )) threads=$(awk '/^Threads/{print $2}' "$S")" \
      "fds=$(ls "/proc/$P/fd" | wc -l) data_MB=$(du -sm "$DATA" 2>/dev/null | cut -f1)" \
      "part_dirs=$(find "$DATA/kafka" -mindepth 2 -maxdepth 2 -type d 2>/dev/null | wc -l) disk_free_GB=$(df -BG --output=avail "$DATA" | tail -1 | tr -dc 0-9)" \
      "busy_s=$(busy_s) $CNT mark='${M:-none}' uptime_s=$(( $(date +%s) - $(stat -c %Y "$ST/redpanda.pid") ))" ;;
  mark)
    alive || { echo "node=$ID not running"; return 1; }
    date -u '+%Y-%m-%d %H:%M:%S' > "$ST/mark.time"
    echo "t=$(date +%s.%N | cut -c1-14) busy_s=$(busy_s)" > "$ST/mark.busy"
    echo "node=$ID marked at $(cat "$ST/mark.time") UTC ($(cat "$ST/mark.busy"))" ;;
  busy)   # + the broker-side produce latency histogram (cumulative counts per log2 bucket, upper bound in seconds)
    alive || { echo "node=$ID not running"; return 1; }
    echo "node=$ID t=$(date +%s.%N | cut -c1-14) busy_s=$(busy_s) produce_hist=$(produce_hist)" ;;
  transfers)  # leadership transfers this node has started (the leader balancer at work) and the unix time of the last one
    alive || { echo "node=$ID not running"; return 1; }
    local n last
    n=$(grep -c 'Starting leadership transfer' "$LOGS/redpanda.log" 2>/dev/null)
    last=$(grep 'Starting leadership transfer' "$LOGS/redpanda.log" 2>/dev/null | tail -1 | awk '{print $2 " " substr($3, 1, 8)}')
    if [ -n "$last" ]; then last=$(date -u -d "$last" +%s 2>/dev/null || echo 0); else last=0; fi
    echo "node=$ID transfers=${n:-0} last=$last now=$(date +%s)" ;;
  threads)  # per-thread CPU over [s] s from /proc, grouped by thread name (digits dropped) + the shards' busy seconds
    alive || { echo "node=$ID not running"; return 1; }
    local s=${1:-3} T b0 b1; T=$(mktemp -d)
    b0=$(busy_s)
    for t in /proc/"$P"/task/*; do echo "${t##*/} $(awk '{print $14 + $15}' "$t/stat" 2>/dev/null) $(cat "$t/comm" 2>/dev/null | tr ' ' '_')"; done > "$T/a"
    sleep "$s"
    for t in /proc/"$P"/task/*; do echo "${t##*/} $(awk '{print $14 + $15}' "$t/stat" 2>/dev/null) $(cat "$t/comm" 2>/dev/null | tr ' ' '_')"; done > "$T/b"
    b1=$(busy_s)
    python3 - "$T" "$s" "$ID" "$b0" "$b1" <<'PY'
import re, sys
t, s, node, b0, b1 = sys.argv[1], float(sys.argv[2]), sys.argv[3], sys.argv[4], sys.argv[5]
hz = 100.0
rd = lambda f: {l.split()[0]: (int(l.split()[1]), l.split()[2] if len(l.split()) > 2 else "?") for l in open(f) if len(l.split()) >= 2}
a, b = rd(t + "/a"), rd(t + "/b")
g = {}
for tid, (v, name) in b.items():
    d = (v - a.get(tid, (v, name))[0]) / hz / s
    key = re.sub(r"\d+", "N", name)
    c, n, mx = g.get(key, (0.0, 0, 0.0))
    g[key] = (c + d, n + 1, max(mx, d))
tot = sum(c for c, _, _ in g.values())
try: busy = (float(b1) - float(b0)) / s
except ValueError: busy = float("nan")
print(f"node={node} total={tot:.2f} cores over {s:.0f}s from /proc ({len(b)} threads) | shards busy (redpanda_cpu_busy_seconds_total) = {busy:.2f} cores")
for k, (c, n, mx) in sorted(g.items(), key=lambda kv: -kv[1][0])[:15]:
    print(f"  {c:6.2f} cores  {n:4d} thr  max1={mx:4.2f}  {k[:70]}")
PY
    rm -rf "$T" ;;
  conf)
    local f; for f in "$CONF" "$RP/conf/.bootstrap.yaml" "$RP/conf/io-config.yaml" "$RP/conf/overrides.txt"; do echo "# $f"; grep -vE '^\s*#|^\s*$' "$f" 2>/dev/null; done ;;
  version)  # versions + what the running process got (Seastar args, limits) + the rendered config's hash
    local h cl=""
    h=$(grep -vE '^\s*#|node_id:|address: ' "$CONF" 2>/dev/null | cat - "$RP/conf/.bootstrap.yaml" 2>/dev/null | grep -v '^#' | md5sum | cut -c1-8)
    if alive; then
      cl=$(tr '\0' ' ' < "/proc/$P/cmdline" | sed 's/ *$//')
      cl="pid=$P cmd=[$cl] nofile=$(awk '/Max open files/{print $4}' "/proc/$P/limits") oom_score_adj=$(cat "/proc/$P/oom_score_adj")"
    fi
    echo "node=$ID redpanda=$(sed -n 's/^redpanda=//p' "$ST/installed" 2>/dev/null | cut -d' ' -f1) conf_hash=$h ${cl:-not-running}" ;;
  ports)
    echo "node=$ID redpanda ports bound: $(listening "$OURS")| other systems: $(listening "$OTHERS")" ;;
  *) awk 'NR > 1 && /^#/ {print; next} NR > 1 {exit}' "$0"; return 2 ;;
  esac
}
main "$@"; exit $?
