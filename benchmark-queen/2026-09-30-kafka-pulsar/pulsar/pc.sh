#!/usr/bin/env bash
# pc.sh (node) — one node of the 3-node Pulsar 4.2.4 cluster: ZooKeeper + bookie + broker, configs from mkconf.sh,
# binaries from install.sh ($REMOTE_ROOT/pulsar/dist). Data $REMOTE_ROOT/data/pulsar, logs $REMOTE_ROOT/logs/pulsar/<role>.
#   pc.sh start <zk|bookie|broker|all>   start (renders the config first if missing); refuses while Queen (6632/7400) or
#                                        Kafka (9092/9093) listens here (FORCE=1 overrides); nofile 1048576
#   pc.sh stop <zk|bookie|broker|all>    SIGTERM, wait STOP_WAIT s (default 30), then SIGKILL; all = broker, bookie, zk
#   pc.sh restart <role>
#   pc.sh wipe                           delete data + logs (refuses while anything runs)
#   pc.sh health [zk|bookie|broker]      one line: zk mode (ruok/mntr), bookie state (http), broker health (this node)
#   pc.sh stats                          per role: RSS, threads, fds, GC cycles/pauses/allocation stalls and ERROR/WARN
#                                        lines since `mark`; ledger/entry-log/journal sizes; bench/ns broker metrics
#   pc.sh threads [broker|bookie|zk|all] [secs]  per-thread CPU over secs (default 3) grouped by name (jstack names tids)
#   pc.sh mark                           remember now (log line counts, time) as the start of the measured window
#   pc.sh mkconf                         render this node's configs (mkconf.sh; knobs from the environment)
#   pc.sh format-bookie                  bookieformat -nonInteractive -force -deleteCookie (bookie must be stopped)
#   pc.sh tool <bin/...> [args]          run a Pulsar CLI of the dist with small heaps + serial GC (never the daemons' env);
#                                        TOOL_HEAP (default 384m; pulsar-perf needs 560m)
# Cluster helpers (cluster.sh runs them on node 1; admin = this node's broker, or broker 1 from a loader):
#   zkquorum | init-metadata | bookies [-shell] | brokers | ns-setup | proof [topic] | balance [-fix]
set -u
. "$(cd "$(dirname "$0")" && pwd)/lib.sh"
DIST=$R/pulsar/dist NODE=$R/pulsar/node DATA=$R/data/pulsar LOGS=$R/logs/pulsar RUN=$R/pulsar/run
IDX=0 IP=""
MYIPS=" $(hostname -I 2>/dev/null) "
for i in "${!B_PRIV[@]}"; do case "$MYIPS" in *" ${B_PRIV[$i]} "*) IDX=$((i + 1)); IP=${B_PRIV[$i]};; esac; done
if [ "$IDX" = 0 ]; then for ip in "${L_PRIV[@]}"; do case "$MYIPS" in *" $ip "*) IP=$ip;; esac; done; fi
if [ "$IDX" -gt 0 ]; then MYB=$IP; else MYB=${B_PRIV[0]}; fi
ADMIN=http://$MYB:8080
J21=$(ls -d /usr/lib/jvm/java-21-openjdk-* 2>/dev/null | head -1)

mainclass() {
  case $1 in
    zk) echo org.apache.zookeeper.server.quorum.QuorumPeerMain ;;
    bookie) echo org.apache.bookkeeper.server.Main ;;
    broker) echo org.apache.pulsar.PulsarBrokerStarter ;;
    *) return 1 ;;
  esac
}
subcmd() { case $1 in zk) echo zookeeper ;; *) echo "$1" ;; esac; }
ports_of() { case $1 in zk) echo "2181 3888 8001" ;; bookie) echo "3181 8000" ;; broker) echo "6650 8080" ;; esac; }
pid_of() { pgrep -f "$(mainclass "$1") .*$NODE/" | head -1; }
listening() { ss -ltnH 2>/dev/null | awk '{print $4}' | grep -qE ":$1\$"; }
foreign() { local p o=""; for p in 6632 7400 9092 9093; do listening $p && o="$o $p"; done; echo "$o"; }
roles() { case ${1:-} in all) echo "$2" ;; zk|bookie|broker) echo "$1" ;; *) echo "role must be zk|bookie|broker|all" >&2; return 2 ;; esac; }

# tool <cmd...>: a Pulsar CLI with small heaps and serial GC. bin/pulsar (initialize-cluster-metadata) and bin/bookkeeper
# would otherwise pre-touch Pulsar's 2 GB default heap (ZGC + AlwaysPreTouch); bin/pulsar-admin folds PULSAR_MEM (minus
# -Xms) into PULSAR_EXTRA_OPTS; bin/pulsar-perf reads ONLY PULSAR_EXTRA_OPTS (its 5-digit HdrHistograms need ~200 MB).
tool() {
  ( mem="-Xms32m -Xmx${TOOL_HEAP:-384m} -XX:MaxDirectMemorySize=256m" gc="-XX:+UseSerialGC -XX:TieredStopAtLevel=1"
    gl="-Xlog:disable -Xlog:all=warning:stderr"
    export PULSAR_MEM="$mem" BOOKIE_MEM="$mem" PULSAR_GC="$gc" BOOKIE_GC="$gc" PULSAR_GC_LOG="$gl" BOOKIE_GC_LOG="$gl"
    export PULSAR_EXTRA_OPTS="$mem $gc $gl"
    export PULSAR_LOG_DIR=$LOGS/tools BOOKIE_LOG_DIR=$LOGS/tools
    if [ -f "$NODE/bookkeeper.conf" ]; then export BOOKIE_CONF=$NODE/bookkeeper.conf PULSAR_BOOKKEEPER_CONF=$NODE/bookkeeper.conf; fi
    if [ -n "$J21" ]; then export JAVA_HOME=$J21; fi
    mkdir -p "$LOGS/tools"; cd "$DIST" && "$@" )
}
zk4() { timeout 3 bash -c 'exec 3<>"/dev/tcp/$0/2181" || exit 1; printf "%s" "$1" >&3; cat <&3' "$1" "$2" 2>/dev/null; }
padmin() { python3 "$PDIR/padmin.py" "$@" --admin "$ADMIN"; }

start_one() {
  local r=$1 p f i
  p=$(pid_of "$r"); if [ -n "$p" ]; then log "node $IDX $r already running (pid $p)"; return 0; fi
  f=$(foreign)
  if [ -n "$f" ] && [ "${FORCE:-0}" != 1 ]; then
    log "node $IDX: another system listens on$f (Queen 6632/7400, Kafka 9092/9093): refusing to start $r (FORCE=1 overrides)"; return 1; fi
  for p in $(ports_of "$r"); do listening "$p" && { log "node $IDX $r: port $p already in use: refusing"; return 1; }; done
  [ -f "$NODE/$r.env" ] || "$PDIR/mkconf.sh" || return 1
  mkdir -p "$LOGS/$r" "$RUN"
  case $r in
    zk) mkdir -p "$DATA/zk/data" "$DATA/zk/txlog"; echo "$IDX" > "$DATA/zk/data/myid" ;;
    bookie) mkdir -p "$DATA/bookie/ledgers" $(awk -F= '/^journalDirectories=/{gsub(",", " ", $2); print $2}' "$NODE/bookkeeper.conf") ;;
  esac
  [ -s "$LOGS/$r/$r.out" ] && mv "$LOGS/$r/$r.out" "$LOGS/$r/$r.out.1"   # keep the previous run's stdout (crash traces)
  ( . "$NODE/$r.env"; ulimit -n 1048576 2>/dev/null || ulimit -n "$(ulimit -Hn)"
    cd "$DIST" && exec setsid nohup bin/pulsar "$(subcmd "$r")" > "$LOGS/$r/$r.out" 2>&1 < /dev/null ) &
  p=""; for i in $(seq 1 50); do p=$(pid_of "$r"); [ -n "$p" ] && break; sleep 0.2; done
  if [ -z "$p" ]; then log "node $IDX $r did not start:"; tail -20 "$LOGS/$r/$r.out"; return 1; fi
  echo "$p" > "$RUN/$r.pid"
  log "node $IDX ($IP) $r started pid $p $(grep -oE '_MEM="[^"]*"' "$NODE/$r.env") nofile=$(awk '/Max open files/{print $4}' "/proc/$p/limits")"
}
stop_one() {
  local r=$1 p w=${STOP_WAIT:-30} i
  p=$(pid_of "$r")
  if [ -z "$p" ]; then rm -f "$RUN/$r.pid"; log "node $IDX $r not running"; return 0; fi
  kill "$p" 2>/dev/null
  for i in $(seq 1 $((w * 10))); do kill -0 "$p" 2>/dev/null || break; sleep 0.1; done
  if kill -0 "$p" 2>/dev/null; then log "node $IDX $r: no exit after ${w}s, SIGKILL $p"; kill -9 "$p" 2>/dev/null; sleep 0.5; fi
  rm -f "$RUN/$r.pid"; log "node $IDX $r stopped (pid $p)"
}

h_zk() { local ok st; ok=$(zk4 "$IP" ruok); st=$(zk4 "$IP" mntr | awk '$1=="zk_server_state"{print $2}'); echo "zk=${st:-down}${ok:+/$ok}"; }
h_bookie() {
  local s; s=$(curl -s -m 3 "http://$IP:8000/api/v1/bookie/state" 2>/dev/null)
  if [ -z "$s" ]; then echo "bookie=down"
  elif echo "$s" | grep -qE '"readOnly" *: *true'; then echo "bookie=ro"
  elif echo "$s" | grep -qE '"running" *: *true'; then echo "bookie=rw"
  else echo "bookie=?($(echo "$s" | tr -d ' \n' | cut -c1-60))"; fi
}
h_broker() { local s; s=$(curl -s -m 15 "http://$IP:8080/admin/v2/brokers/health" 2>/dev/null); echo "broker=${s:-down}"; }

stats() {
  python3 - "$NODE" "$LOGS" "$DATA" "$RUN" "$IDX" "$IP" <<'PY'
import datetime, glob, os, re, subprocess, sys, time, urllib.request
NODE, LOGS, DATA, RUN, IDX, IP = sys.argv[1:7]
ROLES = [("zk", "org.apache.zookeeper.server.quorum.QuorumPeerMain", "zookeeper.log"),
         ("bookie", "org.apache.bookkeeper.server.Main", "bookie.log"),
         ("broker", "org.apache.pulsar.PulsarBrokerStarter", "broker.log")]
def rd(p):
    try:
        with open(p, "rb") as f: return f.read().decode(errors="replace")
    except OSError: return ""
def pid_of(cls):
    for p in os.listdir("/proc"):
        if p.isdigit():
            c = rd(f"/proc/{p}/cmdline").replace("\0", " ")
            if cls + " " in c and NODE + "/" in c and "python3" not in c: return p
    return None
mark = float(rd(f"{RUN}/mark.epoch") or 0)
since = "since mark " + time.strftime("%H:%M:%S", time.gmtime(mark)) + "Z" if mark else "since start"
TSRE = re.compile(r"^\[(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d+)([+-]\d{4})\]")
PAUSE = re.compile(r"\] GC\(\d+\) (?:[YO]: )?Pause .*? ([\d.]+)ms\s*$")
STALL = re.compile(r"Allocation Stall \((.*?)\) ([\d.]+)ms")
CYCLE = re.compile(r"\] GC\(\d+\) (Major|Minor) Collection \(([^)]*)\) .*->.* ([\d.]+)s\s*$")
SAFE = re.compile(r'Safepoint "(\w+)".*Total: (\d+) ns')
hz = os.sysconf("SC_CLK_TCK"); boot = float(rd("/proc/stat").split("btime ")[1].split()[0]) if "btime" in rd("/proc/stat") else 0
for role, cls, logf in ROLES:
    pid = pid_of(cls)
    head = f"node={IDX} {role:6s}"
    if pid:
        st = rd(f"/proc/{pid}/status")
        rss = int(re.search(r"VmRSS:\s+(\d+)", st).group(1)) // 1024 if "VmRSS" in st else 0
        thr = re.search(r"Threads:\s+(\d+)", st).group(1)
        try: fds = len(os.listdir(f"/proc/{pid}/fd"))
        except OSError: fds = -1
        f = rd(f"/proc/{pid}/stat"); f = f[f.rfind(")") + 2:].split()
        up = time.time() - (boot + int(f[19]) / hz)
        cpu = (int(f[11]) + int(f[12])) / hz
        head += f" pid={pid} up={up:.0f}s cpu={cpu:.0f}s rss={rss}MB thr={thr} fds={fds}"
    else:
        head += " not running"
    cyc = maj = pauses = stalls = 0; psum = pmax = smax = safemax = 0.0; stall_thr = {}
    for g in sorted(glob.glob(f"{LOGS}/{role}/*gc*.log*")):
        if g.endswith(".gz"): continue
        for l in rd(g).splitlines():
            m = TSRE.match(l)
            if mark and m:
                try:
                    if datetime.datetime.strptime(m.group(1) + m.group(2), "%Y-%m-%dT%H:%M:%S.%f%z").timestamp() < mark: continue
                except ValueError: pass
            m = PAUSE.search(l)
            if m: pauses += 1; v = float(m.group(1)); psum += v; pmax = max(pmax, v); continue
            m = STALL.search(l)
            if m: stalls += 1; v = float(m.group(2)); smax = max(smax, v); stall_thr[m.group(1)] = stall_thr.get(m.group(1), 0) + 1; continue
            m = CYCLE.search(l)
            if m: cyc += 1; maj += m.group(1) == "Major"; continue
            m = SAFE.search(l)
            if m: safemax = max(safemax, int(m.group(2)) / 1e6)
    gc = f"gc cycles={cyc} (major {maj}) pauses={pauses} max={pmax:.2f}ms sum={psum:.1f}ms stalls={stalls} max={smax:.1f}ms safepoint_max={safemax:.2f}ms"
    lines = rd(f"{LOGS}/{role}/{logf}").splitlines()
    try: skip = int(rd(f"{RUN}/mark.{role}.lines").strip() or 0)
    except ValueError: skip = 0
    if skip > len(lines): skip = 0   # rotated
    new = lines[skip:]
    err = [l for l in new if "] ERROR " in l]; warn = sum(1 for l in new if "] WARN " in l)
    print(f"{head} | {gc} | log ERROR={len(err)} WARN={warn} ({since})")
    if stall_thr:
        print(f"node={IDX} {role:6s}   allocation stalls by thread: " + ", ".join(f"{k}={v}" for k, v in sorted(stall_thr.items(), key=lambda kv: -kv[1])[:5]))
    top = {}
    for l in err:
        k = re.sub(r"\d+", "N", l.split(" - ", 1)[-1])[:140]
        top[k] = top.get(k, 0) + 1
    for k, v in sorted(top.items(), key=lambda kv: -kv[1])[:3]:
        print(f"node={IDX} {role:6s}   ERROR x{v}: {k}")
def du(p):
    try: return int(subprocess.run(["du", "-sm", p], capture_output=True, text=True).stdout.split()[0])
    except (IndexError, ValueError): return 0
jd = sorted(glob.glob(f"{DATA}/bookie/journal*"))
jfiles = sum(len(glob.glob(f"{d}/current/*.txn")) for d in jd)
led = f"{DATA}/bookie/ledgers/current"
elogs = glob.glob(f"{led}/*.log")
df = subprocess.run(["df", "-hP", DATA if os.path.isdir(DATA) else "/"], capture_output=True, text=True).stdout.splitlines()[-1].split()
print(f"node={IDX} disk   data={du(DATA)}MB zk={du(DATA + '/zk')}MB journal={sum(du(d) for d in jd)}MB ({jfiles} .txn in {len(jd)} dirs) "
      f"ledgers={du(DATA + '/bookie/ledgers')}MB (entry logs={len(elogs)}, {sum(os.path.getsize(e) for e in elogs) >> 20}MB; "
      f"locations={du(led + '/locations')}MB ledgers-index={du(led + '/ledgers')}MB) | fs {df[4]} used, {df[3]} free")
try:
    txt = urllib.request.urlopen(f"http://{IP}:8080/metrics/", timeout=10).read().decode(errors="replace")
    want = ("pulsar_topics_count", "pulsar_subscriptions_count", "pulsar_producers_count", "pulsar_consumers_count",
            "pulsar_rate_in", "pulsar_rate_out", "pulsar_throughput_in", "pulsar_throughput_out", "pulsar_msg_backlog",
            "pulsar_storage_size")
    agg = {}
    for l in txt.splitlines():
        if l.startswith("#") or 'namespace="bench/ns"' not in l: continue
        name = l.split("{", 1)[0]
        if name in want:
            try: agg[name] = agg.get(name, 0.0) + float(l.rsplit(" ", 1)[1])
            except ValueError: pass
    if agg:
        print(f"node={IDX} broker bench/ns metrics: " + " ".join(f"{k[7:]}={v:.0f}" for k, v in agg.items()))
except Exception:
    pass
PY
}

threads() {  # threads <role> [secs]: per-thread CPU grouped by thread name (digits dropped), like kc.sh threads
  local r=$1 S=${2:-3} P T
  P=$(pid_of "$r"); [ -n "$P" ] || { echo "node=$IDX $r not running"; return 1; }
  T=$(mktemp -d /tmp/pc-thr.XXXXXX)
  for t in /proc/$P/task/*; do echo "${t##*/} $(awk '{print $14+$15}' "$t/stat" 2>/dev/null)"; done > "$T/a"
  sleep "$S"
  for t in /proc/$P/task/*; do echo "${t##*/} $(awk '{print $14+$15}' "$t/stat" 2>/dev/null)"; done > "$T/b"
  for t in /proc/$P/task/*; do echo "${t##*/} $(cat "$t/comm" 2>/dev/null)"; done > "$T/c"   # kernel names (RocksDB, GC)
  local JST=jstack; [ -n "$J21" ] && JST=$J21/bin/jstack
  $JST "$P" 2>/dev/null | grep -E '^"' | sed -nE 's/^"([^"]+)".* nid=(0x[0-9a-f]+|[0-9]+).*/\2 \1/p' > "$T/n"
  python3 - "$T" "$S" "$IDX" "$r" <<'PY'
import re, sys
t, s, idx, role = sys.argv[1], float(sys.argv[2]), sys.argv[3], sys.argv[4]
rd = lambda f: {l.split()[0]: int(l.split()[1]) for l in open(f) if len(l.split()) == 2}
a, b = rd(t + "/a"), rd(t + "/b")
names = {}
for l in open(t + "/n"):
    nid, name = l.rstrip("\n").split(" ", 1)
    names[str(int(nid, 16) if nid.startswith("0x") else int(nid))] = name
comm = {}
for l in open(t + "/c"):
    f = l.rstrip("\n").split(" ", 1)
    if len(f) == 2: comm[f[0]] = f[1]
g = {}
for tid, v in b.items():
    d = (v - a.get(tid, v)) / 100 / s
    key = re.sub(r"-?\d+", "", names.get(tid) or ("~" + comm.get(tid, "?")))   # ~ = kernel name (not a Java thread)
    c, n, mx = g.get(key, (0.0, 0, 0.0))
    g[key] = (c + d, n + 1, max(mx, d))
tot = sum(c for c, _, _ in g.values())
print(f"node={idx} {role} total={tot:.2f} cores over {s:.0f}s ({len(b)} threads, {len(names)} named by jstack)")
for k, (c, n, mx) in sorted(g.items(), key=lambda kv: -kv[1][0])[:14]:
    print(f"  {c:6.2f} cores  {n:4d} thr  max1={mx:4.2f}  {k[:70]}")
PY
  rm -rf "$T"
}

case ${1:-} in
start)
  RL=$(roles "${2:-}" "zk bookie broker") || exit 2; rc=0
  for r in $RL; do start_one "$r" || rc=1; done; exit $rc ;;
stop)
  RL=$(roles "${2:-}" "broker bookie zk") || exit 2
  for r in $RL; do stop_one "$r"; done ;;
restart)
  RL=$(roles "${2:-}" "") || exit 2; [ -n "$RL" ] || { echo "restart <zk|bookie|broker>"; exit 2; }
  stop_one "$RL"; start_one "$RL" ;;
wipe)
  for r in zk bookie broker; do [ -n "$(pid_of $r)" ] && { log "node $IDX: $r running: stop first"; exit 1; }; done
  case $R in /|"") die "refusing to wipe with REMOTE_ROOT='$R'";; esac
  rm -rf "$DATA" "$LOGS" "$RUN"; log "node $IDX wiped $DATA $LOGS" ;;
health)
  case ${2:-all} in
    zk) h_zk ;; bookie) h_bookie ;; broker) h_broker ;;
    *) echo "node=$IDX ip=$IP $(h_zk) $(h_bookie) $(h_broker)" ;;
  esac ;;
stats) stats ;;
threads)
  RL=$(roles "${2:-all}" "broker bookie zk") || exit 2
  for r in $RL; do threads "$r" "${3:-3}"; done ;;
mark)
  mkdir -p "$RUN"; date +%s.%N > "$RUN/mark.epoch"
  for r in zk bookie broker; do f=$LOGS/$r/$( [ $r = zk ] && echo zookeeper || echo $r ).log; wc -l < "$f" > "$RUN/mark.$r.lines" 2>/dev/null || echo 0 > "$RUN/mark.$r.lines"; done
  log "node $IDX marked" ;;
mkconf) exec "$PDIR/mkconf.sh" ;;
format-bookie)
  [ -n "$(pid_of bookie)" ] && { log "node $IDX bookie running: stop first"; exit 1; }
  tool bin/bookkeeper shell bookieformat -nonInteractive -force -deleteCookie ;;
tool) shift; tool "$@" ;;
# ---- cluster helpers
zkquorum)
  S="" L=0 F=0 SY=0
  for ip in "${B_PRIV[@]}"; do
    M=$(zk4 "$ip" mntr); st=$(echo "$M" | awk '$1=="zk_server_state"{print $2}'); S="$S ${st:-down}"
    case $st in leader) L=$((L + 1)); SY=$(echo "$M" | awk '$1=="zk_synced_followers"{printf "%d", $2}') ;; follower) F=$((F + 1)) ;; esac
  done
  echo "zk states:$S leader=$L followers=$F synced=${SY:-0}" ;;
init-metadata)
  W=""; S=""; for ip in "${B_PRIV[@]}"; do W="${W:+$W,}http://$ip:8080"; S="${S:+$S,}pulsar://$ip:6650"; done
  log "initialize-cluster-metadata --cluster bench --metadata-store zk:$ZKS --web-service-url $W --broker-service-url $S"
  mkdir -p "$LOGS/tools"
  tool bin/pulsar initialize-cluster-metadata --cluster bench --metadata-store "zk:$ZKS" \
    --configuration-metadata-store "zk:$ZKS" --web-service-url "$W" --broker-service-url "$S" > "$LOGS/tools/init-metadata.log" 2>&1
  rc=$?; grep -E 'ERROR|Exception|Cluster metadata for' "$LOGS/tools/init-metadata.log" | sed -E 's/^.* - //; s/ \{\}$//' | tail -5; exit $rc ;;
bookies)
  python3 "$PDIR/padmin.py" bookies --bookie-http "http://$MYB:8000"; rc=$?
  if [ "${2:-}" = -shell ]; then
    echo "bookkeeper shell listbookies -rw:"; tool bin/bookkeeper shell listbookies -rw 2>&1 | grep -E 'BookieID:|Bookies :|No bookie' | sed -E 's/^.* - //; s/ \{\}$//; s/^/  /'
  fi; exit $rc ;;
brokers) padmin brokers ;;
ns-setup) padmin ns-setup ;;
proof)
  OUT=$(padmin proof ${2:+--topic "$2"}); rc=$?
  echo "$OUT" | grep -vE '^(LEDGER|PROBE) '
  L=$(echo "$OUT" | awk '/^LEDGER /{print $2}'); PR=$(echo "$OUT" | awk '/^PROBE /{print $2}')
  if [ -n "$L" ]; then
    echo "bookkeeper shell ledgermetadata -ledgerid $L:"
    tool bin/bookkeeper shell ledgermetadata -ledgerid "$L" 2>&1 | grep -E 'ensembleSize|ledgerID' | sed -E 's/^.* - //; s/ \{\}$//; s/, customMetadata=.*$/, .../; s/^/  /'
  fi
  [ -n "$PR" ] && padmin delete --topic "$PR" > /dev/null
  exit $rc ;;
balance) shift; padmin balance "$@" ;;
*) usage; exit 2 ;;
esac
