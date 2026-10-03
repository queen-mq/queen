#!/usr/bin/env bash
# sampler.sh start <tag> [interval_s] | stop <tag> | once — per-node sampler, one `key=value` line per interval into
# $REMOTE_ROOT/samples/<tag>.txt (report/report.py reads it). Same idea as the 09-28 sampler2.sh, system-agnostic:
#   cpu_busy/cpu_total/cpu_iowait/cpu_steal (/proc/stat ticks), ncpu
#   per role (kafka zk bookie broker queen redpanda kload pload goload qload): <role>_ticks (utime+stime), <role>_rss_kb, <role>_thr
#   (qload is matched before queen: a qload command line may name a Queen path)
#   (redpanda = the Seastar process `redpanda --redpanda-cfg ...` that `rpk redpanda start` execs into; its reactors poll,
#   so its ticks include polling: redpanda/rc.sh busy has the shards' own busy time)
#   MemFree/Cached/Dirty/Writeback/AnonPages (kB), pgmajfault pgscan_direct allocstall pgsteal_kswapd (vmstat)
#   psi_cpu/psi_mem/psi_io (some avg10), disk_* of the disk holding $REMOTE_ROOT (sectors, ios, flushes),
#   net_rx/net_tx bytes of the NIC that holds this host's private IP (from hosts.env)
set -u
D=$(cd "$(dirname "$0")/.." && pwd)
[ -f "$D/hosts.env" ] && . "$D/hosts.env"
ROOT=${REMOTE_ROOT:-/root/bench}
OUT=$ROOT/samples; mkdir -p "$OUT"
MYIPS=" $(hostname -I 2>/dev/null) "
PRIV=""
for ip in ${B_PRIV[@]:-} ${L_PRIV[@]:-}; do case "$MYIPS" in *" $ip "*) PRIV=$ip;; esac; done
IFACE=$(ip -o -4 addr show 2>/dev/null | awk -v ip="${PRIV:-none}" '{split($4,a,"/"); if (a[1]==ip) {print $2; exit}}')
[ -n "$IFACE" ] || IFACE=$(ip -o -4 route show default 2>/dev/null | awk '{print $5; exit}')
SRC=$(findmnt -no SOURCE --target "$ROOT" 2>/dev/null | sed 's#^/dev/##')
DISK=$(lsblk -no PKNAME "/dev/$SRC" 2>/dev/null | head -1); [ -n "$DISK" ] || DISK=$SRC
sample_loop() {  # sample_loop <interval> <iface> <disk> [once] : prints lines forever (or one)
  python3 - "$1" "$2" "$3" ${4:-} <<'PY'
import os, sys, time, re
iv, iface, disk = float(sys.argv[1]), sys.argv[2], sys.argv[3]
once = len(sys.argv) > 4
ROLES = [("kafka", "kafka.Kafka"), ("zk", "QuorumPeerMain"), ("bookie", "org.apache.bookkeeper.server.Main"),
         ("broker", "PulsarBrokerStarter"), ("qload", "bin/txn/qload"), ("queen", "/queen"), ("redpanda", "redpanda --redpanda-cfg"),
         ("kload", "kload"), ("pload", "pload"), ("goload", "goload"), ("qload", "qload")]
def rd(p):
    try:
        with open(p, "rb") as f: return f.read().decode(errors="replace")
    except OSError: return ""
def roles():
    out = {}
    for p in os.listdir("/proc"):
        if not p.isdigit(): continue
        cmd = rd(f"/proc/{p}/cmdline").replace("\0", " ")
        if not cmd: continue
        for r, pat in ROLES:
            if pat in cmd and "sampler" not in cmd and "bash -c" not in cmd[:8]:
                out.setdefault(r, []).append(p); break
    return out
pids, last_scan = {}, 0
while True:
    t = time.time()
    if t - last_scan > 10: pids, last_scan = roles(), t
    kv = []
    st = rd("/proc/stat").split("\n", 1)[0].split()[1:]
    v = [int(x) for x in st[:8]]
    kv += [f"cpu_total={sum(v)}", f"cpu_busy={sum(v) - v[3] - v[4]}", f"cpu_iowait={v[4]}", f"cpu_steal={v[7]}", f"ncpu={os.cpu_count()}"]
    for r, ps in pids.items():
        tk = rss = thr = 0
        for p in ps:
            s = rd(f"/proc/{p}/stat")
            if not s: continue
            f = s[s.rfind(")") + 2:].split()
            tk += int(f[11]) + int(f[12]); thr += int(f[17])
            m = re.search(r"VmRSS:\s+(\d+)", rd(f"/proc/{p}/status")); rss += int(m.group(1)) if m else 0
        kv += [f"{r}_ticks={tk}", f"{r}_rss_kb={rss}", f"{r}_thr={thr}"]
    mi = dict(re.findall(r"^(\w+):\s+(\d+)", rd("/proc/meminfo"), re.M))
    kv += [f"{k}_kb={mi.get(k, 0)}" for k in ("MemFree", "Cached", "Dirty", "Writeback", "AnonPages")]
    vm = dict(re.findall(r"^(\w+) (\d+)", rd("/proc/vmstat"), re.M))
    kv += [f"pgmajfault={vm.get('pgmajfault', 0)}", f"pgscan_direct={vm.get('pgscan_direct', 0)}",
           f"allocstall={int(vm.get('allocstall_normal', 0)) + int(vm.get('allocstall_movable', 0))}",
           f"pgsteal_kswapd={vm.get('pgsteal_kswapd', 0)}"]
    for k in ("cpu", "memory", "io"):
        m = re.search(r"some avg10=([\d.]+)", rd(f"/proc/pressure/{k}"))
        kv.append(f"psi_{k[:3]}={m.group(1) if m else 0}")
    for l in rd("/proc/diskstats").splitlines():
        f = l.split()
        if len(f) > 3 and f[2] == disk:
            kv += [f"disk_rios={f[3]}", f"disk_rsect={f[5]}", f"disk_wios={f[7]}", f"disk_wsect={f[9]}",
                   f"disk_flush={f[18] if len(f) > 18 else 0}"]
            break
    kv += [f"net_rx={rd(f'/sys/class/net/{iface}/statistics/rx_bytes').strip() or 0}",
           f"net_tx={rd(f'/sys/class/net/{iface}/statistics/tx_bytes').strip() or 0}"]
    print(f"{t:.3f} " + " ".join(kv), flush=True)
    if once: break
    time.sleep(max(0.05, iv - (time.time() - t)))
PY
}
case ${1:-} in
start)
  TAG=${2:?tag}; IV=${3:-2}
  [ -f "$OUT/$TAG.pid" ] && kill -0 "$(cat "$OUT/$TAG.pid")" 2>/dev/null && { echo "sampler $TAG already running"; exit 0; }
  echo "# $(date -u +%FT%TZ) host=$(hostname) priv=$PRIV iface=$IFACE disk=$DISK interval=$IV" > "$OUT/$TAG.txt"
  nohup bash -c "$(declare -f sample_loop); sample_loop '$IV' '$IFACE' '$DISK'" >> "$OUT/$TAG.txt" 2>> "$OUT/$TAG.err" < /dev/null &
  echo $! > "$OUT/$TAG.pid"; echo "sampler $TAG started pid $! -> $OUT/$TAG.txt (iface=$IFACE disk=$DISK)" ;;
stop)
  TAG=${2:?tag}; P=$(cat "$OUT/$TAG.pid" 2>/dev/null) || { echo "sampler $TAG not running"; exit 0; }
  pkill -P "$P" 2>/dev/null; kill "$P" 2>/dev/null; rm -f "$OUT/$TAG.pid"; echo "sampler $TAG stopped ($(wc -l < "$OUT/$TAG.txt") lines)" ;;
once) sample_loop 1 "$IFACE" "$DISK" once ;;
*) sed -n '2,8p' "$0"; exit 2 ;;
esac
