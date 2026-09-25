#!/usr/bin/env bash
# One flipped byte in the middle of a follower's queue log: does the node
# refuse to boot, or does it boot and serve a hole?
#
# Suspects (RESEARCH.md §3 lazyfs details):
#   rsm/qlog/mod.rs QLog::open: a bad record ends the scan; everything after
#     it is truncated as if it were a torn tail (truncated_tail=true);
#   rsm/qlog/set.rs reopen_all(): the boot check compares the MAX tail across
#     all queue logs with QLOG_DURABLE_INDEX, so one short queue log passes if
#     another is long enough;
#   the store still names the truncated offsets (they are below its durable
#     index), and fetch/pop skip records that are not there.
#
# How: 5 nodes, two queues; 3000 pushes to queue ha, then 3000 to queue hb (hb
# holds the later entries); wait for a durable point; kill -9 a follower V;
# overwrite ONE byte in the middle of V's ha queue log; start V; fetch ha from
# V and from the leader.
#
# Run on the Jepsen control node with no test running:
#   ./qlog-bitflip-hole.sh /root/bin/queen-wt-snapfix [flip|cut]
#
# flip (default): overwrite one byte inside record ha-01500.
# cut: truncate the ha queue log inside record ha-01500 (a lost tail of
#      acknowledged, fsynced records: nothing valid follows the damage).
set -u
BIN=${1:?queen binary}
MODE=${2:-flip}
NODES=(n1 n2 n3 n4 n5)
ip() { getent hosts "$1" | awk '{print $1}'; }
PEERS=""
for i in "${!NODES[@]}"; do a=$(ip "${NODES[$i]}"); PEERS="$PEERS${PEERS:+,}$((i+1))=$a:7400/$a:6632"; done
h() { curl -s --max-time 2 "http://$(ip "$1"):6632/health"; }
TOK=$(cat /proc/sys/kernel/random/uuid)

start_node() { # name id
  local n=$1 id=$2 a; a=$(ip "$n")
  ssh "$n" "cat > /opt/queen/run-repro.sh" <<EOF
#!/usr/bin/env bash
ulimit -n 1048576
export QUEEN_STORAGE=raft QUEEN_RAFT_REPLICATOR=openraft QUEEN_RAFT_NODE_ID=$id
export QUEEN_RAFT_PEERS='$PEERS' QUEEN_RAFT_LISTEN=$a:7400 QUEEN_RAFT_TOKEN=$TOK
export QUEEN_RAFT_DIR=/opt/queen/data FILE_BUFFER_DIR=/opt/queen/buf QUEEN_BIND_ADDR=$a PORT=6632
export JWT_ENABLED=false QUEEN_TENANCY_HEADER=false LOG_LEVEL=info
echo \$\$ > /opt/queen/wrapper.pid
while true; do /opt/queen/queen; rc=\$?; echo "run-repro.sh: queen exited rc=\$rc"; [ \$rc -eq 75 ] || break; done
EOF
  ssh "$n" "chmod +x /opt/queen/run-repro.sh; setsid nohup /opt/queen/run-repro.sh >> /opt/queen/queen.log 2>&1 < /dev/null &"
}

echo "== a fresh 5-node cluster"
for i in "${!NODES[@]}"; do
  n=${NODES[$i]}
  ssh "$n" 'kill -9 $(cat /opt/queen/wrapper.pid 2>/dev/null) 2>/dev/null; pkill -9 -x queen; sleep 0.3; iptables -F
    mountpoint -q /opt/queen/data && fusermount -uz /opt/queen/data
    rm -rf /opt/queen/data /opt/queen/data.lazyfs /opt/queen/buf /opt/queen/queen.log; mkdir -p /opt/queen/data /opt/queen/buf'
  scp -q "$BIN" "$n:/opt/queen/queen.new" && ssh "$n" "mv /opt/queen/queen.new /opt/queen/queen; chmod +x /opt/queen/queen"
  start_node "$n" $((i+1))
done
for n in "${NODES[@]}"; do for t in $(seq 1 120); do h "$n" | grep -q '"status":"healthy"' && break; sleep 0.5; done; done
sleep 5
L=""; for n in "${NODES[@]}"; do h "$n" | grep -q '"role":"leader"' && L=$n; done
V=""; VID=0; for i in "${!NODES[@]}"; do [ "${NODES[$i]}" != "$L" ] && { V=${NODES[$i]}; VID=$((i+1)); }; done
echo "leader=$L victim=$V"
for q in ha hb; do
  curl -s -X POST "http://$(ip "$L"):6632/api/v1/configure" -H 'content-type: application/json' \
    -d "{\"queue\":\"$q\",\"options\":{\"retentionEnabled\":false,\"deadLetterQueue\":false,\"ttl\":0}}" > /dev/null
done

echo "== 3000 pushes to ha/p0, then 3000 to hb/p0, through the leader"
python3 - "$(ip "$L")" <<'PY'
import http.client, json, sys
c = http.client.HTTPConnection(sys.argv[1], 6632, timeout=10)
for q in ("ha", "hb"):
    ok = 0
    for i in range(3000):
        c.request("POST", "/api/v1/push", json.dumps({"items": [{"queue": q, "partition": "p0", "payload": {"i": i, "pad": "x" * 64}, "transactionId": "%s-%05d" % (q, i)}]}), {"content-type": "application/json"})
        r = c.getresponse(); b = json.loads(r.read())
        ok += (r.status == 201 and b[0]["status"] == "queued")
    print(q, "acked", ok)
PY
sleep 4   # a durable point on every node covers everything
echo "applied: leader $(h "$L" | sed -n 's/.*"applied":\([0-9]*\).*/\1/p'), victim $(h "$V" | sed -n 's/.*"applied":\([0-9]*\).*/\1/p')"

echo "== kill -9 $V; damage its ha queue log (mode $MODE)"
ssh "$V" 'kill -9 $(cat /opt/queen/wrapper.pid) 2>/dev/null; pkill -9 -x queen; true'
ssh "$V" MODE=$MODE python3 - <<'PY'
import glob, os
# The ha queue log: the one holding ha's transaction ids; else the largest non-system log.
files = [f for f in glob.glob("/opt/queen/data/qlog/q*/r*.qlog") if "/q0/" not in f]
data = {f: open(f, "rb").read() for f in files}
f = next((f for f in files if b"ha-01500" in data[f]), None)
if f is None:
    f = max(files, key=lambda f: len(data[f].rstrip(b"\0")))
b = bytearray(data[f])
pos = b.find(b"ha-01500")
end = len(b.rstrip(b"\0"))
if pos < 0:
    pos = end // 2
else:
    pos += 3
if os.environ.get("MODE") == "cut":
    os.truncate(f, pos)
    print("file=%s size=%d logical_end=%d truncated to %d bytes (inside record ha-01500)" % (f, len(b), end, pos))
else:
    old = b[pos]; b[pos] ^= 0xFF
    open(f, "r+b").write(bytes(b))
    print("file=%s size=%d logical_end=%d flipped byte %d (0x%02x -> 0x%02x) %s" % (
        f, len(b), end, pos, old, b[pos], "inside record ha-01500" if data[f].find(b"ha-01500") >= 0 else "at the middle of the logical data"))
PY

echo "== start $V"
start_node "$V" $VID
UP=0; for t in $(seq 1 60); do h "$V" | grep -q '"status":"healthy"' && { UP=1; break; }; sleep 0.5; done
echo "victim healthy: $UP  health: $(h "$V")"
ssh "$V" "grep -E 'rsm qlog open|dropped an unacknowledged|NA-QLOG|FATAL|poison|BEHIND|apply recovered' /opt/queen/queen.log | tail -8 | cut -c1-220"
sleep 2
echo "== fetch ha/p0 and hb/p0 from the victim and from the leader"
python3 - "$(ip "$V")" "$(ip "$L")" <<'PY'
import http.client, json, sys
def fetch(host, q):
    got, off = [], 0
    while True:
        c = http.client.HTTPConnection(host, 6632, timeout=10)
        c.request("POST", "/api/v1/fetch", json.dumps({"entries": [{"queue": q, "partition": "p0", "offset": off}]}), {"content-type": "application/json"})
        e = json.loads(c.getresponse().read())["entries"][0]
        recs = e.get("records", [])
        if not recs:
            return got, e.get("highWatermark"), e.get("error")
        got += [(r["offset"], r["transactionId"]) for r in recs]
        off = recs[-1]["offset"] + 1
for name, host in (("victim", sys.argv[1]), ("leader", sys.argv[2])):
    for q in ("ha", "hb"):
        got, hwm, err = fetch(host, q)
        offs = [o for o, _ in got]
        gaps = [(a, b) for a, b in zip(offs, offs[1:]) if b != a + 1]
        ids = set(t for _, t in got)
        missing = [i for i in range(3000) if "%s-%05d" % (q, i) not in ids]
        print("%s %s: records=%d highWatermark=%s error=%s gaps=%s missing=%d %s" % (
            name, q, len(got), hwm, err, gaps[:3], len(missing),
            ("first missing %s-%05d" % (q, missing[0])) if missing else ""))
PY
echo "== cleanup"
for n in "${NODES[@]}"; do ssh "$n" 'kill -9 $(cat /opt/queen/wrapper.pid 2>/dev/null) 2>/dev/null; pkill -9 -x queen; rm -f /opt/queen/run-repro.sh; true'; done
