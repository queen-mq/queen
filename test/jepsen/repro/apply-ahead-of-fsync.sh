#!/usr/bin/env bash
# Power loss right after a durable point, on a follower whose queue-log fsyncs
# lag (a slow disk): can the store's durable point claim entries this node had
# applied but not yet fsynced?
#
#   rsm/replicator/raft/state_machine.rs: apply waits for log_store::Written
#     (entry WRITTEN to the queue logs, "though not yet fsynced");
#   rsm/apply.rs prepare_point(): the durable point records DURABLE_INDEX and
#     QLOG_DURABLE_INDEX from the applied index, trusting that "the external
#     writer fsync'd every record on the write path";
#   rsm/replicator/raft/log_store.rs open(): at boot the queue logs' tail (the
#     MAX over all queue logs) must not be behind QLOG_DURABLE_INDEX.
#
# How: 5 nodes, every data dir on lazyfs. One follower V runs with
# QUEEN_TEST_FAULTS=durable.store_committed:K (it SIGKILLs itself right after
# its K-th durable point) and QUEEN_RAFT_DURABLE_EVERY_MS=20, with an O_DSYNC
# writer on its disk so its fsyncs queue. When V is dead, lazyfs forgets V's
# un-fsynced writes (the power loss), V restarts, and every acknowledged push
# is fetched from V.
#
# Run on the Jepsen control node with no test running:
#   ./apply-ahead-of-fsync.sh /root/bin/queen-wt-snapfix [K ...]
set -u
BIN=${1:?queen binary}; shift
KS=${*:-"120 240 360"}
NODES=(n1 n2 n3 n4 n5)
LZ=/opt/jepsen/lazyfs/lazyfs
ip() { getent hosts "$1" | awk '{print $1}'; }
PEERS=""
for i in "${!NODES[@]}"; do a=$(ip "${NODES[$i]}"); PEERS="$PEERS${PEERS:+,}$((i+1))=$a:7400/$a:6632"; done
h() { curl -s --max-time 2 "http://$(ip "$1"):6632/health"; }
role() { h "$1" | sed -n 's/.*"role":"\([a-z]*\)".*/\1/p'; }
OUT=/root/runs/repro-apply-ahead; mkdir -p $OUT

node_reset() { # n: stop queen, unmount, wipe, mount lazyfs
  ssh "$1" 'kill -9 $(cat /opt/queen/wrapper.pid 2>/dev/null) 2>/dev/null; pkill -9 -x queen; sleep 0.3
    kill -9 $(cat /opt/queen-hog.pid 2>/dev/null) 2>/dev/null; pkill -9 -x dd; rm -f /opt/queen-hog /opt/queen-hog.pid
    mountpoint -q /opt/queen/data && fusermount -uz /opt/queen/data; sleep 0.3
    rm -rf /opt/queen/data /opt/queen/data.lazyfs /opt/queen/buf /opt/queen/queen.log
    mkdir -p /opt/queen/data /opt/queen/data.lazyfs/data /opt/queen/buf
    cat > /opt/queen/data.lazyfs/config <<EOF
[faults]
fifo_path="/opt/queen/data.lazyfs/fifo"

[cache]
apply_eviction=false

[cache.simple]
custom_size="1GB"
blocks_per_page=1

[filesystem]
logfile="/opt/queen/data.lazyfs/log"
log_all_operations=false
EOF
    touch /opt/queen/data.lazyfs/log
    cd '$LZ' && scripts/mount-lazyfs.sh -c /opt/queen/data.lazyfs/config -m /opt/queen/data -r /opt/queen/data.lazyfs/data > /dev/null 2>&1
    for i in $(seq 1 60); do findmnt /opt/queen/data | grep -q lazyfs && break; sleep 0.5; done
    findmnt /opt/queen/data | grep -q lazyfs || echo "NOT MOUNTED"'
}

node_start() { # n id token extra-env
  local n=$1 id=$2 tok=$3 extra=${4:-} a; a=$(ip "$n")
  ssh "$n" "cat > /opt/queen/run-repro.sh" <<EOF
#!/usr/bin/env bash
ulimit -n 1048576
export QUEEN_STORAGE=raft QUEEN_RAFT_REPLICATOR=openraft QUEEN_RAFT_NODE_ID=$id
export QUEEN_RAFT_PEERS='$PEERS' QUEEN_RAFT_LISTEN=$a:7400 QUEEN_RAFT_TOKEN=$tok
export QUEEN_RAFT_DIR=/opt/queen/data FILE_BUFFER_DIR=/opt/queen/buf QUEEN_BIND_ADDR=$a PORT=6632
export JWT_ENABLED=false QUEEN_TENANCY_HEADER=false LOG_LEVEL=info QUEEN_LANES=16 $extra
echo \$\$ > /opt/queen/wrapper.pid
while true; do /opt/queen/queen; rc=\$?; echo "run-repro.sh: queen exited rc=\$rc"; [ \$rc -eq 75 ] || break; done
EOF
  ssh "$n" "chmod +x /opt/queen/run-repro.sh; setsid nohup /opt/queen/run-repro.sh >> /opt/queen/queen.log 2>&1 < /dev/null &"
}

fifo() { ssh "$1" "echo '$2' > /opt/queen/data.lazyfs/fifo"; }

for K in $KS; do
  echo "================ K=$K $(date -u +%FT%TZ)"
  TOK=$(cat /proc/sys/kernel/random/uuid)
  for n in "${NODES[@]}"; do
    scp -q "$BIN" "$n:/opt/queen/queen.new" && ssh "$n" "cmp -s /opt/queen/queen.new /opt/queen/queen || cp /opt/queen/queen.new /opt/queen/queen; rm -f /opt/queen/queen.new; chmod +x /opt/queen/queen"
    node_reset "$n" &
  done; wait
  for i in "${!NODES[@]}"; do node_start "${NODES[$i]}" $((i+1)) "$TOK"; done
  for n in "${NODES[@]}"; do for t in $(seq 1 120); do h "$n" | grep -q '"status":"healthy"' && break; sleep 0.5; done; done
  L=""; for n in "${NODES[@]}"; do [ "$(role "$n")" = leader ] && L=$n; done
  V=""; for n in "${NODES[@]}"; do [ "$n" != "$L" ] && V=$n; done
  VID=0; for i in "${!NODES[@]}"; do [ "${NODES[$i]}" = "$V" ] && VID=$((i+1)); done
  echo "leader=$L victim=$V (node $VID)"
  for q in rq0 rq1; do
    curl -s -X POST "http://$(ip "$L"):6632/api/v1/configure" -H 'content-type: application/json' \
      -d "{\"queue\":\"$q\",\"options\":{\"retentionEnabled\":false,\"deadLetterQueue\":false,\"ttl\":0}}" > /dev/null
  done
  # Load through the leader, recording every acknowledged push.
  python3 - "$(ip "$L")" 45 > $OUT/acked-K$K.txt 2> $OUT/load-K$K.err <<'PY' &
import http.client, json, sys, threading, time
host, secs = sys.argv[1], float(sys.argv[2])
stop = time.time() + secs
lock = threading.Lock()
def worker(w):
    c = http.client.HTTPConnection(host, 6632, timeout=10); i = 0
    while time.time() < stop:
        q, p = "rq%d" % (w % 2), "p%d" % (w % 8)
        tid = "w%d-%d" % (w, i); i += 1
        try:
            c.request("POST", "/api/v1/push", json.dumps({"items": [{"queue": q, "partition": p, "payload": tid, "transactionId": tid}]}), {"content-type": "application/json"})
            r = c.getresponse(); b = json.loads(r.read())
            if r.status == 201 and b[0]["status"] in ("queued", "duplicate"):
                with lock: print(q, p, b[0]["offset"], tid, flush=True)
        except Exception as e:
            c = http.client.HTTPConnection(host, 6632, timeout=10)
            time.sleep(0.05)
ts = [threading.Thread(target=worker, args=(w,)) for w in range(16)]
[t.start() for t in ts]; [t.join() for t in ts]
PY
  LOAD=$!
  sleep 3
  # The victim: restart it with the crash point armed, a slow disk and 20 ms durable points.
  ssh "$V" 'kill -9 $(cat /opt/queen/wrapper.pid) 2>/dev/null; pkill -9 -x queen; true'
  node_start "$V" $VID "$TOK" "QUEEN_TEST_FAULTS=durable.store_committed:$K QUEEN_RAFT_DURABLE_EVERY_MS=20"
  ssh "$V" "setsid nohup bash -c 'while true; do dd if=/dev/zero of=/opt/queen-hog bs=256k count=256 oflag=dsync 2>/dev/null; done' > /dev/null 2>&1 < /dev/null & echo \$! > /opt/queen-hog.pid"
  # Wait for the crash point; then the power loss.
  FIRED=0
  for t in $(seq 1 600); do
    if ssh "$V" "grep -q 'fault: crash point' /opt/queen/queen.log && ! pgrep -x queen > /dev/null"; then FIRED=1; break; fi
    sleep 0.05
  done
  echo "crash point fired=$FIRED at $(date -u +%T.%N)"
  fifo "$V" "lazyfs::unsynced-data-report"; sleep 0.5
  fifo "$V" "lazyfs::clear-cache"; sleep 1
  ssh "$V" 'kill -9 $(cat /opt/queen-hog.pid 2>/dev/null) 2>/dev/null; pkill -9 -x dd; rm -f /opt/queen-hog /opt/queen-hog.pid'
  ssh "$V" "awk '/report request submitted/{n=NR} {l[NR]=\$0} END{for(i=n;i<=NR;i++) print l[i]}' /opt/queen/data.lazyfs/log" | grep -E "=> file:|un-fsynced:" | sed 's/.*\[lazyfs.cmds\]: report: //' | sort | uniq | tail -8
  wait $LOAD
  echo "acked pushes: $(wc -l < $OUT/acked-K$K.txt)"
  # Restart the victim, clean, and read everything back from it.
  node_start "$V" $VID "$TOK"
  UP=0; for t in $(seq 1 120); do h "$V" | grep -q '"status":"healthy"' && { UP=1; break; }; sleep 0.5; done
  echo "victim healthy after restart: $UP"
  ssh "$V" "grep -E 'NA-QLOG-I1|BEHIND|poison|panicked|queen exited rc' /opt/queen/queen.log | tail -5 | cut -c1-240"
  sleep 3
  python3 - "$(ip "$V")" "$(ip "$L")" $OUT/acked-K$K.txt <<'PY'
import http.client, json, sys
v, l, acked_file = sys.argv[1], sys.argv[2], sys.argv[3]
acked = {}
for line in open(acked_file):
    q, p, off, tid = line.split()
    acked[(q, p, int(off))] = tid
def fetch_all(host):
    got, gaps = {}, []
    for q in ("rq0", "rq1"):
        for pn in range(8):
            p = "p%d" % pn; off = 0
            while True:
                try:
                    c = http.client.HTTPConnection(host, 6632, timeout=10)
                    c.request("POST", "/api/v1/fetch", json.dumps({"entries": [{"queue": q, "partition": p, "offset": off}]}), {"content-type": "application/json"})
                    e = json.loads(c.getresponse().read())["entries"][0]
                except Exception as ex:
                    return None, str(ex)
                recs = e.get("records", [])
                if not recs: break
                for i, r in enumerate(recs):
                    if r["offset"] != off + i: gaps.append((q, p, off + i, r["offset"]))
                    got[(q, p, r["offset"])] = r["transactionId"]
                off = recs[-1]["offset"] + 1
    return got, gaps
for name, host in (("victim", v), ("leader", l)):
    got, gaps = fetch_all(host)
    if got is None:
        print(name, "fetch failed:", gaps); continue
    missing = [k for k in acked if got.get(k) != acked[k]]
    print("%s: records=%d acked=%d missing-or-different=%d gaps=%d %s %s" % (name, len(got), len(acked), len(missing), len(gaps), missing[:5], gaps[:5]))
PY
done
for n in "${NODES[@]}"; do
  ssh "$n" 'kill -9 $(cat /opt/queen/wrapper.pid 2>/dev/null) 2>/dev/null; pkill -9 -x queen; kill -9 $(cat /opt/queen-hog.pid 2>/dev/null) 2>/dev/null; pkill -9 -x dd; rm -f /opt/queen-hog /opt/queen-hog.pid /opt/queen/run-repro.sh; true'
done
echo "done $(date -u +%FT%TZ); nodes left with their lazyfs mounts (the next Jepsen setup unmounts them)"
