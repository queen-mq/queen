#!/usr/bin/env bash
# A leader that can SEND to its followers but never hears their ANSWERS stalls
# the cluster for as long as that lasts: every follower keeps receiving its
# appends and heartbeats, so none campaigns, and nothing commits (the shape of
# openraft GH#2080; its fix, PR #2089, is newer than server/Cargo.toml's pin
# 54094270). For comparison, the same leader made fully deaf (all its input
# from the followers dropped) is replaced within seconds.
#
# The fault, with iptables on the leader only: drop the packets that come FROM
# a follower's Raft port (7400) and carry data (IP length >= 100), and let pure
# TCP ACKs (52 bytes) through, so the leader's connections stay up.
#
# Run on the Jepsen control node (n1..n5 in /etc/hosts, root ssh), with no
# Jepsen test running:
#   ./leader-deaf-stall.sh /root/bin/queen-wt-snapfix
set -u
BIN=${1:?queen binary}
NODES=(n1 n2 n3 n4 n5)
ip() { getent hosts "$1" | awk '{print $1}'; }
PEERS=""
for i in "${!NODES[@]}"; do a=$(ip "${NODES[$i]}"); PEERS="$PEERS${PEERS:+,}$((i+1))=$a:7400/$a:6632"; done
h() { curl -s --max-time 1 "http://$(ip "$1"):6632/health"; }
TOK=$(cat /proc/sys/kernel/random/uuid)

echo "== a fresh 5-node cluster"
for i in "${!NODES[@]}"; do
  n=${NODES[$i]}; a=$(ip "$n")
  ssh "$n" 'kill -9 $(cat /opt/queen/wrapper.pid 2>/dev/null) 2>/dev/null; pkill -9 -x queen; sleep 0.3; iptables -F
    mountpoint -q /opt/queen/data && fusermount -uz /opt/queen/data
    rm -rf /opt/queen/data /opt/queen/data.lazyfs /opt/queen/buf /opt/queen/queen.log; mkdir -p /opt/queen/data /opt/queen/buf'
  scp -q "$BIN" "$n:/opt/queen/queen.new" && ssh "$n" "mv /opt/queen/queen.new /opt/queen/queen; chmod +x /opt/queen/queen"
  ssh "$n" "cat > /opt/queen/run-repro.sh" <<EOF
#!/usr/bin/env bash
ulimit -n 1048576
export QUEEN_STORAGE=raft QUEEN_RAFT_REPLICATOR=openraft QUEEN_RAFT_NODE_ID=$((i+1))
export QUEEN_RAFT_PEERS='$PEERS' QUEEN_RAFT_LISTEN=$a:7400 QUEEN_RAFT_TOKEN=$TOK
export QUEEN_RAFT_DIR=/opt/queen/data FILE_BUFFER_DIR=/opt/queen/buf QUEEN_BIND_ADDR=$a PORT=6632
export JWT_ENABLED=false QUEEN_TENANCY_HEADER=false LOG_LEVEL=info POP_DEFAULT_TIMEOUT_MS=5000
echo \$\$ > /opt/queen/wrapper.pid
while true; do /opt/queen/queen; rc=\$?; echo "run-repro.sh: queen exited rc=\$rc"; [ \$rc -eq 75 ] || break; done
EOF
  ssh "$n" "chmod +x /opt/queen/run-repro.sh; setsid nohup /opt/queen/run-repro.sh >> /opt/queen/queen.log 2>&1 < /dev/null &"
done
for n in "${NODES[@]}"; do for t in $(seq 1 120); do h "$n" | grep -q '"status":"healthy"' && break; sleep 0.5; done; done
sleep 5   # let the preferred-leader handoff settle

for MODE in responses all; do
  L=""; for n in "${NODES[@]}"; do h "$n" | grep -q '"role":"leader"' && L=$n; done
  FOLLOWERS=(); for n in "${NODES[@]}"; do [ "$n" != "$L" ] && FOLLOWERS+=("$n"); done
  P=${FOLLOWERS[0]}
  echo "== mode=$MODE leader=$L; pushes through follower $P; the fault on $L from t=10s to t=30s"
  python3 - "$(ip "$P")" 40 <<'PY' > /tmp/leader-deaf-acks.txt &
import http.client, json, sys, threading, time
host, secs = sys.argv[1], float(sys.argv[2]); t0 = time.time()
lock = threading.Lock(); acks = {}
def w(k):
    c = http.client.HTTPConnection(host, 6632, timeout=8); i = 0
    while time.time() - t0 < secs:
        i += 1
        try:
            c.request("POST", "/api/v1/push", json.dumps({"items":[{"queue":"deaf","partition":"p%d"%k,"payload":i,"transactionId":"%d-%d"%(k,i)}]}), {"content-type":"application/json"})
            r = c.getresponse(); b = r.read()
            if r.status == 201:
                with lock: s = int(time.time() - t0); acks[s] = acks.get(s, 0) + 1
        except Exception:
            c = http.client.HTTPConnection(host, 6632, timeout=8)
ts = [threading.Thread(target=w, args=(k,)) for k in range(8)]; [t.start() for t in ts]; [t.join() for t in ts]
print(" ".join("%d:%d" % (s, acks.get(s, 0)) for s in range(int(secs))))
PY
  LOAD=$!
  sleep 10
  for f in "${FOLLOWERS[@]}"; do
    if [ $MODE = responses ]; then
      ssh "$L" "iptables -A INPUT -s $(ip "$f") -p tcp --sport 7400 -m length --length 100:65535 -j DROP"
    else
      ssh "$L" "iptables -A INPUT -s $(ip "$f") -j DROP"
    fi
  done
  for s in $(seq 1 20); do
    printf "t=%02d " $((10+s)); for n in "${NODES[@]}"; do printf "%s:%s/%s " "$n" "$(h "$n" | sed -n 's/.*"role":"\([a-z]*\)".*/\1/p' | cut -c1-4)" "$(h "$n" | sed -n 's/.*"term":\([0-9]*\).*/\1/p')"; done; echo
    sleep 1
  done
  ssh "$L" "iptables -F"
  wait $LOAD
  echo "acks per second (t:count): $(cat /tmp/leader-deaf-acks.txt)"
done
echo "== cleanup"
for n in "${NODES[@]}"; do ssh "$n" 'kill -9 $(cat /opt/queen/wrapper.pid 2>/dev/null) 2>/dev/null; pkill -9 -x queen; iptables -F; rm -f /opt/queen/run-repro.sh; true'; done
