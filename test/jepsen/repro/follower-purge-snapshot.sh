#!/usr/bin/env bash
# Reproduces: a follower that is away for SECONDS (far below
# QUEEN_RAFT_PURGE_HOLD_S = 600 s) gets a full snapshot and an exit-75 restart
# as soon as leadership moves, because the nodes that are not leading purge
# their raft log with no floor.
#
#   rsm/replicator/raft/mod.rs replicated_floor(): `m.replication` is None off
#   the leader -> u64::MAX, so purge_step() snapshots and purges down to
#   durable - QUEEN_RAFT_LOG_KEEP on every follower; after each snapshot build
#   openraft's own policy purge (max_in_snapshot_log_to_keep, default 1000,
#   not set in raft_config()) cuts further, to snapshot_index - 1000.
#
# Run on the Jepsen control node (n1..n5 in /etc/hosts, root ssh), with no
# Jepsen test running. Uses the same node layout and env as jepsen.queen.db,
# with every Queen default for the purge knobs.
#
#   ./follower-purge-snapshot.sh /root/bin/queen-9284d06c
set -eu
BIN=${1:?queen binary on this node}
NODES=(n1 n2 n3 n4 n5)
TOKEN=$(cat /proc/sys/kernel/random/uuid)
ip() { getent hosts "$1" | awk '{print $1}'; }
PEERS=""
for i in "${!NODES[@]}"; do
  a=$(ip "${NODES[$i]}"); PEERS="$PEERS${PEERS:+,}$((i+1))=$a:7400/$a:6632"
done
h() { curl -s --max-time 2 "http://$(ip "$1"):6632/health"; }
role() { h "$1" | sed -n 's/.*"role":"\([a-z]*\)".*/\1/p'; }
last_log() { # the node's own highest raft log index, from its health (applied)
  h "$1" | sed -n 's/.*"applied":\([0-9]*\).*/\1/p'; }

echo "== 1. a fresh 5-node cluster"
for i in "${!NODES[@]}"; do
  n=${NODES[$i]}; a=$(ip "$n")
  ssh "$n" "pkill -9 -x queen; sleep 0.3; iptables -F; rm -rf /opt/queen/data /opt/queen/buf /opt/queen/queen.log; mkdir -p /opt/queen/data /opt/queen/buf"
  scp -q "$BIN" "$n:/opt/queen/queen.repro" 2>/dev/null || true
  ssh "$n" "cat > /opt/queen/run-repro.sh" <<EOF
#!/usr/bin/env bash
ulimit -n 1048576
export QUEEN_STORAGE=raft QUEEN_RAFT_REPLICATOR=openraft QUEEN_RAFT_NODE_ID=$((i+1))
export QUEEN_RAFT_PEERS='$PEERS' QUEEN_RAFT_LISTEN=$a:7400 QUEEN_RAFT_TOKEN=$TOKEN
export QUEEN_RAFT_DIR=/opt/queen/data FILE_BUFFER_DIR=/opt/queen/buf QUEEN_BIND_ADDR=$a PORT=6632
export JWT_ENABLED=false QUEEN_TENANCY_HEADER=false LOG_LEVEL=info
echo \$\$ > /opt/queen/repro-wrapper.pid
while true; do /opt/queen/queen.repro; rc=\$?; echo "run-repro.sh: queen exited rc=\$rc"; [ \$rc -eq 75 ] || break; done
EOF
  ssh "$n" "chmod +x /opt/queen/run-repro.sh /opt/queen/queen.repro; setsid nohup /opt/queen/run-repro.sh >> /opt/queen/queen.log 2>&1 < /dev/null &"
done
for n in "${NODES[@]}"; do
  for t in $(seq 1 120); do [ "$(h "$n" | grep -c '"status":"healthy"')" = 1 ] && break; sleep 0.5; done
done
LEADER=""; for n in "${NODES[@]}"; do [ "$(role "$n")" = leader ] && LEADER=$n; done
echo "leader: $LEADER"
LAG=""; for n in "${NODES[@]}"; do [ "$n" != "$LEADER" ] && LAG=$n; done
OTHERS=(); for n in "${NODES[@]}"; do [ "$n" != "$LAG" ] && OTHERS+=("$n"); done
PUSHER=""; for n in "${OTHERS[@]}"; do [ "$n" != "$LEADER" ] && PUSHER=$n; done
curl -s -X POST "http://$(ip "$PUSHER"):6632/api/v1/configure" -H 'content-type: application/json' \
  -d '{"queue":"repro","options":{"retentionEnabled":false,"deadLetterQueue":false}}' > /dev/null

echo "== 2. isolate follower $LAG from every other node"
for n in "${OTHERS[@]}"; do
  ssh "$n" "iptables -A INPUT -s $(ip "$LAG") -j DROP"
  ssh "$LAG" "iptables -A INPUT -s $(ip "$n") -j DROP"
done
BEFORE=$(last_log "$LAG"); T0=$(date +%s); echo "$LAG applied=$BEFORE"

echo "== 3. ~7000 sequential pushes via $PUSHER (one raft entry each)"
python3 - "$(ip "$PUSHER")" <<'PY'
import http.client, json, sys
c = http.client.HTTPConnection(sys.argv[1], 6632, timeout=10)
for i in range(7000):
    body = json.dumps({"items": [{"queue": "repro", "partition": "p%d" % (i % 8),
                                  "payload": i, "transactionId": "r%d" % i}]})
    c.request("POST", "/api/v1/push", body, {"content-type": "application/json"})
    r = c.getresponse(); r.read()
    if r.status != 201: print("push", i, r.status)
PY
# Wait until the followers (not the leader) purged past $LAG's position.
purged_past() { # node -> 0 if its last purge point is above $BEFORE
  local p; p=$(ssh "$1" "grep 'purge log, last_purged' /opt/queen/queen.log | tail -1 | sed -n 's/.*purge_upto: [0-9]*\.\([0-9]*\).*/\1/p'")
  [ -n "$p" ] && [ "$p" -gt "$BEFORE" ]; }
for t in $(seq 1 60); do
  ok=1; for n in "${OTHERS[@]}"; do [ "$n" = "$LEADER" ] && continue; purged_past "$n" || ok=0; done
  [ $ok = 1 ] && break; sleep 1
done
for n in "${OTHERS[@]}"; do
  echo "$n: $(role "$n") applied=$(last_log "$n") last purge: $(ssh "$n" "grep 'purge log, last_purged' /opt/queen/queen.log | tail -1 | sed 's/.*purge log, //'")"
done

echo "== 4. kill -9 the leader $LEADER: one of the purged followers takes over"
ssh "$LEADER" 'kill -9 $(cat /opt/queen/repro-wrapper.pid) 2>/dev/null; pkill -9 -x queen.repro; true'
sleep 5
NEW=""; for n in "${OTHERS[@]}"; do [ "$n" != "$LEADER" ] && [ "$(role "$n")" = leader ] && NEW=$n; done
echo "new leader: $NEW"

echo "== 5. heal $LAG (away $(( $(date +%s) - T0 )) s; QUEEN_RAFT_PURGE_HOLD_S is 600)"
for n in "${NODES[@]}"; do ssh "$n" "iptables -F"; done
sleep 15
echo "== 6. what $LAG did"
ssh "$LAG" "grep -E 'exits to load a received snapshot|queen exited rc=75|swapping a received snapshot' /opt/queen/queen.log | cut -c1-220" || echo "(no snapshot: not reproduced)"
echo "== new leader $NEW sent:"
ssh "$NEW" "grep 'rsm: raft: sending a snapshot' /opt/queen/queen.log | head -3 | cut -c1-220" || true

echo "== cleanup"
for n in "${NODES[@]}"; do ssh "$n" 'kill -9 $(cat /opt/queen/repro-wrapper.pid) 2>/dev/null; pkill -9 -x queen.repro; iptables -F; true'; done
