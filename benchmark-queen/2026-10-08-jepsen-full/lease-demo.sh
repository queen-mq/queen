#!/bin/bash
# On a control node, with no campaign running: the leader of a five-node
# cluster is cut off from the four others for a little over openraft's leader
# lease (2 s) while a client keeps writing to it, then the network heals.
# Does the leader take writes afterwards?
#
#   /root/lease-demo.sh <binary> <trials> <cut-ms>
#
# The five nodes start from /opt/queen/run.sh as the last Jepsen test left it
# (its peers, its token), on an empty data directory. Each trial: find the
# leader, keep writing to it (the next write 50 ms after the answer, and a
# write with no answer is given up after 1 s), drop every packet between it
# and the other nodes for <cut-ms>, heal, wait 3 s, and then
#   deposed    another node leads, or the term moved: the cut ended in an
#              election, which restarts everything; nothing to see
#   ok         the same leader in the same term takes a write within 5 s
#   STUCK      the same leader in the same term takes none in 5 s, twice
# A stuck leader is restarted before the next trial.
bin=$1
trials=${2:-15}
cut_ms=${3:-2400}
nodes="n1 n2 n3 n4 n5"
key=/root/.ssh/id_ed25519
on() { ssh -o BatchMode=yes -o ConnectTimeout=6 -i $key root@"$1" "$2"; }
ip_of() { getent hosts "$1" | awk '{print $1}'; }
health() { curl -s --max-time 2 "http://$(ip_of "$1"):6632/health"; }
role() { health "$1" | python3 -c 'import sys,json
try:
    r = json.load(sys.stdin).get("raft", {})
    print(r.get("role", "?"), r.get("term", "?"))
except Exception:
    print("? ?")'; }
leader() {
  for n in $nodes; do
    set -- $(role "$n") "$n"
    [ "$1" = leader ] && { echo "$3 $2"; return 0; }
  done
  return 1
}
await_leader() {
  for _ in $(seq 1 60); do leader && return 0; sleep 0.5; done
  echo "none ?"; return 1
}
push() { # push <node> <max-time> <id> -> the HTTP status
  curl -s -o /dev/null -w '%{http_code}' --max-time "$2" -H 'content-type: application/json' \
    -X POST "http://$(ip_of "$1"):6632/api/v1/push" \
    -d "{\"items\":[{\"queue\":\"lease-demo\",\"partition\":\"p\",\"payload\":$3,\"transactionId\":\"t-$3\"}]}"
}

echo "== $(date -u +%FT%TZ) binary $bin ($(md5sum "$bin" | cut -c1-12)), $trials trials, cut $cut_ms ms"
for n in $nodes n6; do
  on "$n" 'pkill -CONT -x queen; pkill -9 -f queen/[r]un; pkill -9 -x queen; iptables -F -w; iptables -X -w; true' > /dev/null 2>&1
done
for n in $nodes; do
  scp -q -o BatchMode=yes -i $key "$bin" root@"$n":/opt/queen/queen.new
  on "$n" 'mv /opt/queen/queen.new /opt/queen/queen; chmod +x /opt/queen/queen; mkdir -p /opt/queen/data /opt/queen/buf; find /opt/queen/data -mindepth 1 -delete; find /opt/queen/buf -mindepth 1 -delete; : > /opt/queen/queen.log; setsid nohup /opt/queen/run.sh >> /opt/queen/queen.log 2>&1 < /dev/null &'
done
sleep 6
set -- $(await_leader); echo "leader $1 term $2"
[ "$(push "$1" 10 0)" = 201 ] || { sleep 3; push "$1" 10 0; echo " (first push)"; }

id=1; ok=0; stuck=0; deposed=0
for t in $(seq 1 "$trials"); do
  set -- $(await_leader); L=$1; T=$2
  [ "$L" = none ] && { echo "trial $t: no leader"; continue; }
  others=""; for n in $nodes; do [ "$n" != "$L" ] && others="$others $(ip_of "$n")"; done
  # The client: a write every 50 ms for the cut and 1.5 s more.
  ( end=$(( $(date +%s%N) / 1000000 + cut_ms + 1500 ))
    while [ $(( $(date +%s%N) / 1000000 )) -lt "$end" ]; do
      push "$L" 1 $((RANDOM * 32768 + RANDOM + 1000000)) > /dev/null; sleep 0.05
    done ) &
  pusher=$!
  sleep 0.5
  on "$L" "for o in $others; do iptables -A INPUT -s \$o -j DROP -w; iptables -A OUTPUT -d \$o -j DROP -w; done; sleep $(awk -v m="$cut_ms" 'BEGIN{printf "%.3f", m/1000}'); iptables -F -w"
  wait "$pusher" 2>/dev/null
  sleep 3
  set -- $(role "$L"); R=$1; T2=$2
  if [ "$R" != leader ] || [ "$T2" != "$T" ]; then
    deposed=$((deposed + 1)); echo "trial $t: leader $L term $T -> $R term $T2: deposed"; continue
  fi
  id=$((id + 1)); s1=$(push "$L" 5 $id)
  if [ "$s1" = 201 ]; then
    ok=$((ok + 1)); echo "trial $t: leader $L term $T kept leading: ok"
  else
    id=$((id + 1)); s2=$(push "$L" 5 $id)
    set -- $(role "$L")
    if [ "$s2" = 201 ]; then
      ok=$((ok + 1)); echo "trial $t: leader $L term $T kept leading: ok at the second write ($s1 then 201)"
    else
      stuck=$((stuck + 1)); echo "trial $t: leader $L term $T kept leading ($1 term $2 now): STUCK, writes answered $s1 and $s2"
      on "$L" 'pkill -9 -x queen; sleep 1; setsid nohup /opt/queen/run.sh >> /opt/queen/queen.log 2>&1 < /dev/null &'
      sleep 5
    fi
  fi
done
echo "== $bin: $trials trials, cut $cut_ms ms: deposed $deposed, kept leading and took writes $ok, kept leading and STUCK $stuck"
for n in $nodes; do
  echo "$n: gate waits $(on "$n" 'grep -a -c "an entry waited to be logged" /opt/queen/queen.log'), restarts $(on "$n" 'grep -a -c "pipeline restarts from the log" /opt/queen/queen.log')"
  on "$n" 'pkill -9 -f queen/[r]un; pkill -9 -x queen; iptables -F -w; true' > /dev/null 2>&1
done
