#!/usr/bin/env bash
# grun4.sh <name> <groups> <msg/s in total>     env: MODE=sync|nosync, EXTRA="K=V ..."
# The matrix's fixed-rate point (100 per push, 9 loader processes spread over the three brokers, random payloads) with tracing:
# the loader's worst latency per 100 ms, TCP counters and CPU steal on every node over the run, the brokers' own timing lines.
S=$(cd "$(dirname "$0")" && pwd); D=/Users/alice/Work/queen/benchmark-queen/2026-10-11-ephemeral/runs-tail; mkdir -p $D
SSH="ssh -o BatchMode=yes -o ConnectTimeout=10 -o ConnectionAttempts=3"
name=$1; G=$2; RATE=$3
B=(<host-41> <host-42> <host-43>); V=(10.114.0.6 10.114.0.5 10.114.0.4); L=(<host-44> <host-45> <host-46>)
ok=0
for attempt in 1 2 3; do
  for i in 0 1 2; do $SSH root@${B[$i]} "bash -s -- $((i+1)) ${V[$i]} $G ${MODE:-sync} '${EXTRA:-}' ${DBB:-0}" < "$S/gsetup.sh" >/dev/null 2>&1 & done; wait
  for _ in $(seq 1 20); do
    ok=$($SSH root@${B[0]} 'for h in 10.114.0.6 10.114.0.5 10.114.0.4; do curl -s -m 2 http://$h:6632/health | grep -c "\"status\":\"healthy\""; done' | grep -c 1)
    [ "$ok" = 3 ] && break; sleep 1
  done
  [ "$ok" = 3 ] && break
  echo "  start $attempt of $name: the cluster is not healthy, starting it again"
done
echo "== $name: groups=$G mode=${MODE:-sync} extra='${EXTRA:-}' rate=$RATE healthy=$ok at $(date -u +%T)"
[ "$ok" = 3 ] || exit 1
sleep 3
for h in ${B[@]} ${L[@]}; do $SSH root@$h 'nstat -n; head -1 /proc/stat > /root/stat.before' & done; wait
for li in 0 1 2; do
  cmd="ulimit -n 1000000;"
  for t in 0 1 2; do
    ten=$(printf '%08d-0000-4000-8000-000000000000' $t)
    cmd="$cmd (/root/ephload -host ${V[$li]}:6632 -mode pipe -durable -anypart -${PAD:-randpad} -trace -tenant $ten -tag t${t}b${li}- -name $name -duration ${DUR:-15}s -warmup 5s -pairs 67 -batch 100 -popbatch 1000 -rate $(( RATE / 9 )) 2>&1 | grep -E '^RESULT|error' | sed 's/^/t${t}b${li} /') &"
  done
  $SSH root@${L[$li]} "$cmd wait" > "$D/$name.L$li.txt" 2>&1 &
done; wait
POST='nstat -z TcpOutSegs TcpRetransSegs TcpExtTCPTimeouts TcpExtDelayedACKs TcpExtTCPFastRetrans TcpExtTCPLossProbes 2>/dev/null | grep -v "^#"; echo "steal $(( $(head -1 /proc/stat | awk "{print \$9}") - $(awk "{print \$9}" /root/stat.before) )) jiffies_total $(( $(head -1 /proc/stat | awk "{s=0; for(i=2;i<=9;i++) s+=\$i; print s}") - $(awk "{s=0; for(i=2;i<=9;i++) s+=\$i; print s}" /root/stat.before) ))"'
for i in 0 1 2; do
  $SSH root@${B[$i]} "$POST" > "$D/$name.tcp.n$((i+1)).txt" 2>/dev/null &
  $SSH root@${L[$i]} "$POST" > "$D/$name.tcp.l$((i+1)).txt" 2>/dev/null &
  $SSH root@${B[$i]} 'cat /root/samples.txt' > "$D/$name.samples.n$((i+1)).txt" 2>/dev/null &
  $SSH root@${B[$i]} 'grep -a "rsm timing\|apply path stages\| lanes:" /root/queen.log | tail -60' > "$D/$name.log.n$((i+1)).txt" 2>/dev/null &
done; wait
python3 "$S/gtable5.py" "$D" "$name"
