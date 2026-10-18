#!/usr/bin/env bash
# grun3.sh <name> <groups> <msg/s in total, 0 = closed loop> <messages per push>     env: MODE=sync|nosync, EXTRA="K=V ..."
# A fresh three-node cluster with <groups> raft groups. Three tenants (one per group when groups=3; all in the one group when
# groups=1). Each loader node talks to ONE broker and drives all three tenants through it, so every tenant's clients are spread
# evenly over the three nodes whatever leads its group: the same client setup for one group and for three.
# Per process: 67 producers and 67 consumers on 67 partitions of its own queue; consumers pop any partition, commit at delivery.
S=$(cd "$(dirname "$0")" && pwd); D=/Users/alice/Work/queen/benchmark-queen/2026-10-11-ephemeral/${OUT:-runs-matrix}; mkdir -p $D
SSH="ssh -o BatchMode=yes -o ConnectTimeout=10 -o ConnectionAttempts=3"
name=$1; G=$2; RATE=$3; BATCH=$4
B=(${BROKERS_PUB:?the three broker addresses}); V=(10.114.0.6 10.114.0.5 10.114.0.4); L=(${LOADERS_PUB:?the three loader addresses})
RF=""; [ "$RATE" != 0 ] && RF="-rate $(( RATE / 9 ))"
ok=0
for attempt in 1 2 3; do
  for i in 0 1 2; do $SSH root@${B[$i]} "bash -s -- $((i+1)) ${V[$i]} $G ${MODE:-sync} '${EXTRA:-}'" < "$S/gsetup.sh" >/dev/null 2>&1 & done; wait
  for _ in $(seq 1 20); do
    ok=$($SSH root@${B[0]} 'for h in 10.114.0.6 10.114.0.5 10.114.0.4; do curl -s -m 2 http://$h:6632/health | grep -c "\"status\":\"healthy\""; done' | grep -c 1)
    [ "$ok" = 3 ] && break; sleep 1
  done
  [ "$ok" = 3 ] && break
  echo "  start $attempt of $name: the cluster is not healthy, starting it again"
done
echo "== $name: groups=$G mode=${MODE:-sync} extra='${EXTRA:-}' rate=$RATE batch=$BATCH healthy=$ok at $(date -u +%T)"
[ "$ok" = 3 ] || exit 1
sleep 3
for li in 0 1 2; do
  cmd="ulimit -n 1000000;"
  for t in 0 1 2; do
    ten=$(printf '%08d-0000-4000-8000-000000000000' $t)
    cmd="$cmd (/root/ephload -host ${V[$li]}:6632 -mode pipe -durable -anypart -${PAD:-randpad} -tenant $ten -tag t${t}b${li}- -name $name -duration 15s -warmup 5s -pairs 67 -batch $BATCH -popbatch 1000 $RF 2>&1 | grep -E '^RESULT|error' | sed 's/^/t${t}b${li} /') &"
  done
  $SSH root@${L[$li]} "$cmd wait" > "$D/$name.L$li.txt" 2>&1 &
done; wait
for i in 0 1 2; do
  $SSH root@${B[$i]} 'cat /root/samples.txt' > "$D/$name.samples.n$((i+1)).txt" 2>/dev/null &
  $SSH root@${B[$i]} 'grep -a "apply path stages" /root/queen.log | grep -a "rpcs=[1-9]" | tail -3' > "$D/$name.stages.n$((i+1)).txt" 2>/dev/null &
done; wait
python3 "$S/gtable3.py" "$D" "$name" "$G" "${MODE:-sync}" "${EXTRA:-}" "$RATE" "$BATCH"
