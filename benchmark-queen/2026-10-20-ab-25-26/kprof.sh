#!/usr/bin/env bash
# kprof.sh <bin> <rate>: one kab2.sh point, and 20 s of perf (99 Hz, call graphs) on the leader's broker 45 s after the
# point starts (inside the steady window), then the top of the profile by thread and by symbol.
H=$(cd "$(dirname "$0")/../2026-10-12-three-node-new-forms/scripts" && pwd); . "$H/hosts.env"
D=$(cd "$(dirname "$0")" && pwd); bin=$1; rate=$2; tag=kprof-$bin-r$rate
SSH="ssh -o ConnectTimeout=20 -o BatchMode=yes"
( BINS=$bin RATES=$rate PFX=kprof bash $D/kab2.sh > $D/kprof-run.log 2>&1 ) &
sleep 75   # cluster start + topic + groups + ramp
L=""; for i in 0 1 2; do $SSH root@${BP[$i]} "curl -s -m 2 http://${BV[$i]}:6632/health" | grep -q '"role":"leader"' && L=$i; done
echo "leader n$((L+1))"
$SSH root@${BP[$L]} "cd /root && perf record -F 99 -g -p \$(cat /root/p3/run/pid) -o /root/kprof.data -- sleep 20 >/dev/null 2>&1; \
  perf report -i /root/kprof.data --stdio --no-children --sort comm 2>/dev/null | grep -E '^ +[0-9]' | head -12; echo ---; \
  perf report -i /root/kprof.data --stdio --no-children --sort comm,sym 2>/dev/null | grep -E '^ +[0-9]' | head -30" | cut -c1-200
wait
grep -E 'kprof-.*(OK|NOT|only)' $D/kab.out | tail -1 | cut -c1-260
