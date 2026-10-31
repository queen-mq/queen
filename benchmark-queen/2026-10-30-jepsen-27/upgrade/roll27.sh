#!/usr/bin/env bash
# roll27.sh: rolling upgrade 2.6.0 -> 2.7.0 (beta.2) of a three-node cluster on n1..n3 (GCP VMs), run from
# the control, under load, dedup on. 2.7.0 does not raise the cluster version, so every step must keep it
# at 7, and a node must be able to go back to 2.6.0 at any point. roll26.sh's steps, plus what 2.7.0 adds:
#   0. a fresh 2.7.0 cluster must report cluster version 7, then is wiped;
#   1. three 2.6.0 nodes: queues hist (history, never consumed), live (steady traffic), dup (checks);
#      two KV keys and a stream query written by 2.6.0;
#   2. n3 then n2 move to 2.7.0 under a 2.6.0 leader: the version stays 7, dedup works across the mix;
#      a push of 12,000 messages (2.7.0 refuses more than 10,000) through the 2.6.0 leader and through a
#      2.7.0 node; a stream claim through a 2.7.0 node answers 404 from the 2.6.0 leader;
#   3. n3 goes back to 2.6.0, then forward again;
#   4. n1, the last 2.6.0 member, moves: the version stays 7;
#   5. after the roll: dedup for every set, the live totals; KV reads byte for byte against 2.6.0's; the
#      claim answers 200; the 12,000 messages pop back in pops of at most 10,000;
#   6. a full stop and start of the cluster: the duplicates of every set found, the history read back whole;
#   7. kill -9 of the leader under load: same checks;
#   8. after 2.7.0 wrote everything: n3 back on 2.6.0 on its own directory must start and serve; then made
#      the LEADER (the 2.7.0 leaders stopped until n3 wins an election), and must answer dedup and KV
#      reads; then forward again and the cluster whole.
set -u
R=/root/roll/runs/roll27; rm -rf $R; mkdir -p $R
NEW=/root/roll/bin/queen-27b2; OLD=/root/roll/bin/queen-260
Q=/root/bin/qload
U=http://n1:6632,http://n2:6632,http://n3:6632
say() { echo "$(date -u +%H:%M:%S) $*" | tee -a $R/roll.log; }
nd() { ssh -n -o BatchMode=yes n$1 "/root/roll/node.sh ${*:2}"; }
h() { python3 /root/roll/health.py n$1; }
status() { for i in 1 2 3; do say "    n$i: $(h $i)"; done; }
wait_ok() { for _ in $(seq 1 600); do o=$(curl -s -m 2 http://n$1:6632/health); echo "$o" | grep -q '"status":"healthy"' && echo "$o" | grep -q '"lag":0' && return 0; sleep 0.2; done; say "    n$1 NOT healthy within 120 s: $(h $1)"; return 1; }
wait_cv() { for _ in $(seq 1 ${2:-300}); do ok=0; for i in 1 2 3; do curl -s -m 2 http://n$i:6632/health | grep -qE "\"clusterVersion\":$1[,}]" && ok=$((ok + 1)); done; [ $ok = 3 ] && return 0; sleep 1; done; return 1; }
swap() { nd $1 term; nd $1 start $2; wait_ok $1; }
leader() { for i in 1 2 3; do h $i | grep -q " leader " && { echo $i; return; }; done; echo 0; }
since() { ssh -n n$1 "tac /root/roll/logs/queen.log | sed '/^=== /q' | tac"; }   # the node's log since its last start
dup() { say "    dup $1 $2: $(python3 /root/roll/dup.py $U $1 $2 $R)"; }
x() { say "    $(python3 /root/roll/extra27.py $2 http://n$1:6632 $R ${3:-})"; }   # x <node> <cmd> [arg]
same() { if cmp -s $R/$1 $R/$2; then say "    $1 = $2: same bytes"; else say "    $1 != $2: DIFFERENT"; say "      $1: $(head -c 400 $R/$1)"; say "      $2: $(head -c 400 $R/$2)"; fi; }
live() { # live <seconds> <tag>: three loaders, each through its own node, 1,500 msg/s each in pushes of 20, 2 consumers each
  for i in 1 2 3; do
    $Q -urls http://n$i:6632 -loader-index $((i - 1)) -loaders 3 -topic live -topics 1 -partitions 50 -mode batch -batch 20 \
      -rate 1500 -ramp 2 -duration $1 -consumers 2 -pop-batch 200 -pop-width 5 -drain 20 -report 10 -create=true \
      -configure=false -payload 256 -timeout 10s -out $R/live$2-$i.json > $R/live$2-$i.log 2>&1 &
  done; }
livesum() { for i in 1 2 3; do say "    loader via n$i: $(grep '^\[final\]' $R/live$1-$i.log | grep -oE 'pushErr=[0-9]+ \| pushed=[0-9]+ popped=[0-9]+ lag=[-0-9]+ \| popErr=[0-9]+|ackErr=[0-9]+' | tr '\n' ' ')"; done; }

say "roll27: OLD=$(md5sum $OLD | cut -c1-8) (2.6.0) NEW=$(md5sum $NEW | cut -c1-8) (2.7.0-beta.2)"
for i in 1 2 3; do nd $i kill >/dev/null 2>&1; nd $i wipe; done

say "0. a fresh cluster of 2.7.0"
for i in 1 2 3; do nd $i start $NEW; done
for i in 1 2 3; do wait_ok $i || exit 1; done
if wait_cv 7; then say "    clusterVersion 7 on all three: OK"; else say "    FAIL: a fresh 2.7.0 cluster is not at 7"; fi
status
for i in 1 2 3; do nd $i term; done; for i in 1 2 3; do nd $i wipe; done

say "1. three nodes on 2.6.0"
for i in 1 2 3; do nd $i start $OLD; done
for i in 1 2 3; do wait_ok $i || exit 1; done
wait_cv 7 || { say "    FAIL: the 2.6.0 cluster is not at version 7"; status; exit 1; }
status
for q in hist:3600 live:600 dup:3600; do
  curl -s -m 5 -X POST http://n1:6632/api/v1/configure -H 'content-type: application/json' \
    -d "{\"queue\":\"${q%%:*}\",\"options\":{\"dedupWindowSeconds\":${q##*:},\"leaseTime\":30}}" > /dev/null
done
$Q -urls http://n1:6632 -topic hist -topics 1 -partitions 100 -mode batch -batch 50 -rate 8000 -ramp 2 -duration 25 \
  -consumers 0 -create=true -configure=false -payload 256 -out $R/hist-p.json > $R/hist-p.log 2>&1
say "    history written by 2.6.0: $(grep '^\[final\]' $R/hist-p.log | grep -oE 'pushErr=[0-9]+ \| pushed=[0-9]+')"
dup A new
x 1 kvput a; x 1 kvget 260; x 1 query; x 1 claim
live 360 a; sleep 15

say "2. n3, then n2, move to 2.7.0 (leader n$(leader))"
swap 3 $NEW; swap 2 $NEW; sleep 3; status
if wait_cv 7 5; then say "    cluster version 7 with n1 on 2.6.0: OK"; else say "    FAIL: the version moved"; status; fi
say "    leader: n$(leader)"
dup A dup; dup B new
x 1 bigpush 12000; x 2 bigpush 12000
x 2 claim
for i in 1 2 3; do wait_ok $i; done; status

say "3. n3 goes back to 2.6.0, then forward again"
nd 3 term; nd 3 start $OLD
if wait_ok 3; then say "    2.6.0 started on a directory 2.7.0 had written: OK"; else say "    FAIL: the rollback was refused"; since 3 | tail -5 | cut -c1-300 | tee -a $R/roll.log; fi
status
dup B dup
swap 3 $NEW

say "4. n1 moves: the last 2.6.0 member (leader before: n$(leader))"
swap 1 $NEW
if wait_cv 7 30; then say "    every node on 2.7.0, cluster version 7: OK"; else say "    FAIL: the version is not 7"; fi
status

say "5. after the roll"
dup C new
for s in A B C; do dup $s dup; done
x 1 kvget 27; same kv-260-get.bin kv-27-get.bin; same kv-260-batch.bin kv-27-batch.bin
x 1 kvput b; x 1 kvget 27b
x 1 claim
x 2 bigpop
wait %1 %2 %3 2>/dev/null; wait
say "    live traffic over the roll (360 s, 4,500 msg/s offered):"; livesum a

say "6. a full stop and start of the cluster on 2.7.0"
for i in 1 2 3; do nd $i term; done
for i in 1 2 3; do nd $i start $NEW; done
for i in 1 2 3; do wait_ok $i; done
status
for s in A B C; do dup $s dup; done
$Q -urls $U -topic hist -topics 1 -partitions 100 -rate 0 -consumers 8 -pop-batch 500 -pop-width 5 -duration 1 -drain 40 \
  -report 10 -create=false -configure=false -out $R/hist-c.json > $R/hist-c.log 2>&1
say "    history (written by 2.6.0) read back: $(grep '^\[final\]' $R/hist-c.log | grep -oE 'popped=[0-9]+|popErr=[0-9]+' | tr '\n' ' ')"

say "7. kill -9 of the leader under load"
live 90 b; sleep 30
L=$(leader); say "    leader n$L killed"
nd $L kill; sleep 5; nd $L start $NEW; wait_ok $L; status
for s in A B C; do dup $s dup; done
dup D new; dup D dup
wait
livesum b

say "8. n3 back on 2.6.0 after 2.7.0 wrote everything"
nd 3 term; nd 3 start $OLD
if wait_ok 3; then say "    2.6.0 on n3's own directory serves: OK"; else say "    FAIL: 2.6.0 refused 2.7.0's data"; since 3 | grep -E 'ERROR|FATAL|catalogue|version|refus|panicked' | head -4 | cut -c1-300 | tee -a $R/roll.log; fi
status
for s in A B C D; do dup $s dup; done
say "8b. n3 (2.6.0) made the leader: each 2.7.0 leader stopped until n3 wins an election"
for t in 1 2 3 4 5 6 7 8; do
  L=$(leader); [ "$L" = 3 ] && break
  if [ "$L" = 0 ]; then sleep 2; continue; fi
  nd $L term; sleep 6; nd $L start $NEW; wait_ok $L
done
L=$(leader)
if [ "$L" = 3 ]; then
  say "    n3 (2.6.0) leads after $t tries: OK"; status
  for s in A B C D; do dup $s dup; done
  dup E new; dup E dup
  x 3 kvget 260b; same kv-260-get.bin kv-260b-get.bin; same kv-27b-batch.bin kv-260b-batch.bin
else
  say "    FAIL: n3 did not win an election in 8 tries (leader n$L)"; status
fi
swap 3 $NEW; status
dup E dup

say "errors and warnings per node (all starts):"
for i in 1 2 3; do
  say "  n$i FATAL/panic/poison/unknown-version/APPLY FAILED lines: $(ssh -n n$i "grep -cE 'FATAL|panicked|poisoned|unknown effect catalogue|unknown version|APPLY FAILED' /root/roll/logs/queen.log")"
  say "  n$i: $(ssh -n n$i "grep -E ' WARN | ERROR ' /root/roll/logs/queen.log" | cut -c32-200 | sed -E 's/[0-9]{3,}/N/g' | sort | uniq -c | sort -rn | head -8 | tr '\n' ';')"
done
for i in 1 2 3; do ssh -n n$i 'cp /root/roll/logs/queen.log /root/roll/queen-node.log'; scp -q n$i:/root/roll/queen-node.log $R/n$i-queen.log; done
say ROLL_DONE
