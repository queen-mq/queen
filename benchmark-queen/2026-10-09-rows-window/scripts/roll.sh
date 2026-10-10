#!/usr/bin/env bash
# roll.sh <old bin> <new bin>: a rolling upgrade of three nodes on this machine, under load.
#   1. three nodes of the old build, a queue with history (retention off), steady traffic;
#   2. node 3 and node 2 move to the new build: the cluster version must stay where it was;
#   3. node 3 goes BACK to the old build (the rollback the docs promise while the version has
#      not risen), then forward again;
#   4. node 1 moves: the version must rise by itself, the history's rows must go, and the
#      history must be read back whole;
#   5. the old build must refuse to start on the upgraded data.
# Results in /root/improve/runs/roll/
set -u
OLD=$1; NEW=$2
R=/root/improve/runs/roll; rm -rf $R; mkdir -p $R
C=/root/improve/c3.sh; Q="taskset -c 6-15 /root/improve/bin/qload"
U=http://127.0.0.1:6632,http://127.0.0.1:6633,http://127.0.0.1:6634
say() { echo "$(date -u +%H:%M:%S) $*" | tee -a $R/roll.log; }
h() { curl -s -m 2 http://127.0.0.1:$((6631 + $1))/health; }
field() { python3 -c "import sys,json
try:
    d=json.load(sys.stdin); print(d['version'], d['raft']['role'], 'cv', d['raft']['clusterVersion'], 'kinds', d['raft']['kinds'], 'applied', d['raft']['applied'], 'lag', d['raft']['lag'])
except Exception as e: print('down')"; }
status() { for i in 1 2 3; do say "  n$i: $(h $i | field)"; done; }
rows() { for i in 1 2 3; do printf 'n%s=%s ' $i "$(curl -s -m 3 http://127.0.0.1:$((6631 + i))/metrics/prometheus | awk '/^queen_raft_store_ram_rows\{ks="txns",kind="live"\}/{print $2}')"; done; echo; }
wait_ok() { for _ in $(seq 1 300); do o=$(h $1); echo "$o" | grep -q '"status":"healthy"' && echo "$o" | grep -q '"lag":0' && return 0; sleep 0.2; done; say "n$1 NOT healthy after 60 s"; return 1; }
wait_cv() { for _ in $(seq 1 150); do ok=0; for i in 1 2 3; do h $i | grep -q "\"clusterVersion\":$1," && ok=$((ok + 1)); done; [ $ok = 3 ] && return 0; sleep 0.2; done; return 1; }
swap() { # swap <node> <bin>: stop it gracefully, start it on <bin>, wait until it follows again
  $C term $1; for _ in $(seq 1 100); do kill -0 "$(cat /root/improve/c3/run/n$1.pid)" 2>/dev/null || break; sleep 0.1; done
  BIN=$2 taskset -c 6-15 $C start $1; }

taskset -c 6-15 $C stopall >/dev/null 2>&1; $C wipe
for i in 1 2 3; do BIN=$OLD taskset -c 6-15 $C start $i; done
for i in 1 2 3; do wait_ok $i || exit 1; done
wait_cv 5 || { say "the old cluster is not at version 5"; status; exit 1; }
say "1. three nodes on the old build"; status
curl -s -X POST http://127.0.0.1:6632/api/v1/configure -H 'content-type: application/json' -d '{"queue":"hist","options":{"dedupWindowSeconds":5,"leaseTime":30}}' >/dev/null
curl -s -X POST http://127.0.0.1:6632/api/v1/configure -H 'content-type: application/json' -d '{"queue":"live","options":{"dedupWindowSeconds":5,"leaseTime":30}}' >/dev/null
# The history: 300,000 messages in 100 partitions, written by the old build, never consumed yet.
$Q -urls http://127.0.0.1:6632 -topic hist -topics 1 -partitions 100 -mode batch -batch 5 -rate 20000 -ramp 2 -duration 15 -consumers 0 -create=true -configure=false -payload 256 -out $R/hist-p.json > $R/hist-p.log 2>&1
say "   history written: $(grep -o 'pushed=[0-9]*' $R/hist-p.log | tail -1), rows per node: $(rows)"
# Steady traffic for the whole roll, through all three nodes.
$Q -urls $U -topic live -topics 1 -partitions 50 -mode batch -batch 10 -rate 5000 -ramp 2 -duration 150 -consumers 6 -pop-batch 200 -pop-width 5 -drain 20 -report 5 -create=true -configure=false -payload 256 -out $R/live.json > $R/live.log 2>&1 &
LIVE=$!
sleep 10
say "2. node 3, then node 2, move to the new build"
swap 3 $NEW; wait_ok 3; swap 2 $NEW; wait_ok 2; sleep 3; status
if wait_cv 5; then say "   the cluster version is still 5 with one old node: OK"; else say "   FAIL: the version moved with an old node in the cluster"; fi
say "   rows per node (must still be there, the window is not on): $(rows)"
say "3. node 3 goes back to the old build while the version has not risen"
swap 3 $OLD; if wait_ok 3; then say "   the old build started on a directory the new one had written: OK"; else say "   FAIL: rollback refused"; tail -3 /root/improve/c3/logs/n3.log | cut -c1-300 | tee -a $R/roll.log; fi
sleep 3; status
swap 3 $NEW; wait_ok 3
say "4. node 1 moves: the last one"
swap 1 $NEW; wait_ok 1
if wait_cv 6; then say "   the cluster version rose to 6 by itself: OK"; else say "   FAIL: the version did not rise"; fi
status
for _ in $(seq 1 60); do r=$(rows); echo "$r" | grep -q 'n1=0 n2=0 n3=0' && break; sleep 1; done
wait $LIVE
say "   live traffic over the roll: $(grep '^\[final\]' $R/live.log | cut -c1-330)"
say "   rows per node after the window (hist's must be gone): $(rows)"
# The history, written by the old build and now without rows, read back whole.
$Q -urls $U -topic hist -topics 1 -partitions 100 -rate 0 -consumers 8 -pop-batch 500 -pop-width 5 -duration 1 -drain 25 -report 5 -create=false -configure=false -out $R/hist-c.json > $R/hist-c.log 2>&1
say "   history read back: $(grep '^\[final\]' $R/hist-c.log | grep -o 'popped=[0-9]* lag=[-0-9]*.*popErr=[0-9]*' | cut -c1-120)"
say "   claims from the queue log per node: $(for i in 1 2 3; do printf 'n%s=%s ' $i "$(curl -s -m 3 http://127.0.0.1:$((6631 + i))/metrics/prometheus | awk '/^queen_consume_cold_claims_total /{print $2}')"; done)"
say "5. the old build on upgraded data"
$C term 3; sleep 2; BIN=$OLD $C start 3; sleep 4
if h 3 | grep -q healthy; then say "   FAIL: the old build started on version-6 data"; else say "   the old build refused: $(grep -m1 -i 'catalogue version\|FATAL' /root/improve/c3/logs/n3.log | tail -1 | cut -c32-330)"; fi
$C kill 3 2>/dev/null; BIN=$NEW taskset -c 6-15 $C start 3; wait_ok 3; status
say "errors and warnings per node (other than a peer being down during its restart):"
for i in 1 2 3; do say "  n$i: $(grep -E ' WARN | ERROR ' /root/improve/c3/logs/n$i.log | grep -v -E 'Unreachable|heartbeat|failed to send|connection|Connect|transport|read_index|vote|election|replication' | cut -c32-200 | sed -E 's/[0-9]{3,}/N/g' | sort | uniq -c | sort -rn | head -6 | tr '\n' ';')"; done
taskset -c 6-15 $C stopall >/dev/null 2>&1
say ROLL_DONE
