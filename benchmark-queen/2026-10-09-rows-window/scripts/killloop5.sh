#!/usr/bin/env bash
# killloop5.sh <tag> <bin> [cycles]
# Five nodes on this machine, log files sealed after a second, steady traffic
# through all five. Each cycle kills three of them at once (kill -9), the
# leader among them every other cycle, and starts them again a moment later:
# entries that had reached only a minority are overruled when their holders
# come back, which is what Jepsen's kill nemesis does to a five-node cluster.
# Every start must end in a node that serves; one that does not within 40 s
# ends the run, with what its log says. Results in /root/improve/runs/<tag>/
set -u
TAG=$1; BIN=$2; CYCLES=${3:-100}
N=5
ROOT=/root/improve/c5; R=/root/improve/runs/$TAG; rm -rf $R; mkdir -p $R $ROOT/logs $ROOT/run
Q="taskset -c 6-15 /root/improve/bin/qload"
PEERS=""; U=""
for i in $(seq 1 $N); do PEERS="$PEERS${PEERS:+,}$i=127.0.0.1:$((7500 + i))/127.0.0.1:$((6640 + i))"; U="$U${U:+,}http://127.0.0.1:$((6640 + i))"; done
start() {
  local i=$1 dir=$ROOT/data/n$1; mkdir -p "$dir"
  (
    ulimit -n 1048576
    export QUEEN_RAFT_DIR=$dir PORT=$((6640 + i)) QUEEN_BIND_ADDR=127.0.0.1 QUEEN_SERVER_ID=n$i LOG_LEVEL=info
    export QUEEN_RAFT_REPLICATOR=openraft QUEEN_RAFT_NODE_ID=$i QUEEN_RAFT_PEERS="$PEERS"
    export QUEEN_RAFT_LISTEN=127.0.0.1:$((7500 + i)) QUEEN_RAFT_TOKEN=c5-check-token
    export QUEEN_QLOG_SEAL_AGE_S=1 QUEEN_RAFT_SEGMENT_BYTES=262144 QUEEN_RAFT_TXN_WINDOW_MIN_S=2 RETENTION_INTERVAL=1000
    taskset -c 6-15 nohup "$BIN" >>"$ROOT/logs/n$i.log" 2>&1 </dev/null &
    echo $! >"$ROOT/run/n$i.pid"
  )
}
pidof_n() { cat $ROOT/run/n$1.pid 2>/dev/null; }
h() { curl -s -m 2 http://127.0.0.1:$((6640 + $1))/health; }
ok() { h $1 | grep -q '"status":"healthy"'; }
up() { for _ in $(seq 1 200); do ok $1 && return 0; kill -0 "$(pidof_n $1)" 2>/dev/null || return 1; sleep 0.2; done; return 1; }
leader() { for i in $(seq 1 $N); do h $i | grep -q '"role":"leader"' && { echo $i; return; }; done; echo 0; }
say() { echo "$(date -u +%H:%M:%S) $*" | tee -a $R/loop.log; }
stopall() { for i in $(seq 1 $N); do kill -9 "$(pidof_n $i)" 2>/dev/null; done; rm -f $ROOT/run/n*.pid; }
stopall; sleep 1; rm -rf $ROOT/data; rm -f $ROOT/logs/*.log
for i in $(seq 1 $N); do start $i; done
for i in $(seq 1 $N); do up $i || { say "n$i did not start"; exit 1; }; done
curl -s -X POST http://127.0.0.1:6641/api/v1/configure -H 'content-type: application/json' -d '{"queue":"kl","options":{"dedupWindowSeconds":2,"leaseTime":10}}' >/dev/null
$Q -urls $U -topic kl -topics 1 -partitions 50 -mode batch -batch 5 -rate 4000 -ramp 2 -duration $((CYCLES * 20 + 60)) -consumers 5 -pop-batch 200 -pop-width 5 -drain 5 -report 30 -create=true -configure=false -payload 256 -out $R/load.json > $R/load.log 2>&1 &
LOAD=$!
sleep 8
bad=0; c=0
for c in $(seq 1 $CYCLES); do
  L=$(leader)
  # Three victims: with the leader on even cycles, three followers on odd ones.
  V=""
  if [ $((c % 2)) = 0 ] && [ "$L" != 0 ]; then V="$L"; fi
  for i in $(shuf -i 1-$N); do
    [ "$(echo $V | wc -w)" -ge 3 ] && break
    case " $V " in *" $i "*) ;; *) [ "$i" != "$L" ] || [ $((c % 2)) = 0 ] && V="$V $i" ;; esac
  done
  PIDS=""; for i in $V; do PIDS="$PIDS $(pidof_n $i)"; done
  kill -9 $PIDS 2>/dev/null
  sleep 1.$((c % 9))
  for i in $V; do start $i; done
  for i in $V; do
    up $i || { say "cycle $c: n$i did NOT serve again (victims:$V)"; grep -E "FATAL|panicked at" $ROOT/logs/n$i.log | tail -2 | cut -c1-420 | tee -a $R/loop.log; bad=1; }
  done
  [ $bad = 1 ] && break
  [ $((c % 10)) = 0 ] && say "cycle $c: ok; tails overruled so far: $(cat $ROOT/logs/n*.log | grep -c 'truncated a conflicting tail'), first seq lowered: $(cat $ROOT/logs/n*.log | grep -c 'an empty file created for a later seq')"
  sleep 2
done
kill $LOAD 2>/dev/null
say "cycles run: $c of $CYCLES, bad=$bad"
for i in $(seq 1 $N); do say "n$i: FATAL lines $(grep -c FATAL $ROOT/logs/n$i.log), panics $(grep -c 'panicked at' $ROOT/logs/n$i.log), tails overruled $(grep -c 'truncated a conflicting tail' $ROOT/logs/n$i.log), across a sealed file $(grep -c 'truncated a suffix across' $ROOT/logs/n$i.log), first seq lowered $(grep -c 'an empty file created for a later seq' $ROOT/logs/n$i.log)"; done
stopall
echo KILLLOOP_DONE >> $R/loop.log
