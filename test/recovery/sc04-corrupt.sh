#!/usr/bin/env bash
# S4 — one node's files are damaged (bit rot, a torn or truncated file, a file
# overwritten or deleted). For each case: stop a follower, damage one file,
# start it, watch what it does (refuse to boot / boot / exit later), check it
# never SERVES damaged data, then repair it with the replace procedure (or the
# cheaper remedy when the damage is tolerated) and verify everything.
#   sc04-corrupt.sh [case ...]     (default: all cases)
set -uo pipefail
. "$(dirname "$0")/lib.sh"
cd "$REC"; rm -rf state
export QR_ENV="QUEEN_RAFT_SEGMENT_BYTES=262144"   # 256 KiB queue-log files: several sealed ones
D=/var/lib/queen/raft
FLIP='flip() { perl -e '"'"'my ($f,$at)=@ARGV; my $sz=-s $f; my $off=defined $at ? $at : int($sz/2); open(my $h,"+<",$f) or die "$f: $!"; seek($h,$off,0); read($h,my $b,16); $b ^= ("\xa5" x length $b); seek($h,$off,0); print $h $b; close $h; print "flipped 16 bytes at $off of $sz: $f\n"'"'"' "$@"; }'
BUSY='busy() { ls -S qlog/q*/r*.qlog 2>/dev/null | head -1 | xargs dirname; }'
declare -a ALL=(qlog-flip-sealed qlog-flip-active qlog-trunc-active qlog-delete-sealed store-flip store-trunc store-delete
                state-garbage state-delete membership-garbage committed-garbage localdb-garbage dashdb-garbage
                shards-delete seg-garbage snapshot-marker-garbage)
damage() {
  case "$1" in
    qlog-flip-sealed)   echo "$FLIP; $BUSY; d=\$(busy); f=\$(ls \$d/r*.qlog | sort | head -1); flip \$f" ;;
    qlog-flip-active)   echo "$FLIP; $BUSY; d=\$(busy); f=\$(ls \$d/r*.qlog | sort | tail -1); flip \$f 300; flip \$f 3000" ;;
    qlog-trunc-active)  echo "$BUSY; d=\$(busy); f=\$(ls \$d/r*.qlog | sort | tail -1); s=\$(stat -c %s \$f); truncate -s \$((s/3)) \$f; echo truncated \$f \$s to \$((s/3))" ;;
    qlog-delete-sealed) echo "$BUSY; d=\$(busy); f=\$(ls \$d/r*.qlog | sort | head -1); rm -v \$f; ls \$d" ;;
    store-flip)         echo "$FLIP; flip store/data.mdb" ;;
    store-trunc)        echo "s=\$(stat -c %s store/data.mdb); truncate -s \$((s/2)) store/data.mdb; echo truncated data.mdb \$s to \$((s/2))" ;;
    store-delete)       echo "rm -v store/data.mdb store/lock.mdb" ;;
    state-garbage)      echo "printf 'not json at all' > raft/state.json; cat raft/state.json; echo" ;;
    state-delete)       echo "rm -v raft/state.json" ;;
    membership-garbage) echo "printf '{\"broken\":' > raft/membership.json; echo damaged membership.json" ;;
    committed-garbage)  echo "printf 'garbage' > raft/committed.json; echo damaged committed.json" ;;
    localdb-garbage)    echo "$FLIP; ls -la local.db; flip local.db 0; flip local.db" ;;
    dashdb-garbage)     echo "$FLIP; ls -la dash.db; flip dash.db 0; flip dash.db" ;;
    shards-delete)      echo "cat qlog/SHARDS; echo; rm -v qlog/SHARDS" ;;
    seg-garbage)        echo "head -c 4096 /dev/urandom > seg/b000/f0000000000.seg; echo garbage into seg/b000/f0000000000.seg" ;;
    snapshot-marker-garbage) echo "printf 'garbage' > snapshot.pending; echo wrote a garbage snapshot.pending" ;;
  esac
}

one_case() {
  local c="$1"
  hr "S4 case $c"
  wait_all_healthy 3 120 >/dev/null || { say "cluster not healthy before the case"; hs; }
  $WL write --tag "$(echo "$c" | tr -cd 'a-z' | cut -c1-6)$RANDOM" --queues 2 --msgs 400 --kv 10 >/dev/null
  local L V OK
  L=$(leader); V=$(( (L + 1) % 3 )); OK=$(( (V + 1) % 3 )); [ "$OK" = "$V" ] && OK=$L
  say "victim follower index $V (node $((V+1))); leader index $L"
  $QR stop "$V" >/dev/null
  say "damage: $($QR sh "$V" "cd $D && $(damage "$c")" 2>&1 | tr '\n' ' ' | cut -c1-300)"
  local before; before=$(docker inspect -f '{{.RestartCount}}' "queen-mq-v2-$V")
  docker start "queen-mq-v2-$V" >/dev/null
  local outcome="" t
  for t in $(seq 1 30); do
    sleep 1
    local st rc; st=$(docker inspect -f '{{.State.Status}}' "queen-mq-v2-$V"); rc=$(docker inspect -f '{{.RestartCount}}' "queen-mq-v2-$V")
    if [ "$rc" -gt "$before" ] || [ "$st" != running ]; then outcome="REFUSED(exit $(docker inspect -f '{{.State.ExitCode}}' "queen-mq-v2-$V"), restarts +$((rc - before)))"; break; fi
    [ "$(status "$V")" = healthy ] && { outcome="BOOTED"; break; }
  done
  [ -z "$outcome" ] && outcome="NOT-HEALTHY-30s($(status "$V"))"
  if [ "$outcome" = BOOTED ]; then
    sleep 12
    local rc2; rc2=$(docker inspect -f '{{.RestartCount}}' "queen-mq-v2-$V")
    [ "$rc2" -gt "$before" ] && outcome="BOOTED-THEN-EXITED"
  fi
  say "OUTCOME $c: $outcome"
  say "node log (last fatal/error):"; docker logs "queen-mq-v2-$V" 2>&1 | grep -E 'FATAL|"level":"ERROR"|panicked|corrupt|refus' | grep -v 'node registry could not be read' | tail -3 | cut -c1-500
  if [ "$outcome" = BOOTED ]; then
    say "it booted: does it serve correct data on its own?"
    wait_caught_up "$V" 60
    $WL verify --nodes "$V" --allow-loss | head -2 | cut -c1-600
  fi
  echo "$c|$outcome" >> runs/s4-outcomes.txt
  if [ "${REMEDY:-replace}" = replace ] || [ "$outcome" != BOOTED ]; then
    hr "remedy for $c: replace node $((V+1))"
    replace_node "$V" "$OK"
  fi
  $WL verify | cut -c1-600; hs
}

$QR down >/dev/null; $QR up 3 >/dev/null; wait_all_healthy 3 120
$WL write --tag base --msgs 500 >/dev/null; $WL consume --group g1 --max 60
: > runs/s4-outcomes.txt
cases=("$@"); [ ${#cases[@]} -eq 0 ] && cases=("${ALL[@]}")
for c in "${cases[@]}"; do one_case "$c"; done
hr "S4 outcomes"; cat runs/s4-outcomes.txt
