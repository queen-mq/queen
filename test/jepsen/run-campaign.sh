#!/usr/bin/env bash
# run-matrix.sh for a long unattended campaign: the same summary.txt lines,
# plus a wall-clock cap per test (LIMIT seconds, default 2400: past it the
# test's whole process group is killed and the line says CRASHED(rc=124)),
# a look at every node's log for a refusal to start or a panic (the line
# then says REFUSED(...), whatever the history's verdict),
# and one line per test in $RUNS/PROGRESS.txt ($RUNS/progress.py).
#
#   RUNS=/root/runs-p8 BIN=/root/bin/queen-X ./run-campaign.sh matrix.txt
#
# NODES names the test nodes (default n1 to n5). A matrix whose tests take
# --standby-nodes needs more of them: NODES=n1,n2,n3,n4,n5,n6.
#
# Writes its pid to $RUNS/campaign.pid while it runs.
set -u
cd "$(dirname "$0")"
RUNS=${RUNS:-/root/runs}
BIN=${BIN:?set BIN to the queen binary}
LIMIT=${LIMIT:-2400}
NODES=${NODES:-n1,n2,n3,n4,n5}
COMMON="--nodes $NODES --username root --ssh-private-key /root/.ssh/id_ed25519 --bin $BIN --concurrency 2n"
mkdir -p "$RUNS"
echo $$ > "$RUNS/campaign.pid"
echo "$(date -u +%FT%TZ) campaign $1 on $BIN" >> "$RUNS/PROGRESS.txt"
while read -r label args; do
  case "$label" in ''|'#'*) continue ;; esac
  log="$RUNS/$label.log"
  echo "$(date -u +%FT%TZ) start $label: $args" >> "$RUNS/summary.txt"
  # shellcheck disable=SC2086
  setsid lein run test $COMMON $args > "$log" 2>&1 < /dev/null &
  pid=$!
  waited=0
  while kill -0 "$pid" 2>/dev/null && [ "$waited" -lt "$LIMIT" ]; do
    sleep 5; waited=$((waited + 5))
  done
  if kill -0 "$pid" 2>/dev/null; then
    kill -TERM -- "-$pid" 2>/dev/null; sleep 30; kill -KILL -- "-$pid" 2>/dev/null
    wait "$pid" 2>/dev/null; rc=124
  else
    wait "$pid"; rc=$?
  fi
  store=$(sed -n 's#.*Wrote \(/.*/store/.*\)/results.edn.*#\1#p' "$log" | tail -1)
  if grep -q "Everything looks good" "$log"; then v=valid
  elif grep -q "Analysis invalid" "$log"; then v=INVALID
  elif grep -q "Errors occurred during analysis" "$log"; then v=UNKNOWN
  else v="CRASHED(rc=$rc)"; fi
  # A valid history does not say that every node stayed up: one that refused to
  # start, or panicked, is the broker's bug even when the others carried the
  # test (2026-10-09: two of five nodes refused in a test judged valid). The
  # corruption tests damage a node's files on purpose: there a refusal is the
  # right answer.
  case " $args " in
    *" --nemesis corrupt "*) ;;
    *)
      if [ -n "$store" ]; then
        bad=$(grep -l -E "FATAL|panicked at" "$store"/*/queen.log 2>/dev/null | wc -l | tr -d ' ')
        [ "$bad" -gt 0 ] && v="REFUSED($bad node logs with FATAL or a panic; history $v)"
      fi ;;
  esac
  echo "$(date -u +%FT%TZ) done  $label: $v store=$store log=$log" >> "$RUNS/summary.txt"
  python3 "$RUNS/progress.py" "$label" "$RUNS" >> "$RUNS/PROGRESS.txt" 2>&1 || true
done < "${1:?matrix file}"
echo "$(date -u +%FT%TZ) matrix $1 finished" >> "$RUNS/summary.txt"
echo "$(date -u +%FT%TZ) campaign $1 finished" >> "$RUNS/PROGRESS.txt"
rm -f "$RUNS/campaign.pid"
