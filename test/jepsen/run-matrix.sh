#!/usr/bin/env bash
# Runs Jepsen tests one after another on the control node, one line of
# `lein run test` arguments per test, and appends one summary line per test to
# $RUNS/summary.txt (valid?, the store directory, the run log).
#
#   ./run-matrix.sh matrix.txt        # each line: <label> <lein run test args...>
#
# Common arguments (nodes, ssh key, binary) come from COMMON; override BIN.
set -u
cd "$(dirname "$0")"
RUNS=${RUNS:-/root/runs}
BIN=${BIN:-/root/bin/queen-9284d06c}
COMMON="--nodes n1,n2,n3,n4,n5 --username root --ssh-private-key /root/.ssh/id_ed25519 --bin $BIN --concurrency 2n"
mkdir -p "$RUNS"
while read -r label args; do
  case "$label" in ''|'#'*) continue ;; esac
  log="$RUNS/$label.log"
  echo "$(date -u +%FT%TZ) start $label: $args" >> "$RUNS/summary.txt"
  # shellcheck disable=SC2086
  lein run test $COMMON $args > "$log" 2>&1 < /dev/null
  rc=$?
  store=$(sed -n 's#.*Wrote \(/.*/store/.*\)/results.edn.*#\1#p' "$log" | tail -1)
  if grep -q "Everything looks good" "$log"; then v=valid
  elif grep -q "Analysis invalid" "$log"; then v=INVALID
  elif grep -q "Errors occurred during analysis" "$log"; then v=UNKNOWN
  else v="CRASHED(rc=$rc)"; fi
  echo "$(date -u +%FT%TZ) done  $label: $v store=$store log=$log" >> "$RUNS/summary.txt"
done < "${1:?matrix file}"
echo "$(date -u +%FT%TZ) matrix $1 finished" >> "$RUNS/summary.txt"
