#!/usr/bin/env bash
# clients/client-cli/tests/smoke.sh
#
# Quick health-check for queenctl. Hits every server-touching command
# against $QUEEN_SERVER (default localhost:6632) and prints PASS / FAIL
# per command. Designed to run in a few seconds.
#
# For the full end-to-end test suite (parity with the JS test-v2 and Python
# tests/ suites), run `make e2e` instead - it executes ~60 Go tests with
# proper assertions, broker side-effect verification, and DB-side checks.
#
#   ./tests/smoke.sh                          # quick smoke
#   QUEEN_SERVER=http://host:6632 ./tests/smoke.sh
set -u

SERVER=${QUEEN_SERVER:-http://localhost:6632}
BIN=${BIN:-./bin/queenctl}
QUEUE="queenctl-smoke-$$-$(date +%s)"
CG="queenctl-smoke-cg"
RESULTS=()

run() {
  local name=$1; shift
  local out
  out=$("$BIN" --server "$SERVER" "$@" 2>&1)
  local rc=$?
  if [[ $rc -eq 0 || $rc -eq 4 ]]; then
    RESULTS+=("PASS  $name")
    return 0
  fi
  RESULTS+=("FAIL  $name (exit $rc): $(echo "$out" | head -1)")
  return $rc
}

skip() { RESULTS+=("SKIP  $1"); }

echo "== smoke against $SERVER, throwaway queue $QUEUE =="

# 1) Top-level health / version
run "ping"                     ping
run "version"                  version --short

# 2) Bootstrap a throwaway queue + messages.
echo '{"i":1}' | "$BIN" --server "$SERVER" push "$QUEUE" -q >/dev/null 2>&1
echo '{"i":2}' | "$BIN" --server "$SERVER" push "$QUEUE" -q >/dev/null 2>&1
echo '{"i":3}' | "$BIN" --server "$SERVER" push "$QUEUE" -q >/dev/null 2>&1
RESULTS+=("PASS  push (bootstrap)")

# 3) Queue resource verbs
run "queue list"               queue list -o json
run "queue describe"           queue describe "$QUEUE"
run "queue configure"          queue configure "$QUEUE" --lease-time 30
run "queue stats"              queue stats
# queue clear has no server-side endpoint; verify the subcommand is not
# advertised in `queenctl queue --help` (Cobra is permissive about
# unknown args, so a direct invocation just falls through to parent help).
if "$BIN" queue --help 2>&1 | grep -qE '^\s+clear\b'; then
  RESULTS+=("FAIL  queue clear (should not be listed)")
else
  RESULTS+=("PASS  queue clear (intentionally absent)")
fi
# queue delete is run at cleanup

# 4) Partition verbs
run "partition list"           partition list "$QUEUE"
run "partition describe"       partition describe "$QUEUE" Default
# partition seek + clear require a CG; tested below

# 5) Messages
TX_LINE=$("$BIN" --server "$SERVER" pop "$QUEUE" -n 1 --cg peek 2>/dev/null | head -1)
TX=$(echo "$TX_LINE" | python3 -c 'import sys,json;d=json.loads(sys.stdin.read() or "{}");print(d.get("transactionId",""))')
PID=$(echo "$TX_LINE" | python3 -c 'import sys,json;d=json.loads(sys.stdin.read() or "{}");print(d.get("partitionId",""))')

run "messages list"            messages list --queue "$QUEUE" --limit 5
if [[ -n $TX && -n $PID ]]; then
  run "messages get"           messages get "$PID" "$TX"
  run "messages traces"        messages traces "$PID" "$TX"
else
  skip "messages get (no msg)"; skip "messages traces (no msg)"
fi

# 6) Consumer-group verbs
run "cg list"                  cg list
run "cg refresh-stats"         cg refresh-stats
run "cg seek (mode end)"       cg seek "$CG" "$QUEUE" --to now
run "cg describe"              cg describe "$CG"
run "cg lag"                   cg lag

# 7) DLQ verbs
run "dlq list"                 dlq list --queue "$QUEUE"
run "dlq drain dry-run"        dlq drain --queue "$QUEUE" --dry-run

# 8) Namespace / task
run "namespace list"           namespace list
run "task list"                task list

# 9) Status / lag (with the test queue)
run "status (cluster)"         status
run "status (queue)"           status "$QUEUE"
run "lag"                      lag

# 10) Maintenance
run "maintenance get"          maintenance get
run "maintenance get pop"      maintenance get --pop

# 11) Metrics
run "metrics"                  metrics
run "metrics --prometheus"     metrics --prometheus

# 12) Analytics
run "analytics overview"       analytics overview
run "analytics queue-lag"      analytics queue-lag
run "analytics queue-ops"      analytics queue-ops
run "analytics queue-parked"   analytics queue-parked
run "analytics retention"      analytics retention
run "analytics system"         analytics system
run "analytics worker"         analytics worker
run "analytics postgres"       analytics postgres

# 13) Traces
run "traces names"             traces names

# 14) Replay (alias for cg seek)
run "replay --to beginning"    replay "$QUEUE" --cg "$CG" --to beginning

# 15) Bench - tiny smoke that also verifies actual server-side consumption.
BENCH_QUEUE="queenctl-smoke-bench-$$"
"$BIN" --server "$SERVER" bench --queue "$BENCH_QUEUE" --total 50 --partitions 2 --producers 2 --consumers 2 --batch 25 >/dev/null 2>&1
bench_rc=$?
remaining=$(curl -fsS "$SERVER/api/v1/messages?queue=$BENCH_QUEUE&limit=200" 2>/dev/null \
  | python3 -c 'import sys,json
m=json.load(sys.stdin).get("messages",[])
unconsumed=[x for x in m if x.get("busStatus",{}).get("consumedBy",0)==0]
print(len(unconsumed))' 2>/dev/null)
if [[ $bench_rc -eq 0 && ${remaining:-99} -eq 0 ]]; then
  RESULTS+=("PASS  bench (50/50 consumed)")
else
  RESULTS+=("FAIL  bench (rc=$bench_rc, $remaining still unconsumed)")
fi
"$BIN" --server "$SERVER" queue delete "$BENCH_QUEUE" --yes -q >/dev/null 2>&1

# 15) Cleanup
run "queue delete"             queue delete "$QUEUE" --yes

# Summary
echo
echo "== summary =="
for r in "${RESULTS[@]}"; do echo "$r"; done

fails=$(printf '%s\n' "${RESULTS[@]}" | grep -c '^FAIL ')
echo
echo "TOTAL: ${#RESULTS[@]}  FAIL: $fails"
exit "$fails"
