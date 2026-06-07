#!/bin/bash
# Add an OPTIONAL QUEEN_PUSH_MAX_HOLD_MS passthrough to run_test_v3 so we can
# A/B the push fusion-hold (set PUSH_HOLD_MS env when invoking; unset = pushser default).
# Idempotent.
set -e
cd /root/bench-runs
if ! grep -q 'PUSH_HOLD_MS:+' run_test_v3.sh; then
  sed -i 's|-e QUEEN_POP_MAX_CONCURRENT=16 \\|-e QUEEN_POP_MAX_CONCURRENT=16 ${PUSH_HOLD_MS:+-e QUEEN_PUSH_MAX_HOLD_MS=$PUSH_HOLD_MS} \\|' run_test_v3.sh
fi
echo "=== passthrough line ==="
grep -n 'PUSH_HOLD_MS' run_test_v3.sh
