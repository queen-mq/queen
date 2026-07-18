#!/usr/bin/env bash
# run-load.sh (runs on the LOADER VM) — drives goload at the broker over the
# private network, with an mpstat CPU monitor so we can see if the loader itself
# saturates. Prints goload report + loader CPU summary.
set -uo pipefail
BROKER="${BROKER:-10.114.0.2:6682}"; Q="${QUEUE:-segbench}"; DUR="${DUR:-360}"
PROD="${PROD:-800}"; CONS="${CONS:-500}"
PUSHB="${PUSHB:-100}"; POPB="${POPB:-500}"; POPP="${POPP:-10}"; PARTS="${PARTS:-100}"

echo "[loader] goload -> $BROKER queue=$Q prod=$PROD cons=$CONS parts=$PARTS dur=${DUR}s"
mpstat 30 $((DUR/30 + 2)) > /tmp/loadmon.log 2>&1 &
MP=$!
/root/goload -url "http://$BROKER" -queue "$Q" -partitions "$PARTS" \
  -producers "$PROD" -consumers "$CONS" -push-batch "$PUSHB" -pop-batch "$POPB" \
  -pop-partitions "$POPP" -pop-wait=true -pop-timeout=2000 -payload 256 \
  -duration "$DUR" -report 60 2>&1 | tee /tmp/goload.log
kill "$MP" >/dev/null 2>&1 || true
echo "=== loader CPU (mpstat, last 6 samples: %usr %sys ... %idle is last col) ==="
grep -E 'all' /tmp/loadmon.log | tail -6
echo "=== loader loadavg ==="; uptime
