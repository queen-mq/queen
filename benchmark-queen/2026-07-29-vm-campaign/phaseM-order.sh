#!/usr/bin/env bash
# phaseM-order.sh — the CORRECTNESS gate for the minimum pop wait.
#
# Throughput runs verify loss / duplication / isolation from bitmaps but cannot
# verify ORDER (several concurrent producers ⇒ producer order is not storage
# order). `-mode order` builds the one shape where storage order is known — a
# single producer pushing sequentially — and then varies exactly the things the
# window touches: the window itself, the number of consumers competing for the
# same partitions, the batch size, and the long-poll deadline.
#
# Runs on the M build only: with W=0 it IS the pre-feature path, so the W=0 rows
# double as the control.
set -uo pipefail
G=/root/queen/benchmark-queen/2026-07-29-vm-campaign
OUT=/root/campaign/M/order
mkdir -p $OUT
GL=$G/goload/goload

bash $G/m-usebin.sh M
bash $G/reset-cell-db.sh >/dev/null 2>&1

run() { # runid queue window consumers popbatch partitions messages extra...
  local id=$1 q=$2 w=$3 c=$4 pb=$5 p=$6 n=$7; shift 7
  $GL -mode order -target proxy -url http://127.0.0.1:6711 \
    -tenants-file /root/campaign/M/tenants.json -tenant 0 \
    -queue "$q" -group orderers -partitions "$p" -messages "$n" \
    -consumers "$c" -pop-batch "$pb" -min-pop-wait "$w" -drain 30 \
    -out $OUT -run-id "$id" \
    -note "TASK M order gate; build=M; W=${w}ms consumers=$c popBatch=$pb partitions=$p" \
    "$@" >"$OUT/$id.stdout" 2>&1
  local rc=$?
  echo "  [$(date -u +%T)] $id W=${w}ms c=$c pb=$pb parts=$p rc=$rc  $(grep -E '^(pushed|pops|missing|VERDICT)' $OUT/$id.stdout | tr '\n' ' ')"
}

echo "=== ordering / loss / duplication / ack, window OFF vs ON  $(date -u +%T)"
run ord-w0-c3    oq-w0-c3     0 3  50 4 3000
run ord-w25-c3   oq-w25-c3   25 3  50 4 3000
run ord-w50-c3   oq-w50-c3   50 3  50 4 3000
run ord-w100-c3  oq-w100-c3 100 3  50 4 3000

echo "=== single consumer, single partition — the strictest order shape"
run ord-w0-c1p1  oq-w0-c1p1   0 1  50 1 2000
run ord-w100-c1p1 oq-w100-c1p1 100 1 50 1 2000

echo "=== high contention: 8 consumers on 4 partitions with the window on"
run ord-w50-c8   oq-w50-c8   50 8  50 4 3000

echo "=== guards: batch=1 is always full (must never wait), and a short deadline caps the window"
run ord-w100-pb1 oq-w100-pb1 100 3   1 4 1500
run ord-w100-to20 oq-w100-to20 100 3 50 4 1500 -pop-timeout 20

echo "=== phaseM-order done $(date -u +%T)"
