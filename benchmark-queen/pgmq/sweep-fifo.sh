#!/usr/bin/env bash
# pgmq ORDERED (FIFO) sweep — single queue, per-group FIFO via read_grouped_head.
# NUM_PARTITIONS groups = ordered lanes (the analog of Queen partitions). Direct conns.
# Producer tags each message with x-pgmq-group; consumer reads the head of up to
# READ_QTY groups (per-group FIFO) + deletes. Apples-to-apples with Queen's ordering.
set -uo pipefail
cd "$(dirname "$0")"
CLIENTS="${CLIENTS:-25 50 100 200 400 800}"
export MODE=fifo NUM_PARTITIONS="${NUM_PARTITIONS:-1000}" MSGS_PER_PUSH="${MSGS_PER_PUSH:-10}" \
       READ_QTY="${READ_QTY:-100}" DURATION="${DURATION:-60}" PGPORT="${PGPORT:-55432}" QUEUE=bench \
       GROUPED_FN="${GROUPED_FN:-read_grouped_head}"
GTAG=$([ "$GROUPED_FN" = "read_grouped_rr" ] && echo rr || echo head)
echo ">>> pgmq FIFO sweep: clients=[$CLIENTS] groups=$NUM_PARTITIONS fn=$GROUPED_FN send=$MSGS_PER_PUSH read=$READ_QTY dur=${DURATION}s (direct, per-group FIFO)"
for c in $CLIENTS; do
  name="fifo-${GTAG}-g${NUM_PARTITIONS}-c${c}"
  echo; echo "######## FIFO STEP: ${c} clients/role ########"
  ( while true; do printf '%s,' "$(date +%s)"; docker stats --no-stream --format '{{.CPUPerc}},{{.MemUsage}}' pgmq-postgres 2>/dev/null; sleep 3; done ) > "results/${name}.dockerstats.csv" 2>/dev/null &
  DS=$!
  CONNECTIONS="$c" bash run.sh "$name" || echo "!! step c=${c} failed (continuing)"
  kill "$DS" 2>/dev/null || true; wait "$DS" 2>/dev/null || true
  sleep 3
done
echo; echo ">>> FIFO sweep complete. Artifacts in results/fifo-direct-c*/"
