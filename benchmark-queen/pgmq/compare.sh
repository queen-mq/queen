#!/usr/bin/env bash
# Mac head-to-head: run pgmq and Queen back-to-back, same params, same Docker
# budget, then print one side-by-side table. Stacks run SEQUENTIALLY so neither
# competes for host CPU with the other.
#
#   DURATION=120 CONNECTIONS=100 ./compare.sh
set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

DURATION="${DURATION:-120}"
CONNECTIONS="${CONNECTIONS:-100}"
MSGS_PER_PUSH="${MSGS_PER_PUSH:-10}"
READ_QTY="${READ_QTY:-100}"
NUM_PARTITIONS="${NUM_PARTITIONS:-1000}"
STAMP="$(date +%Y%m%d-%H%M%S)"
PGMQ_RUN="cmp-${STAMP}-pgmq"
QUEEN_RUN="cmp-${STAMP}-queen"

export NVM_DIR="${NVM_DIR:-$HOME/.nvm}"
load_node() { [ -s "$NVM_DIR/nvm.sh" ] && . "$NVM_DIR/nvm.sh" && nvm use 22 >/dev/null 2>&1; return 0; }

echo "##############################################################"
echo "# COMPARE  duration=${DURATION}s  conns/role=${CONNECTIONS}  msgs/push=${MSGS_PER_PUSH}"
echo "#          popBatch=${READ_QTY}  partitions=${NUM_PARTITIONS}"
echo "##############################################################"

echo; echo "### Tearing down any prior stacks…"
docker compose -f docker-compose.yml down -v >/dev/null 2>&1 || true
docker compose -f queen-compose.yml down -v >/dev/null 2>&1 || true

echo; echo "### [1/2] pgmq run…"
MODE=fifo CONNECTIONS="$CONNECTIONS" MSGS_PER_PUSH="$MSGS_PER_PUSH" READ_QTY="$READ_QTY" \
  NUM_PARTITIONS="$NUM_PARTITIONS" DURATION="$DURATION" ./run.sh "$PGMQ_RUN"
echo "### tearing down pgmq stack…"
docker compose -f docker-compose.yml down -v >/dev/null 2>&1 || true

echo; echo "### [2/2] Queen run…"
CONNECTIONS="$CONNECTIONS" MSGS_PER_PUSH="$MSGS_PER_PUSH" READ_QTY="$READ_QTY" \
  NUM_PARTITIONS="$NUM_PARTITIONS" DURATION="$DURATION" ./run-queen.sh "$QUEEN_RUN"
echo "### tearing down Queen stack…"
docker compose -f queen-compose.yml down -v >/dev/null 2>&1 || true

echo; echo "### Combined result:"
( load_node; node combine.mjs "results/$PGMQ_RUN" "results/$QUEEN_RUN" "$DURATION" ) | tee "results/cmp-${STAMP}.txt"
echo "Saved: results/cmp-${STAMP}.txt"
