#!/usr/bin/env bash
# Fan-out head-to-head: deliver every message to GROUPS consumer groups.
#   Queen : 1 queue + GROUPS native consumer groups (1 physical copy + GROUPS cursors).
#   pgmq  : GROUPS queues bound to topic '#' (GROUPS physical copies + per-copy delete churn).
# Same Docker budget, sequential, 120s. Shows the write-amplification of simulating
# consumer groups on a queue that doesn't have them.
#
#   GROUPS=10 DURATION=120 ./compare-fanout.sh
set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; cd "$SCRIPT_DIR"

GROUPS="${GROUPS:-10}"
DURATION="${DURATION:-120}"
PROD_CONNS="${PROD_CONNS:-100}"
CONS_CONNS_PER_GROUP="${CONS_CONNS_PER_GROUP:-10}"
MSGS_PER_PUSH="${MSGS_PER_PUSH:-10}"
READ_QTY="${READ_QTY:-100}"
NUM_PARTITIONS="${NUM_PARTITIONS:-1000}"
STAMP="$(date +%Y%m%d-%H%M%S)"
PGMQ_DIR="results/fanout-$STAMP-pgmq"
QUEEN_DIR="results/fanout-$STAMP-queen"
mkdir -p "$PGMQ_DIR" "$QUEEN_DIR"
export NVM_DIR="${NVM_DIR:-$HOME/.nvm}"
load_node() { [ -s "$NVM_DIR/nvm.sh" ] && . "$NVM_DIR/nvm.sh" && nvm use 24 >/dev/null 2>&1; return 0; }
last_idx=$((GROUPS - 1))
# PGMQ_ORDERED=1 -> faithful Smartchat: per-partition FIFO inside each group queue
# (fanoutfifo producer + read_grouped_head consumers + FIFO index). Otherwise
# unordered competing-consumer reads (RabbitMQ-style, no ordering guarantee).
if [ -n "${PGMQ_ORDERED:-}" ]; then PGMQ_PROD_MODE=fanoutfifo; PGMQ_CONS_MODE=fifo; ORDERED_NOTE="ORDERED per-partition (read_grouped_head)"; else PGMQ_PROD_MODE=fanout; PGMQ_CONS_MODE=plain; ORDERED_NOTE="UNORDERED (plain read)"; fi

echo "##### FAN-OUT  groups=$GROUPS duration=${DURATION}s prodConns=$PROD_CONNS consConns/grp=$CONS_CONNS_PER_GROUP"
docker compose -f queen-compose.yml down -v >/dev/null 2>&1 || true
docker compose down -v >/dev/null 2>&1 || true

############################## PGMQ ##############################
echo; echo "### [1/2] pgmq fan-out ($GROUPS queues, topic '#')…"
docker compose up -d
echo -n "   waiting pg/pgbouncer…"
for i in $(seq 1 60); do docker exec pgmq-postgres pg_isready -U postgres >/dev/null 2>&1 && break; sleep 1; done
for i in $(seq 1 30); do nc -z localhost 6432 >/dev/null 2>&1 && break; sleep 1; done; echo " ok"
[ -d node_modules/pg ] || ( load_node; npm install --no-fund --no-audit >/dev/null 2>&1 )

SETUP="CREATE EXTENSION IF NOT EXISTS pgmq CASCADE;"
for i in $(seq 0 $last_idx); do
  SETUP="$SETUP DO \$\$ BEGIN PERFORM pgmq.drop_queue('g$i'); EXCEPTION WHEN OTHERS THEN NULL; END \$\$; SELECT pgmq.create('g$i'); SELECT pgmq.bind_topic('#','g$i');"
  [ -n "${PGMQ_ORDERED:-}" ] && SETUP="$SETUP SELECT pgmq.create_fifo_index('g$i');"
done
docker exec pgmq-postgres psql -U postgres -d postgres -c "$SETUP" >/dev/null 2>&1
echo "   created + bound $GROUPS queues  [$ORDERED_NOTE]"

bash sample-metrics.sh "$PGMQ_DIR/metrics.csv" 1 pgmq-postgres pgmq q_g0 & SP=$!
cleanup_p() { kill "$SP" >/dev/null 2>&1; }
trap cleanup_p EXIT

CPIDS=()
for i in $(seq 0 $last_idx); do
  ( load_node; ROLE=consumer MODE=$PGMQ_CONS_MODE QUEUE=g$i CONNECTIONS=$CONS_CONNS_PER_GROUP READ_QTY=$READ_QTY \
    DURATION=$DURATION VT=60 PGHOST=localhost PGPORT=6432 node pgmq-bench.js ) \
    >"$PGMQ_DIR/consumer-g$i.json" 2>"$PGMQ_DIR/consumer-g$i.err" &
  CPIDS+=($!)
done
sleep 2
( load_node; ROLE=producer MODE=$PGMQ_PROD_MODE ROUTING_KEY=evt CONNECTIONS=$PROD_CONNS MSGS_PER_PUSH=$MSGS_PER_PUSH \
  NUM_PARTITIONS=$NUM_PARTITIONS DURATION=$DURATION PGHOST=localhost PGPORT=6432 node pgmq-bench.js ) \
  >"$PGMQ_DIR/producer.json" 2>"$PGMQ_DIR/producer.err" &
PP=$!
echo "   running ${DURATION}s…"
wait "$PP"; for p in "${CPIDS[@]}"; do wait "$p"; done; sleep 1

docker exec pgmq-postgres psql -U postgres -d postgres -t -A -F',' -c \
  "SELECT COALESCE(sum(n_tup_ins),0),COALESCE(sum(n_tup_upd),0),COALESCE(sum(n_tup_del),0),COALESCE(sum(n_dead_tup),0),COALESCE(sum(pg_total_relation_size(relid)),0) FROM pg_stat_user_tables WHERE schemaname='pgmq' AND relname ~ '^q_g[0-9]+\$'" \
  > "$PGMQ_DIR/agg.csv" 2>/dev/null
cleanup_p; trap - EXIT
echo "   pgmq agg (ins,upd,del,dead,bytes): $(cat "$PGMQ_DIR/agg.csv")"
docker compose down -v >/dev/null 2>&1 || true

############################## QUEEN ##############################
echo; echo "### [2/2] Queen fan-out (1 queue, $GROUPS consumer groups)…"
docker compose -f queen-compose.yml up -d
echo -n "   waiting broker…"
for i in $(seq 1 90); do curl -sf http://localhost:6633/api/v1/status >/dev/null 2>&1 && break; sleep 1; done; echo " ok"
# create the queue up-front so consumer groups can subscribe immediately
curl -sf -X POST http://localhost:6633/api/v1/configure -H 'Content-Type: application/json' \
  -d '{"queue":"fanout-bench","options":{"leaseTime":60,"retryLimit":3,"retentionEnabled":true,"retentionSeconds":7200,"completedRetentionSeconds":1800}}' >/dev/null 2>&1 || true

bash sample-metrics.sh "$QUEEN_DIR/metrics.csv" 1 queen-pg queen messages & SQ=$!
cleanup_q() { kill "$SQ" >/dev/null 2>&1; }
trap cleanup_q EXIT

QCPIDS=()
for i in $(seq 0 $last_idx); do
  ( load_node; SERVER_URL=http://localhost:6633 QUEUE_NAMES=fanout-bench CONSUMER_GROUP=cg$i \
    NUM_WORKERS=1 CONNECTIONS_PER_WORKER=$CONS_CONNS_PER_GROUP CONSUMER_BATCH=$READ_QTY DURATION=$DURATION \
    node queen-consumer.js ) >"$QUEEN_DIR/consumer-cg$i.json" 2>"$QUEEN_DIR/consumer-cg$i.err" &
  QCPIDS+=($!)
done
sleep 2
( load_node; SERVER_URL=http://localhost:6633 QUEUE_NAMES=fanout-bench NUM_WORKERS=2 \
  CONNECTIONS_PER_WORKER=$((PROD_CONNS / 2)) MAX_PARTITION=$NUM_PARTITIONS MSGS_PER_PUSH=$MSGS_PER_PUSH DURATION=$DURATION \
  node queen-producer.js ) >"$QUEEN_DIR/producer.json" 2>"$QUEEN_DIR/producer.err" &
QPP=$!
echo "   running ${DURATION}s…"
wait "$QPP"; for p in "${QCPIDS[@]}"; do wait "$p"; done; sleep 1

curl -sf http://localhost:6633/api/v1/status >"$QUEEN_DIR/status.json" 2>/dev/null || true
docker exec queen-pg psql -U postgres -d postgres -t -A -c "SELECT COALESCE(pg_total_relation_size('queen.messages'),0)" >"$QUEEN_DIR/size.txt" 2>/dev/null || true
cleanup_q; trap - EXIT
docker compose -f queen-compose.yml down -v >/dev/null 2>&1 || true

############################## COMBINE ##############################
echo; echo "### Fan-out result:"
( load_node; node combine-fanout.mjs "$PGMQ_DIR" "$QUEEN_DIR" "$DURATION" "$GROUPS" ) | tee "results/fanout-$STAMP.txt"
echo "Saved: results/fanout-$STAMP.txt"
