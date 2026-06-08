#!/usr/bin/env bash
# pgmq sustained soak — pooled via PgBouncer, single unordered queue (MODE=plain).
# run.sh's 1s sampler (metrics.csv) gives the push/pop time series (n_tup_ins / n_tup_del
# rates) + dead-tuple/bloat over the whole run; we add a PG container CPU/mem sampler.
set -uo pipefail
cd "$(dirname "$0")"
NAME="${NAME:-plain-pooled-soak45}"
echo ">>> soak: $NAME  conns=${CONNECTIONS:-200} dur=${DURATION:-2700}s port=${PGPORT:-6432} send=${MSGS_PER_PUSH:-10} read=${READ_QTY:-100}"
( while true; do
    printf '%s,' "$(date +%s)"
    docker stats --no-stream --format '{{.CPUPerc}},{{.MemUsage}}' pgmq-postgres 2>/dev/null
    sleep 5
  done ) > "results/${NAME}.dockerstats.csv" 2>/dev/null &
DS=$!
MODE=plain QUEUE=bench MSGS_PER_PUSH="${MSGS_PER_PUSH:-10}" READ_QTY="${READ_QTY:-100}" \
  CONNECTIONS="${CONNECTIONS:-200}" DURATION="${DURATION:-2700}" PGPORT="${PGPORT:-6432}" \
  bash run.sh "$NAME"
kill "$DS" 2>/dev/null || true
echo ">>> soak complete: results/${NAME}/"
