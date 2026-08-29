#!/usr/bin/env bash
# JS integration suite: human + stream buckets in one process.
set -eu

export QUEEN_SERVER_URL="$QUEEN_HTTP_URL"
# The suite reads PG_DB (not PG_DATABASE) for its direct cleanup pool.
export PG_HOST="$QUEEN_PG_HOST" PG_PORT="$QUEEN_PG_PORT" \
       PG_DB="$QUEEN_PG_DB" PG_USER="$QUEEN_PG_USER" PG_PASSWORD="$QUEEN_PG_PASSWORD"

/usr/local/bin/wait-for-broker

cd /suite
# Broker-free unit suites first, including the proxy 429/Retry-After contract
# (test-v2/http-unit). They live behind `npm test`, which the harness does not
# use, so they ran nowhere.
#
# The kv-unit suites are here for the same reason: they assert the EXACT JSON
# body of every kv and timer operation against a scripted plan server, which is
# the only check that catches a wrong wire shape without a broker. They used to
# carry a second justification -- that the kv/timer integration tests SKIPPED
# themselves on a cell with the features off, leaving these three files as the
# only guard. There is no such cell any more: `QUEEN_KV_ENABLED` and
# `QUEEN_TIMERS_ENABLED` are gone and every broker has both surfaces, so the
# integration suites below run unconditionally and a 404 from them is a bug.
#
# conflation-unit is here for the first of those reasons: it pins the pop query
# string emitted by BOTH param builders (pop() and the consume loop build them
# in separate code), plus the degrade-loudly raise when a broker does not echo
# `"conflation":true` (PLAN_CONFLATION §4). Neither needs a broker, and the
# degrade case in particular can only be produced by a scripted server -- a live
# broker in this repo always applies the flag.
node --test test-v2/http-unit/retry429.test.js \
             test-v2/kv-unit/kvWire.test.js \
             test-v2/kv-unit/timerWire.test.js \
             test-v2/kv-unit/txnWire.test.js \
             test-v2/conflation-unit/conflationWire.test.js \
             test-v2/runner-unit/fatalExit.test.js

# No argument = human + stream in one process; run.js calls process.exit(fail?1:0).
exec node test-v2/run.js
