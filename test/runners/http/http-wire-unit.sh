#!/usr/bin/env bash
# =============================================================================
#  HTTP WIRE UNIT SUITE — no broker, no database, no SDK.
#  PLAN_KV_TIMERS.md §10.2 (the "HTTP (nessun SDK)" row), §10.4, §8.1, §8.3.
#
#  ---------------------------------------------------------------------------
#  WHAT THIS FILE IS FOR.
#
#  There is no SDK for HTTP, so `kv-timers-wire.sh` IS the client: every request
#  body of the kv and timers surface is built there, in one place, and this file
#  pins the EXACT BYTES each builder produces. That is the contract towards the
#  broker, and it is the only thing that catches a wrong wire shape before
#  production, because a live broker cannot:
#
#    * a broker parses the body and answers from the parse, so a rider that
#      travelled inside `operations` on a broker that ALSO reads the top level
#      would come back 200 and green;
#    * a `ttl` sitting beside a `ttlSeconds` is ignored by the stored procedure
#      and invisible to every integration assertion;
#    * a `getPrefix` smuggled into a query string is answered correctly by a
#      broker that reads the body, while the prefix is already in the access log
#      of everything between the client and the database (§5.5).
#
#  Each of those is a green integration run and a broken contract. So the
#  assertions here are made against the recorded request, not against an answer.
#
#  ---------------------------------------------------------------------------
#  THE SCRIPTED PLAN SERVER (plan-server.py).
#
#  It records the request bytes and replies with a canned status and body that
#  the suite sets per case. The canned half is what makes the SECOND contract of
#  §8.3 testable at all: `commit` must RETURN on `{"success":false,
#  "reason":"kv_precondition"}` and RAISE on everything else. A lost precondition
#  is the expected outcome of every legitimate redelivery and the single most
#  frequent outcome of the product; treating it as an error would put it inside
#  the retry policy, the error budget and the dashboards. Against a real broker
#  the 503 and 500 branches of that rule would need a broken database.
#
#  In shell, "raises" means a non-zero return: under `set -e` it aborts the
#  script, and every caller here checks it explicitly. That is the whole mapping,
#  and it is written down so nobody re-derives it as "commit prints an error".
#
#  ---------------------------------------------------------------------------
#  Usage:  test/runners/http/http-wire-unit.sh            # picks a free port
#          PLAN_PORT=18632 test/runners/http/http-wire-unit.sh
#  Needs:  bash, curl, jq, python3. No broker and no Postgres.
# =============================================================================
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
PLAN_PORT="${PLAN_PORT:-18632}"

PASS=0; FAIL=0
say() { printf '%s\n' "$*"; }
ok()  { PASS=$((PASS+1)); say "  ok   - $1"; }
bad() { FAIL=$((FAIL+1)); say "  FAIL - $1"; }
eq()  { if [ "$2" = "$3" ]; then ok "$1"; else bad "$1"; say "         want: $2"; say "         got : $3"; fi; }

# --- the plan server --------------------------------------------------------
PLAN_DIR="$(mktemp -d -t queen-httpunit.XXXXXX)"
export PLAN_DIR
PLAN_PID=""
# `wait` after the kill so bash reaps the job here instead of printing its own
# "Terminated" line into the middle of the suite's output.
cleanup() {
  if [ -n "$PLAN_PID" ]; then kill "$PLAN_PID" 2>/dev/null; wait "$PLAN_PID" 2>/dev/null; fi
  qw_teardown 2>/dev/null
  rm -rf "$PLAN_DIR"
}
trap cleanup EXIT

command -v python3 >/dev/null || { say "!! python3 is required for the plan server"; say "HTTPUNIT: FAIL"; exit 1; }
command -v jq >/dev/null      || { say "!! jq is required"; say "HTTPUNIT: FAIL"; exit 1; }

python3 "$HERE/plan-server.py" "$PLAN_PORT" & PLAN_PID=$!
for _ in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20; do
  curl -s -o /dev/null --max-time 1 "http://127.0.0.1:$PLAN_PORT/ping" && break
  sleep 0.2
done
if ! curl -s -o /dev/null --max-time 2 "http://127.0.0.1:$PLAN_PORT/ping"; then
  say "!! the plan server did not come up on 127.0.0.1:$PLAN_PORT"
  say "HTTPUNIT: FAIL"; exit 1
fi

# THE CLIENT UNDER TEST. Sourced after the plan server is up so that a missing
# file fails here, loudly, instead of half way through the first case.
QUEEN_HTTP_URL="http://127.0.0.1:$PLAN_PORT"
export QUEEN_HTTP_URL
# shellcheck source=./kv-timers-wire.sh
. "$HERE/kv-timers-wire.sh" || { say "!! cannot source kv-timers-wire.sh"; say "HTTPUNIT: FAIL"; exit 1; }

# --- reading what the client actually sent ----------------------------------
plan_reset() { : > "$PLAN_DIR/requests.jsonl"; rm -f "$PLAN_DIR/reply"; }
plan_reply() { printf '%s\n%s' "$1" "$2" > "$PLAN_DIR/reply"; }

# The LAST recorded request, field by field. `-r` on the body so the comparison
# is against the bytes, not against a JSON-escaped rendering of them.
last() { tail -n 1 "$PLAN_DIR/requests.jsonl" 2>/dev/null | jq -r "$1" 2>/dev/null; }

# The assertion this whole file is built around: one call, one recorded request,
# and its method, path and BODY compared byte for byte.
sent() { # label  "METHOD /path"  expected-body
  local label="$1" line="$2" want="$3"
  local got_line got_body
  got_line="$(last '.method + " " + .path')"
  got_body="$(last '.body')"
  if [ "$got_line" != "$line" ]; then
    bad "$label -- request line"; say "         want: $line"; say "         got : $got_line"; return
  fi
  eq "$label" "$want" "$got_body"
}

nreq() { wc -l < "$PLAN_DIR/requests.jsonl" | tr -d ' '; }

say "== the plan server is up on 127.0.0.1:$PLAN_PORT, PLAN_DIR=$PLAN_DIR =="

# =============================================================================
say ""
say "== 1. the seven KV operations, byte for byte (§5, §6.1) =="
# =============================================================================
plan_reset

qw_req POST /api/v1/kv "$(qw_kv_batch "$(qw_kv_get app 'order/9f1')")"
sent "get" 'POST /api/v1/kv' \
  '[{"op":"get","ns":"app","key":"order/9f1"}]'

qw_req POST /api/v1/kv "$(qw_kv_batch "$(qw_kv_get_many app a b c)")"
sent "getMany carries its keys as an array, in the order given" 'POST /api/v1/kv' \
  '[{"op":"getMany","ns":"app","keys":["a","b","c"]}]'

qw_req POST /api/v1/kv "$(qw_kv_batch "$(qw_kv_get_prefix app 'order:' 50)")"
sent "getPrefix, with its clamped limit" 'POST /api/v1/kv' \
  '[{"op":"getPrefix","ns":"app","prefix":"order:","limit":50}]'

qw_req POST /api/v1/kv "$(qw_kv_batch "$(qw_kv_get_prefix app 'order:' 50 'order:9f1' true)")"
sent "getPrefix with the exclusive keyset cursor and keysOnly" 'POST /api/v1/kv' \
  '[{"op":"getPrefix","ns":"app","prefix":"order:","limit":50,"after":"order:9f1","keysOnly":true}]'

qw_req POST /api/v1/kv "$(qw_kv_batch "$(qw_kv_put app k '{"x":1}' 600)")"
sent "put with ttlSeconds" 'POST /api/v1/kv' \
  '[{"op":"put","ns":"app","key":"k","value":{"x":1},"ttlSeconds":600}]'

qw_req POST /api/v1/kv "$(qw_kv_batch "$(qw_kv_put app k '{"x":1}' forever)")"
sent "put with forever, the OTHER half of the exactly-one-expiry rule (§5.1)" 'POST /api/v1/kv' \
  '[{"op":"put","ns":"app","key":"k","value":{"x":1},"forever":true}]'

qw_req POST /api/v1/kv "$(qw_kv_batch "$(qw_kv_put app k '{"x":1}' 600 7)")"
sent "put with expect: a conditional write goes through the BODY, never If-Match (§8.1)" 'POST /api/v1/kv' \
  '[{"op":"put","ns":"app","key":"k","value":{"x":1},"ttlSeconds":600,"expect":7}]'

qw_req POST /api/v1/kv "$(qw_kv_batch "$(qw_kv_put_if_absent app 'gate:9f1' '{"who":"me"}' 600 true)")"
sent "putIfAbsent with required:true, which is the idempotency gate of §0" 'POST /api/v1/kv' \
  '[{"op":"putIfAbsent","ns":"app","key":"gate:9f1","value":{"who":"me"},"ttlSeconds":600,"required":true}]'

qw_req POST /api/v1/kv "$(qw_kv_batch "$(qw_kv_delete app k)")"
sent "delete, with no precondition" 'POST /api/v1/kv' \
  '[{"op":"delete","ns":"app","key":"k"}]'

qw_req POST /api/v1/kv "$(qw_kv_batch "$(qw_kv_delete app k 7 true)")"
sent "delete fencing on a version, escalated per element" 'POST /api/v1/kv' \
  '[{"op":"delete","ns":"app","key":"k","expect":7,"required":true}]'

# `delta`, and a `max` that makes `applied` the admission decision (§5.4): the
# ceiling belongs to the operation, or the caller compares client-side AFTER
# incrementing and cannot give the budget back.
qw_req POST /api/v1/kv "$(qw_kv_batch "$(qw_kv_incr app 'quota:acme' 1 600 100)")"
sent "incr with delta, expiry and max" 'POST /api/v1/kv' \
  '[{"op":"incr","ns":"app","key":"quota:acme","delta":1,"ttlSeconds":600,"max":100}]'

# =============================================================================
say ""
say "== 2. the two batch envelopes, and the path routes (§8.1) =="
# =============================================================================

qw_req POST /api/v1/kv "$(qw_kv_batch "$(qw_kv_get app a)" "$(qw_kv_get app b)")"
sent "the bare-array envelope keeps the ops in order" 'POST /api/v1/kv' \
  '[{"op":"get","ns":"app","key":"a"},{"op":"get","ns":"app","key":"b"}]'

qw_req POST /api/v1/kv "$(qw_kv_batch_operations "$(qw_kv_get app a)")"
sent "the {\"operations\":[...]} envelope, the same key the transaction wire uses" 'POST /api/v1/kv' \
  '{"operations":[{"op":"get","ns":"app","key":"a"}]}'

# The path routes name the key in the URL. `op`, `ns` and `key` in the body are a
# 400 on the broker (kv_field_in_path) and must never be built here: a body that
# repeats the URL is the silent-override class this product refuses everywhere.
qw_req PUT "/api/v1/kv/app/order/9f1" "$(qw_kv_put_body '{"x":1}' 600)"
sent "PUT names the key in the path, and the body carries only value + expiry" \
  'PUT /api/v1/kv/app/order/9f1' \
  '{"value":{"x":1},"ttlSeconds":600}'

qw_req PUT "/api/v1/kv/app/gate" "$(qw_kv_put_body '{"who":"me"}' 600 0 true)"
sent "PUT with expect:0, which is putIfAbsent spelled on the path route" \
  'PUT /api/v1/kv/app/gate' \
  '{"value":{"who":"me"},"ttlSeconds":600,"expect":0,"required":true}'

qw_req GET "/api/v1/kv/app/order/9f1"
sent "GET sends no body and no query string at all (§5.5)" \
  'GET /api/v1/kv/app/order/9f1' ''

qw_req DELETE "/api/v1/kv/app/k"
sent "DELETE with no body is the common case and must stay bodiless" \
  'DELETE /api/v1/kv/app/k' ''

qw_req DELETE "/api/v1/kv/app/k" "$(qw_kv_delete_body 7)"
sent "DELETE with an expect body" 'DELETE /api/v1/kv/app/k' '{"expect":7}'

# =============================================================================
say ""
say "== 3. the timer operations: delayMs, and a base64 payload (§4.2, §20.6) =="
# =============================================================================

qw_req POST /api/v1/timers "$(qw_timers_batch "$(qw_timer_schedule q tk Default 250 txn-1 "$(qw_b64 '{"a":1}')")")"
sent "schedule: a RELATIVE delay in MILLISECONDS, because 250 ms is a real backoff" \
  'POST /api/v1/timers' \
  '[{"op":"schedule","queue":"q","timerKey":"tk","partition":"Default","delayMs":250,"txn":"txn-1","payload":"eyJhIjoxfQ=="}]'

qw_req POST /api/v1/timers "$(qw_timers_batch "$(qw_timer_schedule q tk '' 60000 txn-1 "$(qw_b64 '{"a":1}')")")"
sent "schedule with no partition: the field is omitted, never sent empty" \
  'POST /api/v1/timers' \
  '[{"op":"schedule","queue":"q","timerKey":"tk","delayMs":60000,"txn":"txn-1","payload":"eyJhIjoxfQ=="}]'

qw_req POST /api/v1/timers "$(qw_timers_batch_operations "$(qw_timer_reschedule q tk Default 90000 txn-1 "$(qw_b64 '{"a":2}')")")"
sent "reschedule is the same upsert under the same name, in the operations envelope" \
  'POST /api/v1/timers' \
  '{"operations":[{"op":"reschedule","queue":"q","timerKey":"tk","partition":"Default","delayMs":90000,"txn":"txn-1","payload":"eyJhIjoyfQ=="}]}'

qw_req POST /api/v1/timers "$(qw_timers_batch "$(qw_timer_cancel q tk)")"
sent "cancel in the batch, which inherits THIS route's authorization class" \
  'POST /api/v1/timers' \
  '[{"op":"cancel","queue":"q","timerKey":"tk"}]'

qw_req POST /api/v1/timers "$(qw_timers_batch "$(qw_timer_cancel q tk txn-1)")"
sent 'cancel echoing the txn it expects, so an `absent` needs no second API (§4.4)' \
  'POST /api/v1/timers' \
  '[{"op":"cancel","queue":"q","timerKey":"tk","txn":"txn-1"}]'

# §9.6: the route that is guaranteed never to be blocked. It is a DELETE with no
# body, and the txn rides in the one query parameter this route reads.
qw_req DELETE "/api/v1/timers/q/tk?txn=txn-1"
sent "the standalone cancel is a bodiless DELETE on its own route (§9.6)" \
  'DELETE /api/v1/timers/q/tk?txn=txn-1' ''

qw_req GET "/api/v1/timers/q/tk"
sent "peek" 'GET /api/v1/timers/q/tk' ''

qw_req GET "/api/v1/timers/q?after=tk-0007&limit=100"
sent "list, with the exclusive keyset cursor" 'GET /api/v1/timers/q?after=tk-0007&limit=100' ''

# =============================================================================
say ""
say "== 4. the transaction bundle: kv and timers are TOP-LEVEL arrays (§10.4) =="
# =============================================================================
#
# This is the section that exists because of a silent failure in Go: two struct
# fields carrying the same JSON key at the same level are BOTH DROPPED by
# encoding/json, with no error and no warning. If the riders lived inside
# `operations`, a typed client's body would leave with zero kv ops, the broker
# would commit a transaction with no gate, and the putIfAbsent the bundle existed
# for would simply never have happened. The HTTP client has no struct to get this
# wrong in, which is exactly why it is the client that pins the shape.

PUSH="$(qw_push_op orders Default '{"id":"9f1"}' push-txn-1)"
GATE="$(qw_kv_put_if_absent app 'gate:9f1' '{"who":"me"}' 600 true)"
TIMER="$(qw_timer_schedule orders 'retry:9f1' Default 250 timer-txn-1 "$(qw_b64 '{"n":1}')")"

qw_req POST /api/v1/transaction "$(qw_txn_body "$(qw_ops "$PUSH")" "$(qw_ops "$GATE")" "$(qw_ops "$TIMER")")"
sent "the full bundle: operations, kv and timers as three SIBLING arrays" \
  'POST /api/v1/transaction' \
  '{"operations":[{"type":"push","queue":"orders","partition":"Default","payload":{"id":"9f1"},"transactionId":"push-txn-1"}],"kv":[{"op":"putIfAbsent","ns":"app","key":"gate:9f1","value":{"who":"me"},"ttlSeconds":600,"required":true}],"timers":[{"op":"schedule","queue":"orders","timerKey":"retry:9f1","partition":"Default","delayMs":250,"txn":"timer-txn-1","payload":"eyJuIjoxfQ=="}]}'

B="$(last '.body')"
eq "\`kv\` is a key of the request ROOT"     "true" "$(printf '%s' "$B" | jq -r 'has("kv")')"
eq "\`timers\` is a key of the request ROOT" "true" "$(printf '%s' "$B" | jq -r 'has("timers")')"
eq "and NO element of operations[] carries a kv or timer type" "0" \
   "$(printf '%s' "$B" | jq -r '[.operations[] | select(.type=="kv" or .type=="timer" or has("op"))] | length')"

# A bundle with neither rider must be byte-identical to what a client that has
# never heard of this feature sends. The riders can only ever appear after the
# last index that exists today (§8.2), and the request must not even carry the
# keys: a `"kv":[]` on every transaction would be a wire change for every client
# that does not use the feature.
qw_req POST /api/v1/transaction "$(qw_txn_body "$(qw_ops "$PUSH")")"
sent "no riders: the body is exactly today's, with no empty arrays added" \
  'POST /api/v1/transaction' \
  '{"operations":[{"type":"push","queue":"orders","partition":"Default","payload":{"id":"9f1"},"transactionId":"push-txn-1"}]}'

# `null` is what every serializer emits for an unset optional field, and the
# broker accepts it as absent. Pinned here because the tolerance is a promise:
# a client that merely ADDS the field must not break.
qw_req POST /api/v1/transaction "$(qw_txn_body "$(qw_ops "$PUSH")" null null)"
sent "explicit nulls are a legal spelling of \"no riders\"" \
  'POST /api/v1/transaction' \
  '{"operations":[{"type":"push","queue":"orders","partition":"Default","payload":{"id":"9f1"},"transactionId":"push-txn-1"}],"kv":null,"timers":null}'

# The KV-only bundle: no push, no ack, and the broker routes it off the wire
# onto the short KV transaction (§2.5, §8.2 point 6). On the wire it is still a
# transaction body, and `operations` stays present and empty.
qw_req POST /api/v1/transaction "$(qw_txn_body "$(qw_ops)" "$(qw_ops "$GATE")")"
sent "a KV-only bundle still sends operations:[], and no timers key" \
  'POST /api/v1/transaction' \
  '{"operations":[],"kv":[{"op":"putIfAbsent","ns":"app","key":"gate:9f1","value":{"who":"me"},"ttlSeconds":600,"required":true}]}'

# =============================================================================
say ""
say "== 5. commit RETURNS on the precondition and RAISES on everything else (§8.3) =="
# =============================================================================
BUNDLE="$(qw_txn_body "$(qw_ops "$PUSH")" "$(qw_ops "$GATE")")"

plan_reply 200 '{"transactionId":"t-1","success":true,"results":[{"index":0,"type":"push"},{"index":1,"type":"kv","opIndex":0,"applied":true}]}'
if qw_commit "$BUNDLE"; then ok "a committed bundle returns 0"; else bad "a committed bundle raised"; fi
eq "and its verdict is 'committed'" "committed" "${QW_VERDICT:-}"

# THE ONE THAT MATTERS. 200, success:false, and a machine-readable reason. It is
# a verdict, not an error: it must not reach a retry policy or an error metric.
plan_reply 200 '{"transactionId":"t-2","success":false,"reason":"kv_precondition","failedIndex":4,"kvReason":"exists","version":9007199254740993,"value":{"who":"first"}}'
if qw_commit "$BUNDLE"; then ok "a lost precondition RETURNS, it does not raise"; else bad "a lost precondition raised: it would poison every retry policy"; fi
eq "verdict"     "kv_precondition"  "${QW_VERDICT:-}"
eq "failedIndex is read as sent, and it is the FLAT ordinal (§8.2 point 4)" "4" "${QW_FAILED_INDEX:-}"
eq "kvReason"    "exists"           "${QW_KV_REASON:-}"
eq "value of the winner comes back, so the loser needs no second round trip" \
   '{"who":"first"}' "${QW_VALUE:-}"
# A version is a BIGINT. Any client that routes it through a double loses the
# low bits and fences against a version that never existed; 2^53+1 is where that
# starts, so the test asks for exactly that number back.
eq "version survives as a 64-bit integer, not a double" "9007199254740993" "${QW_VERSION:-}"

plan_reply 200 '{"transactionId":"t-3","success":false,"reason":"duplicate","error":"QDUP"}'
if qw_commit "$BUNDLE"; then bad "a non-precondition failure returned as a verdict"; else ok "any OTHER success:false raises"; fi

plan_reply 400 '{"transactionId":"t-4","success":false,"reason":"bad_request","error":"kv operation at index 0 is not an object"}'
if qw_commit "$BUNDLE"; then bad "a 400 returned as a verdict"; else ok "a 400 raises"; fi

plan_reply 500 '{"error":"db_error"}'
if qw_commit "$BUNDLE"; then bad "a 500 returned as a verdict"; else ok "a 500 raises"; fi

plan_reply 503 '{"error":"kv_unavailable","reason":"kv_pool_exhausted"}'
if qw_commit "$BUNDLE"; then bad "a 503 returned as a verdict"; else ok "a 503 raises"; fi

# Transport failure. No status, no body: the distinction "the broker said no" vs
# "nobody answered" has to survive, or a network outage reads as a business
# verdict and the caller skips the effect it was gating.
if ( QUEEN_HTTP_URL="http://127.0.0.1:1"; QW_TIMEOUT=3; qw_commit '{"operations":[]}' ) >/dev/null 2>&1
then bad "a transport failure returned as a verdict"
else ok "a transport failure raises, it is not a verdict"; fi

# The status-code rule applies to the standalone routes too: applied:false is an
# outcome, and the client must not turn a 200 into an error (§8.1).
plan_reply 200 '{"index":0,"op":"put","applied":false,"reason":"expect","key":"k","value":{"who":"first"},"version":90101}'
qw_req PUT "/api/v1/kv/app/k" "$(qw_kv_put_body '{"who":"me"}' 600 3)"
eq "a lost race on a path route is HTTP 200" "200" "$QW_RC"
eq "and the client reads the verdict from the body" "false" "$(qw_jv '.applied')"

# =============================================================================
say ""
say "== 6. the rules that hold across EVERY request this client can make =="
# =============================================================================
#
# Cross-cutting assertions over everything recorded so far. They are the cheapest
# possible guard against the four shape mistakes that are invisible in an
# integration run, and they get stronger every time a case is added above.
plan_reply 200 '{"ok":true}'

N="$(nreq)"
if [ "${N:-0}" -ge 30 ]; then ok "$N requests recorded to check against"; else bad "only ${N:-0} requests recorded; the cross-cutting checks would be vacuous"; fi

# §5.5 and §8.1: a prefix in a URL is recorded by every access log, proxy sample
# and tracing span between the client and the database. getPrefix lives in the
# POST body and NOWHERE else.
HITS="$(jq -r 'select(.path | test("prefix")) | .path' "$PLAN_DIR/requests.jsonl" | wc -l | tr -d ' ')"
eq "no request ever put a prefix in a query string" "0" "$HITS"

# §20.6, the declared rule: durations that can be sub-second are in ms, the ones
# that cannot are in seconds. Every timer op carries delayMs; no kv write carries
# anything but ttlSeconds or forever.
BADDUR="$(jq -r '.body | select(length>0) | try fromjson catch empty | [..|objects|select(.op=="schedule" or .op=="reschedule")|select(has("delayMs")|not)] | length' "$PLAN_DIR/requests.jsonl" | awk '{s+=$1} END {print s+0}')"
eq "every schedule carries delayMs" "0" "$BADDUR"
BADSEC="$(jq -r '.body | select(length>0) | try fromjson catch empty | [..|objects|select(has("delaySeconds") or has("delay") or has("ttlMillis") or has("ttlMs") or has("ttl"))] | length' "$PLAN_DIR/requests.jsonl" | awk '{s+=$1} END {print s+0}')"
eq "and no request ever spelled a duration any other way" "0" "$BADSEC"

# §13.4: a timer payload is opaque bytes end to end (it may be zstd-compressed
# and it may be encrypted), so there is no point on the wire at which it is a
# JSON value.
BADPAY="$(jq -r '.body | select(length>0) | try fromjson catch empty | [..|objects|select(has("payload") and (.op=="schedule" or .op=="reschedule"))|select(.payload|type != "string")] | length' "$PLAN_DIR/requests.jsonl" | awk '{s+=$1} END {print s+0}')"
eq "every timer payload is a base64 STRING, never a JSON object" "0" "$BADPAY"

# §10.4 again, as a standing guard: not one body ever put a rider inside
# operations[]. This is the assertion that fails the day somebody "simplifies"
# the builders back into a single flat array.
BADRIDER="$(jq -r '.body | select(length>0) | try fromjson catch empty | select(type=="object") | [(.operations // [])[] | select(.type=="kv" or .type=="timer")] | length' "$PLAN_DIR/requests.jsonl" | awk '{s+=$1} END {print s+0}')"
eq "no operations[] element was ever a kv or timer op" "0" "$BADRIDER"

# Every body this client sends is valid JSON. A shell client builds JSON by
# concatenation, so this is not a tautology: it is the check that catches an
# unescaped quote in a key or a value.
BADJSON="$(jq -r 'select(.body|length>0) | .body | try (fromjson|"ok") catch "BAD"' "$PLAN_DIR/requests.jsonl" | grep -c BAD | tr -d ' ')"
eq "every non-empty body was valid JSON" "0" "$BADJSON"

# Compact, always: no pretty printing on the wire. A body with newlines in it is
# a body somebody built by hand.
BADFMT="$(jq -r 'select(.body|length>0) | select(.body|test("\n")) | .path' "$PLAN_DIR/requests.jsonl" | wc -l | tr -d ' ')"
eq "every body was sent compact" "0" "$BADFMT"

CT="$(jq -r 'select(.body|length>0) | .contentType' "$PLAN_DIR/requests.jsonl" | sort -u | tr '\n' ' ')"
eq "every body was sent as application/json" "application/json " "$CT"

say ""
say "passed: $PASS   failed: $FAIL"
if [ "$FAIL" = "0" ]; then say "HTTPUNIT: PASS"; exit 0; fi
say "HTTPUNIT: FAIL"
exit 1
