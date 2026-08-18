#!/usr/bin/env bash
# =============================================================================
#  THE HTTP CLIENT OF THE KV AND TIMER SURFACE — PLAN_KV_TIMERS.md §10.2.
#
#  The matrix of §10.2 has seven SDK rows and one row that is not an SDK: HTTP,
#  "corpo grezzo", validated by a script executed in CI. This file is that row.
#  There is no SDK to hold the wire shape, so this file holds it: every request
#  body of the kv and timers surface is built HERE, once, and both suites use it.
#
#    http-wire-unit.sh    no broker. Pins the exact BYTES of every body against
#                         a scripted plan server. It is the contract.
#    http-wire-check.sh   a real broker. Exercises every route and every wire
#                         form, and asserts the answers.
#
#  Splitting it this way is the point of the row: if the integration suite built
#  its own bodies inline, the unit suite would be pinning bytes nobody sends.
#
#  ---------------------------------------------------------------------------
#  THREE RULES THIS FILE ENCODES, AND WHY THEY ARE HERE AND NOT IN A COMMENT.
#
#  1. THE RIDER ARRAYS ARE TOP-LEVEL (§10.4, §6.3, §8.2). `kv` and `timers` are
#     siblings of `operations` in the request body, never elements of it. The
#     reason is a silent failure in a typed language: two Go struct fields with
#     the same JSON key at the same level are BOTH DROPPED by encoding/json, no
#     error and no warning, so the body would leave with zero kv ops, the broker
#     would commit a transaction with no gate, and the putIfAbsent the bundle
#     existed for would never have happened. `qw_txn_body` is the only function
#     that assembles a transaction body, so the shape has one definition.
#
#  2. COMMIT RETURNS ON THE PRECONDITION AND RAISES ON EVERYTHING ELSE (§8.3).
#     A lost `required` gate is the expected outcome of every legitimate
#     redelivery and the most frequent outcome of the product; it arrives as HTTP
#     200 with success:false and a machine-readable reason. In shell, "raises" is
#     a non-zero return: it aborts under `set -e` and every caller here tests it.
#     `qw_commit` returns 0 on a commit AND on a lost precondition, and non-zero
#     on every other answer, including no answer at all.
#
#  3. THE CLIENT NEVER STRING-MATCHES A MESSAGE. It branches on `error`,
#     `reason`, `status`, `applied`, `found` and `ok`, which are the closed
#     taxonomies of §9.5 and §4.1. String matching on prose is forbidden
#     everywhere in this codebase, and this file is the example seven SDKs are
#     read against.
#
#  ---------------------------------------------------------------------------
#  CONVENTIONS.
#
#  * Every builder PRINTS one compact JSON document to stdout and touches no
#    global. They compose: `qw_kv_batch "$(qw_kv_get app k)" "$(qw_kv_put ...)"`.
#  * jq builds every document. A shell client that concatenates JSON by hand
#    breaks on the first key containing a quote, and the escaping bug it produces
#    is exactly the class this suite exists to catch.
#  * An EMPTY STRING argument means "omit this field", never "send it empty".
#    The difference is load-bearing on `partition` (empty is a 400 in the SP) and
#    on the expiry, where "omit" is what proves the exactly-one-of rule of §5.1.
#  * Durations follow the declared product rule (§20.6): what can be sub-second
#    is in milliseconds (`delayMs`), what cannot is in seconds (`ttlSeconds`).
#  * The caller owns URL encoding. Keys may contain slashes because the route is
#    a catch-all (`order/9f1/items` writes naturally); a key with a LITERAL slash
#    in it is written `%2F` by the caller, and nothing here second-guesses that.
#
#  Requires: bash, curl, jq. Sourced, never executed.
# =============================================================================

# Response of the last call. Deliberately globals with a prefix rather than a
# return value: a shell function that must report a status code, a body and a
# header set has no other way, and the alternative (echoing a packed string the
# caller splits) is the shape that loses embedded newlines.
QW_RC=""
QW_BODY=""
QW_VERDICT=""
QW_FAILED_INDEX=""
QW_KV_REASON=""
QW_VERSION=""
QW_VALUE=""
QW_ERROR=""

QW_TMP="$(mktemp -d -t queen-wire.XXXXXX)"
QW_BODYF="$QW_TMP/body"
QW_HDRF="$QW_TMP/head"

# Callers put this in their EXIT trap. Not a trap of its own: this file is
# SOURCED, and installing a trap here would silently replace the caller's.
qw_teardown() { rm -rf "$QW_TMP"; }

# ---------------------------------------------------------------------------
# Transport.
# ---------------------------------------------------------------------------

## qw_req METHOD PATH [BODY]
##
## Sets QW_RC (the HTTP status, or "000" when nothing answered) and QW_BODY.
## Never returns non-zero for an HTTP status: `applied:false`, `found:false`,
## `ok:false` and a 4xx are all outcomes this client reports rather than
## outcomes it hides, which is the §8.1 status-code rule seen from the caller's
## side. `QUEEN_HTTP_URL` is read at CALL time so a caller can point one call
## somewhere else without re-sourcing.
qw_req() {
  local m="$1" p="$2" b="${3:-}"
  local url="${QUEEN_HTTP_URL:?QUEEN_HTTP_URL is not set}"
  local args
  args=(-s -o "$QW_BODYF" -D "$QW_HDRF" -w '%{http_code}' --max-time "${QW_TIMEOUT:-30}" -X "$m")
  if [ -n "$b" ]; then
    args+=(-H 'Content-Type: application/json' --data-binary "$b")
  fi
  if [ -n "${QUEEN_TENANT:-}" ]; then
    args+=(-H "x-queen-tenant: $QUEEN_TENANT")
  fi
  QW_RC="$(curl "${args[@]}" "$url$p" 2>/dev/null)" || QW_RC="000"
  QW_BODY="$(cat "$QW_BODYF" 2>/dev/null)"
  return 0
}

## qw_jv JQ-EXPRESSION -> the value from the last response body.
##
## `tostring` and never `// "default"`: jq's `//` treats a literal `false` as
## empty, which would silently turn a real `"applied":false` into the fallback.
## That trap is documented in the tenancy runner and it costs a whole afternoon.
qw_jv() { printf '%s' "${QW_BODY:-}" | jq -r "($1) | tostring" 2>/dev/null || echo "?"; }

## qw_header NAME -> the value of a response header of the last call, lowercased
## name, empty when absent. Used for the ETag of §8.1 (ETag yes, If-Match no).
qw_header() {
  tr -d '\r' < "$QW_HDRF" 2>/dev/null \
    | awk -v want="$(printf '%s' "$1" | tr 'A-Z' 'a-z')" \
        '{i=index($0,":"); if(i>0){k=substr($0,1,i-1); v=substr($0,i+2); if(tolower(k)==want) print v}}'
}

## qw_commit BODY
##
## The §8.3 contract, and the reason this file exists in the shape it does.
## Returns 0 with QW_VERDICT=committed, or 0 with QW_VERDICT=kv_precondition and
## QW_FAILED_INDEX / QW_KV_REASON / QW_VERSION / QW_VALUE filled from the body.
## Returns 1 with QW_ERROR on anything else, including a transport failure: "the
## broker said no" and "nobody answered" must not collapse into one another, or a
## network outage reads as a business verdict and the caller skips the effect it
## was gating.
qw_commit() {
  QW_VERDICT=""; QW_FAILED_INDEX=""; QW_KV_REASON=""; QW_VERSION=""; QW_VALUE=""; QW_ERROR=""
  qw_req POST /api/v1/transaction "$1"
  case "$QW_RC" in
    200) : ;;
    000) QW_ERROR="transport: no answer from ${QUEEN_HTTP_URL:-}"; return 1 ;;
    *)   QW_ERROR="http $QW_RC reason=$(qw_jv '.reason') error=$(qw_jv '.error')"; return 1 ;;
  esac
  if [ "$(qw_jv '.success')" = "true" ]; then
    QW_VERDICT="committed"
    return 0
  fi
  if [ "$(qw_jv '.reason')" != "kv_precondition" ]; then
    QW_ERROR="commit failed: reason=$(qw_jv '.reason') error=$(qw_jv '.error')"
    return 1
  fi
  # The verdict, with everything needed to act on it. `failedIndex` is in the
  # FLAT space of results[] (§8.2 point 4): it indexes the bundle, not the kv
  # array, and a client that assumes otherwise blames somebody else's operation.
  QW_VERDICT="kv_precondition"
  QW_FAILED_INDEX="$(qw_jv '.failedIndex')"
  QW_KV_REASON="$(qw_jv '.kvReason')"
  QW_VERSION="$(qw_jv '.version')"
  QW_VALUE="$(qw_jv '.value')"
  return 0
}

# ---------------------------------------------------------------------------
# Small helpers.
# ---------------------------------------------------------------------------

## qw_b64 STRING -> base64 with no line breaks.
##
## The wrapping matters: BSD base64 and busybox base64 both fold at 76 columns,
## and a folded payload is not a JSON string the broker will decode.
qw_b64() { printf '%s' "$1" | base64 | tr -d '\n'; }

## qw_ops [JSON...] -> a JSON array of the arguments, in order. Zero arguments
## is `[]`, which is a real case: a KV-only bundle sends `operations:[]`.
qw_ops() {
  if [ "$#" -eq 0 ]; then printf '[]'; return; fi
  printf '%s\n' "$@" | jq -cs '.'
}

# ---------------------------------------------------------------------------
# KV operations (§5, §6.1). Seven of them, and no eighth: `getPrefix` is the one
# that exists ONLY on POST /api/v1/kv, never in a transaction and never in a
# query string (§5.5).
#
# The expiry argument of every write is one of:
#   <n>        ttlSeconds: <n>
#   forever    forever: true
#   ""         neither, which is a 400 the SP owns (§5.1: exactly one of them)
# ---------------------------------------------------------------------------

# Shared jq fragment for the expiry rule. Kept as one string so put, putIfAbsent
# and incr cannot drift apart on the one rule §5.1 calls cross-cutting.
QW_JQ_EXPIRY='(if $exp == "" then {} elif $exp == "forever" then {forever:true} else {ttlSeconds:($exp|tonumber)} end)'

## qw_kv_get NS KEY
qw_kv_get() {
  jq -cn --arg ns "$1" --arg key "$2" '{op:"get", ns:$ns, key:$key}'
}

## qw_kv_get_many NS KEY...
qw_kv_get_many() {
  local ns="$1"; shift
  jq -cn --arg ns "$ns" '{op:"getMany", ns:$ns, keys:$ARGS.positional}' --args "$@"
}

## qw_kv_get_prefix NS PREFIX [LIMIT] [AFTER] [KEYSONLY]
##
## `after` is an EXCLUSIVE keyset cursor and `limit` is CLAMPED by the server,
## never rejected: a 400 on a too-large limit is an error the caller cannot fix
## without reading the server's configuration (§5.5).
qw_kv_get_prefix() {
  jq -cn --arg ns "$1" --arg prefix "$2" --arg limit "${3:-}" --arg after "${4:-}" --arg keysOnly "${5:-}" '
    {op:"getPrefix", ns:$ns, prefix:$prefix}
    + (if $limit    == "" then {} else {limit: ($limit|tonumber)} end)
    + (if $after    == "" then {} else {after: $after} end)
    + (if $keysOnly == "" then {} else {keysOnly: ($keysOnly == "true")} end)'
}

## qw_kv_put NS KEY VALUE-JSON EXPIRY [EXPECT] [REQUIRED]
##
## `expect` is the ONLY way to express a precondition (§8.1: ETag yes, If-Match
## no). `expect:0` means "must not exist". `required:true` escalates a lost
## precondition from a returned verdict to a rolled-back transaction, per
## element, which is what makes the gate of §0 a gate.
qw_kv_put() {
  jq -cn --arg ns "$1" --arg key "$2" --argjson value "$3" \
         --arg exp "${4:-}" --arg expect "${5:-}" --arg required "${6:-}" "
    {op:\"put\", ns:\$ns, key:\$key, value:\$value}
    + $QW_JQ_EXPIRY
    + (if \$expect   == \"\" then {} else {expect: (\$expect|tonumber)} end)
    + (if \$required == \"\" then {} else {required: (\$required == \"true\")} end)"
}

## qw_kv_put_if_absent NS KEY VALUE-JSON EXPIRY [REQUIRED]
##
## Sugar for put with expect:0, and it desugars to exactly that inside the SP —
## which is why supplying a different `expect` alongside is a contradiction the
## server refuses rather than resolves.
qw_kv_put_if_absent() {
  jq -cn --arg ns "$1" --arg key "$2" --argjson value "$3" \
         --arg exp "${4:-}" --arg required "${5:-}" "
    {op:\"putIfAbsent\", ns:\$ns, key:\$key, value:\$value}
    + $QW_JQ_EXPIRY
    + (if \$required == \"\" then {} else {required: (\$required == \"true\")} end)"
}

## qw_kv_delete NS KEY [EXPECT] [REQUIRED]
qw_kv_delete() {
  jq -cn --arg ns "$1" --arg key "$2" --arg expect "${3:-}" --arg required "${4:-}" '
    {op:"delete", ns:$ns, key:$key}
    + (if $expect   == "" then {} else {expect: ($expect|tonumber)} end)
    + (if $required == "" then {} else {required: ($required == "true")} end)'
}

## qw_kv_incr NS KEY DELTA EXPIRY [MAX] [MIN]
##
## incr takes NO expect: it is the way out of CAS, and a precondition would
## reintroduce the loop it exists to remove (§5.4). `max` is what makes `applied`
## the admission decision instead of an after-the-fact comparison.
qw_kv_incr() {
  jq -cn --arg ns "$1" --arg key "$2" --arg delta "$3" \
         --arg exp "${4:-}" --arg max "${5:-}" --arg min "${6:-}" "
    {op:\"incr\", ns:\$ns, key:\$key, delta: (\$delta|tonumber)}
    + $QW_JQ_EXPIRY
    + (if \$max == \"\" then {} else {max: (\$max|tonumber)} end)
    + (if \$min == \"\" then {} else {min: (\$min|tonumber)} end)"
}

## qw_kv_batch OP... -> the bare-array body of POST /api/v1/kv
qw_kv_batch() { qw_ops "$@"; }

## qw_kv_batch_operations OP... -> the {"operations":[...]} body of the same
## route. Both are accepted, and the second is the same key the transaction wire
## uses, so one shape is learned once.
qw_kv_batch_operations() {
  jq -cn --argjson ops "$(qw_ops "$@")" '{operations: $ops}'
}

## qw_kv_put_body VALUE-JSON EXPIRY [EXPECT] [REQUIRED] -> body of PUT /api/v1/kv/:ns/*key
##
## No `op`, no `ns`, no `key`: the URL names them, and repeating them in the body
## is a 400 (`kv_field_in_path`), not a silent override.
qw_kv_put_body() {
  jq -cn --argjson value "$1" --arg exp "${2:-}" --arg expect "${3:-}" --arg required "${4:-}" "
    {value: \$value}
    + $QW_JQ_EXPIRY
    + (if \$expect   == \"\" then {} else {expect: (\$expect|tonumber)} end)
    + (if \$required == \"\" then {} else {required: (\$required == \"true\")} end)"
}

## qw_kv_delete_body [EXPECT] [REQUIRED] -> body of DELETE /api/v1/kv/:ns/*key.
## With no arguments it is `{}`; the route also accepts no body at all, and both
## are exercised by the suites.
qw_kv_delete_body() {
  jq -cn --arg expect "${1:-}" --arg required "${2:-}" '
    {}
    + (if $expect   == "" then {} else {expect: ($expect|tonumber)} end)
    + (if $required == "" then {} else {required: ($required == "true")} end)'
}

# ---------------------------------------------------------------------------
# Timer operations (§4).
#
# `delayMs` is RELATIVE and in milliseconds; an absolute instant is not
# expressible on this wire, on purpose: one clock, Postgres's, and no
# inter-broker skew can enter anywhere. A delay in the past is LEGAL and fires on
# the first cycle.
#
# The payload is BASE64 TEXT, never a JSON object: it is opaque bytes end to end
# (it may be zstd-compressed and it may be encrypted at rest), so there is no
# point on the wire at which it is a JSON value.
# ---------------------------------------------------------------------------

# schedule and reschedule are the same upsert under the same name, so they are
# the same builder with a different verb. A client retry after a crash is safe by
# construction, and `status` in the answer says which one happened.
qw_timer_op() { # OP QUEUE TIMERKEY PARTITION DELAYMS TXN PAYLOAD-B64
  jq -cn --arg op "$1" --arg queue "$2" --arg key "$3" --arg part "${4:-}" \
         --arg delay "$5" --arg txn "$6" --arg payload "$7" '
    {op:$op, queue:$queue, timerKey:$key}
    + (if $part == "" then {} else {partition: $part} end)
    + {delayMs: ($delay|tonumber), txn: $txn, payload: $payload}'
}

## qw_timer_schedule QUEUE TIMERKEY PARTITION DELAYMS TXN PAYLOAD-B64
qw_timer_schedule() { qw_timer_op schedule "$@"; }

## qw_timer_reschedule QUEUE TIMERKEY PARTITION DELAYMS TXN PAYLOAD-B64
qw_timer_reschedule() { qw_timer_op reschedule "$@"; }

## qw_timer_cancel QUEUE TIMERKEY [TXN]
##
## In a POST batch this inherits the batch route's authorization class, and on a
## blocked cluster a batch that also carries a schedule is refused WHOLE. The
## route that is guaranteed to take a cancel is DELETE /api/v1/timers/:queue/*key
## (§9.6), which carries no body at all.
qw_timer_cancel() {
  jq -cn --arg queue "$1" --arg key "$2" --arg txn "${3:-}" '
    {op:"cancel", queue:$queue, timerKey:$key}
    + (if $txn == "" then {} else {txn: $txn} end)'
}

## qw_timers_batch OP...            -> the bare-array body of POST /api/v1/timers
## qw_timers_batch_operations OP... -> the {"operations":[...]} body
qw_timers_batch() { qw_ops "$@"; }
qw_timers_batch_operations() {
  jq -cn --argjson ops "$(qw_ops "$@")" '{operations: $ops}'
}

# ---------------------------------------------------------------------------
# The transaction bundle.
# ---------------------------------------------------------------------------

## qw_push_op QUEUE PARTITION PAYLOAD-JSON [TRANSACTION-ID]
qw_push_op() {
  jq -cn --arg queue "$1" --arg part "${2:-}" --argjson payload "$3" --arg txn "${4:-}" '
    {type:"push", queue:$queue}
    + (if $part == "" then {} else {partition: $part} end)
    + {payload: $payload}
    + (if $txn == "" then {} else {transactionId: $txn} end)'
}

## qw_ack_op TRANSACTION-ID PARTITION-ID STATUS [CONSUMER-GROUP]
qw_ack_op() {
  jq -cn --arg txn "$1" --arg pid "$2" --arg status "$3" --arg grp "${4:-}" '
    {type:"ack", transactionId:$txn, partitionId:$pid, status:$status}
    + (if $grp == "" then {} else {consumerGroup: $grp} end)'
}

## qw_txn_body OPERATIONS-ARRAY [KV] [TIMERS]
##
## THE ONE FUNCTION THAT ASSEMBLES A TRANSACTION BODY, so the shape of §10.4 has
## a single definition. `kv` and `timers` are keys of the ROOT, beside
## `operations` and never inside it.
##
## Each rider argument is one of:
##   ""       the key is ABSENT. A bundle with no riders must be byte-identical
##            to what a client that never heard of this feature sends, or every
##            such client has a wire change it did not ask for.
##   null     the key is present and null, which the broker accepts as absent.
##            That tolerance is a promise: it is what every serializer emits for
##            an unset optional field, so adding the field must not break a body.
##   [...]    the array.
qw_txn_body() {
  jq -cn --argjson ops "${1:-[]}" --arg kv "${2:-}" --arg timers "${3:-}" '
    {operations: $ops}
    + (if $kv     == "" then {} else {kv:     ($kv|fromjson)} end)
    + (if $timers == "" then {} else {timers: ($timers|fromjson)} end)'
}
