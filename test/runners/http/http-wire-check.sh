#!/usr/bin/env bash
# =============================================================================
#  HTTP WIRE GATE — every kv and timer route, and every form of the wire,
#  against a real broker, with no SDK anywhere in the path.
#  PLAN_KV_TIMERS.md §10.2 (the "HTTP (nessun SDK)" row), §8.1, §8.2, §4, §5.
#
#  ---------------------------------------------------------------------------
#  WHY THIS SUITE IS WRITTEN FIRST AND WHY IT IS NOT AN SDK TEST.
#
#  §10.2 orders HTTP before the six SDKs for one reason: it is the only client
#  that shows the wire. Every other suite asserts through a client library, so a
#  wire it gets wrong and re-reads the same wrong way is green. Here the request
#  is curl and the response is jq, so what is asserted is what a customer's
#  `curl`, their nginx, their Postman collection and their sixth language will
#  see. It is the executable half of the stored procedure's specification.
#
#  It is deliberately BROAD rather than deep: every route, every op, every
#  envelope, every documented refusal. Depth lives elsewhere and is not
#  duplicated here:
#
#    test/runners/txnsemantics   the four transactional properties (a lost gate
#                                rolls back the push AND the timer, commit does
#                                not raise, an expired lease annuls the KV write,
#                                results are index-aligned).
#    server/tests/timers_*       claim, fire, retry, DLQ, fault injection.
#    server/tests/kv_*           the SQL helpers, boot idempotence, quota.
#
#  What this file adds that none of them have: the SHAPE of every call, made the
#  way an integrator makes it.
#
#  ---------------------------------------------------------------------------
#  THE CLIENT IS test/runners/http/kv-timers-wire.sh.
#
#  Every body sent from here is built by that file, and its exact bytes are
#  pinned WITHOUT a broker by http-wire-unit.sh. That split is the point: bodies
#  built inline here would make the unit suite pin bytes nobody sends. Read
#  kv-timers-wire.sh for the three rules it encodes (top-level rider arrays, the
#  commit contract, no string matching on messages).
#
#  ---------------------------------------------------------------------------
#  cleanupTestData IS LOAD-BEARING, NOT COSMETIC (§10.4).
#
#  Without a purge a `putIfAbsent` test is green on its first run and red for
#  ever after, and an `incr` test accumulates between runs. So the namespace and
#  the timer queues are purged BOTH at the start and at the end, the purge itself
#  is asserted, and two assertions in this file are built to FAIL if the purge
#  ever stops working: a fixed key name that must be absent when the suite starts
#  and a fixed counter that must reach a fixed value.
#
#  The related rule, also §10.4: `forever` is forbidden in examples that run in
#  CI, because a test that goes wrong leaves immortal state in a shared database.
#  `forever:true` IS a wire form that has to be exercised, so it is exercised on
#  exactly one key, that key is inside the purged prefix, and the purge runs from
#  an EXIT trap so it also runs when the suite dies half way.
#
#  ---------------------------------------------------------------------------
#  Requires: bash, curl, jq, and a broker. Nothing else — kv and timers are part
#  of the engine and every broker has both surfaces, so there is no flag to set
#  and no lane on which this suite would pass vacuously. The fire assertion
#  additionally needs the sweeper, which is on unless QUEEN_SWEEPER=false.
#
#  Usage:  QUEEN_HTTP_URL=http://localhost:6632 test/runners/http/http-wire-check.sh
# =============================================================================
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
QUEEN_HTTP_URL="${QUEEN_HTTP_URL:-http://localhost:16633}"
export QUEEN_HTTP_URL

# shellcheck source=./kv-timers-wire.sh
. "$HERE/kv-timers-wire.sh" || { echo "!! cannot source kv-timers-wire.sh"; echo "HTTPWIRE: FAIL"; exit 1; }

# FIXED names, not per-run ones. A random suffix would make the suite pass on a
# database full of its own leftovers, which is exactly the failure §10.4 asks to
# be made impossible: the purge below is what keeps repeated runs identical.
NS="httpwire"
KP="hw:"                      # every key starts here, so one getPrefix purges all
TQ="httpwire-timers"          # timers that are cancelled, never fired
FQ="httpwire-fire"            # the one queue where a timer is allowed to fire
GRP="httpwire-grp"          # made per-run below, see RUN

# AND THE ONE THING THAT MUST NOT BE FIXED, which is the other half of the same
# rule. Anything that reaches the message log carries a transactionId, and the
# broker deduplicates transaction ids for `dedupWindowSeconds` (3600 by default).
# No purge can reach that window: it lives in the broker's dedup ring, not in a
# table this suite can delete. A fixed id would therefore make the SECOND run
# inside an hour see its push refused as a duplicate and its fired timer silently
# suppressed — green once, red for an hour, which is the §10.4 failure wearing a
# different hat. So identifiers that enter the log are per-run, and everything a
# `DELETE` can reach stays fixed.
RUN="$(date +%s)-$$"
# The consumer group is per-run for the same family of reasons, and one more of
# its own: a group carries a CURSOR, which is state no DELETE in this file can
# reset. A shared group would make the fire assertion depend on where a previous
# run left the cursor, which is a flaky test wearing the costume of a real one.
GRP="$GRP-$RUN"

PASS=0; FAIL=0
say()  { printf '%s\n' "$*"; }
ok()   { PASS=$((PASS+1)); say "  ok   - $1"; }
bad()  { FAIL=$((FAIL+1)); say "  FAIL - $1"; }
eq()   { if [ "$2" = "$3" ]; then ok "$1"; else bad "$1 (want '$2', got '$3')"; fi; }
rc2xx() { case "$QW_RC" in 2??) ok "$1 ($QW_RC)";; *) bad "$1: HTTP $QW_RC $(printf '%s' "$QW_BODY" | head -c 200)";; esac; }

# ---------------------------------------------------------------------------
# cleanupTestData. Each half in its own guard, like the SDK suites do, because
# the schema, the namespace and the queues may legitimately not exist.
# ---------------------------------------------------------------------------
purge_kv() {
  local i keys n
  for i in 1 2 3 4 5 6 7 8 9 10; do
    qw_req POST /api/v1/kv "$(qw_kv_batch "$(qw_kv_get_prefix "$NS" "$KP" 250 '' true)")"
    case "$QW_RC" in 2??) : ;; *) return 0 ;; esac
    keys="$(printf '%s' "$QW_BODY" | jq -r '(.results[0].rows // [])[].key' 2>/dev/null)"
    [ -n "$keys" ] || return 0
    n=0; local ops=""
    while IFS= read -r k; do
      [ -n "$k" ] || continue
      ops="$ops$(qw_kv_delete "$NS" "$k")
"
      n=$((n+1))
    done <<EOF
$keys
EOF
    [ "$n" = 0 ] && return 0
    # printf, not qw_kv_batch "$ops": the ops arrived as one newline-separated
    # blob and jq -s wants them as separate documents, which is what this is.
    qw_req POST /api/v1/kv "$(printf '%s' "$ops" | jq -cs '.')"
  done
}

purge_timers() {
  local q keys
  for q in "$TQ" "$FQ"; do
    qw_req GET "/api/v1/timers/$q?limit=1000"
    case "$QW_RC" in 2??) : ;; *) continue ;; esac
    keys="$(printf '%s' "$QW_BODY" | jq -r '(.rows // [])[].timerKey' 2>/dev/null)"
    while IFS= read -r k; do
      [ -n "$k" ] || continue
      qw_req DELETE "/api/v1/timers/$q/$k"
    done <<EOF
$keys
EOF
  done
}

cleanup_test_data() { purge_kv; purge_timers; }

# The purge runs from the trap as well, so a suite that dies half way still
# leaves nothing behind — including the one key written with forever:true.
trap 'cleanup_test_data >/dev/null 2>&1; qw_teardown' EXIT

# =============================================================================
say "== preflight =="
# =============================================================================
export QUEEN_WAIT_URLS="${QUEEN_WAIT_URLS:-$QUEEN_HTTP_URL/health}"
if command -v wait-for-broker >/dev/null 2>&1; then wait-for-broker; fi

RC="$(curl -s -o /dev/null -w '%{http_code}' --max-time 10 "$QUEEN_HTTP_URL/health" 2>/dev/null)" || RC=000
[ "$RC" = "200" ] || { say "!! broker not healthy at $QUEEN_HTTP_URL (health -> $RC)"; say "HTTPWIRE: FAIL"; exit 1; }
ok "broker healthy at $QUEEN_HTTP_URL"

# Every broker that runs this binary has both surfaces — there is no flag left
# that could withhold them — so this preflight no longer asks "was the feature
# turned on". It asks the one question still worth asking: IS THE THING AT THE
# OTHER END OF $QUEEN_HTTP_URL A BROKER WITH THESE ROUTES. A stale image, the
# proxy, or a typo in the URL all reach here, and every assertion below would be
# vacuous against them.
#
# AND THE STATUS CODE IS NOT THE TEST, which is the trap here and is worth the
# four extra lines: a route this broker does not have does NOT answer 404. It
# falls through to the SPA dashboard served from the assets embedded at compile
# time, so a `GET /api/v1/kv/ns/key` that reaches no handler comes back **HTTP
# 200 with a page of HTML**. A preflight that accepted any 2xx would declare the
# surface present, and the whole suite would then measure a dashboard. So the
# probe asks for a FIELD only the real handler can produce: a `get` answers
# `found` (true or false), a peek answers `found`; HTML answers neither.
preflight_surface() { # label  path  hint
  qw_req GET "$2"
  case "$(qw_jv '.found')" in
    true|false) ok "$1 answers ($QW_RC)";;
    *) say "!! $1 is not there: GET $2 -> $QW_RC, and the body has no \`found\` field."
       say "!! First 120 bytes: $(printf '%s' "$QW_BODY" | tr -d '\n' | head -c 120)"
       say "!! $3"
       say "HTTPWIRE: FAIL"; exit 1;;
  esac
}
preflight_surface "the kv surface"    "/api/v1/kv/$NS/preflight" \
  "Every broker has this route, so this is not a missing feature: check that QUEEN_HTTP_URL points at a broker of this build and not at the proxy or an older image."
preflight_surface "the timer surface" "/api/v1/timers/$TQ/preflight" \
  "Same as above, and both surfaces are probed rather than one: they ship together, so one answering without the other means the binary is not the one this gate describes."

qw_req POST /api/v1/configure "{\"queue\":\"$TQ\",\"leaseTime\":30,\"retryLimit\":3}"
qw_req POST /api/v1/configure "{\"queue\":\"$FQ\",\"leaseTime\":30,\"retryLimit\":3}"
ok "queues configured"

cleanup_test_data
qw_req POST /api/v1/kv "$(qw_kv_batch "$(qw_kv_get_prefix "$NS" "$KP" 250 '' true)")"
eq "the namespace starts empty (this is cleanupTestData working)" "0" \
   "$(printf '%s' "$QW_BODY" | jq -r '(.results[0].rows // []) | length')"

# The one timer this suite lets fire is scheduled HERE, at the top, and collected
# in section 7. Not because the fire is slow but because the WAKE is a declared
# seam (§7.4: the in-process hint "goes HERE" and is not wired yet), so the only
# thing that notices a new timer is the sweeper's next poll, and an idle sweeper
# sleeps up to QUEEN_SWEEPER_IDLE_MAX_SLEEP_MS (30 s by default). Scheduling it
# first puts that sleep behind the rest of the suite instead of in front of a
# stopwatch. `deliverAt` is "not before", never "exactly at", so this is a
# property of the product and not a flaky test.
FTXN="${KP}fire-$RUN"
FPAY="$(qw_b64 '{"fired":true}')"
qw_req POST /api/v1/timers "$(qw_timers_batch "$(qw_timer_schedule "$FQ" "${KP}fire1" Default 0 "$FTXN" "$FPAY")")"
rc2xx "a timer scheduled for now, to be collected at the end"
FMID="$(qw_jv '.results[0].messageId')"

# =============================================================================
say ""
say "== 1. POST /api/v1/kv: the complete surface, and both envelopes (§8.1) =="
# =============================================================================
# THE ASSERTION THAT PROVES THE PURGE. A fixed key, putIfAbsent, applied:true.
# It is green on the first run of a virgin database whatever the purge does, and
# red on the second unless the purge really ran. Which is the whole of §10.4.
qw_req POST /api/v1/kv "$(qw_kv_batch "$(qw_kv_put_if_absent "$NS" "${KP}gate:fixed" '{"who":"first"}' 600)")"
rc2xx "putIfAbsent on a fixed key name"
eq "putIfAbsent applies on a purged namespace, run after run" "true" "$(qw_jv '.results[0].applied')"
eq "and the result is index-aligned to the input array (§6.4)" "0" "$(qw_jv '.results[0].index')"

# The loser gets the WINNER'S value back, so it needs no second round trip: that
# is the entire point of the marker (§5.3, §10.3 point 3).
qw_req POST /api/v1/kv "$(qw_kv_batch "$(qw_kv_put_if_absent "$NS" "${KP}gate:fixed" '{"who":"second"}' 600)")"
eq "a lost putIfAbsent is HTTP 200, not a 4xx (§8.1)" "200" "$QW_RC"
eq "applied:false" "false" "$(qw_jv '.results[0].applied')"
eq "reason names the closed taxonomy, never prose" "exists" "$(qw_jv '.results[0].reason')"
eq "and the WINNER's value comes back with it" "first" "$(qw_jv '.results[0].value.who')"

# All seven ops in one call, in the {"operations":[...]} envelope. getPrefix and
# incr exist ONLY here: incr has no path route because no literal segment may be
# added under /api/v1/kv/:ns/ (it would shadow a key of that name), and getPrefix
# has none because a prefix in a URL is recorded by every access log in between.
#
# The key deleted at the end is NOT one of the keys written earlier in the same
# call: §6.1 point 3 allows at most one WRITE per key per call, and that rule is
# what makes the intra-space lock order total. It is asserted on its own below.
qw_req PUT "/api/v1/kv/$NS/${KP}doomed" "$(qw_kv_put_body '{"n":0}' 600)"
qw_req POST /api/v1/kv "$(qw_kv_batch_operations \
  "$(qw_kv_put            "$NS" "${KP}a" '{"n":1}' 600)" \
  "$(qw_kv_put_if_absent  "$NS" "${KP}b" '{"n":2}' 600)" \
  "$(qw_kv_get            "$NS" "${KP}a")" \
  "$(qw_kv_get_many       "$NS" "${KP}a" "${KP}b" "${KP}nope")" \
  "$(qw_kv_get_prefix     "$NS" "$KP" 100)" \
  "$(qw_kv_incr           "$NS" "${KP}counter:fixed" 3 600)" \
  "$(qw_kv_delete         "$NS" "${KP}doomed")")"
rc2xx "all seven operations in one batch"
eq "seven ops in, seven results out"        "7"     "$(qw_jv '(.results // []) | length')"
eq "every result carries its own index"     "0,1,2,3,4,5,6" "$(qw_jv '[.results[].index] | join(",")')"
eq "put"                                    "true"  "$(qw_jv '.results[0].applied')"
eq "putIfAbsent"                            "true"  "$(qw_jv '.results[1].applied')"
eq "get finds the value written by op 0 in the SAME call" "1" "$(qw_jv '.results[2].value.n')"
eq "getMany returns the hits"               "2"     "$(qw_jv '(.results[3].rows // []) | length')"
# Absence is a DATUM, not a hole the caller computes by difference (§5.5).
eq "and names the misses explicitly"        "${KP}nope" "$(qw_jv '.results[3].missing[0]')"
eq "getPrefix pages the namespace"          "false" "$(qw_jv '.results[4].truncated')"
eq "incr creates the counter at the delta"  "3"     "$(qw_jv '.results[5].value')"
eq "delete applies"                         "true"  "$(qw_jv '.results[6].applied')"

# §6.1 point 3, and it is load-bearing rather than defensive: at most one WRITE
# per key per call is what makes the order inside the KV lock space TOTAL, so two
# writers of the same key can never interleave into a cycle. Reads are exempt,
# because they take no row lock.
qw_req POST /api/v1/kv "$(qw_kv_batch \
  "$(qw_kv_put    "$NS" "${KP}twice" '{"n":1}' 600)" \
  "$(qw_kv_delete "$NS" "${KP}twice")")"
eq "two writes of one key in one call are refused" "400" "$QW_RC"
eq "named kv_duplicate_key_in_call" "kv_duplicate_key_in_call" "$(qw_jv '.reason')"

# =============================================================================
say ""
say "== 2. the three path routes, and the catch-all key (§8.1) =="
# =============================================================================
qw_req PUT "/api/v1/kv/$NS/${KP}order/9f1/items" "$(qw_kv_put_body '{"items":2}' 600)"
rc2xx "PUT with a key that contains slashes"
eq "the single-op routes answer with the ELEMENT, not a batch envelope" "put" "$(qw_jv '.op')"
eq "and the key is stored WITHOUT a leading slash" "${KP}order/9f1/items" "$(qw_jv '.key')"

qw_req GET "/api/v1/kv/$NS/${KP}order/9f1/items"
eq "GET finds it"          "true" "$(qw_jv '.found')"
eq "with its value"        "2"    "$(qw_jv '.value.items')"
VER="$(qw_jv '.version')"
eq "and an ETag naming the version (§8.1: ETag yes, If-Match no)" "\"$VER\"" "$(qw_header etag)"

# The same key through the batch surface must be the SAME ROW. If the catch-all
# arrived with a leading slash, the two surfaces would silently address different
# keys and nothing anywhere would error.
qw_req POST /api/v1/kv "$(qw_kv_batch "$(qw_kv_get "$NS" "${KP}order/9f1/items")")"
eq "the batch surface reads the same row the path route wrote" "2" "$(qw_jv '.results[0].value.items')"

qw_req GET "/api/v1/kv/$NS/${KP}absent-key"
eq "a miss is 200 with found:false, never a 404 (§8.1)" "200" "$QW_RC"
eq "found:false"                    "false" "$(qw_jv '.found')"
eq "and a miss carries NO ETag, because there is no version to name" "" "$(qw_header etag)"

# §5.5 and §13.5: the route rejects a query string outright instead of ignoring
# it, because ?prefix=quota:acme: is already in the access logs by the time the
# handler could ignore it.
qw_req GET "/api/v1/kv/$NS/${KP}a?prefix=quota:acme:"
eq "a query string on a path route is refused" "400" "$QW_RC"
eq "with a named reason"  "kv_no_query_string" "$(qw_jv '.reason')"

# The URL names op, ns and key. A body repeating them is refused, not silently
# ignored: silent override is the class this product refuses everywhere.
qw_req PUT "/api/v1/kv/$NS/${KP}a" '{"key":"somewhere-else","value":{"n":9},"ttlSeconds":60}'
eq "a body field the URL already names is refused" "400" "$QW_RC"
eq "with a named reason" "kv_field_in_path" "$(qw_jv '.reason')"

qw_req DELETE "/api/v1/kv/$NS/${KP}order/9f1/items"
eq "DELETE with no body at all is the common case" "200" "$QW_RC"
eq "and it applies" "true" "$(qw_jv '.applied')"
qw_req DELETE "/api/v1/kv/$NS/${KP}order/9f1/items" "$(qw_kv_delete_body)"
eq "deleting it again is 200 with applied:false, the house rule of queue delete" "200" "$QW_RC"
eq "applied:false" "false" "$(qw_jv '.applied')"
eq "reason:absent"  "absent" "$(qw_jv '.reason')"

# =============================================================================
say ""
say "== 3. expect, and the repair that matters most (§5.3) =="
# =============================================================================
qw_req PUT "/api/v1/kv/$NS/${KP}cas" "$(qw_kv_put_body '{"v":1}' 600)"
V1="$(qw_jv '.version')"
qw_req PUT "/api/v1/kv/$NS/${KP}cas" "$(qw_kv_put_body '{"v":2}' 600 "$V1")"
eq "an expect matching the current version applies" "true" "$(qw_jv '.applied')"
V2="$(qw_jv '.version')"
qw_req PUT "/api/v1/kv/$NS/${KP}cas" "$(qw_kv_put_body '{"v":3}' 600 "$V1")"
eq "a stale expect does not apply"      "false"   "$(qw_jv '.applied')"
eq "and says which precondition lost"   "version" "$(qw_jv '.reason')"
eq "handing back the CURRENT version, so the loser can retry"  "$V2" "$(qw_jv '.version')"

# THE REPAIR. In the naive ON CONFLICT form an expect:N>0 against an absent key
# falls into the INSERT branch and CREATES the row — in a saga, that fires the
# compensating command the expect existed to prevent.
qw_req PUT "/api/v1/kv/$NS/${KP}never" "$(qw_kv_put_body '{"v":1}' 600 42)"
eq "an expect:N>0 on an absent key does not apply" "false"  "$(qw_jv '.applied')"
eq "with reason absent"                            "absent" "$(qw_jv '.reason')"
qw_req GET "/api/v1/kv/$NS/${KP}never"
eq "and CREATES NOTHING (§5.3: the repair that matters most)" "false" "$(qw_jv '.found')"

# §5.1: exactly one of ttlSeconds and forever. Zero declarations is the same
# error as two, because both mean the caller did not decide, and a default here
# is how a marker becomes immortal.
qw_req PUT "/api/v1/kv/$NS/${KP}noexpiry" '{"value":{"v":1}}'
eq "a write with no expiry is a 400"          "400" "$QW_RC"
eq "named kv_expiry_not_specified"  "kv_expiry_not_specified" "$(qw_jv '.reason')"
qw_req PUT "/api/v1/kv/$NS/${KP}noexpiry" '{"value":{"v":1},"ttlSeconds":60,"forever":true}'
eq "and so is a write with BOTH"              "400" "$QW_RC"
eq "same reason: the caller did not decide" "kv_expiry_not_specified" "$(qw_jv '.reason')"

# forever:true, the other half of the rule, on the one key allowed to carry it.
# It is inside the purged prefix and the purge runs from an EXIT trap, so a run
# that dies here still leaves nothing immortal behind (§10.4).
qw_req PUT "/api/v1/kv/$NS/${KP}forever:one" "$(qw_kv_put_body '{"v":1}' forever)"
eq "forever:true is accepted"    "true" "$(qw_jv '.applied')"
qw_req GET "/api/v1/kv/$NS/${KP}forever:one"
eq "and the key has no expiry"   "null" "$(qw_jv '.expiresAt')"
qw_req DELETE "/api/v1/kv/$NS/${KP}forever:one"
eq "deleted immediately, as the CI rule requires" "true" "$(qw_jv '.applied')"

# =============================================================================
say ""
say "== 4. incr, getMany and getPrefix: the reads and the counter (§5.4, §5.5) =="
# =============================================================================
# The counter is a FIXED key with a FIXED expected total: it can only be right if
# the purge really emptied the namespace before this run.
qw_req POST /api/v1/kv "$(qw_kv_batch \
  "$(qw_kv_incr "$NS" "${KP}counter:fixed" 2 600)" \
  "$(qw_kv_incr "$NS" "${KP}counter2" 5 600 6)")"
eq "incr accumulates onto the value from section 1" "5" "$(qw_jv '.results[0].value')"
eq "and a first incr under its max applies"         "true" "$(qw_jv '.results[1].applied')"

# `max` makes `applied` the admission decision: the request that would break the
# ceiling does not consume budget, so there is nothing to give back (§5.4).
qw_req POST /api/v1/kv "$(qw_kv_batch "$(qw_kv_incr "$NS" "${KP}counter2" 5 600 6)")"
eq "an incr that would pass max does not apply" "false" "$(qw_jv '.results[0].applied')"
eq "reason:limit"                               "limit" "$(qw_jv '.results[0].reason')"
eq "and the value is the CURRENT one, never the would-be one" "5" "$(qw_jv '.results[0].value')"

# incr takes no expect: it is the way OUT of CAS (§5.4).
qw_req POST /api/v1/kv '[{"op":"incr","ns":"'"$NS"'","key":"'"${KP}counter2"'","delta":1,"ttlSeconds":600,"expect":3}]'
eq "incr with an expect is a 400" "400" "$QW_RC"

# getPrefix: keyset paging, a clamped limit, and the truthful `truncated`.
qw_req POST /api/v1/kv "$(qw_kv_batch "$(qw_kv_get_prefix "$NS" "$KP" 2)")"
eq "a limited page reports truncation"   "true" "$(qw_jv '.results[0].truncated')"
eq "two rows in the page"                "2"    "$(qw_jv '(.results[0].rows // []) | length')"
NEXT="$(qw_jv '.results[0].nextAfter')"
eq "and hands back the exclusive cursor" "$(qw_jv '.results[0].rows[1].key')" "$NEXT"
qw_req POST /api/v1/kv "$(qw_kv_batch "$(qw_kv_get_prefix "$NS" "$KP" 250 "$NEXT" true)")"
eq "the next page starts strictly after the cursor" "true" \
   "$(printf '%s' "$QW_BODY" | jq -r --arg a "$NEXT" '[(.results[0].rows // [])[].key | select(. <= $a)] | length == 0')"
eq "keysOnly omits the values" "0" \
   "$(printf '%s' "$QW_BODY" | jq -r '[(.results[0].rows // [])[] | select(has("value"))] | length')"

# A limit above the cap is CLAMPED, never rejected: a 400 there is an error the
# caller cannot fix without reading the server's configuration (§5.5).
qw_req POST /api/v1/kv "$(qw_kv_batch "$(qw_kv_get_prefix "$NS" "$KP" 999999)")"
rc2xx "an over-large limit is clamped, not refused"

# A namespace is a boundary, not a table to enumerate.
qw_req POST /api/v1/kv '[{"op":"getPrefix","ns":"'"$NS"'","prefix":""}]'
eq "an empty prefix is refused"          "400" "$QW_RC"
eq "named kv_prefix_required" "kv_prefix_required" "$(qw_jv '.reason')"

# The tenant is an argument of the procedure, never a field of an op (§6.1.6).
qw_req POST /api/v1/kv '[{"op":"get","ns":"'"$NS"'","key":"x","tenant":"00000000-0000-0000-0000-0000000000ff"}]'
eq "a tenant field inside an op is refused, never ignored" "400" "$QW_RC"

# =============================================================================
say ""
say "== 5. TTL: an expired key was never there (§5.7) =="
# =============================================================================
qw_req PUT "/api/v1/kv/$NS/${KP}ttl:short" "$(qw_kv_put_body '{"v":1}' 1)"
rc2xx "a key with ttlSeconds:1"
sleep 2
qw_req GET "/api/v1/kv/$NS/${KP}ttl:short"
eq "after its TTL it reads as absent, with no sweeper involved" "false" "$(qw_jv '.found')"
qw_req POST /api/v1/kv "$(qw_kv_batch "$(qw_kv_put_if_absent "$NS" "${KP}ttl:short" '{"v":2}' 600)")"
eq "and an expired row does not block a putIfAbsent" "true" "$(qw_jv '.results[0].applied')"

# =============================================================================
say ""
say "== 6. the four timer routes (§8.1, §4) =="
# =============================================================================
PAY="$(qw_b64 '{"attempt":1}')"
qw_req POST /api/v1/timers "$(qw_timers_batch "$(qw_timer_schedule "$TQ" "${KP}t1" Default 60000 "${KP}txn1" "$PAY")")"
rc2xx "schedule"
eq "status:scheduled on the first insert"  "scheduled" "$(qw_jv '.results[0].status')"
eq "ok:true"                               "true"      "$(qw_jv '.results[0].ok')"
MID="$(qw_jv '.results[0].messageId')"
eq "and the messageId is PROMISED at schedule time, not at fire" "true" \
   "$(printf '%s' "$MID" | grep -Eqi '^[0-9a-f-]{36}$' && echo true || echo false)"
DELIVER="$(qw_jv '.results[0].deliverAt')"

# Same upsert under the same name, so a client retry after a crash is safe by
# construction, and `status` says which of the two happened.
qw_req POST /api/v1/timers "$(qw_timers_batch_operations "$(qw_timer_reschedule "$TQ" "${KP}t1" Default 120000 "${KP}txn1" "$(qw_b64 '{"attempt":2}')")")"
eq "reschedule of a live key reports rescheduled" "rescheduled" "$(qw_jv '.results[0].status')"
eq "and it moved the delivery"  "true" \
   "$(printf '%s' "$(qw_jv '.results[0].deliverAt')" | awk -v a="$DELIVER" '{print ($0 > a) ? "true" : "false"}')"

qw_req GET "/api/v1/timers/$TQ/${KP}t1"
eq "peek finds it"                       "true"  "$(qw_jv '.found')"
eq "and returns the payload as stored, base64 and opaque (§13.4)" \
   "$(qw_b64 '{"attempt":2}')" "$(qw_jv '.payload')"
eq "a row in nobody's hands reads claimed:false" "false" "$(qw_jv '.claimed')"
eq "attempts start at zero after a reschedule"   "0"     "$(qw_jv '.attempts')"

qw_req GET "/api/v1/timers/$TQ/${KP}nothing"
eq "a peek miss is 200 with found:false, not a 404" "200" "$QW_RC"
eq "found:false" "false" "$(qw_jv '.found')"

# The list is scoped to a queue because the queue is a PATH SEGMENT and not a
# filter: there is no tenant-wide list, on purpose, because that is a scan an end
# user of the customer could trigger (§4.1).
qw_req POST /api/v1/timers "$(qw_timers_batch \
  "$(qw_timer_schedule "$TQ" "${KP}t2" '' 60000 "${KP}txn2" "$PAY")" \
  "$(qw_timer_schedule "$TQ" "${KP}t3" '' 60000 "${KP}txn3" "$PAY")")"
rc2xx "two more timers, in one batch"
eq "a batch answers one result per op, in order" "2" "$(qw_jv '(.results // []) | length')"
eq "a schedule with no partition lands on Default" "Default" \
   "$(qw_req GET "/api/v1/timers/$TQ/${KP}t2"; qw_jv '.partition')"

qw_req GET "/api/v1/timers/$TQ?limit=2"
eq "list pages with a clamped limit"     "2"    "$(qw_jv '(.rows // []) | length')"
eq "and reports truncation truthfully"   "true" "$(qw_jv '.truncated')"
TNEXT="$(qw_jv '.nextAfter')"
eq "the cursor is the last row of the page" "$(qw_jv '.rows[1].timerKey')" "$TNEXT"
qw_req GET "/api/v1/timers/$TQ?after=$TNEXT&limit=100"
eq "the next page is strictly after it, and it is a keyset not an offset" "true" \
   "$(printf '%s' "$QW_BODY" | jq -r --arg a "$TNEXT" '[(.rows // [])[].timerKey | select(. <= $a)] | length == 0')"
eq "the list carries no payload (that is what peek is for)" "0" \
   "$(printf '%s' "$QW_BODY" | jq -r '[(.rows // [])[] | select(has("payload"))] | length')"

# §9.6: the cancel has its own route and its own class, and it is the one call
# that is never blocked. It carries no body; the txn rides in a query parameter.
qw_req DELETE "/api/v1/timers/$TQ/${KP}t1?txn=${KP}txn1"
eq "cancel is 200"           "200"       "$QW_RC"
eq "status:cancelled"        "cancelled" "$(qw_jv '.status')"
eq "ok:true"                 "true"      "$(qw_jv '.ok')"
qw_req DELETE "/api/v1/timers/$TQ/${KP}t1?txn=${KP}txn1"
# §4.4, and this is where a user gets hurt: there is no tombstone. `absent` means
# "no longer pending" and MAY MEAN ALREADY DELIVERED, so it is ok:false and the
# expected txn comes back for the check against the log.
eq "cancelling it again is 200"         "200"      "$QW_RC"
eq "status:absent"                      "absent"   "$(qw_jv '.status')"
eq "ok:FALSE, because absent may mean already delivered (§4.4)" "false" "$(qw_jv '.ok')"
eq "and the txn is echoed back, so the check needs no second API" "${KP}txn1" "$(qw_jv '.txn')"

# The batch route takes cancels too, and they inherit ITS authorization class.
qw_req POST /api/v1/timers "$(qw_timers_batch "$(qw_timer_cancel "$TQ" "${KP}t2" "${KP}txn2")")"
eq "a cancel through the batch route works as well" "cancelled" "$(qw_jv '.results[0].status')"

# --- the refusals this route owns -------------------------------------------
qw_req POST /api/v1/timers "[{\"op\":\"schedule\",\"queue\":\"$TQ\",\"timerKey\":\"${KP}bad\",\"txn\":\"x\",\"payload\":\"e30=\"}]"
eq "a schedule with no delayMs is a 400"      "400" "$QW_RC"
eq "named"  "timers_delay_required" "$(qw_jv '.reason')"

# The horizon is FINITE by default (90 days) and a breach is 403, not 400: it is
# a plan verdict and §9.5 keeps the two apart.
qw_req POST /api/v1/timers "$(qw_timers_batch "$(qw_timer_schedule "$TQ" "${KP}bad" '' 999999999999 x e30=)")"
eq "a delay beyond the horizon is 403, not 400" "403" "$QW_RC"
eq "with its own code" "timer_horizon_exceeded" "$(qw_jv '.error')"

qw_req POST /api/v1/timers "[{\"op\":\"schedule\",\"queue\":\"$TQ\",\"timerKey\":\"${KP}bad\",\"delayMs\":1000,\"txn\":\"x\",\"payload\":\"not-base64!!\"}]"
eq "a payload that is not base64 is a 400" "400" "$QW_RC"
eq "named" "timers_payload_not_base64" "$(qw_jv '.reason')"

# The forged-provenance hole: the stored procedure cannot tell the broker's own
# _messageId from a client's, so the HTTP edge is where a supplied one dies.
for F in '"producerSub":"billing-service"' '"_messageId":"11111111-1111-7111-8111-111111111111"'; do
  qw_req POST /api/v1/timers "[{\"op\":\"schedule\",\"queue\":\"$TQ\",\"timerKey\":\"${KP}bad\",\"delayMs\":1000,\"txn\":\"x\",\"payload\":\"e30=\",$F}]"
  case "$QW_RC" in
    4??) ok "a timer op carrying ${F%%:*} is refused ($QW_RC)";;
    *)   bad "a timer op carrying ${F%%:*} answered $QW_RC: provenance is forgeable";;
  esac
done

# =============================================================================
say ""
say "== 7. a timer becomes a message, with the id it was promised (§4.2) =="
# =============================================================================
# The only assertion in this file that depends on the sweeper, collecting the
# timer scheduled during the preflight. It is here because nothing else in CI
# proves the promise of the product's second feature THROUGH HTTP: that a
# scheduled timer turns into a poppable frame, under the txn it was given and the
# messageId the schedule already handed back.
GOT=""
for _ in $(seq 1 60); do
  sleep 1
  qw_req GET "/api/v1/pop/queue/$FQ?batch=10&consumerGroup=$GRP&subscriptionMode=all&autoAck=true&wait=false"
  GOT="$(printf '%s' "$QW_BODY" | jq -r --arg t "$FTXN" '[(.messages // [])[] | select(.transactionId == $t)][0] // empty' 2>/dev/null)"
  [ -n "$GOT" ] && break
done
if [ -n "$GOT" ]; then
  ok "the timer fired into $FQ within the sweeper's cycle"
  eq "the delivered frame carries the messageId promised at schedule time" \
     "$FMID" "$(printf '%s' "$GOT" | jq -r '.id')"
  eq "and the payload the schedule carried, decoded" "true" \
     "$(printf '%s' "$GOT" | jq -r '.data.fired | tostring')"
else
  # Say WHICH of the two failures it is, or the next reader re-runs it three
  # times and calls it flaky. A row still in the table means the sweeper never
  # took it (not running, or wedged); no row and no message means it fired and
  # the frame went somewhere else, and `attempts`/`lastError` name the reason.
  bad "no timer frame reached $FQ in 60s (is the sweeper running? QUEEN_SWEEPER=true)"
  qw_req GET "/api/v1/timers/$FQ/${KP}fire1"
  say "         the timer row now: $(printf '%s' "$QW_BODY" | jq -c '{found,attempts,lastError,claimed,deliverAt}' 2>/dev/null || printf '%s' "$QW_BODY" | head -c 200)"
fi

# =============================================================================
say ""
say "== 8. the transaction wire: three sibling arrays and one flat result space =="
# =============================================================================
# The depth of this is txnsemantics' job (atomicity, rollback, alignment). What
# is asserted here is the SHAPE: which spellings the wire accepts, which it
# refuses, and by what name.
# One fresh transaction id per push, for the dedup reason at the top of the file:
# a fixed one is a duplicate on the next run inside the dedup window, and the
# bundle would come back `reason:"duplicate"` with nothing wrong in this file.
PUSH="$(qw_push_op "$FQ" Default '{"id":"wire-1"}' "${KP}push-$RUN-1")"
GATE="$(qw_kv_put_if_absent "$NS" "${KP}wire:gate" '{"who":"bundle"}' 600 true)"
TIMER="$(qw_timer_schedule "$TQ" "${KP}wire:t" Default 60000 "${KP}wire-txn" "$PAY")"

if qw_commit "$(qw_txn_body "$(qw_ops "$PUSH")" "$(qw_ops "$GATE")" "$(qw_ops "$TIMER")")"; then
  ok "a full bundle commits, and commit returns"
else
  bad "the full bundle raised: $QW_ERROR"
fi
eq "verdict"                                  "committed" "${QW_VERDICT:-}"
eq "one result per FLAT ordinal, riders APPENDED after the operations" "3" "$(qw_jv '(.results // []) | length')"
eq "and each result names the kind of op at ITS flat ordinal" "push,kv,timer" \
   "$(qw_jv '[.results[].type] | join(",")')"
eq "the riders also carry their own array's ordinal, so the map is inspectable" "0,0" \
   "$(qw_jv '[.results[] | select(.type=="kv" or .type=="timer") | .opIndex] | join(",")')"

# A bundle with no riders must produce exactly today's results[]: a push never
# changes ordinal because a rider exists (§8.2).
qw_commit "$(qw_txn_body "$(qw_ops \
  "$(qw_push_op "$FQ" Default '{"id":"wire-2"}' "${KP}push-$RUN-2")" \
  "$(qw_push_op "$FQ" Default '{"id":"wire-3"}' "${KP}push-$RUN-3")")")"
eq "a bundle with no riders answers with today's shape" "2" "$(qw_jv '(.results // []) | length')"
eq "and nothing but pushes in it" "push,push" "$(qw_jv '[.results[].type] | join(",")')"

# `null` riders: what every serializer emits for an unset optional field.
qw_commit "$(qw_txn_body "$(qw_ops "$(qw_push_op "$FQ" Default '{"id":"wire-4"}' "${KP}push-$RUN-4")")" null null)"
eq "explicit nulls are accepted as absent" "committed" "${QW_VERDICT:-}"

# A KV-only bundle is routed off the wire onto the short KV transaction (§2.5),
# and that must be invisible from out here: same envelope, same result shape.
qw_commit "$(qw_txn_body "$(qw_ops)" "$(qw_ops "$(qw_kv_put "$NS" "${KP}wire:kvonly" '{"n":1}' 600)")")"
eq "a KV-only bundle commits"                    "committed" "${QW_VERDICT:-}"
eq "and still answers in the transaction's shape" "kv"        "$(qw_jv '.results[0].type')"

# A timers-only bundle is deliberately NOT routed off the wire (§2.5 names KV
# only), which again must be invisible from here.
qw_commit "$(qw_txn_body "$(qw_ops)" '' "$(qw_ops "$(qw_timer_schedule "$TQ" "${KP}wire:t2" '' 60000 "${KP}wire-txn2" "$PAY")")")"
eq "a timers-only bundle commits" "committed" "${QW_VERDICT:-}"
eq "with a timer result"          "timer"     "$(qw_jv '.results[0].type')"

# The precondition, through the real broker and the real client: 200, a returned
# verdict, and a failedIndex in the FLAT space (the losing gate is at flat 1,
# and it is kv-array ordinal 0 — a broker reporting the array ordinal says 0).
if qw_commit "$(qw_txn_body \
  "$(qw_ops "$(qw_push_op "$FQ" Default '{"id":"wire-5"}' "${KP}push-$RUN-5")")" \
  "$(qw_ops "$(qw_kv_put_if_absent "$NS" "${KP}wire:gate" '{"who":"loser"}' 600 true)")")"; then
  ok "a lost gate RETURNS from commit instead of raising (§8.3)"
else
  bad "a lost gate raised: $QW_ERROR"
fi
eq "verdict"      "kv_precondition" "${QW_VERDICT:-}"
eq "kvReason"     "exists"          "${QW_KV_REASON:-}"
eq "failedIndex is the FLAT ordinal, not the kv array's (§8.2 point 4)" "1" "${QW_FAILED_INDEX:-}"
eq "and the winner's value rides along" "bundle" \
   "$(printf '%s' "${QW_VALUE:-}" | jq -r '.who' 2>/dev/null)"

# --- the spellings the wire refuses, each with its own name ------------------
qw_req POST /api/v1/transaction "{\"operations\":[{\"type\":\"kv\",\"op\":\"put\",\"ns\":\"$NS\",\"key\":\"${KP}inline\",\"value\":{\"x\":1},\"ttlSeconds\":60}]}"
eq "a kv op INSIDE operations[] is a named 400, never a silent drop (§10.4)" "400" "$QW_RC"
qw_req GET "/api/v1/kv/$NS/${KP}inline"
eq "and it wrote nothing, so the 400 is not cosmetic" "false" "$(qw_jv '.found')"

qw_req POST /api/v1/transaction '{"operations":[],"kv":{"op":"put"}}'
eq "a rider that is not an array is refused" "400" "$QW_RC"
eq "and the message says where it belongs" "true" \
   "$(qw_jv '.error' | grep -q 'TOP LEVEL' && echo true || echo false)"

# getPrefix is refused inside a transaction: read work whose cost the caller does
# not bound a priori, under the outermost lock space of the product (§5.5).
qw_req POST /api/v1/transaction "{\"operations\":[],\"kv\":[{\"op\":\"getPrefix\",\"ns\":\"$NS\",\"prefix\":\"$KP\"}]}"
eq "getPrefix inside a transaction is a 400" "400" "$QW_RC"
eq "on the wire and not only in the documentation" "true" \
   "$(qw_jv '.error' | grep -q 'getPrefix' && echo true || echo false)"

# =============================================================================
say ""
say "== 9. cleanupTestData, asserted =="
# =============================================================================
cleanup_test_data
qw_req POST /api/v1/kv "$(qw_kv_batch "$(qw_kv_get_prefix "$NS" "$KP" 250 '' true)")"
eq "the namespace is empty again" "0" "$(printf '%s' "$QW_BODY" | jq -r '(.results[0].rows // []) | length')"
qw_req GET "/api/v1/timers/$TQ?limit=1000"
eq "and so is the timer queue"    "0" "$(printf '%s' "$QW_BODY" | jq -r '(.rows // []) | length')"

say ""
say "passed: $PASS   failed: $FAIL"
if [ "$FAIL" = "0" ]; then say "HTTPWIRE: PASS"; exit 0; fi
say "HTTPWIRE: FAIL"
exit 1
