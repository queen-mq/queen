#!/usr/bin/env bash
# =============================================================================
#  TRANSACTIONAL SEMANTICS GATE
#  PLAN_KV_TIMERS.md §15 row "Transazionali", phase F4. MERGE CRITERION.
#
#  ---------------------------------------------------------------------------
#  WHAT THIS FILE IS FOR.
#
#  PLAN_KV_TIMERS.md §0 says the value of the product is not the KV store: it is
#  that the idempotency marker, the effect and the cursor advance COMMIT
#  TOGETHER. Everything else in the plan is machinery for that one sentence.
#  This file is the sentence, written as four assertions:
#
#    1. A LOST GATE BLOCKS THE PUSH AND THE TIMER.
#       No message, no timer row, no state row. One losing precondition rolls
#       back every effect of the bundle, in all three spaces at once. If this
#       ever goes green-on-partial the product is a KV store bolted next to a
#       queue, which is exactly what §0 says it must not be.
#
#    2. COMMIT DOES NOT RAISE ON THE PRECONDITION.
#       Losing the marker is the EXPECTED outcome of every legitimate
#       redelivery (§2.5 point 1: "il fallimento e' il percorso comune"). It
#       comes back as HTTP 200 with success:false and a machine-readable
#       reason:"kv_precondition" — a verdict, not an error. If it arrived as a
#       4xx/5xx it would poison every retry policy, every dashboard and every
#       error budget in the product, on its single most frequent code path.
#
#    3. AN ACK THAT FAILS ON AN EXPIRED LEASE ANNULS THE KV WRITE.
#       This is the one that is easy to get wrong and impossible to notice.
#       §5.2: "la transazione di ack e' il recinto primario, expect e'
#       l'asserzione secondaria" — a CAS cannot stop a zombie worker, because
#       an expect against a still-matching version SUCCEEDS from a zombie. The
#       KV write in this test carries forever:true and NO expect, deliberately,
#       so that when it does not land there is exactly one possible reason: the
#       transaction. Not the TTL. Not expect.
#       The control case that follows (fresh lease, same write, must land)
#       exists so a broker that simply never writes KV cannot pass this.
#
#    4. RESULTS ARE INDEX-ALIGNED IN THE FLAT SPACE.
#       §8.2 is the layer no design document had: the client sends operations[]
#       plus two sibling arrays, the SP speaks parallel arrays, and something in
#       the middle has to scatter the per-array results back to flat ordinals.
#       §6.4's count guard cannot be expressed across two index spaces. The
#       cheap bug is failedIndex reported in the kv array's ordinal space, which
#       makes every client blame the wrong operation — so this file pins a
#       bundle where the two numbers DIFFER.
#
#  All four are HTTP-only assertions on purpose: they are the contract seven
#  clients depend on, so they must hold at the wire, not just in the database.
#
#  ---------------------------------------------------------------------------
#  THE WIRE SHAPE THIS FILE ASSERTS, AND WHY IT IS NOT `type:"kv"`.
#
#  KV and timer operations are TOP-LEVEL ARRAYS of the request body — `kv` and
#  `timers`, siblings of `operations` — never elements of `operations` with a
#  `type` discriminator. An element carrying `type:"kv"` or `type:"timer"` is a
#  400 that names where it should have gone.
#
#  This is §10.4, and the reason is Go: `Operation` cannot grow. Two Go struct
#  fields with the same JSON key at the same level are BOTH DROPPED by
#  encoding/json, silently — the body would leave with zero kv ops, the broker
#  would commit a transaction with no gate, and the putIfAbsent would simply
#  never have existed. There is no error and no warning anywhere in that chain.
#  §6.3 agrees in the other direction: 005_log_ack.sql reads `p->'kv'` and
#  `p->'timers'` off the payload root.
#
#  So the FLAT SPACE IS APPEND-ONLY, in exactly this order:
#
#      [0, ops_flat)                 operations[], expanded (items[] counts once
#                                    per item — that is what makes the space
#                                    non-trivial and is why §8.2 needs a map)
#      [ops_flat, +kv_n)             the top-level `kv` array
#      [ops_flat + kv_n, +timers_n)  the top-level `timers` array
#
#  A push or an ack therefore NEVER changes ordinal because a rider was added,
#  and a bundle with neither array produces exactly today's results[]. Each
#  result carries `index` (flat) and, for the two riders, `opIndex` (the ordinal
#  inside its own array) so the mapping stays inspectable from outside.
#
#  One more shape detail this file depends on: a timer `payload` is BASE64 TEXT,
#  not a JSON object. It is opaque bytes end to end (it may be zstd-compressed
#  and it may be encrypted, §13.4), so there is no point in the wire at which it
#  is a JSON value.
#  ---------------------------------------------------------------------------
# =============================================================================
set -uo pipefail

URL="${QUEEN_HTTP_URL:-http://localhost:16633}"
RID="ts$$"
NS="txnsem"
Q="txnsem-q-$RID"
LQ="txnsem-lease-$RID"
TQ="txnsem-timers-$RID"
GRP="txnsem-grp"

PASS=0; FAIL=0
say()  { printf '%s\n' "$*"; }
ok()   { PASS=$((PASS+1)); say "  ok   - $1"; }
bad()  { FAIL=$((FAIL+1)); say "  FAIL - $1"; }
eq()   { if [ "$2" = "$3" ]; then ok "$1"; else bad "$1 (want '$2', got '$3')"; fi; }

TMP="$(mktemp -d -t queen-txnsem.XXXXXX)"
trap 'rm -rf "$TMP"' EXIT
BODYF="$TMP/body"

req() { # method path [body] -> RC, BODY
  local m=$1 p=$2 b=${3:-}
  local args=(-s -o "$BODYF" -w '%{http_code}' --max-time 30 -X "$m")
  [ -n "$b" ] && args+=(-H 'Content-Type: application/json' --data-binary "$b")
  RC=$(curl "${args[@]}" "$URL$p" 2>/dev/null) || RC="000"
  BODY=$(cat "$BODYF" 2>/dev/null)
}
# `tostring`, never `// "default"`: jq's `//` treats a literal false as empty,
# which would silently turn a real "success":false into the fallback — the exact
# trap the tenancy runner documents.
jv() { printf '%s' "${BODY:-}" | jq -r "($1) | tostring" 2>/dev/null || echo "?"; }
nmsgs() { [ -n "${BODY:-}" ] || { echo 0; return; }; printf '%s' "$BODY" | jq -r '(.messages // []) | length' 2>/dev/null || echo 0; }
# Three-way, never two-way. A read-back that answers neither true nor false (a
# 404, an empty body, a 5xx) means the assertion COULD NOT BE MADE — reporting
# that as "the row exists" would send the next reader hunting a rollback bug
# that is really a missing route.
absent() { # found-value  ok-label  present-label
  case "$1" in
    false) ok "$2" ;;
    true)  bad "$3" ;;
    *)     bad "$3 -- unreadable, found='$1' body=$(printf '%s' "${BODY:-}" | head -c 160)" ;;
  esac
}

# --- preflight --------------------------------------------------------------
say "== preflight =="
export QUEEN_WAIT_URLS="${QUEEN_WAIT_URLS:-$URL/health}"
if command -v wait-for-broker >/dev/null 2>&1; then wait-for-broker; fi

RC=$(curl -s -o /dev/null -w '%{http_code}' --max-time 10 "$URL/health" 2>/dev/null) || RC=000
[ "$RC" = "200" ] || { say "!! broker not healthy at $URL (health -> $RC)"; say "TXNSEMANTICS: FAIL"; exit 1; }
ok "broker healthy at $URL"

# Both surfaces are part of the engine and no flag can withhold them, so this is
# not a "was the feature enabled" check: it establishes that the thing answering
# at $URL is a broker of this build. If it is not, the wire demux answers 400
# "supports only push and ack operations" and every assertion below is vacuously
# "no push happened". Refuse rather than report that as green.
NOTBROKER="the routes are on every broker of this build, so check that QUEEN_HTTP_URL is not the proxy or an older image"
req PUT "/api/v1/kv/$NS/preflight:$RID" '{"value":{"p":1},"ttlSeconds":60}'
KVRC="$RC"
case "$KVRC" in
  2??) ok "kv surface answers ($KVRC)";;
  *)   say "!! kv surface answered $KVRC: $(head -c 300 "$BODYF")"
       say "!! $NOTBROKER"
       say "TXNSEMANTICS: FAIL"; exit 1;;
esac

# Both surfaces, not one. Scenario 1 asserts "no timer row", which a broker that
# never had the timer route satisfies vacuously.
req GET "/api/v1/timers/preflight-$RID/nothing"
case "$RC" in
  2??) ok "timer surface answers ($RC)";;
  *)   say "!! timer surface answered $RC: $(head -c 300 "$BODYF")"
       say "!! $NOTBROKER"
       say "TXNSEMANTICS: FAIL"; exit 1;;
esac

req POST /api/v1/configure "{\"queue\":\"$Q\",\"leaseTime\":30,\"retryLimit\":5}"
req POST /api/v1/configure "{\"queue\":\"$LQ\",\"leaseTime\":30,\"retryLimit\":5}"
ok "queues configured"

# =============================================================================
say ""
say "== 1. a lost gate blocks the push AND the timer (no push, no timer, no state row) =="
# =============================================================================
GATE="gate:$RID"
STATE="state:$RID"
TK="tk:$RID"
PUSHTXN="txnsem-push-$RID"
TIMERTXN="txnsem-timer-$RID"

# --- CONTROL FIRST. -----------------------------------------------------------
# The negatives below ("no message", "no timer", "no state row") are all
# satisfied by a broker that simply drops kv/timer bundles on the floor. So the
# IDENTICAL bundle shape runs first against an UNTAKEN gate and must deliver all
# three effects. Only then does the absence of those same three effects mean
# "the gate rolled them back".
CGATE="gate-ctl:$RID"; CSTATE="state-ctl:$RID"; CTK="tk-ctl:$RID"
req POST /api/v1/transaction "{
  \"operations\":[
    {\"type\":\"push\",\"queue\":\"$Q\",\"partition\":\"Default\",\"payload\":{\"marker\":\"ctl-$RID\"},\"transactionId\":\"txnsem-ctl-$RID\"}],
  \"kv\":[
    {\"op\":\"putIfAbsent\",\"ns\":\"$NS\",\"key\":\"$CGATE\",\"value\":{\"who\":\"ctl\"},\"ttlSeconds\":600,\"required\":true},
    {\"op\":\"put\",\"ns\":\"$NS\",\"key\":\"$CSTATE\",\"value\":{\"step\":\"done\"},\"ttlSeconds\":600}],
  \"timers\":[
    {\"op\":\"schedule\",\"queue\":\"$TQ\",\"timerKey\":\"$CTK\",\"partition\":\"Default\",\"delayMs\":60000,\"txn\":\"txnsem-ctltimer-$RID\",\"payload\":\"$(printf '{"marker":"ctl"}' | base64)\"}]
}"
eq "CONTROL: the same bundle shape with a WON gate commits" "true" "$(jv '.success')"
req GET "/api/v1/pop/queue/$Q?batch=10&consumerGroup=$GRP&subscriptionMode=all&autoAck=true&wait=false"
eq "CONTROL: and its push is deliverable" "1" "$(nmsgs)"
req GET "/api/v1/timers/$TQ/$CTK"
eq "CONTROL: and its timer row exists" "true" "$(jv '.found')"
req GET "/api/v1/kv/$NS/$CSTATE"
eq "CONTROL: and its state row exists" "true" "$(jv '.found')"
req DELETE "/api/v1/timers/$TQ/$CTK" '{}'

# --- now the real thing -------------------------------------------------------
# The winner takes the gate. forever:true so no TTL can be blamed later.
req PUT "/api/v1/kv/$NS/$GATE" '{"value":{"who":"first"},"forever":true}'
case "$RC" in 2??) ok "gate seeded by the winner";; *) bad "seeding the gate returned $RC: $BODY";; esac

# The loser's bundle. FOUR flat ordinals, laid out so the losing gate does NOT
# sit at flat 0: one push comes first, so the gate is kv-array ordinal 0 but
# flat ordinal 1. Scenario 2's failedIndex therefore has an answer that
# discriminates — a broker reporting the array ordinal would say 0.
#
#   flat 0  push          (operations[0])
#   flat 1  kv  putIfAbsent   <- kv[0], THE LOSER
#   flat 2  kv  put           <- kv[1], the sibling write
#   flat 3  timer schedule    <- timers[0]
LOSER_PAYLOAD_B64=$(printf '{"marker":"%s"}' "$RID" | base64)
LOSER_BUNDLE=$(cat <<JSON
{"operations":[
  {"type":"push","queue":"$Q","partition":"Default","payload":{"marker":"$RID"},"transactionId":"$PUSHTXN"}
],
"kv":[
  {"op":"putIfAbsent","ns":"$NS","key":"$GATE","value":{"who":"second"},"ttlSeconds":600,"required":true},
  {"op":"put","ns":"$NS","key":"$STATE","value":{"step":"done"},"ttlSeconds":600}
],
"timers":[
  {"op":"schedule","queue":"$TQ","timerKey":"$TK","partition":"Default","delayMs":60000,"txn":"$TIMERTXN","payload":"$LOSER_PAYLOAD_B64"}
]}
JSON
)
req POST /api/v1/transaction "$LOSER_BUNDLE"
LOSER_RC="$RC"; LOSER_BODY="$BODY"

# --- 1a. the push did not happen
req GET "/api/v1/pop/queue/$Q?batch=10&consumerGroup=$GRP&subscriptionMode=all&wait=false"
N=$(nmsgs)
eq "no message reached $Q (the push rolled back with the gate)" "0" "$N"

# --- 1b. the timer row does not exist
req GET "/api/v1/timers/$TQ/$TK"
absent "$(jv '.found')" \
  "no timer row for $TK (the schedule rolled back with the gate)" \
  "timer $TK survives a lost gate: the bundle was not atomic"

# --- 1c. the OTHER kv write in the same bundle did not land either.
# This is the assertion that separates "the bundle rolled back" from "the kv
# op that lost was skipped": the state row would have applied cleanly on its own.
req GET "/api/v1/kv/$NS/$STATE"
absent "$(jv '.found')" \
  "no state row for $STATE (the sibling kv write rolled back too)" \
  "state row $STATE EXISTS after a lost gate: the bundle was not atomic"

# --- 1d. and the winner's value is untouched
req GET "/api/v1/kv/$NS/$GATE"
eq "the winner's gate value survived" "first" "$(jv '.value.who')"

# =============================================================================
say ""
say "== 2. commit does not raise on the precondition: 200 + success:false + reason =="
# =============================================================================
BODY="$LOSER_BODY"; RC="$LOSER_RC"
eq "HTTP status of a lost precondition is 200, not 4xx/5xx" "200" "$LOSER_RC"
# §15 states the verdict shape as {success:false, reason:'kv_precondition'};
# §8.3's prose writes the boolean as `ok:false`. `success` is the field the
# existing wire already returns (txn_fail_body), and it is the one ratified in
# the F4 brief, so it is what this gate asserts. `ok` may be present as well.
eq "success:false" "false" "$(jv '.success')"
eq "reason is the machine-readable kv_precondition" "kv_precondition" "$(jv '.reason')"
# 1, not 0: one push precedes the kv array, so the flat ordinal and the kv-array
# ordinal differ. A broker answering 0 is reporting the array ordinal.
LOSER_FI=$(jv '.failedIndex')
if [ "$LOSER_FI" = "1" ]; then
  ok "failedIndex names the losing op in FLAT space (1, not the kv array's 0)"
elif [ "$LOSER_FI" = "0" ]; then
  bad "failedIndex is 0: that is the kv ARRAY ordinal, not the flat one (§8.2 point 4)"
else
  bad "failedIndex is '$LOSER_FI'; expected 1 (flat ordinal of the losing op)"
fi
eq "kvReason discriminates WHY it lost" "exists" "$(jv '.kvReason')"

# §6.1 point 5: the loser must not need a second round trip. The winner's value
# and version ride back on the failure.
VER=$(jv '.version')
case "$VER" in ''|*[!0-9]*) bad "version of the winner missing from the verdict (got '$VER')";; *) ok "the winner's version rides back ($VER)";; esac
eq "the winner's VALUE rides back, so the loser needs no kv.get" "first" "$(jv '.value.who')"

# §13.5: the RAISE MESSAGE is opaque; namespace and key live only in the DETAIL,
# which the broker reads programmatically. They must not reach the client-facing
# error string, because handlers forward it into shared logs and error trackers.
ERRTXT=$(printf '%s' "$LOSER_BODY" | jq -r '.error // ""' 2>/dev/null)
case "$ERRTXT" in
  *"$GATE"*|*"$NS"*) bad "the error string leaks the namespace/key: '$ERRTXT'";;
  *)                 ok "the error string is opaque (no namespace, no key)";;
esac

# Contrast: a genuine failure must NOT be labelled kv_precondition, or the
# client's switch on reason is meaningless.
req POST /api/v1/transaction "{\"operations\":[{\"type\":\"ack\",\"transactionId\":\"does-not-exist-$RID\",\"partitionId\":\"00000000-0000-0000-0000-000000000000\",\"consumerGroup\":\"$GRP\",\"status\":\"completed\",\"leaseId\":\"nope-$RID\"}],\"requiredLeases\":[\"nope-$RID\"]}"
eq "a genuine failure still reports success:false" "false" "$(jv '.success')"
R2=$(jv '.reason')
if [ "$R2" != "kv_precondition" ]; then
  ok "a genuine failure is NOT labelled kv_precondition (reason='$R2')"
else
  bad "a bogus ack was labelled kv_precondition: the reason field carries no information"
fi

# =============================================================================
say ""
say "== 3. an ack that fails on an expired lease ANNULS the kv write =="
# =============================================================================
ZK="zombie:$RID"
ZK2="zombie-control:$RID"

req POST /api/v1/push "{\"items\":[{\"queue\":\"$LQ\",\"partition\":\"Default\",\"payload\":{\"z\":1}}]}"
case "$RC" in 2??) ok "seeded one message on $LQ";; *) bad "seeding $LQ returned $RC";; esac

# leaseSeconds=1 is the per-request override (RUSTFIX item 18): a one-second
# lease, then we sleep past it. The worker that comes back after this sleep is a
# zombie by definition — it still believes it holds the batch.
req GET "/api/v1/pop/queue/$LQ?batch=1&consumerGroup=$GRP&subscriptionMode=all&leaseSeconds=1&wait=false"
PID=$(jv '.messages[0].partitionId'); ZTXN=$(jv '.messages[0].transactionId'); LEASE=$(jv '.messages[0].leaseId')
if [ -z "$PID" ] || [ "$PID" = "?" ] || [ "$PID" = "null" ]; then
  bad "could not pop a leased message from $LQ; scenario 3 cannot run"
else
  ok "popped one message with a 1s lease (lease=$LEASE)"
  say "     waiting out the lease"
  sleep 4

  # THE ZOMBIE BUNDLE. Note what the kv op does NOT carry:
  #   * no `expect`      -> a CAS cannot be what stops it
  #   * `forever:true`   -> a TTL cannot be what stops it
  # If the row is absent afterwards, the transaction is the only thing left that
  # could have stopped it. That is §5.2's hierarchy, made falsifiable.
  req POST /api/v1/transaction "{
    \"operations\":[
      {\"type\":\"ack\",\"transactionId\":\"$ZTXN\",\"partitionId\":\"$PID\",\"consumerGroup\":\"$GRP\",\"leaseId\":\"$LEASE\",\"status\":\"completed\"}],
    \"kv\":[{\"op\":\"put\",\"ns\":\"$NS\",\"key\":\"$ZK\",\"value\":{\"wrote\":\"zombie\"},\"forever\":true}],
    \"requiredLeases\":[\"$LEASE\"]}"
  eq "the zombie bundle is rejected (expired lease)" "false" "$(jv '.success')"

  req GET "/api/v1/kv/$NS/$ZK"
  absent "$(jv '.found')" \
    "the zombie's kv write did NOT land -- the transaction stopped it, not the TTL and not expect" \
    "the zombie's kv write LANDED: a stale worker can still overwrite shared state"

  # CONTROL. Same write, same shape, but with a live lease. Without this a broker
  # that never applies kv writes at all would pass the assertion above.
  req GET "/api/v1/pop/queue/$LQ?batch=1&consumerGroup=$GRP&leaseSeconds=60&wait=false"
  PID2=$(jv '.messages[0].partitionId'); ZTXN2=$(jv '.messages[0].transactionId'); LEASE2=$(jv '.messages[0].leaseId')
  if [ -z "$PID2" ] || [ "$PID2" = "?" ] || [ "$PID2" = "null" ]; then
    bad "the message was not redelivered after the lease expired; the control case cannot run"
  else
    req POST /api/v1/transaction "{
      \"operations\":[
        {\"type\":\"ack\",\"transactionId\":\"$ZTXN2\",\"partitionId\":\"$PID2\",\"consumerGroup\":\"$GRP\",\"leaseId\":\"$LEASE2\",\"status\":\"completed\"}],
      \"kv\":[{\"op\":\"put\",\"ns\":\"$NS\",\"key\":\"$ZK2\",\"value\":{\"wrote\":\"live\"},\"forever\":true}],
      \"requiredLeases\":[\"$LEASE2\"]}"
    eq "CONTROL: the same bundle with a LIVE lease commits" "true" "$(jv '.success')"
    req GET "/api/v1/kv/$NS/$ZK2"
    eq "CONTROL: and its kv write landed" "true" "$(jv '.found')"
  fi
fi

# =============================================================================
say ""
say "== 4. results are index-aligned in the FLAT space =="
# =============================================================================
# Three operations plus two riders, SEVEN flat ordinals: the first push carries
# items[] and expands to two. That expansion is what makes the flat space
# non-trivial and is the reason §8.2's map cannot just be "op number".
#
#   flat 0  push   (operations[0].items[0])
#   flat 1  push   (operations[0].items[1])
#   flat 2  push   (operations[1])
#   flat 3  kv     put            <- kv[0]
#   flat 4  kv     get            <- kv[1]
#   flat 5  timer  schedule       <- timers[0]
#   flat 6  timer  cancel         <- timers[1]
#
# The cancel targets a key scheduled by a PREVIOUS request, never one scheduled
# in this same bundle: log_timers_apply_v1 applies ops in (queue, timer_key)
# order, so a schedule and a cancel of the same key inside one array have an
# ordering this gate has no business pinning. Alignment is what is under test.
ALIGN_TK="align:$RID"
ALIGN_TK2="align2:$RID"
req POST /api/v1/timers "[{\"op\":\"schedule\",\"queue\":\"$TQ\",\"timerKey\":\"$ALIGN_TK2\",\"partition\":\"Default\",\"delayMs\":60000,\"txn\":\"align2-$RID\",\"payload\":\"$(printf '{"i":9}' | base64)\"}]"
case "$RC" in 2??) : ;; *) bad "seeding the cancel target returned $RC: $BODY";; esac

req POST /api/v1/transaction "{
  \"operations\":[
    {\"type\":\"push\",\"items\":[
       {\"queue\":\"$Q\",\"partition\":\"Default\",\"payload\":{\"a\":0}},
       {\"queue\":\"$Q\",\"partition\":\"Default\",\"payload\":{\"a\":1}}]},
    {\"type\":\"push\",\"queue\":\"$Q\",\"partition\":\"Default\",\"payload\":{\"a\":4}}],
  \"kv\":[
    {\"op\":\"put\",\"ns\":\"$NS\",\"key\":\"align:$RID\",\"value\":{\"i\":3},\"ttlSeconds\":600},
    {\"op\":\"get\",\"ns\":\"$NS\",\"key\":\"align:$RID\"}],
  \"timers\":[
    {\"op\":\"schedule\",\"queue\":\"$TQ\",\"timerKey\":\"$ALIGN_TK\",\"partition\":\"Default\",\"delayMs\":60000,\"txn\":\"align-$RID\",\"payload\":\"$(printf '{"i":5}' | base64)\"},
    {\"op\":\"cancel\",\"queue\":\"$TQ\",\"timerKey\":\"$ALIGN_TK2\"}]
}"
eq "the mixed bundle commits" "true" "$(jv '.success')"
eq "results has one entry per FLAT ordinal" "7" "$(jv '(.results // []) | length')"
# index must equal position, and no ordinal may be a silent null (§6.4: an
# unfilled ordinal becoming a JSON null in place is the exact disalignment class
# 003_log_push.sql:372-375 fails loudly on).
NRES=$(printf '%s' "$BODY" | jq -r '(.results // []) | length' 2>/dev/null)
BADIDX=$(printf '%s' "$BODY" | jq -r '[(.results // []) | to_entries[] | select(.value == null or (.value.index != .key)) | .key] | join(",")' 2>/dev/null)
# Guarded on NRES: on an empty results[] the jq below returns the empty string,
# which would read as "all aligned" — a green that means nothing ran.
if [ "$NRES" != "7" ]; then
  bad "cannot check alignment: results has $NRES entries, not 7"
elif [ -z "$BADIDX" ]; then
  ok "every results[i].index == i, and no ordinal is null"
else
  bad "results misaligned or null at flat ordinal(s): $BADIDX"
fi
WANT="push,push,push,kv,kv,timer,timer"
GOT=$(printf '%s' "$BODY" | jq -r '[(.results // [])[] | (.type // "?")] | join(",")' 2>/dev/null)
eq "each result carries the type of the op at ITS flat ordinal" "$WANT" "$GOT"
# And the ARRAY-LOCAL ordinal survives alongside the flat one, so the mapping is
# inspectable from outside instead of being a broker-private convention.
WANTOP="0,1,0,1"
GOTOP=$(printf '%s' "$BODY" | jq -r '[(.results // [])[] | select(.type=="kv" or .type=="timer") | (.opIndex|tostring)] | join(",")' 2>/dev/null)
eq "each rider result also carries its own array's ordinal (opIndex)" "$WANTOP" "$GOTOP"

say ""
say "-- and failedIndex is flat, not the kv array's ordinal --"
# Flat layout:
#   0,1  push items[]
#   2    push
#   3    kv put            <- kv array ordinal 0
#   4    kv putIfAbsent    <- kv array ordinal 1, and THE LOSER
#   5    timer schedule
# A broker that reports the kv array's ordinal says 1. The right answer is 4.
req POST /api/v1/transaction "{
  \"operations\":[
    {\"type\":\"push\",\"items\":[
       {\"queue\":\"$Q\",\"partition\":\"Default\",\"payload\":{\"b\":0}},
       {\"queue\":\"$Q\",\"partition\":\"Default\",\"payload\":{\"b\":1}}]},
    {\"type\":\"push\",\"queue\":\"$Q\",\"partition\":\"Default\",\"payload\":{\"b\":2}}],
  \"kv\":[
    {\"op\":\"put\",\"ns\":\"$NS\",\"key\":\"fi-ok:$RID\",\"value\":{\"i\":3},\"ttlSeconds\":600},
    {\"op\":\"putIfAbsent\",\"ns\":\"$NS\",\"key\":\"$GATE\",\"value\":{\"who\":\"third\"},\"ttlSeconds\":600,\"required\":true}],
  \"timers\":[
    {\"op\":\"schedule\",\"queue\":\"$TQ\",\"timerKey\":\"fi:$RID\",\"partition\":\"Default\",\"delayMs\":60000,\"txn\":\"fi-$RID\",\"payload\":\"$(printf '{"b":5}' | base64)\"}]
}"
eq "the bundle with a losing gate at flat 4 returns 200" "200" "$RC"
eq "success:false" "false" "$(jv '.success')"
eq "reason:kv_precondition" "kv_precondition" "$(jv '.reason')"
FI=$(jv '.failedIndex')
if [ "$FI" = "4" ]; then
  ok "failedIndex is 4 (flat), not 1 (the kv array's ordinal)"
elif [ "$FI" = "1" ]; then
  bad "failedIndex is 1: it is the kv ARRAY ordinal. Every client will blame the wrong operation (§8.2 point 4)"
else
  bad "failedIndex is '$FI'; expected 4 (flat ordinal of the losing op)"
fi

# =============================================================================
say ""
say "== 5. the WRONG shape fails loudly, and the riders are refusable as a unit =="
# =============================================================================
# §8.2's "desirable side effect": an unknown op type is a clean, NAMED 400. This
# is the assertion that keeps the Go trap of §10.4 from ever being reintroduced
# quietly — if someone moves kv back inside operations[], the old broker's
# generic "supports only push and ack" is the best case and a silent commit
# without the gate is the worst.
req POST /api/v1/transaction "{\"operations\":[
  {\"type\":\"kv\",\"op\":\"put\",\"ns\":\"$NS\",\"key\":\"wrong-shape:$RID\",\"value\":{\"x\":1},\"ttlSeconds\":600}
]}"
case "$RC" in
  4??) ok "an inline type:\"kv\" op is refused at the boundary ($RC), not silently dropped" ;;
  2??) bad "an inline type:\"kv\" op was ACCEPTED ($RC): the flat space now has two encodings" ;;
  *)   bad "an inline type:\"kv\" op answered $RC: $(printf '%s' "$BODY" | head -c 160)" ;;
esac
req GET "/api/v1/kv/$NS/wrong-shape:$RID"
absent "$(jv '.found')" \
  "and it wrote nothing" \
  "the refused inline op WROTE ANYWAY: the 4xx is cosmetic"

# The op-level fields the server owns are refused inside a rider op. This is the
# only place a forged messageId or a forged producerSub can die: the stored
# procedure COALESCEs `_messageId` blindly and cannot tell the broker's
# injection from a client's (§8.2 point 5).
for FIELD in '"producerSub":"someone-else"' '"_messageId":"11111111-1111-7111-8111-111111111111"' '"_tenant":"00000000-0000-0000-0000-0000000000ff"'; do
  req POST /api/v1/transaction "{\"operations\":[],\"timers\":[
    {\"op\":\"schedule\",\"queue\":\"$TQ\",\"timerKey\":\"forge:$RID\",\"partition\":\"Default\",\"delayMs\":60000,\"txn\":\"forge-$RID\",\"payload\":\"e30=\",$FIELD}
  ]}"
  case "$RC" in
    4??) ok "a timer op carrying ${FIELD%%:*} is refused ($RC)" ;;
    *)   bad "a timer op carrying ${FIELD%%:*} answered $RC: provenance is forgeable" ;;
  esac
done

say ""
say "passed: $PASS   failed: $FAIL"
if [ "$FAIL" = "0" ]; then say "TXNSEMANTICS: PASS"; exit 0; fi
say "TXNSEMANTICS: FAIL"
exit 1
