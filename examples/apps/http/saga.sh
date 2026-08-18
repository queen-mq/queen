# docs:start(app-http-saga)
#!/usr/bin/env bash
#
# A booking saga whose compensation is a timer, with nothing but curl.
#
# The war story is a room hold that never came back. A booking system held
# inventory when a reservation started and released it when the payment either
# settled or failed, and the release lived in a sleeping worker. A rolling
# deploy replaced the workers; every hold in flight lost its release; and a
# fortnight later somebody noticed a hotel had been sold out on paper for nine
# nights it had spent empty.
#
# The release was not slow, it was in the wrong place. A compensation is not a
# timeout, it is an obligation, and an obligation has to outlive the process
# that took it on. Here the gate, the saga state, the compensation timer, the
# payment request and the acknowledgement are ONE PostgreSQL transaction. If the
# room is held, the compensation exists. If the room is not held, nothing else
# happened either.
#
#   bookings
#     |-- group "reserver"    ONE bundle: gate + state + timer + push + ack
#     |     `-- payments (partitioned by booking)
#     |           `-- group "payer"   confirm + CANCEL the timer + ack, one bundle
#     `-- expiries (delivered by the timer, at the hold's expiry)
#           `-- group "compensator"   reads the state BEFORE compensating
#
# There is no client library here and none is needed, and this file is worth
# reading even if you use one. `kv` and `timers` are keys of the ROOT of the
# transaction request, beside `operations` and never elements of it, and a timer
# payload travels base64. Everything an SDK hides is written out.
#
# Run it:
#   QUEEN_URL=http://localhost:6632 bash saga.sh

set -euo pipefail

QUEEN_URL="${QUEEN_URL:-http://localhost:6632}"

# A suffix on the queue names AND on the KV namespace. The queues need it
# because delete-then-recreate leaves stale partition state for up to 30
# seconds; the namespace needs it for the opposite reason -- a saga row outlives
# the run that wrote it, so a second run under the same namespace would find
# every booking already held and measure nothing. $$ is the process id, which
# keeps two runs in the same second apart.
RUN="$(date +%s)-$$"
BOOKINGS="app-http-saga-bookings-$RUN"
PAYMENTS="app-http-saga-payments-$RUN"
EXPIRIES="app-http-saga-expiries-$RUN"
NS="app-http-saga-$RUN"

# A group's cursor lives on the queue, and the queue names are already unique
# per run, so these need no suffix.
RESERVER=app-http-reserver
PAYER=app-http-payer
COMPENSATOR=app-http-compensator

# Four bookings, five submissions. B-2 is submitted twice: the same booking, two
# messages, which is what a redelivery looks like from the reserver's side and
# the reason the bundle opens with a gate rather than with a check.
BOOKING_IDS="B-1 B-2 B-3 B-4"
SUBMISSIONS="B-1 B-2 B-2 B-3 B-4"
SUBMISSION_COUNT=5
BOOKING_COUNT=4

# B-3's card is declined, so its saga never reaches "confirmed" and the timer is
# the thing that gives the room back.
DECLINED=B-3

# B-4 pays, but its cancel is deliberately skipped, which makes the race
# deterministic: a cancel that arrives after the fire answers `absent`, and
# ABSENT MAY MEAN ALREADY DELIVERED. So the compensation for a confirmed booking
# has to be refused by the consumer that receives it, never prevented by the
# cancel alone.
CANCEL_SKIPPED=B-4

# How long a room stays held before the compensation fires. It has to outlast
# the reserve and pay phases, or a timer would fire before the payment that
# cancels it and the run would be measuring a race rather than a design.
# deliverAt is a floor and never a ceiling, so a timer can only be late: a
# margin here is sound, where a margin the other way would not be.
HOLD_MS=15000

# Every phase ends on a COUNT, and these are the deadlines behind the counts.
# Never wait for silence; wait for a total, with a deadline. The compensation
# phase gets a longer one: a timer fires no earlier than its delay plus one
# sweeper cycle, and a broker whose timer table has been empty for a while wakes
# up lazily.
PHASE_MS=20000
TIMER_DEADLINE_MS=90000

# Every pop long-polls for this many milliseconds and no longer.
POLL_MS=1000

command -v jq >/dev/null 2>&1 || { echo "FAIL: jq is not installed"; exit 1; }

CHECKS=0
TMP="$(mktemp -d)"

# One line per delivery handled: "<group> <booking> <outcome>".
OBSERVED="$TMP/observed"
: > "$OBSERVED"

# One line per room actually put back on sale. This is the compensation's
# external effect, and the whole reason the program exists.
RELEASED="$TMP/released"
: > "$RELEASED"

fail() { FAILURE="$*"; exit 1; }

# check <actual> <expected> <description>
check() {
  [ "$1" = "$2" ] || fail "$3 (expected [$2], got [$1])"
  CHECKS=$((CHECKS + 1))
  echo "  ok: $3"
}

# ok <description>: records a check whose condition was already tested, for the
# assertions that are not an equality.
ok() {
  CHECKS=$((CHECKS + 1))
  echo "  ok: $1"
}

# A millisecond clock. GNU date spells it %3N; BSD date (macOS) has no %N and
# leaves the unconverted tail in the output, so a probe for anything that is not
# a digit tells the two apart, and perl, whose Time::HiRes is core, is the
# fallback.
if [ -z "$(date +%s%3N 2>/dev/null | tr -d '0-9')" ]; then
  now_ms() { date +%s%3N; }
else
  command -v perl >/dev/null 2>&1 \
    || { echo "FAIL: need GNU date or perl for a millisecond clock"; exit 1; }
  now_ms() { perl -MTime::HiRes -e 'printf "%d", Time::HiRes::time() * 1000'; }
fi

# Sets $STATUS to the HTTP status code and writes the response body to $OUT.
# There is no --fail: Queen reports outcomes in the body and several of the
# interesting ones arrive as 200, so read the status, then the body.
OUT="$TMP/body"
request() {
  local method="$1" path="$2" body="${3:-}"
  if [ -n "$body" ]; then
    STATUS="$(curl -sS -o "$OUT" -w '%{http_code}' \
      -X "$method" "$QUEEN_URL$path" \
      -H 'content-type: application/json' -d "$body")"
  else
    STATUS="$(curl -sS -o "$OUT" -w '%{http_code}' -X "$method" "$QUEEN_URL$path")"
  fi
}

# saga_key <booking>: the state key. It derives from the booking id, which is
# also the partition key of the payments queue, and that derivation is what
# makes the payer's read-then-write safe further down.
saga_key() { printf 'saga:%s' "$1"; }

# kv_get <key>: prints the whole row as JSON, {"found":false,...} when absent.
# `found` is a field of its own because null is a legal stored value: absence is
# never inferred from the value being empty. Everything goes through
# POST /api/v1/kv, including single-key reads, which keeps the key out of access
# logs, proxy samples and tracing spans.
kv_get() {
  local body
  body="$(jq -cn --arg ns "$NS" --arg key "$1" \
    '{operations: [{op: "get", ns: $ns, key: $key}]}')"
  request POST /api/v1/kv "$body"
  [ "$STATUS" = 200 ] || fail "kv get returned HTTP $STATUS"
  jq -c '.results[0]' "$OUT"
}

# One exit path for everything. A failed check calls fail(), which records the
# reason and exits 1; any other command that fails under `set -e` arrives here
# too, with its own status. FAIL is printed exactly once, and only on failure.
#
# It also purges, and there are three things to remove. Two of them are the ones
# that are easy to forget: the saga rows live in their own table, and a PENDING
# TIMER lives in the staging table keyed by NAME, so neither is reached by
# deleting the queue -- and a timer whose queue no longer exists still fires and
# provisions the queue again on the way out.
#
# The purge is UNCONDITIONAL, because a run that failed is exactly the run whose
# leftovers matter: an armed timer would deliver into the next run, and a
# surviving saga row would make the next run pass without holding anything. And
# it is best effort, with `|| true` throughout, so a purge that fails cannot
# overwrite the verdict.
cleanup() {
  local status=$?
  purge || true
  rm -rf "$TMP"
  if [ "$status" -ne 0 ]; then
    echo
    echo "FAIL: ${FAILURE:-a command exited with status $status}"
  fi
  exit "$status"
}

purge() {
  local booking keys body
  for booking in $BOOKING_IDS; do
    # The cancel route, DELETE /api/v1/timers/:queue/*timerKey. It is the one
    # route a proxy may never block, because the fire never switches itself off.
    request DELETE "/api/v1/timers/$EXPIRIES/$booking" || true
  done
  keys="$(printf '%s\n' $BOOKING_IDS | jq -R 'sub("^"; "saga:")' | jq -sc .)" || return 0
  body="$(jq -cn --arg ns "$NS" --argjson keys "$keys" \
    '{operations: [$keys[] | {op: "delete", ns: $ns, key: .}]}')" || return 0
  request POST /api/v1/kv "$body" || true
  request DELETE "/api/v1/resources/queues/$BOOKINGS" || true
  request DELETE "/api/v1/resources/queues/$PAYMENTS" || true
  request DELETE "/api/v1/resources/queues/$EXPIRIES" || true
}
trap cleanup EXIT

echo "broker $QUEEN_URL"

# Every broker serves /api/v1/kv and /api/v1/timers: there is no flag that turns
# them on. What can still refuse is an operator's runtime kill switch (503) or a
# quota (403), so probe once here and name that, rather than letting the first
# real call fail with something that reads like a bug.
request POST /api/v1/kv '{"operations":[{"op":"get","ns":"probe","key":"probe"}]}'
[ "$STATUS" = 200 ] \
  || fail "the kv probe returned HTTP $STATUS: $(cat "$OUT") (503 is an operator's kill switch, 403 a quota; see /deploy/state)"
request GET "/api/v1/timers/$EXPIRIES?limit=1"
[ "$STATUS" = 200 ] \
  || fail "the timers probe returned HTTP $STATUS: $(cat "$OUT") (503 is an operator's kill switch, 403 a quota; see /deploy/state)"

# /configure is a full replace rather than a patch, so what is not named here is
# reset to its default.
for queue in "$BOOKINGS" "$PAYMENTS" "$EXPIRIES"; do
  body="$(jq -n --arg queue "$queue" '{queue: $queue, options: {leaseTime: 30, retryLimit: 3}}')"
  request POST /api/v1/configure "$body"
  [ "$STATUS" = 200 ] || fail "configure of $queue returned HTTP $STATUS"
done
check "$(jq -r .configured "$OUT")" true 'three queues exist, each with a 30 second lease'

# ---------------------------------------------------------------------- queuing
echo
echo "submitting bookings"
index=0
room=101
cents=24000
for booking in $SUBMISSIONS; do
  # Distinct transaction ids on purpose. Deduplication would swallow the
  # duplicate submission and the gate would never be tested, and a real
  # redelivery arrives with an identity of its own too. The duplicate carries
  # the same room and price, being the same booking submitted twice.
  case "$booking" in
    B-1) room=101; cents=24000 ;;
    B-2) room=102; cents=31000 ;;
    B-3) room=103; cents=18000 ;;
    B-4) room=104; cents=27000 ;;
  esac
  body="$(jq -n --arg queue "$BOOKINGS" --arg booking "$booking" --arg room "$room" \
    --argjson cents "$cents" --argjson i "$index" \
    '{items: [{queue: $queue, transactionId: ("submit-" + ($i|tostring) + "-" + $booking),
               payload: {bookingId: $booking, room: $room, cents: $cents}}]}')"
  request POST /api/v1/push "$body"
  [ "$STATUS" = 201 ] || fail "push of $booking returned HTTP $STATUS"
  # HTTP 201 is not proof the message was stored: "buffered" and "failed" also
  # come back 201. The per-item status is the only answer.
  [ "$(jq -r '.[0].status' "$OUT")" = queued ] \
    || fail "push of $booking came back $(jq -r '.[0].status' "$OUT")"
  index=$((index + 1))
done
echo "  $SUBMISSION_COUNT submissions for $BOOKING_COUNT bookings"

# ---------------------------------------------------------------------------
# handle_reserve: one delivery from the bookings queue.
#
# The bundle, and the whole point of the example: five things commit together,
# so there is no ordering between them left to get wrong.
# ---------------------------------------------------------------------------
handle_reserve() {
  local booking room cents txn partition lease body payload why ack_body
  booking="$(jq -r '.messages[0].data.bookingId' "$TMP/pop")"
  room="$(jq -r '.messages[0].data.room' "$TMP/pop")"
  cents="$(jq -r '.messages[0].data.cents' "$TMP/pop")"
  txn="$(jq -r '.messages[0].transactionId' "$TMP/pop")"
  partition="$(jq -r '.messages[0].partitionId' "$TMP/pop")"
  # The lease minted for THIS pop. It is what says the worker still owns the
  # message, and it is the reason the acknowledgement below can refuse.
  lease="$(jq -r '.leaseId' "$TMP/pop")"

  # `kv` and `timers` are keys of the ROOT of this body, beside `operations` and
  # not inside it. That is not a style choice: they are separate top-level
  # fields precisely so that no client can send them under one key by accident.
  #
  # kv:      required:true is what makes putIfAbsent a GATE rather than a
  #          verdict. Without it a lost race would come back applied:false while
  #          the payment and the timer went out anyway. ttlSeconds is mandatory
  #          on every KV write: a row with no expiry is a row nothing will ever
  #          delete. `forever: true` is the other legal answer, and it is
  #          exactly what an example must never write.
  # timers:  the obligation. From the moment this commits it is a row in the
  #          broker's own table, so it survives this handler, this process, this
  #          deploy and this machine. The key is ours, which is the entire
  #          reason it can be cancelled later by name. The payload is base64,
  #          and delayMs is milliseconds from now -- an absolute instant is not
  #          expressible, because deliverAt is computed in PostgreSQL and there
  #          is exactly one clock.
  # push:    partitioned by booking, so every message about one booking is in
  #          one lane.
  # ack:     carrying this delivery's lease. An expired lease refuses the ack
  #          and takes the other three down with it, which is the guarantee no
  #          compare-and-swap can give.
  payload="$(jq -rn --arg booking "$booking" --arg room "$room" \
    '{bookingId: $booking, room: $room} | tojson | @base64')"
  body="$(jq -cn --arg ns "$NS" --arg key "$(saga_key "$booking")" \
    --arg booking "$booking" --arg room "$room" --argjson cents "$cents" \
    --arg payments "$PAYMENTS" --arg expiries "$EXPIRIES" --argjson hold "$HOLD_MS" \
    --arg payload "$payload" \
    --arg txn "$txn" --arg pid "$partition" --arg grp "$RESERVER" --arg lease "$lease" '
    {operations: [{type: "push",
                   items: [{queue: $payments, partition: $booking,
                            transactionId: ("pay-" + $booking),
                            payload: {bookingId: $booking, cents: $cents}}]},
                  {type: "ack", transactionId: $txn, partitionId: $pid,
                   consumerGroup: $grp, leaseId: $lease, status: "completed"}],
     kv: [{op: "putIfAbsent", ns: $ns, key: $key,
           value: {step: "held", room: $room, cents: $cents},
           ttlSeconds: 3600, required: true}],
     timers: [{op: "schedule", queue: $expiries, timerKey: $booking,
               delayMs: $hold, txn: ("hold-" + $booking), payload: $payload}]}')"
  request POST /api/v1/transaction "$body"
  [ "$STATUS" = 200 ] || fail "the reserving bundle for $booking returned HTTP $STATUS: $(cat "$OUT")"

  # A lost gate is RETURNED, not thrown: HTTP 200 with success:false and
  # reason "kv_precondition". It is the ordinary outcome of every legitimate
  # redelivery, which makes it one of the most frequent answers this product
  # gives, and it does not belong in an error path, a retry policy or an error
  # metric -- which is exactly why it is not a 409.
  if [ "$(jq -r '.success' "$OUT")" != true ]; then
    [ "$(jq -r '.reason' "$OUT")" = kv_precondition ] \
      || fail "the reserving bundle for $booking failed: $(jq -r '.error' "$OUT")"
    # Read the verdict BEFORE the next call: $OUT is one file and the ack below
    # overwrites it. `kvReason` is the closed taxonomy of the KV refusal --
    # here `exists`, the row was already there.
    why="$(jq -r '.kvReason' "$OUT")"
    # Nothing was written: no second payment, no second timer, no second row.
    # The message still has to leave the cursor, so it is acknowledged alone.
    ack_body="$(jq -cn --arg txn "$txn" --arg pid "$partition" --arg grp "$RESERVER" --arg lease "$lease" \
      '{transactionId: $txn, partitionId: $pid, consumerGroup: $grp, leaseId: $lease, status: "completed"}')"
    request POST /api/v1/ack "$ack_body"
    [ "$STATUS" = 200 ] || fail "ack returned HTTP $STATUS"
    printf '%s %s rolled-back\n' "$RESERVER" "$booking" >> "$OBSERVED"
    echo "  $booking: already held, whole bundle rolled back ($why)"
    return 0
  fi

  printf '%s %s held\n' "$RESERVER" "$booking" >> "$OBSERVED"
  echo "  $booking: room $room held, compensation armed for $HOLD_MS ms"
}

# ---------------------------------------------------------------------------
# handle_pay: one delivery from the payments queue.
#
# A settled payment confirms the state and calls the compensation off in one
# commit; a declined card leaves the state where it is and lets the timer do its
# work.
# ---------------------------------------------------------------------------
handle_pay() {
  local booking txn partition lease state version value body timers ack_body
  booking="$(jq -r '.messages[0].data.bookingId' "$TMP/pop")"
  txn="$(jq -r '.messages[0].transactionId' "$TMP/pop")"
  partition="$(jq -r '.messages[0].partitionId' "$TMP/pop")"
  lease="$(jq -r '.leaseId' "$TMP/pop")"

  # A read in one call and a write in the next. It is safe HERE because the key
  # derives from the partition key: every message about this booking arrives in
  # one lane of this queue, and a lane has one reader per group. Where a key
  # does not derive from the partition key this shape is a race and the atomics
  # are the answer, which is exactly the compensator's situation further down.
  state="$(kv_get "$(saga_key "$booking")")"
  version="$(printf '%s' "$state" | jq -r '.version')"
  value="$(printf '%s' "$state" | jq -c '.value')"

  if [ "$booking" = "$DECLINED" ]; then
    # A declined card is a business outcome, not a delivery failure: the message
    # is done with. The room stays held, and nothing in this process is
    # responsible for giving it back.
    ack_body="$(jq -cn --arg txn "$txn" --arg pid "$partition" --arg grp "$PAYER" --arg lease "$lease" \
      '{transactionId: $txn, partitionId: $pid, consumerGroup: $grp, leaseId: $lease, status: "completed"}')"
    request POST /api/v1/ack "$ack_body"
    [ "$STATUS" = 200 ] || fail "ack returned HTTP $STATUS"
    printf '%s %s declined\n' "$PAYER" "$booking" >> "$OBSERVED"
    echo "  $booking: card declined, hold left to expire"
    return 0
  fi

  # The cancel rides the bundle: either the booking is confirmed and the
  # compensation is called off, or neither happened. Inside a transaction a
  # cancel necessarily travels in the timers array and inherits the bundle's
  # fate, which is the entire point of putting it there.
  if [ "$booking" = "$CANCEL_SKIPPED" ]; then
    timers='[]'
  else
    timers="$(jq -cn --arg expiries "$EXPIRIES" --arg booking "$booking" \
      '[{op: "cancel", queue: $expiries, timerKey: $booking}]')"
  fi

  # `expect` makes the serialisation assumption falsifiable instead of silent.
  # If the lane really serialises, it never fails and costs nothing; the day it
  # fails, two consumers are serving one partition and you learn it as a verdict
  # rather than as a wrong total.
  body="$(jq -cn --arg ns "$NS" --arg key "$(saga_key "$booking")" \
    --argjson value "$value" --argjson version "$version" --argjson timers "$timers" \
    --arg txn "$txn" --arg pid "$partition" --arg grp "$PAYER" --arg lease "$lease" '
    {operations: [{type: "ack", transactionId: $txn, partitionId: $pid,
                   consumerGroup: $grp, leaseId: $lease, status: "completed"}],
     kv: [{op: "put", ns: $ns, key: $key, value: ($value + {step: "confirmed"}),
           ttlSeconds: 3600, expect: $version, required: true}],
     timers: $timers}')"
  request POST /api/v1/transaction "$body"
  [ "$STATUS" = 200 ] || fail "the confirming bundle for $booking returned HTTP $STATUS: $(cat "$OUT")"
  [ "$(jq -r '.success' "$OUT")" = true ] \
    || fail "$booking: confirmation lost its fence ($(jq -r '.kvReason' "$OUT"))"

  printf '%s %s confirmed\n' "$PAYER" "$booking" >> "$OBSERVED"
  if [ "$booking" = "$CANCEL_SKIPPED" ]; then
    echo "  $booking: paid and confirmed, compensation deliberately NOT cancelled"
  else
    echo "  $booking: paid and confirmed, compensation cancelled"
  fi
}

# ---------------------------------------------------------------------------
# handle_compensate: one delivery from the expiries queue, which is to say one
# message a timer produced.
#
# A compensation message is not an instruction, it is a question: is this saga
# still open? A fired timer leaves no tombstone, so a cancel that arrives a
# millisecond late answers `absent` and the message goes out anyway. The state
# is the authority and it is read first.
#
# And here the key does NOT derive from the partition key: this message arrives
# on another queue entirely, in a lane that has nothing to do with the payments
# lane, so no partitioning could serialise the two writers. That is what
# `expect` is for, and on this path it is load-bearing rather than an assertion.
# ---------------------------------------------------------------------------
handle_compensate() {
  local booking room txn partition lease state step version value body ack_body
  booking="$(jq -r '.messages[0].data.bookingId' "$TMP/pop")"
  room="$(jq -r '.messages[0].data.room' "$TMP/pop")"
  txn="$(jq -r '.messages[0].transactionId' "$TMP/pop")"
  partition="$(jq -r '.messages[0].partitionId' "$TMP/pop")"
  lease="$(jq -r '.leaseId' "$TMP/pop")"

  state="$(kv_get "$(saga_key "$booking")")"
  step="$(printf '%s' "$state" | jq -r '.value.step // "gone"')"
  version="$(printf '%s' "$state" | jq -r '.version')"
  value="$(printf '%s' "$state" | jq -c '.value')"

  ack_body="$(jq -cn --arg txn "$txn" --arg pid "$partition" --arg grp "$COMPENSATOR" --arg lease "$lease" \
    '{transactionId: $txn, partitionId: $pid, consumerGroup: $grp, leaseId: $lease, status: "completed"}')"

  if [ "$step" != held ]; then
    # The booking was confirmed before this fired. Compensating here is how a
    # saga unwinds a sale that has already shipped.
    request POST /api/v1/ack "$ack_body"
    [ "$STATUS" = 200 ] || fail "ack returned HTTP $STATUS"
    printf '%s %s refused\n' "$COMPENSATOR" "$booking" >> "$OBSERVED"
    echo "  $booking: state is $step, compensation refused"
    return 0
  fi

  body="$(jq -cn --arg ns "$NS" --arg key "$(saga_key "$booking")" \
    --argjson value "$value" --argjson version "$version" \
    --arg txn "$txn" --arg pid "$partition" --arg grp "$COMPENSATOR" --arg lease "$lease" '
    {operations: [{type: "ack", transactionId: $txn, partitionId: $pid,
                   consumerGroup: $grp, leaseId: $lease, status: "completed"}],
     kv: [{op: "put", ns: $ns, key: $key, value: ($value + {step: "expired"}),
           ttlSeconds: 3600, expect: $version, required: true}]}')"
  request POST /api/v1/transaction "$body"
  [ "$STATUS" = 200 ] || fail "the compensating bundle for $booking returned HTTP $STATUS: $(cat "$OUT")"

  if [ "$(jq -r '.success' "$OUT")" != true ]; then
    # Somebody confirmed it between the read and the commit. The fence held,
    # nothing was written, and the room stays sold.
    [ "$(jq -r '.reason' "$OUT")" = kv_precondition ] \
      || fail "the compensating bundle for $booking failed: $(jq -r '.error' "$OUT")"
    request POST /api/v1/ack "$ack_body"
    [ "$STATUS" = 200 ] || fail "ack returned HTTP $STATUS"
    printf '%s %s refused\n' "$COMPENSATOR" "$booking" >> "$OBSERVED"
    echo "  $booking: confirmed under us, compensation refused by the fence"
    return 0
  fi

  printf '%s\n' "$room" >> "$RELEASED"
  printf '%s %s released\n' "$COMPENSATOR" "$booking" >> "$OBSERVED"
  echo "  $booking: hold expired, room $room released"
}

# ---------------------------------------------------------------------------
# drain <queue> <group> <handler> <deliveries> <deadline_ms>: pop and handle
# until this group has handled that many deliveries, or the deadline passes. The
# count is the bound and the deadline is the net; neither is a wait for silence.
# ---------------------------------------------------------------------------
drain() {
  local queue="$1" group="$2" handler="$3" wanted="$4" budget="$5" deadline
  deadline=$(( $(now_ms) + budget ))

  while [ "$(grep -c "^$group " "$OBSERVED" || true)" -lt "$wanted" ]; do
    [ "$(now_ms)" -lt "$deadline" ] || break

    # subscriptionMode=all is what makes a group created now read what was
    # pushed before it existed: a new cursor is seeded at the TAIL unless you
    # say otherwise. batch=1 keeps one message in flight.
    request GET "/api/v1/pop/queue/$queue?consumerGroup=$group&subscriptionMode=all&batch=1&wait=true&timeout=$POLL_MS"
    # 204 is an empty pop, with no body at all. Go round again until the
    # deadline.
    [ "$STATUS" != 204 ] || continue
    [ "$STATUS" = 200 ] || fail "pop returned HTTP $STATUS"
    cp "$OUT" "$TMP/pop"
    "$handler"
  done
}

# -------------------------------------------------------------------- reserving
echo
echo "reserving"
drain "$BOOKINGS" "$RESERVER" handle_reserve "$SUBMISSION_COUNT" "$PHASE_MS"

check "$(grep -c "^$RESERVER " "$OBSERVED" || true)" "$SUBMISSION_COUNT" \
  'the reserver reached a decision on every submission'
check "$(grep -c "^$RESERVER .* rolled-back$" "$OBSERVED" || true)" 1 \
  'the duplicate submission lost the gate exactly once'

# Pending timers are a table you can read, not a promise you have to trust.
request GET "/api/v1/timers/$EXPIRIES?limit=50"
[ "$STATUS" = 200 ] || fail "listing timers returned HTTP $STATUS"
echo "  timers armed: $(jq -r '[.rows[].timerKey] | sort | join(", ")' "$OUT")"
check "$(jq -r '.rows | length' "$OUT")" "$BOOKING_COUNT" \
  'one compensation is armed per booking and the duplicate added none'

# ----------------------------------------------------------------------- paying
echo
echo "paying"
drain "$PAYMENTS" "$PAYER" handle_pay "$BOOKING_COUNT" "$PHASE_MS"

check "$(grep -c "^$PAYER " "$OBSERVED" || true)" "$BOOKING_COUNT" \
  'every booking was asked to pay once and the duplicate produced no second payment'
check "$(awk -v g="$PAYER" '$1 == g {print $2}' "$OBSERVED" | sort -u | wc -l | tr -d ' ')" \
  "$BOOKING_COUNT" 'no booking was asked to pay twice'

# The cancel is observable before anything is delivered: the row is gone from
# the staging table. A peek is how you ask, and a miss is {"found":false} with
# HTTP 200, never a 404.
request GET "/api/v1/timers/$EXPIRIES/B-1"
[ "$STATUS" = 200 ] || fail "peek returned HTTP $STATUS"
check "$(jq -r '.found' "$OUT")" false \
  'the compensation cancelled inside the confirming bundle is gone from the table'
request GET "/api/v1/timers/$EXPIRIES/$DECLINED"
check "$(jq -r '.found' "$OUT")" true \
  "$DECLINED was never confirmed, so its compensation is still armed"
request GET "/api/v1/timers/$EXPIRIES/$CANCEL_SKIPPED"
check "$(jq -r '.found' "$OUT")" true \
  "$CANCEL_SKIPPED is confirmed but its compensation is still armed on purpose"

# ----------------------------------------------------------------- compensating
echo
echo "compensating"
# Two timers were left armed, so two messages must arrive: that is the count,
# and TIMER_DEADLINE_MS is the deadline behind it.
drain "$EXPIRIES" "$COMPENSATOR" handle_compensate 2 "$TIMER_DEADLINE_MS"
check "$(grep -c "^$COMPENSATOR " "$OBSERVED" || true)" 2 \
  'both uncancelled compensations were delivered'

# Then a bounded second pass with room for two more. It is the only honest way
# to say "a cancelled timer never arrived": the first pass would have stopped at
# two whatever those two were, so the claim is really that nothing else shows up
# afterwards.
drain "$EXPIRIES" "$COMPENSATOR" handle_compensate 4 4000

# --------------------------------------------------------------------- checking
echo
echo "checking"

check "$(grep -c "^$COMPENSATOR " "$OBSERVED" || true)" 2 \
  'nothing else arrived on a second pass: still 2 compensations'
check "$(grep -c "^$COMPENSATOR B-1 \|^$COMPENSATOR B-2 " "$OBSERVED" || true)" 0 \
  'a cancelled compensation was never delivered'
check "$(cat "$RELEASED" | tr -d ' \n')" 103 \
  'exactly one room went back on sale, the one whose card was declined'
check "$(grep -c "^$COMPENSATOR $CANCEL_SKIPPED refused$" "$OBSERVED" || true)" 1 \
  'the compensation for the confirmed booking was refused by the consumer, not prevented by the cancel'

# The saga rows are readable state, not an internal detail: a support engineer
# can answer "what happened to this booking" without a second system. getMany
# reports `missing` explicitly, because absence is a datum and not a hole
# computed by difference.
keys="$(printf '%s\n' $BOOKING_IDS | jq -R 'sub("^"; "saga:")' | jq -sc .)"
body="$(jq -cn --arg ns "$NS" --argjson keys "$keys" \
  '{operations: [{op: "getMany", ns: $ns, keys: $keys}]}')"
request POST /api/v1/kv "$body"
[ "$STATUS" = 200 ] || fail "kv getMany returned HTTP $STATUS"
check "$(jq -r '.results[0].rows | length' "$OUT")" "$BOOKING_COUNT" \
  'every booking left exactly one saga row'
check "$(jq -r '.results[0].missing | length' "$OUT")" 0 \
  'no booking is missing its saga row'
check "$(jq -r '[.results[0].rows[] | select(.value.step == "confirmed") | .key] | sort | join(",")' "$OUT")" \
  "saga:B-1,saga:B-2,saga:$CANCEL_SKIPPED" \
  "three bookings ended confirmed, $CANCEL_SKIPPED included, after its compensation was delivered"
check "$(jq -r --arg key "saga:$DECLINED" '.results[0].rows[] | select(.key == $key) | .value.step' "$OUT")" \
  expired "$DECLINED was unwound by its timer, with nobody awake to do it"

echo
echo "  final: $(jq -r '[.results[0].rows[] | (.key | sub("^saga:"; "")) + "=" + .value.step] | sort | join(", ")' "$OUT")"

echo
echo "PASS: $CHECKS checks"
# docs:end
