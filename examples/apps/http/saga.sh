# docs:start(app-http-saga)
#!/usr/bin/env bash
#
# A booking saga, with nothing but curl: a room is held, paid for, and released
# by a timer when the payment never comes.
#
# The release is the part that usually breaks. Kept as a sleep in a worker, it
# dies with the worker, and a deploy in the middle of a hold leaves the room
# held forever. Here the release is a timer in the broker, scheduled in the same
# transaction that holds the room: the saga's state, the timer, the payment
# request and the ack of the booking commit as one log entry. If the room is
# held, its release exists; if anything failed, none of it happened.
#
#   bookings
#     `-- group "reserver"     ONE transaction: state + timer + push + ack
#           |-- payments (one partition per booking)
#           |     `-- group "payer"        confirm + cancel the timer + ack
#           `-- expiries (the timer delivers here when the hold runs out)
#                 `-- group "compensator"  reads the state before it releases
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

# Fresh queues and a fresh KV namespace per run, so runs never share state. The
# namespace matters most: the saga's state is KV entries, which outlive the
# queues (they expire with their TTL), so a second run in the same namespace
# would find every booking already held. $$ is the process id, which keeps two
# runs in the same second apart.
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

# Four bookings, five submissions: B-2 is submitted twice, which is what a
# redelivery looks like from the reserver's side, and the reason the
# transaction opens with a gate.
BOOKING_IDS="B-1 B-2 B-3 B-4"
SUBMISSIONS="B-1 B-2 B-2 B-3 B-4"
SUBMISSION_COUNT=5
BOOKING_COUNT=4

# B-3's card is declined, so its saga never reaches "confirmed" and the timer is
# what gives the room back.
DECLINED=B-3

# B-4 pays, but its cancel is skipped on purpose. That is the cancel that comes
# too late, made reproducible: a cancel that arrives after the fire answers
# `absent`, which may mean the release was already delivered. So the release
# for a confirmed booking has to be refused by the compensator that receives
# it.
CANCEL_SKIPPED=B-4

# How long a room stays held before it is released. In production this is
# minutes; it is the only number that changes. It has to outlast the reserving
# and paying phases below, or a release would fire before the payment that
# cancels it and the run would measure a race.
HOLD_MS=10000

# Each phase ends on a count of messages, with a deadline behind it so a stall
# fails the run instead of hanging it. Timers fire on the leader's next tick
# after they are due (every 50 ms), so the compensation phase only needs the
# hold plus a margin.
PHASE_MS=20000
TIMER_DEADLINE_MS=$((HOLD_MS + 20000))

# Every pop long-polls for this many milliseconds and no longer.
POLL_MS=1000

command -v jq >/dev/null 2>&1 || { echo "FAIL: jq is not installed"; exit 1; }

CHECKS=0
TMP="$(mktemp -d)"

# One line per delivery handled: "<group> <booking> <outcome>".
OBSERVED="$TMP/observed"
: > "$OBSERVED"

# One line per room actually put back on sale. This is the release's external
# effect, and the reason the program exists.
RELEASED="$TMP/released"
: > "$RELEASED"

fail() { FAILURE="$*"; exit 1; }

# check <actual> <expected> <description>
check() {
  [ "$1" = "$2" ] || fail "$3 (expected [$2], got [$1])"
  CHECKS=$((CHECKS + 1))
  echo "  ok: $3"
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

# kv_get <key>: prints the whole result as JSON, {"found":false,...} when
# absent. `found` is a field of its own because null is a legal stored value:
# absence is never inferred from the value being empty. Everything goes through
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
# It also cleans up, in every case, a failed run included. Deleting the queues
# is not enough: the saga's state is KV entries, and a pending timer is stored
# by its queue and key, so deleting the queues removes neither, and a timer that
# fires into a deleted queue creates the queue again. So the timers are
# cancelled and the entries deleted first. The cleanup is best effort, with
# `|| true` throughout, so a cleanup that fails cannot overwrite the verdict.
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
    # The cancel route, DELETE /api/v1/timers/:queue/*timerKey. Neither a quota
    # nor an operator's switch ever blocks it: a pending timer fires whatever
    # the quota says, so its cancel always has to get through.
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
# quota (403), so probe once here and name that. Otherwise the first real call
# fails with something that reads like a bug.
request POST /api/v1/kv '{"operations":[{"op":"get","ns":"probe","key":"probe"}]}'
[ "$STATUS" = 200 ] \
  || fail "the kv probe returned HTTP $STATUS: $(cat "$OUT") (503 is an operator's kill switch, 403 a quota; GET /api/v1/system/kv-timers shows the switches)"
request GET "/api/v1/timers/$EXPIRIES?limit=1"
[ "$STATUS" = 200 ] \
  || fail "the timers probe returned HTTP $STATUS: $(cat "$OUT") (503 is an operator's kill switch, 403 a quota; GET /api/v1/system/kv-timers shows the switches)"

# /configure merges: an option not named here keeps whatever the queue already
# has. These three names are unique per run, so what is not named lands on its
# default.
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
  # One transaction id per submission, so the duplicate of B-2 is stored and
  # reaches the reserver, where the gate has to catch it. The duplicate carries
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
  # HTTP 201 is not proof the message was stored: an item the broker refused
  # comes back "error" inside a 201. The per-item status is the only answer.
  [ "$(jq -r '.[0].status' "$OUT")" = queued ] \
    || fail "push of $booking came back $(jq -r '.[0].status' "$OUT")"
  index=$((index + 1))
done
echo "  $SUBMISSION_COUNT submissions for $BOOKING_COUNT bookings"

# ---------------------------------------------------------------------------
# handle_reserve: one delivery from the bookings queue.
#
# The transaction is the whole point of the example: four things commit
# together, so there is no order between them to get wrong. Written as four
# calls, a crash between the timer and the push leaves a release for a payment
# that was never asked for, and a crash the other way round leaves a hold with
# no release.
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

  # `kv` and `timers` are keys of the ROOT of this body, beside `operations`.
  # They are separate top-level fields so that no client can send them under
  # one key by accident.
  #
  # kv:      the gate and the first state, in one KV entry. required:true makes
  #          the putIfAbsent a gate: if the booking is already held, the whole
  #          transaction rolls back and nothing else in it happens. Without it
  #          a lost race would come back applied:false while the payment and the
  #          timer went out anyway. ttlSeconds is mandatory on every KV write
  #          (`forever: true` is the only alternative), so an entry nobody
  #          deletes still goes away.
  # timers:  the release. From this commit on it is a record in the broker's
  #          replicated state, independent of this process. The key is ours,
  #          which is what lets the payer cancel it by name. The payload is
  #          base64, and delayMs is milliseconds from now: an absolute instant
  #          is not expressible, because the broker computes the due time on
  #          its own clock.
  # push:    the payment request, in the booking's own partition. It carries no
  #          transactionId: it can only commit together with the saga's state,
  #          so the gate is its idempotency key, and the broker mints an id.
  #          With an id of our own, the duplicate of B-2 would be refused as a
  #          duplicate push (reason "duplicate") before the gate could answer.
  # ack:     with this delivery's lease. If the lease ran out, the ack is
  #          refused and the other three are refused with it.
  payload="$(jq -rn --arg booking "$booking" --arg room "$room" \
    '{bookingId: $booking, room: $room} | tojson | @base64')"
  body="$(jq -cn --arg ns "$NS" --arg key "$(saga_key "$booking")" \
    --arg booking "$booking" --arg room "$room" --argjson cents "$cents" \
    --arg payments "$PAYMENTS" --arg expiries "$EXPIRIES" --argjson hold "$HOLD_MS" \
    --arg payload "$payload" \
    --arg txn "$txn" --arg pid "$partition" --arg grp "$RESERVER" --arg lease "$lease" '
    {operations: [{type: "push",
                   items: [{queue: $payments, partition: $booking,
                            payload: {bookingId: $booking, cents: $cents}}]},
                  {type: "ack", transactionId: $txn, partitionId: $pid,
                   consumerGroup: $grp, leaseId: $lease, status: "completed"}],
     kv: [{op: "putIfAbsent", ns: $ns, key: $key,
           value: {step: "held", room: $room, cents: $cents},
           ttlSeconds: 3600, required: true}],
     timers: [{op: "schedule", queue: $expiries, timerKey: $booking,
               delayMs: $hold, txn: ("hold-" + $booking), payload: $payload}]}')"
  request POST /api/v1/transaction "$body"
  [ "$STATUS" = 200 ] || fail "the reserving transaction for $booking returned HTTP $STATUS: $(cat "$OUT")"

  # A lost gate comes back as a value: HTTP 200 with success:false and reason
  # "kv_precondition". A duplicate is a normal outcome, so it is handled here
  # and kept out of the error path, where a retry would be the reflex.
  if [ "$(jq -r '.success' "$OUT")" != true ]; then
    [ "$(jq -r '.reason' "$OUT")" = kv_precondition ] \
      || fail "the reserving transaction for $booking failed: $(jq -r '.error' "$OUT")"
    # Read the verdict before the next call: $OUT is one file and the ack below
    # overwrites it. `kvReason` says why the precondition failed: here
    # `exists`, the booking's state was already there.
    why="$(jq -r '.kvReason' "$OUT")"
    # Nothing was written: no second payment, no second timer, no second state.
    # The message still has to leave the cursor, so it is acknowledged on its
    # own.
    ack_body="$(jq -cn --arg txn "$txn" --arg pid "$partition" --arg grp "$RESERVER" --arg lease "$lease" \
      '{transactionId: $txn, partitionId: $pid, consumerGroup: $grp, leaseId: $lease, status: "completed"}')"
    request POST /api/v1/ack "$ack_body"
    [ "$STATUS" = 200 ] || fail "ack returned HTTP $STATUS"
    printf '%s %s rolled-back\n' "$RESERVER" "$booking" >> "$OBSERVED"
    echo "  $booking: already held, nothing written ($why)"
    return 0
  fi

  printf '%s %s held\n' "$RESERVER" "$booking" >> "$OBSERVED"
  echo "  $booking: room $room held, release armed for $HOLD_MS ms"
}

# ---------------------------------------------------------------------------
# handle_pay: one delivery from the payments queue.
#
# A settled payment confirms the saga and cancels the release in one commit. A
# declined card leaves the state alone and lets the timer do its work.
# ---------------------------------------------------------------------------
handle_pay() {
  local booking txn partition lease state version value body timers ack_body
  booking="$(jq -r '.messages[0].data.bookingId' "$TMP/pop")"
  txn="$(jq -r '.messages[0].transactionId' "$TMP/pop")"
  partition="$(jq -r '.messages[0].partitionId' "$TMP/pop")"
  lease="$(jq -r '.leaseId' "$TMP/pop")"

  # A read now and a write in the transaction below. That is safe here because
  # the key derives from the partition key: every message about this booking is
  # in one partition, and a partition is held by one worker of the group at a
  # time. Where a key does not derive from the partition key this shape is a
  # race, which is the compensator's situation further down.
  state="$(kv_get "$(saga_key "$booking")")"
  version="$(printf '%s' "$state" | jq -r '.version')"
  value="$(printf '%s' "$state" | jq -c '.value')"

  if [ "$booking" = "$DECLINED" ]; then
    # A declined card is a business outcome, not a failed delivery: the message
    # is done. The room stays held, and nothing in this process is responsible
    # for giving it back.
    ack_body="$(jq -cn --arg txn "$txn" --arg pid "$partition" --arg grp "$PAYER" --arg lease "$lease" \
      '{transactionId: $txn, partitionId: $pid, consumerGroup: $grp, leaseId: $lease, status: "completed"}')"
    request POST /api/v1/ack "$ack_body"
    [ "$STATUS" = 200 ] || fail "ack returned HTTP $STATUS"
    printf '%s %s declined\n' "$PAYER" "$booking" >> "$OBSERVED"
    echo "  $booking: card declined, hold left to expire"
    return 0
  fi

  # The cancel rides the transaction: the booking is confirmed and its release
  # cancelled, or neither happens. Inside a transaction a cancel travels in the
  # timers array and shares the transaction's fate.
  if [ "$booking" = "$CANCEL_SKIPPED" ]; then
    timers='[]'
  else
    timers="$(jq -cn --arg expiries "$EXPIRIES" --arg booking "$booking" \
      '[{op: "cancel", queue: $expiries, timerKey: $booking}]')"
  fi

  # `expect` makes the "one worker per booking" assumption checkable: if it ever
  # fails, two consumers were serving one partition, and the run says so.
  body="$(jq -cn --arg ns "$NS" --arg key "$(saga_key "$booking")" \
    --argjson value "$value" --argjson version "$version" --argjson timers "$timers" \
    --arg txn "$txn" --arg pid "$partition" --arg grp "$PAYER" --arg lease "$lease" '
    {operations: [{type: "ack", transactionId: $txn, partitionId: $pid,
                   consumerGroup: $grp, leaseId: $lease, status: "completed"}],
     kv: [{op: "put", ns: $ns, key: $key, value: ($value + {step: "confirmed"}),
           ttlSeconds: 3600, expect: $version, required: true}],
     timers: $timers}')"
  request POST /api/v1/transaction "$body"
  [ "$STATUS" = 200 ] || fail "the confirming transaction for $booking returned HTTP $STATUS: $(cat "$OUT")"
  [ "$(jq -r '.success' "$OUT")" = true ] \
    || fail "$booking: confirmation lost its fence ($(jq -r '.kvReason' "$OUT"))"

  printf '%s %s confirmed\n' "$PAYER" "$booking" >> "$OBSERVED"
  if [ "$booking" = "$CANCEL_SKIPPED" ]; then
    echo "  $booking: paid and confirmed, release deliberately NOT cancelled"
  else
    echo "  $booking: paid and confirmed, release cancelled"
  fi
}

# ---------------------------------------------------------------------------
# handle_compensate: one delivery from the expiries queue, which is to say one
# message a timer produced.
#
# A release message asks a question: is this booking still only held? A fired
# timer leaves nothing behind, so a cancel that arrives a moment too late
# answers `absent` and the release is delivered anyway. The saga's state
# decides, and it is read first.
#
# This message arrives on another queue, in a partition unrelated to the
# payments partition, so nothing serialises the compensator with the payer.
# Here `expect` is what stops a release computed from a stale read from
# overwriting a confirmation that landed in between.
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
    # The booking was confirmed before this fired. Releasing the room now would
    # put a sold room back on sale.
    request POST /api/v1/ack "$ack_body"
    [ "$STATUS" = 200 ] || fail "ack returned HTTP $STATUS"
    printf '%s %s refused\n' "$COMPENSATOR" "$booking" >> "$OBSERVED"
    echo "  $booking: state is $step, release refused"
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
  [ "$STATUS" = 200 ] || fail "the compensating transaction for $booking returned HTTP $STATUS: $(cat "$OUT")"

  if [ "$(jq -r '.success' "$OUT")" != true ]; then
    # Confirmed between the read and the commit. The fence held, nothing was
    # written, and the room stays sold.
    [ "$(jq -r '.reason' "$OUT")" = kv_precondition ] \
      || fail "the compensating transaction for $booking failed: $(jq -r '.error' "$OUT")"
    request POST /api/v1/ack "$ack_body"
    [ "$STATUS" = 200 ] || fail "ack returned HTTP $STATUS"
    printf '%s %s refused\n' "$COMPENSATOR" "$booking" >> "$OBSERVED"
    echo "  $booking: confirmed in the meantime, release refused"
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
  'the reserver decided every submission'
check "$(grep -c "^$RESERVER .* rolled-back$" "$OBSERVED" || true)" 1 \
  'the duplicate submission of B-2 lost the gate, once'

# Pending timers can be listed: each release is a record in the broker.
request GET "/api/v1/timers/$EXPIRIES?limit=50"
[ "$STATUS" = 200 ] || fail "listing timers returned HTTP $STATUS"
echo "  timers armed: $(jq -r '[.rows[].timerKey] | sort | join(", ")' "$OUT")"
check "$(jq -r '.rows | length' "$OUT")" "$BOOKING_COUNT" \
  'one release per booking, none for the duplicate'

# ----------------------------------------------------------------------- paying
echo
echo "paying"
drain "$PAYMENTS" "$PAYER" handle_pay "$BOOKING_COUNT" "$PHASE_MS"

check "$(grep -c "^$PAYER " "$OBSERVED" || true)" "$BOOKING_COUNT" \
  'every booking was asked to pay once, B-2 included'
check "$(awk -v g="$PAYER" '$1 == g {print $2}' "$OBSERVED" | sort -u | wc -l | tr -d ' ')" \
  "$BOOKING_COUNT" 'no booking was asked to pay twice'

# A cancelled timer is gone before it fires. A peek is how you ask, and it
# answers {"found":false} with HTTP 200 for a timer that does not exist.
request GET "/api/v1/timers/$EXPIRIES/B-1"
[ "$STATUS" = 200 ] || fail "peek returned HTTP $STATUS"
check "$(jq -r '.found' "$OUT")" false \
  'the release cancelled with the confirmation is gone'
request GET "/api/v1/timers/$EXPIRIES/$DECLINED"
check "$(jq -r '.found' "$OUT")" true \
  "$DECLINED was never confirmed, so its release is still armed"
request GET "/api/v1/timers/$EXPIRIES/$CANCEL_SKIPPED"
check "$(jq -r '.found' "$OUT")" true \
  "$CANCEL_SKIPPED is confirmed and its release is still armed, on purpose"

# ----------------------------------------------------------------- compensating
echo
echo "compensating"
# Two releases were left armed, so two messages have to arrive: that is the
# count, and TIMER_DEADLINE_MS is the deadline behind it.
drain "$EXPIRIES" "$COMPENSATOR" handle_compensate 2 "$TIMER_DEADLINE_MS"
check "$(grep -c "^$COMPENSATOR " "$OBSERVED" || true)" 2 \
  'both armed releases were delivered'

# Then a short second pass with room for two more. The only way to show that
# the cancelled releases never arrive is to wait for them and see nothing: the
# first pass would have stopped at two, whatever those two were.
drain "$EXPIRIES" "$COMPENSATOR" handle_compensate 4 4000

# --------------------------------------------------------------------- checking
echo
echo "checking"

check "$(grep -c "^$COMPENSATOR " "$OBSERVED" || true)" 2 \
  'nothing else arrived on a second pass: still 2 releases'
check "$(grep -c "^$COMPENSATOR B-1 \|^$COMPENSATOR B-2 " "$OBSERVED" || true)" 0 \
  'no cancelled release was ever delivered'
check "$(cat "$RELEASED" | tr -d ' \n')" 103 \
  'exactly one room went back on sale, the declined one'
check "$(grep -c "^$COMPENSATOR $CANCEL_SKIPPED refused$" "$OBSERVED" || true)" 1 \
  "the late release for $CANCEL_SKIPPED was refused by the compensator"

# The saga's state is readable: "what happened to this booking" has an answer
# in the broker. getMany reports `missing` explicitly, so an absent key is named
# in the answer and never inferred by difference.
keys="$(printf '%s\n' $BOOKING_IDS | jq -R 'sub("^"; "saga:")' | jq -sc .)"
body="$(jq -cn --arg ns "$NS" --argjson keys "$keys" \
  '{operations: [{op: "getMany", ns: $ns, keys: $keys}]}')"
request POST /api/v1/kv "$body"
[ "$STATUS" = 200 ] || fail "kv getMany returned HTTP $STATUS"
check "$(jq -r '.results[0].rows | length' "$OUT")" "$BOOKING_COUNT" \
  'every booking has exactly one saga entry'
check "$(jq -r '.results[0].missing | length' "$OUT")" 0 \
  'no booking is missing its saga entry'
check "$(jq -r '[.results[0].rows[] | select(.value.step == "confirmed") | .key] | sort | join(",")' "$OUT")" \
  "saga:B-1,saga:B-2,saga:$CANCEL_SKIPPED" \
  "B-1, B-2 and $CANCEL_SKIPPED ended confirmed"
check "$(jq -r --arg key "saga:$DECLINED" '.results[0].rows[] | select(.key == $key) | .value.step' "$OUT")" \
  expired "$DECLINED was released by its timer, with no process waiting for it"

echo
echo "  final: $(jq -r '[.results[0].rows[] | (.key | sub("^saga:"; "")) + "=" + .value.step] | sort | join(", ")' "$OUT")"

echo
echo "PASS: $CHECKS checks"
# docs:end
