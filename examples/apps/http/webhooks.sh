# docs:start(app-http-webhooks)
#!/usr/bin/env bash
#
# A webhook sender, with nothing but curl: ordered per endpoint, retried by the
# broker, and dead-lettered with its error when it never succeeds.
#
# Deliveries to one endpoint have to arrive in the order the events happened,
# an endpoint that is down must not slow anybody else down, a failure is
# retried a bounded number of times, and what never succeeds has to end up
# somewhere a person can read it. Each endpoint gets a partition of its own,
# created by its first delivery, so a dead endpoint backs up its own partition
# and nothing else. Retries are the broker's retry budget, and an exhausted
# delivery lands in the dead-letter queue with the error attached.
#
#   webhook-deliveries (one partition per endpoint)
#     └── group "sender"  POSTs each delivery; a failure spends one retry
#           └── retryLimit spent -> dead-letter queue, with the error
#
# One sender runs per endpoint, as a background subshell on the
# partition-scoped pop route, so the isolation is real here: the dead endpoint
# spends the whole run failing and retrying while the two healthy ones are
# already finished.
#
# Run it:
#   QUEEN_URL=http://localhost:6632 bash webhooks.sh

set -euo pipefail

QUEEN_URL="${QUEEN_URL:-http://localhost:6632}"

# The name carries the language and a per-run suffix, so every application in
# every language can share one broker and no run inherits another's state.
RUN="$(date +%s)-$$"
DELIVERIES="app-http-webhooks-$RUN"
GROUP=app-http-sender

# Three subscribers: endpoint and whether it is healthy. One of them answers 500
# to everything. It is listed first, so its deliveries are the oldest in the
# queue and its sender starts first; the checks below show that the healthy
# endpoints get every event anyway, in order.
ENDPOINTS='initech.example no
acme.example yes
globex.example yes'
EVENTS_PER_ENDPOINT=3
RETRY_LIMIT=2

# 1,2,3: what a healthy subscriber must receive, in that order.
EXPECTED_SEQS="$(seq 1 "$EVENTS_PER_ENDPOINT" | paste -sd, -)"

# Every pop long-polls for this many milliseconds and no longer, so a sender
# re-checks its own progress at least once a second.
POLL_MS=1000

# The bound that keeps a stall from becoming a hang. A sender that has not
# finished its endpoint by then stops, and the checks that follow report what is
# missing. Never wait for silence; wait for a total, with a deadline.
SEND_MS=30000

command -v jq >/dev/null 2>&1 || { echo "FAIL: jq is not installed"; exit 1; }

CHECKS=0
TMP="$(mktemp -d)"

# One exit path for everything. A failed check calls fail(), which records the
# reason and exits 1; any other command that fails under `set -e` arrives here
# too, with its own status. FAIL is printed exactly once, and only on failure.
cleanup() {
  local status=$?
  rm -rf "$TMP"
  if [ "$status" -ne 0 ]; then
    echo
    echo "FAIL: ${FAILURE:-a command exited with status $status}"
  fi
  exit "$status"
}
trap cleanup EXIT

fail() { FAILURE="$*"; exit 1; }

# check <actual> <expected> <description>
check() {
  [ "$1" = "$2" ] || fail "$3 (expected [$2], got [$1])"
  CHECKS=$((CHECKS + 1))
  echo "  ok: $3"
}

# A millisecond clock, for the deadline only. GNU date spells it %3N; BSD date
# (macOS) has no %N and leaves the unconverted tail in the output, so a probe for
# anything that is not a digit tells the two apart, and perl, whose Time::HiRes
# is core, is the fallback.
if [ -z "$(date +%s%3N 2>/dev/null | tr -d '0-9')" ]; then
  now_ms() { date +%s%3N; }
else
  command -v perl >/dev/null 2>&1 \
    || { echo "FAIL: need GNU date or perl for a millisecond clock"; exit 1; }
  now_ms() { perl -MTime::HiRes -e 'printf "%d", Time::HiRes::time() * 1000'; }
fi

# Sets $STATUS to the HTTP status code and writes the response body to $OUT.
#
# $OUT is per-process: each sender below runs as a background subshell and points
# it at its own file, so three concurrent pops never overwrite each other's
# response. There is no --fail, because Queen reports outcomes in the body and
# several of the interesting ones arrive as 200.
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

# healthy <endpoint>: prints yes or no. The shell this has to run on has no
# associative arrays, so the subscriber table is a few lines of text and awk is
# the lookup.
healthy() {
  printf '%s\n' "$ENDPOINTS" | awk -v e="$1" '$1 == e { print $2 }'
}

# Stands in for the HTTP POST to the subscriber. A real sender calls curl and
# fails on any status that is not 2xx, which is what returning non-zero does
# here.
post_to_endpoint() {
  [ "$(healthy "$1")" = yes ] || return 1
  return 0
}

# lines <file>: how many lines a tally file holds, 0 when it does not exist yet.
lines() {
  [ -f "$1" ] || { echo 0; return; }
  wc -l < "$1" | tr -d ' '
}

echo "broker $QUEEN_URL"

# ---------------------------------------------------------------------------
# retryLimit is the delivery budget: a delivery that fails RETRY_LIMIT + 1
# times is filed in the dead-letter queue with its last error. Dead-lettering
# is on by default on a new queue (deadLetterQueue and dlqAfterMaxRetries); with
# both off, a delivery that spends its budget is skipped and gone. Both flags
# are sent anyway, because /configure merges: an option this body does not name
# keeps whatever the queue already has, and depending on that is how a
# configuration drifts.
#
# leaseTime is how long the broker waits for a sender that took a delivery and
# never came back before it hands the delivery to another sender.
# ---------------------------------------------------------------------------
configure_body="$(jq -n --arg queue "$DELIVERIES" --argjson retry "$RETRY_LIMIT" \
  '{queue: $queue,
    options: {leaseTime: 30, retryLimit: $retry,
              deadLetterQueue: true, dlqAfterMaxRetries: true}}')"
request POST /api/v1/configure "$configure_body"
[ "$STATUS" = 200 ] || fail "configure returned HTTP $STATUS"
check "$(jq -r .configured "$OUT")" true \
  "the queue was created with a delivery budget of $RETRY_LIMIT retries"

# ------------------------------------------------------------------------ queuing
#
# The application emits events. Each delivery goes into the partition of the
# endpoint it is for, which is what makes "in order per subscriber" a property
# of the storage instead of something the sender has to arrange. Nothing was
# declared for a subscriber in advance: the partition comes into existence with
# the first delivery to it.
echo
echo "queuing deliveries"
seq_no=1
while [ "$seq_no" -le "$EVENTS_PER_ENDPOINT" ]; do
  while read -r endpoint is_healthy; do
    # The event id makes the enqueue idempotent: an application that retries its
    # own emit does not create a second delivery. The wire field for the body is
    # "payload"; "data" is what a pop calls it on the way back.
    body="$(jq -n --arg queue "$DELIVERIES" --arg endpoint "$endpoint" \
      --argjson seq "$seq_no" \
      '{items: [{
         queue:     $queue,
         partition: $endpoint,
         transactionId: ($endpoint + "-evt-" + ($seq | tostring)),
         payload: {endpoint: $endpoint, seq: $seq, type: "invoice.paid",
                   invoiceId: ("INV-" + ($seq | tostring))}
       }]}')"
    request POST /api/v1/push "$body"
    [ "$STATUS" = 201 ] || fail "push of $endpoint/$seq_no returned HTTP $STATUS"
    # HTTP 201 is not proof the message was stored: an item the broker refused
    # comes back "error" inside a 201. The per-item status is the only answer.
    [ "$(jq -r '.[0].status' "$OUT")" = queued ] \
      || fail "push of $endpoint/$seq_no came back $(jq -r '.[0].status' "$OUT")"
  done <<EOF
$ENDPOINTS
EOF
  seq_no=$((seq_no + 1))
done
echo "  $((EVENTS_PER_ENDPOINT * 3)) deliveries queued"

# ------------------------------------------------------------------------ sending
#
# die() is a sender's fail(): a sender is a subshell, so its variables die with
# it and the parent would never see FAILURE. It leaves the reason in a file the
# parent reads after wait().
die() { printf '%s\n' "$*" > "$TMP/sender-error"; exit 1; }

# send_endpoint <endpoint>
#
# One sender, one endpoint. It pops that endpoint's partition by name, posts
# what it gets, and reports the outcome back to the broker; the loop is what an
# SDK's consume() does, and the ack status is what an SDK's autoAck derives from
# a handler that returned or threw.
#
# It stops when the endpoint's deliveries are resolved, either because every
# event was delivered or because every event was dead-lettered, and in any case
# at the deadline.
send_endpoint() {
  local endpoint="$1"
  local deadline popfile delivered_file attempts_file dead_file
  local txn partition_id lease event_seq attempt aborted ack_body

  OUT="$TMP/$endpoint-body"
  popfile="$TMP/$endpoint-pop"
  delivered_file="$TMP/delivered-$endpoint"
  attempts_file="$TMP/attempts-$endpoint"
  dead_file="$TMP/dead-$endpoint"
  : > "$delivered_file"
  : > "$attempts_file"
  : > "$dead_file"
  deadline=$(( $(now_ms) + SEND_MS ))

  while [ "$(( $(lines "$delivered_file") + $(lines "$dead_file") ))" \
          -lt "$EVENTS_PER_ENDPOINT" ]; do
    [ "$(now_ms)" -lt "$deadline" ] || break

    # The partition-scoped pop route claims exactly the partition you name. The
    # queue-scoped one lets the broker pick, and with `partitions` at its
    # default of 1 (and no autopilot=true) it would claim a single partition per
    # call anyway; naming the partition is what makes this sender belong to one
    # endpoint.
    #
    # subscriptionMode=all is what makes a group created now read what was pushed
    # before it existed: a new cursor is seeded at the TAIL unless you say
    # otherwise. It seeds a cursor that does not exist yet, and is ignored on
    # every later pop.
    request GET "/api/v1/pop/queue/$DELIVERIES/partition/$endpoint?consumerGroup=$GROUP&subscriptionMode=all&batch=10&wait=true&timeout=$POLL_MS"
    # 204 is an empty pop, with no body at all. Here it means the partition is
    # quiet for the moment; the loop condition decides whether that is the end.
    [ "$STATUS" != 204 ] || continue
    [ "$STATUS" = 200 ] || die "pop on $endpoint returned HTTP $STATUS"
    cp "$OUT" "$popfile"

    # Every popped message carries deliveryAttempt: 1 the first time, one more
    # each time the same delivery comes back.
    lease="$(jq -r .leaseId "$popfile")"
    jq -r '.messages[] | [.transactionId, .partitionId, (.data.seq | tostring),
                          (.deliveryAttempt | tostring)] | @tsv' \
      "$popfile" > "$TMP/$endpoint-batch"

    aborted=0
    while IFS=$'\t' read -r txn partition_id event_seq attempt; do
      echo "$event_seq" >> "$attempts_file"

      if ! post_to_endpoint "$endpoint"; then
        echo "  $endpoint <- event $event_seq failed on attempt $attempt: $endpoint answered 500"
        # -------------------------------------------------------------------
        # The delivery failed. A `failed` ack is the nack: it leaves the cursor
        # just below this message, so this one and everything after it in the
        # batch is delivered again, and it spends one retry of the budget.
        # There is no loop in this sender and no sleep: the redelivery is the
        # retry, and it survives this process dying halfway, which a retry
        # loop inside the sender would not.
        #
        # A lease that merely runs out spends nothing: only a `failed` ack
        # does, so a sender that keeps crashing never uses up a delivery's
        # budget by crashing. deliveryAttempt is how a sender can set a limit
        # of its own for that case.
        #
        # `error` is the reason. A plain nack does not store it; the nack that
        # dead-letters the delivery files it with the dead letter, where a
        # person reads it later.
        #
        # The rest of the batch is left alone. The nack released this
        # partition's lease, so an ack of any later message of this batch would
        # be refused, and those messages are coming back anyway.
        # -------------------------------------------------------------------
        ack_body="$(jq -n --arg txn "$txn" --arg partitionId "$partition_id" \
          --arg group "$GROUP" --arg lease "$lease" \
          --arg error "$endpoint answered 500" \
          '{transactionId: $txn, partitionId: $partitionId, consumerGroup: $group,
            leaseId: $lease, status: "failed", error: $error}')"
        request POST /api/v1/ack "$ack_body"
        [ "$STATUS" = 200 ] || die "ack for $endpoint returned HTTP $STATUS"
        [ "$(jq -r '.[0].success' "$OUT")" = true ] \
          || die "ack refused for $endpoint: $(jq -r '.[0].error' "$OUT")"

        # dlq on the ack result is how a sender on raw HTTP learns the budget ran
        # out: true means this nack filed the dead letter and moved the cursor
        # past the delivery.
        if [ "$(jq -r '.[0].dlq' "$OUT")" = true ]; then
          echo "$event_seq" >> "$dead_file"
          echo "  $endpoint dead-lettered event $event_seq, its retry budget is spent"
        fi
        aborted=1
        break
      fi

      echo "$event_seq" >> "$delivered_file"
      echo "  $endpoint <- event $event_seq"
    done < "$TMP/$endpoint-batch"

    # Everything in the batch was posted, so commit the batch with one ack of its
    # LAST message: an ack is a cursor commit, so that completes every earlier
    # message of this partition for this group too. consumerGroup is mandatory,
    # here as everywhere: omit it and the ack goes to __QUEUE_MODE__, a cursor
    # this sender never read from, so it is refused and the batch comes back
    # every time its lease runs out.
    if [ "$aborted" = 0 ]; then
      ack_body="$(jq -c --arg group "$GROUP" '{
        transactionId: .messages[-1].transactionId,
        partitionId:   .messages[-1].partitionId,
        consumerGroup: $group,
        leaseId:       .leaseId,
        status:        "completed"
      }' "$popfile")"
      request POST /api/v1/ack "$ack_body"
      [ "$STATUS" = 200 ] || die "ack for $endpoint returned HTTP $STATUS"
      [ "$(jq -r '.[0].success' "$OUT")" = true ] \
        || die "ack refused for $endpoint: $(jq -r '.[0].error' "$OUT")"
    fi
  done
}

echo
echo "sending"
rm -f "$TMP/sender-error"
PIDS=""
while read -r endpoint is_healthy; do
  send_endpoint "$endpoint" &
  PIDS="$PIDS $!"
done <<EOF
$ENDPOINTS
EOF
for pid in $PIDS; do
  wait "$pid" \
    || fail "a sender stopped: $(cat "$TMP/sender-error" 2>/dev/null || echo 'no reason recorded')"
done

# ----------------------------------------------------------------------- checking
echo
echo "checking"

while read -r endpoint is_healthy; do
  [ "$is_healthy" = yes ] || continue
  check "$(lines "$TMP/delivered-$endpoint")" "$EVENTS_PER_ENDPOINT" \
    "$endpoint received all $EVENTS_PER_ENDPOINT events"
  check "$(paste -sd, - < "$TMP/delivered-$endpoint")" "$EXPECTED_SEQS" \
    "$endpoint received them in the order they happened"
done <<EOF
$ENDPOINTS
EOF

check "$(lines "$TMP/delivered-initech.example")" 0 \
  'the dead endpoint received nothing'

check "$(lines "$TMP/attempts-initech.example")" \
  "$((EVENTS_PER_ENDPOINT * (RETRY_LIMIT + 1)))" \
  "each dead delivery was tried $((RETRY_LIMIT + 1)) times before it was given up"

check "$(lines "$TMP/dead-initech.example")" "$EVENTS_PER_ENDPOINT" \
  'the broker reported a dead letter on the ack that spent each budget'

# ---------------------------------------------------------------------------
# The dead letters are records you can query. Each one keeps the payload, so it
# names the endpoint and the invoice, and the last error, which is what answers
# "why did this customer not get the webhook".
#
# Dead letters follow the queue's retention: one goes when retention removes
# the message it holds. With retention off, as here, a dead letter stays until
# you replay it with POST /api/v1/messages/:partitionId/:transactionId/retry,
# delete it, or delete the queue.
# ---------------------------------------------------------------------------
request GET "/api/v1/dlq?queue=$DELIVERIES&limit=50"
[ "$STATUS" = 200 ] || fail "the dead-letter listing returned HTTP $STATUS"
cp "$OUT" "$TMP/dlq"

check "$(jq '[.messages[] | select(.data.endpoint == "initech.example")] | length' "$TMP/dlq")" \
  "$EVENTS_PER_ENDPOINT" \
  "all $EVENTS_PER_ENDPOINT dead deliveries are in the dead-letter queue"
check "$(jq '[.messages[] | select((.errorMessage // "") | contains("answered 500"))] | length' "$TMP/dlq")" \
  "$EVENTS_PER_ENDPOINT" 'each dead letter carries the error that killed it'
check "$(jq '[.messages[] | select(.data.endpoint != "initech.example")] | length' "$TMP/dlq")" 0 \
  'no healthy delivery ended up in the dead-letter queue'

echo
echo "  dead letters: $(jq -r '[.messages[] | .data.endpoint + "/" + .data.invoiceId + ": " + (.errorMessage // "")] | sort | join("; ")' "$TMP/dlq")"

# Clean up on success only: a failed run leaves the queue, and its dead letters,
# on the broker to be looked at. Deleting a queue that does not exist also
# answers 200, with deleted:false, so the field is what to check.
request DELETE "/api/v1/resources/queues/$DELIVERIES"
[ "$(jq -r .deleted "$OUT")" = true ] || fail 'the queue was not deleted'

echo
echo "PASS: $CHECKS checks"
# docs:end
