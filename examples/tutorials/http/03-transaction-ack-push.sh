# docs:start(tut-http-transaction-ack-push)
#!/usr/bin/env bash
#
# Tutorial 3 of 4: acknowledge and push in one transaction, over plain HTTP.
#
# Tutorial 2 handed work from one queue to the next in two steps: push the
# derived message, then acknowledge the source. Between those two requests a
# crash duplicates work, and in the other order it loses work. Two HTTP calls
# can never be one commit.
#
# POST /api/v1/transaction closes that window: the acknowledgement of the input
# and the push of the output travel in one body and become one PostgreSQL
# transaction. Both land or neither does.
#
# Run it:
#   QUEEN_URL=http://localhost:6632 bash 03-transaction-ack-push.sh

set -euo pipefail

QUEEN_URL="${QUEEN_URL:-http://localhost:6632}"
RUN="$(date +%s)-$$"
ORDERS="tut-http-tx-orders-$RUN"
INVOICES="tut-http-tx-invoices-$RUN"
GROUP=tut-http-invoicing

IDLE_MS=5000

# orderId customer total
INPUT='A-1 acme 120.5
B-1 globex 88.75
C-1 initech 310.0'
ORDER_COUNT=3

command -v jq >/dev/null 2>&1 || { echo "FAIL: jq is not installed"; exit 1; }

CHECKS=0
TMP="$(mktemp -d)"

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

check() {
  [ "$1" = "$2" ] || fail "$3 (expected [$2], got [$1])"
  CHECKS=$((CHECKS + 1))
  echo "  ok: $3"
}

request() {
  local method="$1" path="$2" body="${3:-}"
  if [ -n "$body" ]; then
    STATUS="$(curl -sS -o "$TMP/body" -w '%{http_code}' \
      -X "$method" "$QUEEN_URL$path" \
      -H 'content-type: application/json' -d "$body")"
  else
    STATUS="$(curl -sS -o "$TMP/body" -w '%{http_code}' -X "$method" "$QUEEN_URL$path")"
  fi
}

echo "broker $QUEEN_URL"

echo
echo "pushing $ORDER_COUNT orders"
while read -r id customer total; do
  body="$(jq -n --arg queue "$ORDERS" --arg partition "$customer" \
    --arg id "$id" --argjson total "$total" \
    '{items: [{queue: $queue, partition: $partition,
               payload: {orderId: $id, customer: $partition, total: $total}}]}')"
  request POST /api/v1/push "$body"
  [ "$STATUS" = 201 ] || fail "push of $id returned HTTP $STATUS"
  [ "$(jq -r '.[0].status' "$TMP/body")" = queued ] || fail "push of $id was not queued"
done <<EOF
$INPUT
EOF

# ---------------------------------------------------------------------------
# Invoicing.
#
# Nothing here acks on its own: autoAck defaults to false on the pop route, and
# leaving it false is what makes this tutorial possible. The acknowledgement is
# not a separate call any more, it is an operation inside the transaction below.
# (autoAck=true would commit the cursor inside the pop's own transaction, before
# the response is even written: at-most-once, and nothing left to ack.)
# ---------------------------------------------------------------------------
echo
echo "invoicing"
INVOICED=0
FIRST=yes

while [ "$INVOICED" -lt "$ORDER_COUNT" ]; do
  request GET "/api/v1/pop/queue/$ORDERS?consumerGroup=$GROUP&subscriptionMode=all&batch=10&wait=true&timeout=$IDLE_MS"
  [ "$STATUS" != 204 ] || fail "invoicing went idle after $INVOICED of $ORDER_COUNT orders"
  [ "$STATUS" = 200 ] || fail "pop returned HTTP $STATUS"
  cp "$TMP/body" "$TMP/pop"

  LEASE="$(jq -r .leaseId "$TMP/pop")"

  # Everything the transaction needs about a message: its address (transactionId
  # plus its own partitionId) and the payload fields the next stage wants.
  jq -r '.messages[] | [.transactionId, .partitionId, .data.orderId,
                        .data.customer, (.data.total | tostring)] | @tsv' \
    "$TMP/pop" > "$TMP/batch"

  while IFS=$'\t' read -r txn partition_id id customer total; do
    # -----------------------------------------------------------------------
    # One body, two operations, one commit.
    #
    # The ack operation carries consumerGroup EXPLICITLY. Leave it out and it
    # defaults to __QUEUE_MODE__, so the transaction would commit a cursor this
    # consumer never read from and the order would be redelivered forever. This
    # is the single easiest thing to get wrong on this route.
    #
    # The popped leaseId goes ON THE ACK OPERATION. That is the field the broker
    # checks: a lease that has expired, or that belongs to another worker, is
    # refused with "invalid or expired lease; transaction rolled back", and
    # neither the ack nor the push lands. The SDK builders send the lease in a
    # top-level "requiredLeases" array instead; this route accepts that too, but
    # only as a hint it falls back to when it cannot resolve the live lease on
    # its own, so a wrong value there is silently ignored and the transaction
    # commits anyway. From curl, put it on the operation.
    #
    # The pushed transactionId is derived from the order, not random. The commit
    # is atomic but the round trip is not: if the response is lost you cannot
    # know whether it committed, and a retry with a deterministic id either
    # commits once or is rejected as a duplicate. Both end states are the same
    # one. With a random id, a retry writes the invoice twice.
    # -----------------------------------------------------------------------
    tx_body="$(jq -n \
      --arg txn "$txn" --arg partitionId "$partition_id" --arg group "$GROUP" \
      --arg lease "$LEASE" --arg queue "$INVOICES" --arg partition "$customer" \
      --arg id "$id" --argjson total "$total" \
      '{
        operations: [
          { type: "ack",
            transactionId: $txn,
            partitionId:   $partitionId,
            consumerGroup: $group,
            leaseId:       $lease,
            status:        "completed" },
          { type: "push",
            items: [{ queue: $queue,
                      partition: $partition,
                      transactionId: ("INV-" + $id),
                      payload: { invoiceId: ("INV-" + $id),
                                 orderId: $id,
                                 amount: $total } }] }
        ]
      }')"

    if [ "$FIRST" = yes ]; then
      echo
      echo "POST $QUEEN_URL/api/v1/transaction"
      echo "$tx_body" | jq .
    fi

    request POST /api/v1/transaction "$tx_body"

    if [ "$FIRST" = yes ]; then
      echo "-> HTTP $STATUS"
      jq . "$TMP/body"
      echo
      FIRST=no
    fi

    # A rollback is a 200 with success:false and the reason at the top level, so
    # the status code is not the answer here either. Check the transaction, not
    # just the absence of a connection error.
    [ "$STATUS" = 200 ] || fail "transaction returned HTTP $STATUS"
    [ "$(jq -r .success "$TMP/body")" = true ] \
      || fail "transaction rejected: $(jq -r '.error // "no reason given"' "$TMP/body")"

    INVOICED=$((INVOICED + 1))
    echo "  $id -> INV-$id"
  done < "$TMP/batch"
done

check "$INVOICED" "$ORDER_COUNT" 'every order was invoiced once'

# The commit fails if the lease has expired, which is what stops a slow consumer
# from acking work the broker has already handed to someone else. Nothing to
# assert here: the check above is that assertion, since a rejected commit would
# have failed the run.

# ---------------------------------------------------------------------------
# The invoices went to one partition per customer, and a pop claims a single
# partition unless you say otherwise: partitions=10 lets this one call claim up
# to ten of them, with batch as the total budget shared across all of them, not
# a per-partition limit. Without it this pop would return one customer's invoice
# and the check below would fail on a queue that is perfectly fine.
# ---------------------------------------------------------------------------
echo
echo "checking the output queue"
echo "GET /api/v1/pop/queue/$INVOICES?batch=10&partitions=10&wait=true&timeout=$IDLE_MS"
request GET "/api/v1/pop/queue/$INVOICES?batch=10&partitions=10&wait=true&timeout=$IDLE_MS"
[ "$STATUS" != 204 ] || fail 'no invoices were delivered'
[ "$STATUS" = 200 ] || fail "invoice pop returned HTTP $STATUS"
cp "$TMP/body" "$TMP/pop"
echo "-> HTTP 200, $(jq -r .partitionsClaimed "$TMP/pop") partitions claimed"

check "$(jq '.messages | length' "$TMP/pop")" "$ORDER_COUNT" "$ORDER_COUNT invoices exist"
check "$(jq -r '[.messages[].data.orderId] | sort | join(",")' "$TMP/pop")" \
  'A-1,B-1,C-1' 'each invoice matches an order, none duplicated'

# And the input queue is committed for this group: the acks were part of the
# same transactions that produced those invoices, so the two states cannot
# disagree. wait is left off, so this is a single attempt: 204 means the cursor
# is at the end.
request GET "/api/v1/pop/queue/$ORDERS?consumerGroup=$GROUP&batch=10"
check "$STATUS" 204 'the source queue is committed for this group'

request DELETE "/api/v1/resources/queues/$ORDERS"
request DELETE "/api/v1/resources/queues/$INVOICES"

echo
echo "PASS: $CHECKS checks"
# docs:end
