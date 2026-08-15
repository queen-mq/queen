# docs:start(tut-http-replay)
#!/usr/bin/env bash
#
# Tutorial 4 of 4: replay, over plain HTTP.
#
# Acknowledging a message does not delete it. Consumption is a cursor per
# consumer group, and the messages stay until retention removes them, so a new
# group can read the whole history and an existing group can be moved back.
#
# This is the tutorial that shows what a cursor buys you: reprocessing after a
# bug, backfilling a new consumer, and auditing what was delivered, all without
# asking the producer to send anything twice. Two routes do all of it, and
# neither of them touches a message: subscriptionMode on the pop seeds a cursor
# that does not exist yet, and the seek route moves one that does.
#
# Run it:
#   QUEEN_URL=http://localhost:6632 bash 04-replay.sh

set -euo pipefail

QUEEN_URL="${QUEEN_URL:-http://localhost:6632}"
RUN="$(date +%s)-$$"
EVENTS="tut-http-replay-$RUN"
PARTITION=order-1

LIVE=tut-http-live
AUDIT=tut-http-audit

IDLE_MS=4000
EVENT_COUNT=4
EXPECTED=1,2,3,4

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

# Commits the batch in $TMP/pop by acking its last message: the cursor moves to
# that offset and everything before it in the same partition is completed with
# it. Acking is what makes a group's progress durable; it is not what makes the
# messages go away, which is the whole point of this tutorial.
ack_batch() {
  local ack_body
  ack_body="$(jq -c --arg group "$1" '{
    transactionId: .messages[-1].transactionId,
    partitionId:   .messages[-1].partitionId,
    consumerGroup: $group,
    leaseId:       .leaseId,
    status:        "completed"
  }' "$TMP/pop")"
  request POST /api/v1/ack "$ack_body"
  [ "$STATUS" = 200 ] || fail "ack returned HTTP $STATUS"
  [ "$(jq -r '.[0].success' "$TMP/body")" = true ] \
    || fail "ack refused: $(jq -r '.[0].error' "$TMP/body")"
}

# drain <group> <expected> [subscriptionMode]
#
# Reads the lane through one group's cursor until it has seen <expected> events,
# acking as it goes, and leaves the sequence numbers it saw in $SEEN as a
# comma-separated list. A pop that long-polls its way to a 204 fails the run:
# with a known event count, silence means a message is missing.
drain() {
  local group="$1" expected="$2" mode="${3:-}" query seen
  query="consumerGroup=$group&batch=10"
  [ -z "$mode" ] || query="$query&subscriptionMode=$mode"

  : > "$TMP/seen"
  seen=0
  while [ "$seen" -lt "$expected" ]; do
    request GET "/api/v1/pop/queue/$EVENTS/partition/$PARTITION?$query&wait=true&timeout=$IDLE_MS"
    [ "$STATUS" != 204 ] || fail "$group went idle after $seen of $expected events"
    [ "$STATUS" = 200 ] || fail "pop for $group returned HTTP $STATUS"
    cp "$TMP/body" "$TMP/pop"
    jq -r '.messages[].data.seq' "$TMP/pop" >> "$TMP/seen"
    seen="$(wc -l < "$TMP/seen" | tr -d ' ')"
    ack_batch "$group"
  done
  SEEN="$(paste -sd, - < "$TMP/seen")"
}

echo "broker $QUEEN_URL"

# ---------------------------------------------------------------------------
# One call, four items. The items array is a batch: items in one request may
# even target different queues and partitions, and each (queue, partition) group
# becomes one segment and one commit. All four here share a partition, so they
# share a segment, and their order in the array is their order in the lane.
# ---------------------------------------------------------------------------
push_body="$(jq -n --arg queue "$EVENTS" --arg partition "$PARTITION" '{
  items: [
    {queue: $queue, partition: $partition, payload: {seq: 1, type: "created"}},
    {queue: $queue, partition: $partition, payload: {seq: 2, type: "updated"}},
    {queue: $queue, partition: $partition, payload: {seq: 3, type: "shipped"}},
    {queue: $queue, partition: $partition, payload: {seq: 4, type: "delivered"}}
  ]
}')"

echo
echo "POST $QUEEN_URL/api/v1/push"
echo "$push_body" | jq -c '.items[]'
request POST /api/v1/push "$push_body"
[ "$STATUS" = 201 ] || fail "push returned HTTP $STATUS"
echo "-> HTTP $STATUS"
jq -c '.[]' "$TMP/body"

# One element per item, in request order. Anything other than "queued" here
# would mean the message is not in PostgreSQL, whatever the 201 says.
[ "$(jq '[.[] | select(.status == "queued")] | length' "$TMP/body")" = "$EVENT_COUNT" ] \
  || fail "not every event was queued"

# ---------------------------------------------------------------------------
# The live consumer. It drains the lane and commits as it goes.
# ---------------------------------------------------------------------------
echo
echo "the live consumer"
drain "$LIVE" "$EVENT_COUNT" all
echo "  saw $SEEN"
check "$SEEN" "$EXPECTED" 'the live group read the lane in order'

# ---------------------------------------------------------------------------
# A second group, created now, after every message was already stored and
# acknowledged by someone else. subscriptionMode=all is what points its new
# cursor at the oldest retained message: the default for a new group is the
# tail, so without it this group would sit idle waiting for a fifth event and
# the pop would 204 out.
#
# The mode applies when the cursor is created and never again, so it cannot
# rewind a group that already exists. That is what seek below is for.
# ---------------------------------------------------------------------------
echo
echo "a new group, backfilled from the beginning"
drain "$AUDIT" "$EVENT_COUNT" all
echo "  saw $SEEN"
check "$SEEN" "$EXPECTED" 'a new group replayed the whole history'

# ---------------------------------------------------------------------------
# Nothing was re-pushed and nothing was copied: both groups read the same stored
# messages through their own cursors.
#
# Now rewind an existing group. The queue-scoped seek moves every partition of
# the queue for that group. The body takes either {"toEnd": true} or a
# timestamp; a body with neither is a 400.
#
# An hour ago is before anything in this run was pushed. Resolution is
# segment-granular, not per-message: the cursor lands just before the first
# segment created at or after the timestamp, and a timestamp older than what is
# retained lands on the oldest retained segment. The seek also releases any live
# lease, so an in-flight batch is abandoned rather than acknowledged.
#
# date is the one part of this script that is not the same on every machine:
# BSD date (macOS) spells it -v-1H, GNU date spells it -d '1 hour ago'.
# ---------------------------------------------------------------------------
echo
echo "rewinding an existing group"
HOUR_AGO="$(date -u -v-1H +%Y-%m-%dT%H:%M:%SZ 2>/dev/null \
  || date -u -d '1 hour ago' +%Y-%m-%dT%H:%M:%SZ)"

seek_body="$(jq -n --arg timestamp "$HOUR_AGO" '{timestamp: $timestamp}')"
echo "POST $QUEEN_URL/api/v1/consumer-groups/$LIVE/queues/$EVENTS/seek"
echo "$seek_body" | jq .
request POST "/api/v1/consumer-groups/$LIVE/queues/$EVENTS/seek" "$seek_body"
echo "-> HTTP $STATUS"
jq . "$TMP/body"

# A seek that matched no partition is a 404 with success:false, so this one is
# worth reading rather than assuming.
[ "$STATUS" = 200 ] || fail "seek returned HTTP $STATUS"
[ "$(jq -r .success "$TMP/body")" = true ] \
  || fail "seek failed: $(jq -r '.error // "no reason given"' "$TMP/body")"

# No subscriptionMode this time: the group's cursor already exists, so the
# parameter would be ignored. The seek is what moved it.
drain "$LIVE" "$EVENT_COUNT"
echo "  saw $SEEN"
check "$SEEN" "$EXPECTED" 'the rewound group read the same events again, in the same order'

# ---------------------------------------------------------------------------
# Replay is per group. The audit group was not moved, so it stays where it was
# and sees nothing new: a single-attempt pop (wait off) answers 204 immediately.
# ---------------------------------------------------------------------------
request GET "/api/v1/pop/queue/$EVENTS/partition/$PARTITION?consumerGroup=$AUDIT&batch=10"
check "$STATUS" 204 'rewinding one group left the other where it was'

request DELETE "/api/v1/resources/queues/$EVENTS"

echo
echo "PASS: $CHECKS checks"
# docs:end
