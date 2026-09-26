#!/usr/bin/env bash
# Two-tenant isolation, driven DIRECTLY at the brokers with the trusted
# `x-queen-tenant` header and QUEEN_TENANCY_HEADER=true on every node. No proxy
# in the picture: with the embedded proxy in front, it is the proxy that sets
# this header from the caller's credential.
#
# Why this exists: a shared cell runs the broker with the tenant header on, so
# every piece of per-queue state — queue identity and config, partitions,
# consumer-group cursors, the dedup window, the parked-pop wake gates — must be
# keyed by (tenant, queue), never by the queue name alone. What this runner pins
# down is that one tenant can neither SEE, nor advance, nor HIDE or DELAY another
# tenant's data on a shared cell.
#
# Shape of every scenario: the SAME queue name, the SAME partition name and the
# SAME consumer-group name owned by two different tenants, with traffic driven
# through BOTH URLs. On the `ha-tenanted` topology QUEEN_A_URL and QUEEN_B_URL
# are two nodes of the raft cluster (a follower forwards to the leader, so the
# traffic crosses nodes); on `tenanted` both are the single node.
set -uo pipefail

A="${QUEEN_A_URL:?QUEEN_A_URL not set}"
B="${QUEEN_B_URL:?QUEEN_B_URL not set}"

# Opaque scoping keys; the broker does not validate them against any registry.
TA="${QUEEN_TENANT_A:-11111111-1111-1111-1111-111111111111}"
TB="${QUEEN_TENANT_B:-22222222-2222-2222-2222-222222222222}"

PASS=0; FAIL=0; NOTES=0
say()  { printf '%s\n' "$*"; }
ok()   { PASS=$((PASS+1)); say "  ok   - $1"; }
bad()  { FAIL=$((FAIL+1)); say "  FAIL - $1"; }
note() { NOTES=$((NOTES+1)); say "  note - $1"; }
eq()   { if [ "$2" = "$3" ]; then ok "$1"; else bad "$1 (want '$2', got '$3')"; fi; }

RC=""; BODY=""
call() { # method base tenant path [json-body]
  local m=$1 base=$2 t=$3 p=$4 b=${5:-}
  local args=(-s -o /tmp/tenbody -w '%{http_code}' -X "$m" -H "x-queen-tenant: $t")
  [ -n "$b" ] && args+=(-H 'Content-Type: application/json' --data-binary "$b")
  RC=$(curl "${args[@]}" "$base$p" 2>/dev/null)
  BODY=$(cat /tmp/tenbody 2>/dev/null)
}

# messages[] length of the last pop. An empty pop answers 204, which carries NO
# body at all, so the empty-body case has to be handled before jq sees it.
nmsgs() {
  [ -n "${BODY:-}" ] || { echo 0; return; }
  printf '%s' "$BODY" | jq -r '(.messages // []) | length' 2>/dev/null || echo 0
}
# sorted-unique list of payload .t markers in the last pop, comma joined.
tags() {
  [ -n "${BODY:-}" ] || { echo ""; return; }
  printf '%s' "$BODY" | jq -r '[(.messages // [])[].data.t] | unique | join(",")' 2>/dev/null || echo ""
}
# jq scalar out of the last body. NB `tostring`, never `// "default"`: jq's `//`
# treats a literal `false` as empty, which would silently turn a real
# "success":false into the fallback.
jv() { printf '%s' "${BODY:-}" | jq -r "($1) | tostring" 2>/dev/null || echo "?"; }

export QUEEN_WAIT_URLS="$A/health $B/health"
/usr/local/bin/wait-for-broker

# Refuse to "pass" against a broker that ignores the header — otherwise every
# assertion below would compare the default tenant with itself and be vacuous.
call POST "$A" "$TA" /api/v1/configure '{"queue":"tenancy-probe","leaseTime":123}'
call POST "$B" "$TB" /api/v1/configure '{"queue":"tenancy-probe","leaseTime":456}'
call GET  "$A" "$TA" /api/v1/status/queues/tenancy-probe
PROBE=$(printf '%s' "$BODY" | jq -r 'try .queue.config.leaseTime // "?"')
if [ "$PROBE" != "123" ]; then
  say "!! tenancy probe failed: tenant A sees leaseTime=$PROBE after tenant B configured 456."
  say "!! Either QUEEN_TENANCY_HEADER is not true on these brokers, or scoping is broken."
  say "TENANCY: FAIL"
  exit 1
fi
say "tenancy probe: header honoured (A=123 while B=456)"

Q=tenancy-iso
P=shared-part

say ""
say "== 1. queue identity + config are per-tenant (configured on DIFFERENT brokers) =="
call POST "$A" "$TA" /api/v1/configure \
  "{\"queue\":\"$Q\",\"namespace\":\"ns-a\",\"task\":\"task-a\",\"leaseTime\":300,\"retryLimit\":3}"
eq "configure A on queen-a" 200 "$RC"
call POST "$B" "$TB" /api/v1/configure \
  "{\"queue\":\"$Q\",\"namespace\":\"ns-b\",\"task\":\"task-b\",\"leaseTime\":77,\"retryLimit\":9}"
eq "configure B on queen-b" 200 "$RC"

QID_A=""; QID_B=""
for pair in "queen-a:$A" "queen-b:$B"; do
  n="${pair%%:*}"; u="${pair#*:}"
  call GET "$u" "$TA" "/api/v1/status/queues/$Q"
  la=$(printf '%s' "$BODY" | jq -r 'try .queue.config.leaseTime // "?"')
  ra=$(printf '%s' "$BODY" | jq -r 'try .queue.config.retryLimit // "?"')
  na=$(printf '%s' "$BODY" | jq -r 'try .queue.namespace // "?"')
  QID_A=$(printf '%s' "$BODY" | jq -r 'try .queue.id // ""')
  eq "$n: tenant A leaseTime unchanged by B's configure" "300" "$la"
  eq "$n: tenant A retryLimit unchanged by B's configure" "3" "$ra"
  eq "$n: tenant A namespace" "ns-a" "$na"
  call GET "$u" "$TB" "/api/v1/status/queues/$Q"
  lb=$(printf '%s' "$BODY" | jq -r 'try .queue.config.leaseTime // "?"')
  rb=$(printf '%s' "$BODY" | jq -r 'try .queue.config.retryLimit // "?"')
  nb=$(printf '%s' "$BODY" | jq -r 'try .queue.namespace // "?"')
  QID_B=$(printf '%s' "$BODY" | jq -r 'try .queue.id // ""')
  eq "$n: tenant B leaseTime is its own" "77" "$lb"
  eq "$n: tenant B retryLimit is its own" "9" "$rb"
  eq "$n: tenant B namespace" "ns-b" "$nb"
done
if [ -n "$QID_A" ] && [ "$QID_A" != "$QID_B" ]; then
  ok "same queue name resolves to distinct queue ids per tenant"
else
  bad "queue ids collide across tenants (A='$QID_A' B='$QID_B')"
fi

say ""
say "== 2. no message crosses tenants (push on one broker, pop on the other) =="
call POST "$A" "$TA" /api/v1/push \
  "{\"items\":[{\"queue\":\"$Q\",\"partition\":\"$P\",\"payload\":{\"t\":\"A\",\"n\":1}},
               {\"queue\":\"$Q\",\"partition\":\"$P\",\"payload\":{\"t\":\"A\",\"n\":2}},
               {\"queue\":\"$Q\",\"partition\":\"$P\",\"payload\":{\"t\":\"A\",\"n\":3}}]}"
eq "tenant A pushes 3 to queen-a" 201 "$RC"
call POST "$B" "$TB" /api/v1/push \
  "{\"items\":[{\"queue\":\"$Q\",\"partition\":\"$P\",\"payload\":{\"t\":\"B\",\"n\":1}},
               {\"queue\":\"$Q\",\"partition\":\"$P\",\"payload\":{\"t\":\"B\",\"n\":2}}]}"
eq "tenant B pushes 2 to queen-b" 201 "$RC"

call GET "$B" "$TA" "/api/v1/pop/queue/$Q?batch=50&partitions=8&wait=true&timeout=8000"
NA=$(nmsgs); TAGA=$(tags); PID_A=$(printf '%s' "$BODY" | jq -r 'try .messages[0].partitionId // ""')
TXN_A=$(printf '%s' "$BODY" | jq -r 'try .messages[0].transactionId // ""')
eq "tenant A pops its 3 from queen-b (cross-broker)" "3" "$NA"
eq "tenant A sees ONLY its own payloads" "A" "$TAGA"

call GET "$A" "$TB" "/api/v1/pop/queue/$Q?batch=50&partitions=8&wait=true&timeout=8000"
NB=$(nmsgs); TAGB=$(tags); PID_B=$(printf '%s' "$BODY" | jq -r 'try .messages[0].partitionId // ""')
eq "tenant B pops its 2 from queen-a (cross-broker)" "2" "$NB"
eq "tenant B sees ONLY its own payloads" "B" "$TAGB"
if [ -n "$PID_A" ] && [ "$PID_A" != "$PID_B" ]; then
  ok "same partition name resolves to distinct partition ids per tenant"
else
  bad "partition ids collide across tenants (A='$PID_A' B='$PID_B')"
fi

say ""
say "== 3. a foreign partitionId cannot advance another tenant's cursor =="
if [ -n "$PID_A" ] && [ -n "$TXN_A" ]; then
  call POST "$B" "$TB" /api/v1/ack \
    "{\"transactionId\":\"$TXN_A\",\"partitionId\":\"$PID_A\",\"status\":\"completed\"}"
  OKF=$(jv '.[0].success'); ERR=$(jv '.[0].error')
  if [ "$OKF" = "false" ]; then ok "tenant B acking tenant A's partitionId is rejected ($ERR)"
  else bad "foreign-pid ack ACCEPTED: $BODY"; fi
  call POST "$B" "$TA" /api/v1/ack \
    "{\"transactionId\":\"$TXN_A\",\"partitionId\":\"$PID_A\",\"status\":\"completed\"}"
  eq "the owning tenant's ack succeeds" "true" "$(jv '.[0].success')"
else
  bad "could not extract partitionId/transactionId from tenant A's pop"
fi

say ""
say "== 4. dedup keys do not collide across tenants =="
call POST "$A" "$TA" /api/v1/push \
  "{\"items\":[{\"queue\":\"$Q\",\"partition\":\"$P\",\"payload\":{\"t\":\"A\",\"n\":9},\"transactionId\":\"TEN-DUP-1\"}]}"
S1=$(printf '%s' "$BODY" | jq -r 'try .[0].status // "?"')
call POST "$B" "$TB" /api/v1/push \
  "{\"items\":[{\"queue\":\"$Q\",\"partition\":\"$P\",\"payload\":{\"t\":\"B\",\"n\":9},\"transactionId\":\"TEN-DUP-1\"}]}"
S2=$(printf '%s' "$BODY" | jq -r 'try .[0].status // "?"')
eq "tenant A's TEN-DUP-1 is queued" "queued" "$S1"
eq "tenant B's identical transactionId is ALSO queued (no cross-tenant dedup)" "queued" "$S2"
call POST "$B" "$TB" /api/v1/push \
  "{\"items\":[{\"queue\":\"$Q\",\"partition\":\"$P\",\"payload\":{\"t\":\"B\",\"n\":9},\"transactionId\":\"TEN-DUP-1\"}]}"
S3=$(printf '%s' "$BODY" | jq -r 'try .[0].status // "?"')
eq "tenant B's own replay IS deduped (dedup still works within a tenant)" "duplicate" "$S3"

say ""
say "== 5. listings are scoped =="
for pair in "queen-a:$A" "queen-b:$B"; do
  n="${pair%%:*}"; u="${pair#*:}"
  call GET "$u" "$TA" /api/v1/resources/queues
  ns=$(printf '%s' "$BODY" | jq -r "try ([.queues[] | select(.name==\"$Q\") | .namespace] | join(\",\")) // \"\"")
  eq "$n: resources/queues for A shows only A's '$Q'" "ns-a" "$ns"
  call GET "$u" "$TB" /api/v1/resources/queues
  ns=$(printf '%s' "$BODY" | jq -r "try ([.queues[] | select(.name==\"$Q\") | .namespace] | join(\",\")) // \"\"")
  eq "$n: resources/queues for B shows only B's '$Q'" "ns-b" "$ns"
done

say ""
say "== 6. a consumer-group NAME shared by both tenants keeps independent cursors =="
G=shared-cg
# Tenant A drains the group on queen-a (autoAck advances A's cursor server-side).
# subscriptionMode=all on the FIRST contact of each tenant's '$G': the messages this
# section consumes were pushed back in §2/§4, before this group existed, and the
# broker's default (DEFAULT_SUBSCRIPTION_MODE=new) would seed the cursor at the tail
# and deliver nothing — which would turn every assertion below into a vacuous pass.
call GET "$A" "$TA" "/api/v1/pop/queue/$Q?consumerGroup=$G&batch=50&partitions=8&autoAck=true&subscriptionMode=all&wait=true&timeout=8000"
GA1=$(nmsgs); GTAG=$(tags)
eq "A's first '$G' pop sees only A's payloads" "A" "$GTAG"
[ "$GA1" -ge 1 ] && ok "A's first '$G' pop delivered $GA1 message(s)" \
  || bad "A's first '$G' pop delivered nothing"
call GET "$A" "$TA" "/api/v1/pop/queue/$Q?consumerGroup=$G&batch=50&partitions=8&autoAck=true"
eq "A's '$G' cursor is now drained" "0" "$(nmsgs)"
# The interesting one: B's cursor for the SAME group name must be untouched.
call GET "$B" "$TB" "/api/v1/pop/queue/$Q?consumerGroup=$G&batch=50&partitions=8&autoAck=true&subscriptionMode=all&wait=true&timeout=8000"
GB1=$(nmsgs); GTAG=$(tags)
[ "$GB1" -ge 1 ] && ok "B's '$G' cursor was NOT advanced by A ($GB1 message(s) delivered)" \
  || bad "B's '$G' cursor was advanced by tenant A's consumption — messages hidden"
eq "B's '$G' pop sees only B's payloads" "B" "$GTAG"
call GET "$B" "$TB" "/api/v1/pop/queue/$Q?consumerGroup=$G&batch=50&partitions=8&autoAck=true"
eq "B's '$G' cursor is now drained too" "0" "$(nmsgs)"

say ""
say "== 7. same (queue, group, partition) names: A's claim+ack cannot hide or delay B =="
# Every name collides, so the two tenants' pending messages sit in state that is
# told apart ONLY by the tenant. If any piece of the pop path (the ready set of a
# wildcard pop, the partition lookup, the cursor, a wake) were keyed by the names
# alone, A's claim / ack / empty poll would clear or skip B's still-pending
# message and B would get it late or never. What this proves is the absence of
# that hide-and-delay.
#
# Both tenants pop through A and push through B, so on the cluster the writes and
# the reads enter on different nodes.
G2=ring-cg
# Seed both cursors so the first-contact bootstrap is out of the way and the
# pops below take the steady-state path.
call GET "$A" "$TA" "/api/v1/pop/queue/$Q?consumerGroup=$G2&batch=50&partitions=8&autoAck=true&wait=true&timeout=6000"
call GET "$A" "$TB" "/api/v1/pop/queue/$Q?consumerGroup=$G2&batch=50&partitions=8&autoAck=true&wait=true&timeout=6000"
call GET "$A" "$TA" "/api/v1/pop/queue/$Q?consumerGroup=$G2&batch=50&partitions=8&autoAck=true"
call GET "$A" "$TB" "/api/v1/pop/queue/$Q?consumerGroup=$G2&batch=50&partitions=8&autoAck=true"

# Both tenants push one message to the identically-named partition through B,
# then A consumes first through A: its claim and ack act on the same names B's
# pending message carries.
call POST "$B" "$TA" /api/v1/push \
  "{\"items\":[{\"queue\":\"$Q\",\"partition\":\"$P\",\"payload\":{\"t\":\"A\",\"n\":77}}]}"
eq "ring: tenant A pushes 1 to queen-b" 201 "$RC"
call POST "$B" "$TB" /api/v1/push \
  "{\"items\":[{\"queue\":\"$Q\",\"partition\":\"$P\",\"payload\":{\"t\":\"B\",\"n\":77}}]}"
eq "ring: tenant B pushes 1 to queen-b" 201 "$RC"

call GET "$A" "$TA" "/api/v1/pop/queue/$Q?consumerGroup=$G2&batch=50&partitions=8&autoAck=true&wait=true&timeout=8000"
RA=$(nmsgs); RTAG=$(tags)
eq "ring: A gets its message back from queen-a" "1" "$RA"
eq "ring: and only its own" "A" "$RTAG"

# The assertion that matters: A's consumption must not have hidden or DELAYED B's
# still-pending message. The latency budget is the real discriminator — shared
# state could still deliver B eventually (a periodic rescan), just tens of seconds
# late, so asserting delivery alone would pass on the broken shape.
T0=$(date +%s)
call GET "$A" "$TB" "/api/v1/pop/queue/$Q?consumerGroup=$G2&batch=50&partitions=8&autoAck=true&wait=true&timeout=8000"
RB=$(nmsgs); RTAG=$(tags); T1=$(date +%s)
if [ "$RB" = "1" ]; then
  ok "ring: B's message survived A's claim+ack ($((T1-T0))s)"
  eq "ring: and B sees only its own" "B" "$RTAG"
  if [ $((T1-T0)) -le 3 ]; then
    ok "ring: B was served promptly, not by a periodic rescan ($((T1-T0))s <= 3s)"
  else
    bad "ring: B waited $((T1-T0))s — delivered late, so the two tenants share pop state"
  fi
else
  bad "ring: B's pending message was NOT delivered within 8s after A consumed (got $RB)"
  # Diagnose: hidden-until-reseed (recoverable, a latency bug) vs actually lost.
  say "       diagnosing: is it hidden until some periodic rescan, or lost?"
  call GET "$A" "$TB" "/api/v1/pop/queue/$Q?consumerGroup=$G2&batch=50&partitions=8&autoAck=true&wait=true&timeout=45000"
  T2=$(date +%s)
  if [ "$(nmsgs)" -ge 1 ]; then
    say "       -> delivered after $((T2-T0))s: HIDDEN by cross-tenant crosstalk, not lost."
    say "          Visibility-latency defect."
  else
    say "       -> still nothing after $((T2-T0))s: the message is not being delivered at all."
  fi
fi

say ""
say "== 8. parked long-poll: a foreign tenant's push delivers nothing =="
# The parked-pop wake gate is keyed by (tenant, queue), so a push by tenant B does
# not wake tenant A's parked long-poll on the same queue name. NB the wake is not
# observable from HTTP either way: a woken pop that finds
# nothing keeps looping to its deadline, so the elapsed time is ~timeout in BOTH
# shapes. The delivery assertion below is the honest one; the elapsed time is
# reported, not asserted (see the design's §6g — do not write a test that claims to
# prove something the surface cannot show).
G3=park-cg
call GET "$A" "$TA" "/api/v1/pop/queue/$Q?consumerGroup=$G3&batch=10&subscriptionMode=new" >/dev/null
sleep 1
( sleep 2; curl -s -o /dev/null -X POST -H "x-queen-tenant: $TB" -H 'Content-Type: application/json' \
    --data-binary "{\"items\":[{\"queue\":\"$Q\",\"partition\":\"$P\",\"payload\":{\"t\":\"B\",\"n\":99}}]}" \
    "$B/api/v1/push" ) &
WAKER=$!
S0=$(date +%s)
call GET "$A" "$TA" "/api/v1/pop/queue/$Q?consumerGroup=$G3&batch=10&wait=true&timeout=6000"
S1=$(date +%s)
wait "$WAKER" 2>/dev/null
eq "a tenant-B push delivers NOTHING to tenant A's parked long-poll" "0" "$(nmsgs)"
note "A's 6s long-poll returned after $((S1-S0))s while tenant B pushed to the same"
note "     queue name through B. The wake gate is keyed by (tenant, queue), so only"
note "     B's parked pops are woken — A neither receives data nor re-queries on it."
# Drain what B pushed so the stack is left tidy.
call GET "$B" "$TB" "/api/v1/pop/queue/$Q?batch=50&partitions=8&autoAck=true&wait=true&timeout=5000" >/dev/null

say ""
say "== result: $PASS ok, $FAIL fail, $NOTES notes =="
if [ "$FAIL" = 0 ]; then say "TENANCY: PASS"; else say "TENANCY: FAIL"; fi
[ "$FAIL" -eq 0 ]
