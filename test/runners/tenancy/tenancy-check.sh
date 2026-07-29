#!/usr/bin/env bash
# Two-tenant isolation over the HA pair, driven DIRECTLY at the brokers with the
# trusted `x-queen-tenant` header (no proxy in the picture — the proxy's own
# end-to-end check is queen_proxy/scripts/isolation-smoke.sh).
#
# Why this exists: every cloud cell runs the broker with QUEEN_TENANCY_HEADER=true
# behind a mesh pair, so the in-memory discovery layer — the hot-list ring
# (server/src/hotlist.rs), the parked-pop wake gates (server/src/notify.rs) and the
# mesh frames that feed them (server/src/mesh.rs) — must be keyed by (tenant, queue),
# not by queue name. It is: the ring/gate key is `tenant_queue_key(tenant, queue)` and
# every queue-carrying frame carries a `tenant` field (a frame WITHOUT one, i.e. a
# pre-Track-B peer mid rolling upgrade, fans out to every tenant holding that name —
# a safe over-wake). Those structures are still only *hints*: Postgres is the sole
# authority and every SP call carries the tenant, so a leak was never possible. What
# this runner pins down is that one tenant's ring traffic cannot HIDE or DELAY
# another tenant's pending message on a shared cell.
#
# Shape of every scenario: the SAME queue name, the SAME partition name and the
# SAME consumer-group name owned by two different tenants, with traffic driven on
# BOTH brokers so the mesh and both brokers' caches are in play.
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
say "allowing time for the mesh dial + HELLO handshake"
sleep 3

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
# Crosstalk, by design: the mesh QUEUE_CONFIG_SET frame carries only the queue
# NAME, so the peer drops the lease/encryption cache entry of EVERY tenant that
# holds that name (server/src/main.rs on_queue_config_set). Over-invalidation ⇒
# one extra lazy re-fetch; the assertions above are what proves it is harmless.
note "mesh QUEUE_CONFIG_SET is tenant-inert: B's configure invalidates A's cached"
note "     entry for the same queue name on the peer (over-invalidation, lazy refetch)"

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
call GET "$A" "$TA" "/api/v1/pop/queue/$Q?consumerGroup=$G&batch=50&partitions=8&autoAck=true&wait=true&timeout=8000"
GA1=$(nmsgs); GTAG=$(tags)
eq "A's first '$G' pop sees only A's payloads" "A" "$GTAG"
[ "$GA1" -ge 1 ] && ok "A's first '$G' pop delivered $GA1 message(s)" \
  || bad "A's first '$G' pop delivered nothing"
call GET "$A" "$TA" "/api/v1/pop/queue/$Q?consumerGroup=$G&batch=50&partitions=8&autoAck=true"
eq "A's '$G' cursor is now drained" "0" "$(nmsgs)"
# The interesting one: B's cursor for the SAME group name must be untouched.
call GET "$B" "$TB" "/api/v1/pop/queue/$Q?consumerGroup=$G&batch=50&partitions=8&autoAck=true&wait=true&timeout=8000"
GB1=$(nmsgs); GTAG=$(tags)
[ "$GB1" -ge 1 ] && ok "B's '$G' cursor was NOT advanced by A ($GB1 message(s) delivered)" \
  || bad "B's '$G' cursor was advanced by tenant A's consumption — messages hidden"
eq "B's '$G' pop sees only B's payloads" "B" "$GTAG"
call GET "$B" "$TB" "/api/v1/pop/queue/$Q?consumerGroup=$G&batch=50&partitions=8&autoAck=true"
eq "B's '$G' cursor is now drained too" "0" "$(nmsgs)"

say ""
say "== 7. hot-list ring isolation: same (queue, group, partition) names, two tenants =="
# server/src/hotlist.rs: the ready ring is keyed by (tenant, queue) + group and the
# partition-name interning is nested inside that per-(tenant, queue) state, so the
# two tenants own DISJOINT entries for ($Q, $G2, $P) even though every name
# collides. Keyed by queue name alone they would share one entry (and one interned
# index, so note_id would map BOTH partition uuids onto it): A's claim / ack /
# empty-CAS would then clear or wheel B's still-pending message, hiding it until the
# QUEEN_HOTLIST_RESEED_MS floor. PG was always the authority (log_pop_list_v1 carries
# the tenant) so a *leak* was never possible; what this proves is the absence of that
# hide-and-delay.
#
# The hot-list lives in each broker's process memory, so both tenants must pop
# from the SAME broker for the entry to actually be shared — hence every pop in
# this section goes to queen-a. The pushes go to queen-b so the ring is fed the
# way it is in production: by a name-only HOTLIST_DIRTY / MESSAGE_AVAILABLE mesh
# frame from the peer.
G2=ring-cg
# Seed both cursors so the first-contact wildcard bootstrap is out of the way and
# subsequent pops genuinely take the ring path.
call GET "$A" "$TA" "/api/v1/pop/queue/$Q?consumerGroup=$G2&batch=50&partitions=8&autoAck=true&wait=true&timeout=6000"
call GET "$A" "$TB" "/api/v1/pop/queue/$Q?consumerGroup=$G2&batch=50&partitions=8&autoAck=true&wait=true&timeout=6000"
call GET "$A" "$TA" "/api/v1/pop/queue/$Q?consumerGroup=$G2&batch=50&partitions=8&autoAck=true"
call GET "$A" "$TB" "/api/v1/pop/queue/$Q?consumerGroup=$G2&batch=50&partitions=8&autoAck=true"

# Both tenants push one message to the identically-named partition on the PEER,
# then A consumes first on queen-a. A's take_batch/checkin/promote all act on the
# ring entry B's message is also riding.
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
# still-pending message on the same broker. The latency budget is the real
# discriminator — a shared ring still delivers B eventually (the reseed floor), just
# tens of seconds late, so asserting delivery alone would pass on the broken shape.
T0=$(date +%s)
call GET "$A" "$TB" "/api/v1/pop/queue/$Q?consumerGroup=$G2&batch=50&partitions=8&autoAck=true&wait=true&timeout=8000"
RB=$(nmsgs); RTAG=$(tags); T1=$(date +%s)
if [ "$RB" = "1" ]; then
  ok "ring: B's message survived A's claim+ack ($((T1-T0))s)"
  eq "ring: and B sees only its own" "B" "$RTAG"
  if [ $((T1-T0)) -le 3 ]; then
    ok "ring: B was served from its OWN ring, not the reseed floor ($((T1-T0))s <= 3s)"
  else
    bad "ring: B waited $((T1-T0))s — served by the reseed floor, so the ring is shared"
  fi
else
  bad "ring: B's pending message was NOT delivered within 8s after A consumed (got $RB)"
  # Diagnose: hidden-until-reseed (recoverable, a latency bug) vs actually lost.
  say "       diagnosing: is it hidden until the hot-list reseed floor, or lost?"
  call GET "$A" "$TB" "/api/v1/pop/queue/$Q?consumerGroup=$G2&batch=50&partitions=8&autoAck=true&wait=true&timeout=45000"
  T2=$(date +%s)
  if [ "$(nmsgs)" -ge 1 ]; then
    say "       -> delivered after $((T2-T0))s: HIDDEN by shared-ring crosstalk until the"
    say "          QUEEN_HOTLIST_RESEED_MS floor, not lost. Visibility-latency defect."
  else
    say "       -> still nothing after $((T2-T0))s: the message is not being delivered at all."
  fi
fi

say ""
say "== 8. parked long-poll: a foreign tenant's push delivers nothing =="
# The parked-pop wake gate (notifier.wait_queue) is keyed by (tenant, queue), so a
# push by tenant B no longer wakes tenant A's parked long-poll on the same queue
# name. NB the wake is not observable from HTTP either way: a woken pop that finds
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
note "     queue name on the peer. The mesh frame now carries B's tenant, so only B's"
note "     gate is woken — A neither receives data nor burns a re-query on B's push."
# Drain what B pushed so the stack is left tidy.
call GET "$B" "$TB" "/api/v1/pop/queue/$Q?batch=50&partitions=8&autoAck=true&wait=true&timeout=5000" >/dev/null

say ""
say "== result: $PASS ok, $FAIL fail, $NOTES notes =="
if [ "$FAIL" = 0 ]; then say "TENANCY: PASS"; else say "TENANCY: FAIL"; fi
[ "$FAIL" -eq 0 ]
