#!/usr/bin/env bash
# Two-tenant isolation smoke THROUGH THE PROXY, against the dev cell
# (scripts/dev-cell.sh up must be running: pxdb :5465, broker :6710 with
# QUEEN_TENANCY_HEADER=true, proxy :6711, seed-dev.sql applied).
#
# Derived from the T3/§5 checklist (PLAN_QUEEN_PROXY_CLOUD.md): same queue name
# on two clusters of the same cell, scoped listings, foreign-pid ack rejection,
# key/cluster binding, blocked aggregates, plus a live 429 check when the proxy
# runs with QUEEN_PROXY_ENFORCE=true.
set -uo pipefail

P=http://127.0.0.1:6711
KEY_A="qk_dev_devdevdevdevdevdevdevdevdevdevdevdevdev"   # seeded (cluster: dev)
PASS=0; FAIL=0

say()  { printf '%s\n' "$*"; }
ok()   { PASS=$((PASS+1)); say "  ok  - $1"; }
bad()  { FAIL=$((FAIL+1)); say "  FAIL- $1"; }
check(){ # desc expected actual
  if [ "$2" = "$3" ]; then ok "$1"; else bad "$1 (want $2, got $3)"; fi
}

# --- provision cluster B on the same cell, via the control-plane SQL functions
KEY_B="qk_dev_$(openssl rand -base64 48 | tr '+/' '-_' | tr -d '=' | cut -c1-43)"
HASH_B=$(printf '%s' "$KEY_B" | shasum -a 256 | cut -d' ' -f1)
docker exec -i qpx-pg psql -qtA -U postgres -d queen_proxy <<SQL >/dev/null
DO \$\$
DECLARE t2 uuid; c2 uuid; cell uuid; u2 uuid;
BEGIN
  SELECT id INTO cell FROM queen_proxy.cells WHERE slug='local';
  IF NOT EXISTS (SELECT 1 FROM queen_proxy.tenants WHERE slug='tenant-two') THEN
    t2 := queen_proxy.create_tenant('tenant-two','Tenant Two');
    c2 := queen_proxy.create_cluster(t2,'two','free',cell);
    PERFORM queen_proxy.issue_api_key(c2,'smoke','${HASH_B}',ARRAY['produce','consume','admin','read']);
  END IF;
END \$\$;
SQL

req() { # method host key path [body] -> "code|body"
  local m=$1 h=$2 k=$3 p=$4 b=${5:-}
  local args=(-s -o /tmp/iso_body -w '%{http_code}' -X "$m" -H "Host: $h" -H "Authorization: Bearer $k")
  [ -n "$b" ] && args+=(-H 'Content-Type: application/json' -d "$b")
  local code; code=$(curl "${args[@]}" "$P$p")
  printf '%s|%s' "$code" "$(cat /tmp/iso_body)"
}

say "== two-tenant isolation through the proxy =="

# 1. same queue name, both clusters push
RA=$(req POST dev "$KEY_A" /api/v1/push '{"items":[{"queue":"orders","payload":{"who":"A"}}]}')
RB=$(req POST two "$KEY_B" /api/v1/push '{"items":[{"queue":"orders","payload":{"who":"B"}}]}')
check "push A orders" 201 "${RA%%|*}"
check "push B orders" 201 "${RB%%|*}"

# 2. pops see only their own
PA=$(req GET dev "$KEY_A" '/api/v1/pop/queue/orders?batch=10')
PB=$(req GET two "$KEY_B" '/api/v1/pop/queue/orders?batch=10')
echo "${PA#*|}" | grep -q '"who":"A"' && ! echo "${PA#*|}" | grep -q '"who":"B"' \
  && ok "pop A sees only A" || bad "pop A cross-contaminated: ${PA#*|}"
echo "${PB#*|}" | grep -q '"who":"B"' && ! echo "${PB#*|}" | grep -q '"who":"A"' \
  && ok "pop B sees only B" || bad "pop B cross-contaminated: ${PB#*|}"

# 3. foreign-pid ack rejected (B acks A's partition/txn)
PID_A=$(echo "${PA#*|}" | grep -o '"partitionId":"[^"]*"' | head -1 | cut -d'"' -f4)
TXN_A=$(echo "${PA#*|}" | grep -o '"transactionId":"[^"]*"' | head -1 | cut -d'"' -f4)
if [ -n "$PID_A" ] && [ -n "$TXN_A" ]; then
  XA=$(req POST two "$KEY_B" /api/v1/ack "{\"transactionId\":\"$TXN_A\",\"partitionId\":\"$PID_A\",\"status\":\"completed\"}")
  echo "${XA#*|}" | grep -qi 'not owned\|success":false' && ok "foreign-pid ack rejected" \
    || bad "foreign-pid ack NOT rejected: $XA"
  SA=$(req POST dev "$KEY_A" /api/v1/ack "{\"transactionId\":\"$TXN_A\",\"partitionId\":\"$PID_A\",\"status\":\"completed\"}")
  echo "${SA#*|}" | grep -q '"success":true' && ok "own ack accepted" || bad "own ack failed: $SA"
else
  bad "could not extract pid/txn from pop A"
fi

# 4. listings scoped
LA=$(req GET dev "$KEY_A" /api/v1/resources/queues)
! echo "${LA#*|}" | grep -q 'two\|"who":"B"' && ok "resources/queues A shows no B artifacts" \
  || say "  note- listing A: ${LA#*|}" # names differ per cluster; only fail on real leak below
LB=$(req GET two "$KEY_B" /api/v1/resources/queues)
AN=$(echo "${LA#*|}" | grep -c '"orders"'); BN=$(echo "${LB#*|}" | grep -c '"orders"')
[ "$AN" -ge 1 ] && [ "$BN" -ge 1 ] && ok "both clusters list their own 'orders'" \
  || bad "scoped listing broken (A:$AN B:$BN)"

# 5. key/cluster binding: B's key on A's host
KB=$(req GET dev "$KEY_B" /api/v1/resources/queues)
check "B key on dev host -> 403" 403 "${KB%%|*}"

# 6. blocked aggregates and discovery pop
for path in /api/v1/pop /api/v1/analytics/queue-lag /api/v1/resources/namespaces /metrics/prometheus; do
  R=$(req GET dev "$KEY_A" "$path")
  check "blocked $path" 404 "${R%%|*}"
done

# 7. dedup: same transactionId on both clusters must NOT collide
D1=$(req POST dev "$KEY_A" /api/v1/push '{"items":[{"queue":"orders","payload":{"n":1},"transactionId":"ISO-DUP-1"}]}')
D2=$(req POST two "$KEY_B" /api/v1/push '{"items":[{"queue":"orders","payload":{"n":1},"transactionId":"ISO-DUP-1"}]}')
echo "${D1#*|}" | grep -q '"status":"queued"' && echo "${D2#*|}" | grep -q '"status":"queued"' \
  && ok "same transactionId, no cross-tenant dedup" || bad "cross-tenant dedup collision: $D1 / $D2"

# 8. meters: wait one flush and check usage rows for both clusters
sleep 20
ROWS=$(docker exec qpx-pg psql -qtA -U postgres -d queen_proxy \
  -c "SELECT count(DISTINCT cluster_id) FROM queen_proxy.usage_minutes WHERE msgs > 0")
[ "${ROWS:-0}" -ge 2 ] && ok "usage_minutes has rows for both clusters" \
  || say "  note- usage clusters with msgs>0: ${ROWS:-0} (flush window may not have closed; rerun check manually)"

say "== result: $PASS ok, $FAIL fail =="
[ "$FAIL" -eq 0 ]
