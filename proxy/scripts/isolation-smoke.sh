#!/usr/bin/env bash
# Two-tenant isolation smoke THROUGH THE PROXY, against the dev cell
# (scripts/dev-cell.sh up must be running: pxdb :5465, broker :6710 with
# QUEEN_TENANCY_HEADER=true, proxy :6711, seed-dev.sql applied).
#
# Derived from the T3/§5 checklist (PLAN_QUEEN_PROXY_CLOUD.md): same queue name
# on two clusters of the same cell, scoped listings and aggregates asserted on
# CONTENT (not just HTTP 200), foreign-pid rejection on every MUTATING route,
# trace-name isolation, cross-tenant retention, api-key scopes, key/session
# revocation, key/cluster binding, blocked operator surfaces, storage quota,
# meters, SHARED-HOST key routing (section 20, decision z: one hostname, the
# cluster from the credential) -- plus a live 429 check when the proxy runs with
# QUEEN_PROXY_ENFORCE=true (`QUEEN_PROXY_ENFORCE=true scripts/dev-cell.sh up`).
# In shadow mode those three checks are reported as COUNTED SKIPS, never as
# silent passes, so the printed tally means the same thing in both modes.
#
# What it provisions on the dev cell's pxdb (idempotent, reuses what exists):
#   * cluster `two`  (tenant-two,  plan free) -- tenant B, the adversary
#   * cluster `pro1` (tenant-pro1, plan pro)  -- traces need a plan with the
#   * cluster `pro2` (tenant-pro2, plan pro)     traces feature; the seeded free
#                                                plan has none, and features come
#                                                from the plan only (limit
#                                                overrides cannot turn one on --
#                                                cache.rs::parse_features)
#   * cluster `rl`   (tenant-rl,   plan free) -- kept on the STOCK free limits
#                                                (5 req/s, burst 25) purely so
#                                                the 429 check has something to
#                                                overrun
# and a fresh api key per run per cluster (revoked again on exit).
#
# LIMIT OVERRIDES: every working cluster (dev/two/pro1/pro2) gets a generous
# max_req_per_sec/max_msgs_per_sec/max_queues override for the duration of the
# run, so that a cell started with QUEEN_PROXY_ENFORCE=true does not 429 the
# smoke itself. `rl` deliberately keeps the stock free plan. The EXIT trap
# clears every override it set -- important, since the storage-quota section
# parks a 64-byte max_retained_bytes on cluster `dev` mid-run.
#
# Needs: docker (qpx-pg), curl, jq, openssl, shasum, perl (sub-second clock for
# the §19e wake-latency assertion).
set -uo pipefail

P=http://127.0.0.1:6711
KEY_A="qk_dev_devdevdevdevdevdevdevdevdevdevdevdevdev"   # seeded (cluster: dev)
RUN=$(date +%s | tail -c 7)
PASS=0; FAIL=0; SKIP=0
TMP=$(mktemp -d); BODYF="$TMP/body"; HDRF="$TMP/hdr"
# Startup line of the proxy that fronts this cell -- the only place the
# enforce/shadow mode is observable from outside the process.
PROXY_LOG="${QUEEN_PROXY_LOG:-$(cd "$(dirname "$0")/.." && pwd)/.devcell/proxy.log}"
# Same, for the broker behind it: section 19 needs to know the hot-list is ON
# (its boot line) before it can claim to have exercised the candidate ring.
BROKER_LOG="${QUEEN_BROKER_LOG:-$(cd "$(dirname "$0")/.." && pwd)/.devcell/broker.log}"

command -v jq >/dev/null || { echo "isolation-smoke: jq is required" >&2; exit 2; }

say()  { printf '%s\n' "$*"; }
ok()   { PASS=$((PASS+1)); say "  ok  - $1"; }
bad()  { FAIL=$((FAIL+1)); say "  FAIL- $1"; }
skip() { SKIP=$((SKIP+1)); say "  skip- $1"; }
check(){ # desc expected actual
  if [ "$2" = "$3" ]; then ok "$1"; else bad "$1 (want $2, got $3)"; fi
}
short(){ printf '%.220s' "$1"; }
has()  { case "$2" in *"$1"*) return 0;; *) return 1;; esac; }
want_in()  { if has "$2" "$3"; then ok "$1"; else bad "$1 (no '$2' in: $(short "$3"))"; fi; }
want_out() { if has "$2" "$3"; then bad "$1 (leaked '$2' in: $(short "$3"))"; else ok "$1"; fi; }
j()    { printf '%s' "$2" | jq -r "$1" 2>/dev/null; }   # j <filter> <json>

px()   { docker exec -i qpx-pg psql -qtA -U postgres -d queen_proxy "$@"; }

req() { # method host key path [body] -> "code|body"
  local m=$1 h=$2 k=$3 p=$4 b=${5:-}
  local args=(-s -o "$BODYF" -D "$HDRF" -w '%{http_code}' -X "$m" -H "Host: $h" -H "Authorization: Bearer $k")
  [ -n "$b" ] && args+=(-H 'Content-Type: application/json' -d "$b")
  local code; code=$(curl "${args[@]}" "$P$p")
  printf '%s|%s' "$code" "$(cat "$BODYF")"
}
treq() { # method host key path -> "code|seconds|body"   (req + wall time)
  local m=$1 h=$2 k=$3 p=$4 out
  out=$(curl -s -o "$BODYF" -D "$HDRF" -w '%{http_code} %{time_total}' \
        -X "$m" -H "Host: $h" -H "Authorization: Bearer $k" "$P$p")
  printf '%s|%s|%s' "${out%% *}" "${out##* }" "$(cat "$BODYF")"
}
bgreq() { # method host key path outfile   -- async treq; reap with `wait`
  ( local out
    out=$(curl -s -o "$5.body" -w '%{http_code} %{time_total}' \
          -X "$1" -H "Host: $2" -H "Authorization: Bearer $3" "$P$4")
    printf '%s|%s|%s' "${out%% *}" "${out##* }" "$(cat "$5.body")" >"$5" ) &
}
# float compare for the long-poll latency assertions ("a < b")
lt() { awk -v a="$1" -v b="$2" 'BEGIN{exit !(a+0 < b+0)}'; }
creq() { # method host cookie path -> "code|body"   (console/session surface)
  local m=$1 h=$2 c=$3 p=$4
  local code; code=$(curl -s -o "$BODYF" -D "$HDRF" -w '%{http_code}' -X "$m" \
    -H "Host: $h" -H "Cookie: $c" "$P$p")
  printf '%s|%s' "$code" "$(cat "$BODYF")"
}
hdr() { grep -i "^$1:" "$HDRF" | tail -1 | tr -d '\r' | sed "s/^[^:]*:[[:space:]]*//"; }

# --- control-plane helpers ---------------------------------------------------
ensure_cluster() { # tenant-slug tenant-name cluster-slug plan -> cluster uuid
  px >/dev/null <<SQL
DO \$\$
DECLARE t uuid; cell uuid;
BEGIN
  SELECT id INTO cell FROM queen_proxy.cells WHERE slug='local';
  IF NOT EXISTS (SELECT 1 FROM queen_proxy.clusters WHERE slug='$3') THEN
    SELECT id INTO t FROM queen_proxy.tenants WHERE slug='$1';
    IF t IS NULL THEN t := queen_proxy.create_tenant('$1','$2'); END IF;
    PERFORM queen_proxy.create_cluster(t,'$3','$4',cell);
  END IF;
END \$\$;
SQL
  px -c "SELECT id FROM queen_proxy.clusters WHERE slug='$3'"
}
issue_key() { # cluster-uuid label scopes-sql -> "plaintext|key uuid"
  local k h id
  k="qk_dev_$(openssl rand -base64 48 | tr '+/' '-_' | tr -d '=' | cut -c1-43)"
  h=$(printf '%s' "$k" | shasum -a 256 | cut -d' ' -f1)
  id=$(px -c "SELECT queen_proxy.issue_api_key('$1'::uuid,'iso-$RUN-$2','$h',ARRAY[$3])")
  printf '%s|%s' "$k" "$id"
}
set_ovr() { # cluster-uuid json|NULL
  local v
  if [ "$2" = "NULL" ]; then v="NULL"; else v="'$2'::jsonb"; fi
  px -c "SELECT queen_proxy.set_limit_override('$1'::uuid, $v)" >/dev/null
}

# Wide enough that the smoke's own traffic never trips a limit; `rl` is the
# cluster left on the stock free plan for the 429 check. WIDE_Q is the same set
# plus the tiny storage cap section 16 parks on cluster `dev`.
WIDE='{"max_req_per_sec":500,"req_burst":2000,"max_msgs_per_sec":5000,"msgs_burst":20000,"max_queues":500}'
WIDE_Q='{"max_req_per_sec":500,"req_burst":2000,"max_msgs_per_sec":5000,"msgs_burst":20000,"max_queues":500,"max_retained_bytes":64}'

CID_A=""; CID_B=""; CID_P1=""; CID_P2=""; CID_RL=""
KEY_B=""; KEY_P1=""
cleanup() {
  # Drop this run's queues, so a long-lived dev cell does not accumulate ~10
  # queues per smoke run (and eventually meet the max_queues cap). Best-effort
  # and silent: a cleanup failure must not be mistaken for a check.
  for q in "orders-$RUN" "ret-$RUN" "iso-a-$RUN" "adv-$RUN" "scope-$RUN" "quota-$RUN" "hot-$RUN" "hot4-$RUN" "sh20-$RUN"; do
    req DELETE dev "$KEY_A" "/api/v1/resources/queues/$q" >/dev/null 2>&1
  done
  for q in "orders-$RUN" "ret-$RUN" "iso-b-$RUN" "adv-$RUN" "txn-$RUN" "hot-$RUN" "sh20-$RUN" "sh20b-$RUN"; do
    [ -n "$KEY_B" ] && req DELETE two "$KEY_B" "/api/v1/resources/queues/$q" >/dev/null 2>&1
  done
  [ -n "$KEY_P1" ] && req DELETE pro1 "$KEY_P1" "/api/v1/resources/queues/tq-$RUN" >/dev/null 2>&1
  for c in $CID_A $CID_B $CID_P1 $CID_P2; do [ -n "$c" ] && set_ovr "$c" NULL; done
  # best-effort: revoke every key this run issued, so a dev pxdb does not
  # accumulate one live key per smoke run.
  px >/dev/null 2>&1 <<SQL || true
DO \$\$
DECLARE r RECORD;
BEGIN
  FOR r IN SELECT id FROM queen_proxy.api_keys
            WHERE name LIKE 'iso-$RUN-%' AND revoked_at IS NULL LOOP
    PERFORM queen_proxy.revoke_api_key(r.id);
  END LOOP;
END \$\$;
SQL
  rm -rf "$TMP"
}

say "== two-tenant isolation through the proxy (run $RUN) =="

# ============================================================================
# 0. provisioning
# ============================================================================
CID_A=$(px -c "SELECT id FROM queen_proxy.clusters WHERE slug='dev'")
if [ -z "$CID_A" ]; then
  say "  FAIL- cluster 'dev' not seeded; run scripts/dev-cell.sh up first"; exit 2
fi
CID_B=$(ensure_cluster tenant-two  'Tenant Two'      two  free)
CID_P1=$(ensure_cluster tenant-pro1 'Tenant Pro One'  pro1 pro)
CID_P2=$(ensure_cluster tenant-pro2 'Tenant Pro Two'  pro2 pro)
CID_RL=$(ensure_cluster tenant-rl   'Rate Limit Rig'  rl   free)
trap cleanup EXIT

R=$(issue_key "$CID_B"  b      "'produce','consume','admin','read'"); KEY_B=${R%%|*}
R=$(issue_key "$CID_P1" p1     "'produce','consume','admin','read'"); KEY_P1=${R%%|*}
R=$(issue_key "$CID_P2" p2     "'produce','consume','admin','read'"); KEY_P2=${R%%|*}
R=$(issue_key "$CID_RL" rl     "'read'");                             KEY_RL=${R%%|*}
R=$(issue_key "$CID_A"  prod   "'produce'");                          KEY_PROD=${R%%|*}
R=$(issue_key "$CID_A"  cons   "'consume'");                          KEY_CONS=${R%%|*}
R=$(issue_key "$CID_A"  read   "'read'");                             KEY_READ=${R%%|*}
R=$(issue_key "$CID_A"  tmp    "'read'"); KEY_TMP=${R%%|*}; KEY_TMP_ID=${R#*|}

for c in "$CID_A" "$CID_B" "$CID_P1" "$CID_P2"; do set_ovr "$c" "$WIDE"; done

# enforce/shadow, straight off the proxy's own startup line (`queen-proxy up
# addr=... enforce=<bool>`); tr/sed strip the ANSI colouring.
ENFORCING=unknown
if [ -r "$PROXY_LOG" ]; then
  LINE=$(grep -a "queen-proxy up" "$PROXY_LOG" | tail -1 | tr -cd '[:print:]\n' | sed 's/\[[0-9;]*m//g')
  case "$LINE" in
    *enforce=true*)  ENFORCING=yes ;;
    *enforce=false*) ENFORCING=no  ;;
  esac
fi
say "  ...  proxy enforcing: $ENFORCING (log: $PROXY_LOG)"

# The limit override above only reaches the proxy when its host-cache entry
# expires (30s TTL). Wait it out -- but only when enforcing, and only for as
# long as the cluster actually answers 429.
if [ "$ENFORCING" = "yes" ]; then
  DEADLINE=$((SECONDS+45))
  while [ $SECONDS -lt $DEADLINE ]; do
    HIT=0
    for _ in 1 2 3 4 5 6 7 8 9 10; do
      R=$(req GET dev "$KEY_A" /api/v1/resources/queues)
      if [ "${R%%|*}" = "429" ]; then HIT=1; break; fi
    done
    [ "$HIT" = "0" ] && break
    say "  ...  waiting for the wide limit override to reach the proxy (host cache TTL)"
    sleep 5
  done
fi

# ============================================================================
# 1. same queue name, both clusters push
# ============================================================================
# One name, both tenants -- that is the point of the section. It carries the run
# id all the same: a pop leases its partition for the queue's leaseTime (300s by
# default) and the smoke does not ack every message it pops, so a fixed name
# would make two runs inside five minutes read each other's leases and report a
# phantom isolation failure.
SHQ="orders-$RUN"
RA=$(req POST dev "$KEY_A" /api/v1/push "{\"items\":[{\"queue\":\"$SHQ\",\"payload\":{\"who\":\"A\"}}]}")
RB=$(req POST two "$KEY_B" /api/v1/push "{\"items\":[{\"queue\":\"$SHQ\",\"payload\":{\"who\":\"B\"}}]}")
check "push A orders" 201 "${RA%%|*}"
check "push B orders" 201 "${RB%%|*}"

# retention setup happens here so the sweep has time to run before section 12
# checks it (RETENTION_INTERVAL_MS defaults to 5000).
RETQ="ret-$RUN"
req POST dev "$KEY_A" /api/v1/configure \
  "{\"queue\":\"$RETQ\",\"options\":{\"retentionEnabled\":true,\"retentionSeconds\":3600}}" >/dev/null
req POST two "$KEY_B" /api/v1/configure \
  "{\"queue\":\"$RETQ\",\"options\":{\"retentionEnabled\":true,\"retentionSeconds\":1}}" >/dev/null
req POST dev "$KEY_A" /api/v1/push "{\"items\":[{\"queue\":\"$RETQ\",\"payload\":{\"who\":\"A-keep\"}}]}" >/dev/null
req POST two "$KEY_B" /api/v1/push "{\"items\":[{\"queue\":\"$RETQ\",\"payload\":{\"who\":\"B-sweep\"}}]}" >/dev/null
RET_T0=$SECONDS

# ============================================================================
# 2. pops see only their own
# ============================================================================
PA=$(req GET dev "$KEY_A" "/api/v1/pop/queue/$SHQ?batch=10")
PB=$(req GET two "$KEY_B" "/api/v1/pop/queue/$SHQ?batch=10")
if has '"who":"A"' "${PA#*|}" && ! has '"who":"B"' "${PA#*|}"; then
  ok "pop A sees only A"; else bad "pop A cross-contaminated: $(short "${PA#*|}")"; fi
if has '"who":"B"' "${PB#*|}" && ! has '"who":"A"' "${PB#*|}"; then
  ok "pop B sees only B"; else bad "pop B cross-contaminated: $(short "${PB#*|}")"; fi

# ============================================================================
# 3. foreign-pid ack rejected (B acks A's partition/txn)
# ============================================================================
PID_A=$(j '.partitionId' "${PA#*|}")
TXN_A=$(j '.messages[0].transactionId' "${PA#*|}")
if [ -n "$PID_A" ] && [ "$PID_A" != "null" ] && [ -n "$TXN_A" ] && [ "$TXN_A" != "null" ]; then
  XA=$(req POST two "$KEY_B" /api/v1/ack "{\"transactionId\":\"$TXN_A\",\"partitionId\":\"$PID_A\",\"status\":\"completed\"}")
  if has 'not owned' "${XA#*|}" || has '"success":false' "${XA#*|}"; then
    ok "foreign-pid ack rejected"; else bad "foreign-pid ack NOT rejected: $(short "$XA")"; fi
  SA=$(req POST dev "$KEY_A" /api/v1/ack "{\"transactionId\":\"$TXN_A\",\"partitionId\":\"$PID_A\",\"status\":\"completed\"}")
  want_in "own ack accepted" '"success":true' "${SA#*|}"
else
  bad "could not extract pid/txn from pop A"
  bad "own ack not attempted (no pid/txn)"
fi

# ============================================================================
# 4. listings are scoped -- asserted on CONTENT, both directions
# ============================================================================
QA="iso-a-$RUN"; QB="iso-b-$RUN"; NSA="nsa-$RUN"; NSB="nsb-$RUN"
req POST dev "$KEY_A" /api/v1/configure "{\"queue\":\"$QA\",\"namespace\":\"$NSA\",\"task\":\"ta\"}" >/dev/null
req POST two "$KEY_B" /api/v1/configure "{\"queue\":\"$QB\",\"namespace\":\"$NSB\",\"task\":\"tb\"}" >/dev/null
# 4 x 32 bytes of payload so retainedBytes has something to track in section 8.
PAYLOAD='{"pad":"0123456789abcdef0123456789abcde"}'
for _ in 1 2 3 4; do
  req POST dev "$KEY_A" /api/v1/push "{\"items\":[{\"queue\":\"$QA\",\"payload\":$PAYLOAD}]}" >/dev/null
done
req POST two "$KEY_B" /api/v1/push "{\"items\":[{\"queue\":\"$QB\",\"payload\":$PAYLOAD}]}" >/dev/null

LA=$(req GET dev "$KEY_A" /api/v1/resources/queues); LA=${LA#*|}
LB=$(req GET two "$KEY_B" /api/v1/resources/queues); LB=${LB#*|}
want_in  "resources/queues A lists A's own queue"   "\"$QA\"" "$LA"
want_out "resources/queues A hides B's queue"       "\"$QB\"" "$LA"
want_in  "resources/queues B lists B's own queue"   "\"$QB\"" "$LB"
want_out "resources/queues B hides A's queue"       "\"$QA\"" "$LB"
AN=$(j "[.queues[]|select(.name==\"$SHQ\")]|length" "$LA")
BN=$(j "[.queues[]|select(.name==\"$SHQ\")]|length" "$LB")
if [ "$AN" = "1" ] && [ "$BN" = "1" ]; then
  ok "both clusters list exactly their own shared-name queue"
else bad "scoped listing broken (A:$AN B:$BN)"; fi

# ============================================================================
# 5. key/cluster binding: B's key on A's host
# ============================================================================
KB_ON_A=$(req GET dev "$KEY_B" /api/v1/resources/queues)
check "B key on dev host -> 403" 403 "${KB_ON_A%%|*}"

# ============================================================================
# 6. blocked operator surfaces and discovery pop
# ============================================================================
for path in /api/v1/pop /api/v1/status /api/v1/analytics/postgres-stats /metrics/prometheus; do
  R=$(req GET dev "$KEY_A" "$path")
  check "blocked $path" 404 "${R%%|*}"
done

# ============================================================================
# 7. dedup: same transactionId on both clusters must NOT collide
# ============================================================================
D1=$(req POST dev "$KEY_A" /api/v1/push "{\"items\":[{\"queue\":\"$SHQ\",\"payload\":{\"n\":1},\"transactionId\":\"ISO-DUP-$RUN\"}]}")
D2=$(req POST two "$KEY_B" /api/v1/push "{\"items\":[{\"queue\":\"$SHQ\",\"payload\":{\"n\":1},\"transactionId\":\"ISO-DUP-$RUN\"}]}")
if has '"status":"queued"' "${D1#*|}" && has '"status":"queued"' "${D2#*|}"; then
  ok "same transactionId, no cross-tenant dedup"
else bad "cross-tenant dedup collision: $(short "$D1") / $(short "$D2")"; fi

# ============================================================================
# 8. scoped aggregates (Track B2) -- CONTENT, not just HTTP 200
# ============================================================================
# 8a. retainedBytes must be present AND track what A actually pushed. The
# broker refreshes its stats table on STATS_INTERVAL_MS (10s in the dev cell),
# so poll rather than read once.
PUSHED=$(( ${#PAYLOAD} * 4 ))
DEADLINE=$((SECONDS+60)); RB_SEEN=""
while [ $SECONDS -lt $DEADLINE ]; do
  QQ=$(req GET dev "$KEY_A" /api/v1/resources/queues); QQ=${QQ#*|}
  RB_SEEN=$(j "[.queues[]|select(.name==\"$QA\")|.retainedBytes]|first" "$QQ")
  [ -n "$RB_SEEN" ] && [ "$RB_SEEN" != "null" ] && [ "$RB_SEEN" -gt 0 ] 2>/dev/null && break
  sleep 3
done
if [ -n "$RB_SEEN" ] && [ "$RB_SEEN" != "null" ]; then
  ok "retainedBytes present in resources/queues"
else
  bad "retainedBytes missing for $QA: $(short "$QQ")"
fi
# Generous band: the segment frames carry per-message envelope on top of the
# raw payload, so require at least the payload bytes and no more than 8x them.
if [ -n "$RB_SEEN" ] && [ "$RB_SEEN" != "null" ] \
   && [ "$RB_SEEN" -ge "$PUSHED" ] 2>/dev/null && [ "$RB_SEEN" -le $((PUSHED*8)) ] 2>/dev/null; then
  ok "retainedBytes tracks A's pushed bytes ($RB_SEEN for ${PUSHED}B pushed)"
else
  bad "retainedBytes does not track A's pushed bytes (got '$RB_SEEN', pushed ${PUSHED}B)"
fi

# 8b. overview counts are the tenant's own, not the cell's. Both sides of the
# comparison are tenant-scoped views of the same set, so they must converge;
# the stats table lags the listing by one refresh, hence the poll.
overview_matches_listing() { # host key -> "overview_queues|listing_queues"
  local o l
  o=$(req GET "$1" "$2" /api/v1/resources/overview); o=${o#*|}
  l=$(req GET "$1" "$2" /api/v1/resources/queues);   l=${l#*|}
  printf '%s|%s' "$(j '.queues' "$o")" "$(j '.queues|length' "$l")"
}
for pair in "dev:$KEY_A:A" "two:$KEY_B:B"; do
  h=${pair%%:*}; rest=${pair#*:}; k=${rest%%:*}; nm=${rest##*:}
  DEADLINE=$((SECONDS+60)); M=""
  while [ $SECONDS -lt $DEADLINE ]; do
    M=$(overview_matches_listing "$h" "$k")
    [ "${M%%|*}" = "${M#*|}" ] && break
    sleep 3
  done
  if [ "${M%%|*}" = "${M#*|}" ] && [ -n "${M%%|*}" ] && [ "${M%%|*}" != "null" ]; then
    ok "overview queue count is tenant $nm's own (${M%%|*})"
  else
    bad "overview leaks/omits queues for tenant $nm (overview ${M%%|*} vs listing ${M#*|})"
  fi
done

# 8c. namespaces
NA=$(req GET dev "$KEY_A" /api/v1/resources/namespaces); NA=${NA#*|}
NB=$(req GET two "$KEY_B" /api/v1/resources/namespaces); NB=${NB#*|}
want_in  "namespaces A lists A's own namespace" "\"$NSA\"" "$NA"
want_out "namespaces A hides B's namespace"     "\"$NSB\"" "$NA"
want_in  "namespaces B lists B's own namespace" "\"$NSB\"" "$NB"
want_out "namespaces B hides A's namespace"     "\"$NSA\"" "$NB"

SYS=$(req GET dev "$KEY_A" /api/v1/analytics/system-metrics)
check "system-metrics stays blocked" 404 "${SYS%%|*}"

# ============================================================================
# 9. ADVERSARIAL: tenant B against tenant A's resources on every mutating route
#    Every check verifies A's data afterwards, not just B's return code --
#    several of these routes answer 200 to B and simply do nothing.
# ============================================================================
ADVQ="adv-$RUN"; CG="cg-$RUN"; TXNQ="txn-$RUN"
req POST dev "$KEY_A" /api/v1/configure \
  "{\"queue\":\"$ADVQ\",\"namespace\":\"$NSA\",\"task\":\"adv-a\",\"options\":{\"leaseTime\":300,\"retryLimit\":3}}" >/dev/null
req POST dev "$KEY_A" /api/v1/push "{\"items\":[{\"queue\":\"$ADVQ\",\"payload\":{\"n\":1}},{\"queue\":\"$ADVQ\",\"payload\":{\"n\":2}}]}" >/dev/null
AQ_BEFORE=$(req GET dev "$KEY_A" "/api/v1/resources/queues/$ADVQ"); AQ_BEFORE=${AQ_BEFORE#*|}
AQ_ID=$(j '.id' "$AQ_BEFORE")

# subscriptionMode=all is LOAD-BEARING, not decoration. This group is created by
# this very pop, and the broker's DEFAULT_SUBSCRIPTION_MODE is `new` (the flip
# restored in server/src/config.rs), so a fresh group starts at the TAIL and this
# pop answers 204 with no body -- the two messages above were pushed before the
# group existed. Every check in this section then works on an empty lease/txn and
# fails for a reason that has nothing to do with isolation. The group's mode is
# persisted on its first registration, so the later pop on $CG inherits it.
POP1=$(req GET dev "$KEY_A" "/api/v1/pop/queue/$ADVQ?batch=1&consumerGroup=$CG&subscriptionMode=all"); POP1=${POP1#*|}
ADV_PID=$(j '.partitionId' "$POP1")
ADV_TXN=$(j '.messages[0].transactionId' "$POP1")
ADV_LEASE=$(j '.leaseId' "$POP1")
if [ -n "$ADV_TXN" ] && [ "$ADV_TXN" != "null" ] && [ -n "$ADV_LEASE" ] && [ "$ADV_LEASE" != "null" ]; then
  ok "section 9 fixture: A holds a real lease + transaction to attack"
else
  bad "section 9 fixture: pop returned no lease/txn ($(short "$POP1")); the checks below cannot mean anything"
fi

# 9a. DELETE queue
req DELETE two "$KEY_B" "/api/v1/resources/queues/$ADVQ" >/dev/null
AQ_AFTER=$(req GET dev "$KEY_A" "/api/v1/resources/queues/$ADVQ"); AQ_AFTER=${AQ_AFTER#*|}
if [ "$(j '.id' "$AQ_AFTER")" = "$AQ_ID" ] && [ "$AQ_ID" != "null" ] && [ -n "$AQ_ID" ]; then
  ok "B DELETE of A's queue leaves A's queue intact"
else bad "A's queue changed after B's DELETE: $(short "$AQ_AFTER")"; fi

# 9b. DELETE message by A's (pid, txn)
DM=$(req DELETE two "$KEY_B" "/api/v1/messages/$ADV_PID/$ADV_TXN"); DM=${DM#*|}
want_in "B DELETE of A's message is a no-op" '"success":false' "$DM"
GM=$(req GET dev "$KEY_A" "/api/v1/messages/$ADV_PID/$ADV_TXN")
if [ "${GM%%|*}" = "200" ] && ! has '"success":false' "${GM#*|}"; then
  ok "A's message still readable after B's DELETE"
else bad "A's message gone after B's DELETE: $(short "$GM")"; fi

# 9c. lease extend
LE_B=$(req POST two "$KEY_B" "/api/v1/lease/$ADV_LEASE/extend" '{"seconds":600}'); LE_B=${LE_B#*|}
if [ "$(j '.renewed' "$LE_B")" = "0" ]; then
  ok "B cannot extend A's lease (renewed 0)"
else bad "B extended A's lease: $(short "$LE_B")"; fi
LE_A=$(req POST dev "$KEY_A" "/api/v1/lease/$ADV_LEASE/extend" '{"seconds":600}'); LE_A=${LE_A#*|}
if [ "$(j '.renewed' "$LE_A")" -ge 1 ] 2>/dev/null; then
  ok "A can extend its own lease (control: the lease was real)"
else bad "A could not extend its own lease: $(short "$LE_A")"; fi

# 9d. /transaction carrying A's pid. The push op in the same transaction is the
#     rollback witness: if the ack is refused the whole batch must be undone.
TX=$(req POST two "$KEY_B" /api/v1/transaction \
  "{\"operations\":[{\"type\":\"push\",\"queue\":\"$TXNQ\",\"payload\":{\"rollback\":\"me\"}},{\"type\":\"ack\",\"transactionId\":\"$ADV_TXN\",\"partitionId\":\"$ADV_PID\",\"consumerGroup\":\"$CG\",\"leaseId\":\"$ADV_LEASE\",\"status\":\"completed\"}]}")
TX=${TX#*|}
if has '"success":false' "$TX" && has 'not owned' "$TX"; then
  ok "B transaction acking A's pid is refused"
else bad "B transaction acking A's pid was not refused: $(short "$TX")"; fi
TXP=$(req GET two "$KEY_B" "/api/v1/pop/queue/$TXNQ?batch=1")
if [ "${TXP%%|*}" = "204" ] || [ "${TXP%%|*}" = "404" ] \
   || [ "$(j '.messages|length' "${TXP#*|}")" = "0" ]; then
  ok "the refused transaction rolled back its own push too"
else bad "refused transaction still stored its push: $(short "$TXP")"; fi

# A acks its own message: the ack wire is (consumerGroup, leaseId)-addressed, so
# both travel with it -- the pop above used a named group, not queue mode.
ACK_A=$(req POST dev "$KEY_A" /api/v1/ack \
  "{\"transactionId\":\"$ADV_TXN\",\"partitionId\":\"$ADV_PID\",\"consumerGroup\":\"$CG\",\"leaseId\":\"$ADV_LEASE\",\"status\":\"completed\"}")
want_in "A can still ack its own message afterwards" '"success":true' "${ACK_A#*|}"

# 9e. /configure on A's queue NAME
req POST two "$KEY_B" /api/v1/configure \
  "{\"queue\":\"$ADVQ\",\"namespace\":\"$NSB\",\"task\":\"adv-b\",\"options\":{\"leaseTime\":1,\"retryLimit\":9}}" >/dev/null
AQ_CFG=$(req GET dev "$KEY_A" "/api/v1/resources/queues/$ADVQ"); AQ_CFG=${AQ_CFG#*|}
if [ "$(j '.id' "$AQ_CFG")" = "$AQ_ID" ] && [ "$(j '.task' "$AQ_CFG")" = "adv-a" ]; then
  ok "B configure of A's queue name does not touch A's queue"
else bad "A's queue config changed after B's configure: $(short "$AQ_CFG")"; fi

# 9f. consumer-group mutations
SEEK=$(req POST two "$KEY_B" "/api/v1/consumer-groups/$CG/queues/$ADVQ/seek" '{"toEnd":true}')
if [ "$(j '.partitionsUpdated' "${SEEK#*|}")" = "0" ]; then
  ok "B seek of A's consumer group updates nothing"
else bad "B seek touched A's group: $(short "$SEEK")"; fi
DCGQ=$(req DELETE two "$KEY_B" "/api/v1/consumer-groups/$CG/queues/$ADVQ")
if [ "$(j '.deletedPartitions' "${DCGQ#*|}")" = "0" ]; then
  ok "B delete of A's group-on-queue deletes nothing"
else bad "B deleted A's group-on-queue: $(short "$DCGQ")"; fi
DCG=$(req DELETE two "$KEY_B" "/api/v1/consumer-groups/$CG")
if [ "$(j '.deletedPartitions' "${DCG#*|}")" = "0" ]; then
  ok "B delete of A's consumer group deletes nothing"
else bad "B deleted A's consumer group: $(short "$DCG")"; fi
SUB=$(req POST two "$KEY_B" "/api/v1/consumer-groups/$CG/subscription" '{"subscriptionTimestamp":"2020-01-01T00:00:00Z"}')
if [ "$(j '.rowsUpdated' "${SUB#*|}")" = "0" ]; then
  ok "B subscription change on A's group updates nothing"
else bad "B changed A's subscription: $(short "$SUB")"; fi
CGL=$(req GET dev "$KEY_A" /api/v1/consumer-groups); CGL=${CGL#*|}
want_in "A's consumer group survives B's mutations" "\"$CG\"" "$CGL"
POP2=$(req GET dev "$KEY_A" "/api/v1/pop/queue/$ADVQ?batch=1&consumerGroup=$CG")
if [ "$(j '.messages[0].data.n' "${POP2#*|}")" = "2" ]; then
  ok "A's group offset untouched: the second message is still delivered"
else bad "A's group offset was moved by B: $(short "$POP2")"; fi

# 9g. POST /traces with A's pid, from a plan without the traces feature: the
#     proxy's own gate must close first (the broker-side gate is section 10).
TRG=$(req POST two "$KEY_B" /api/v1/traces \
  "{\"transactionId\":\"$ADV_TXN\",\"partitionId\":\"$ADV_PID\",\"data\":{\"evil\":1},\"traceNames\":[\"evil-$RUN\"]}")
check "B POST /traces without the plan feature -> 403" 403 "${TRG%%|*}"
want_in "  ... with code feature_gated" '"code":"feature_gated"' "${TRG#*|}"

# ============================================================================
# 10. TRACES ISOLATION (pro1 vs pro2 -- both plans carry the traces feature)
#     GET /traces/names and /traces/by-name were a cross-tenant read leak until
#     011_traces.sql started resolving each trace's owner through its partition.
# ============================================================================
TN="tn-$RUN"
TQ="tq-$RUN"
req POST pro1 "$KEY_P1" /api/v1/push "{\"items\":[{\"queue\":\"$TQ\",\"payload\":{\"who\":\"p1\"}}]}" >/dev/null
TPOP=$(req GET pro1 "$KEY_P1" "/api/v1/pop/queue/$TQ?batch=1"); TPOP=${TPOP#*|}
T_PID=$(j '.partitionId' "$TPOP"); T_TXN=$(j '.messages[0].transactionId' "$TPOP")
TREC=$(req POST pro1 "$KEY_P1" /api/v1/traces \
  "{\"transactionId\":\"$T_TXN\",\"partitionId\":\"$T_PID\",\"data\":{\"step\":1},\"traceNames\":[\"$TN\"]}")
check "pro1 records a trace" 201 "${TREC%%|*}"

N1=$(req GET pro1 "$KEY_P1" /api/v1/traces/names); N1=${N1#*|}
N2=$(req GET pro2 "$KEY_P2" /api/v1/traces/names); N2=${N2#*|}
want_in  "traces/names shows pro1 its own trace name (control)" "\"$TN\"" "$N1"
want_out "traces/names hides pro1's trace name from pro2"       "\"$TN\"" "$N2"

B1=$(req GET pro1 "$KEY_P1" "/api/v1/traces/by-name/$TN"); B1=${B1#*|}
B2=$(req GET pro2 "$KEY_P2" "/api/v1/traces/by-name/$TN"); B2=${B2#*|}
if [ "$(j '.total' "$B1")" -ge 1 ] 2>/dev/null; then
  ok "traces/by-name returns pro1 its own rows (control)"
else bad "pro1 cannot read its own trace: $(short "$B1")"; fi
if [ "$(j '.total' "$B2")" = "0" ] && [ "$(j '.traces|length' "$B2")" = "0" ]; then
  ok "traces/by-name returns pro2 nothing of pro1's"
else bad "traces/by-name leaked pro1's rows to pro2: $(short "$B2")"; fi

TW=$(req POST pro2 "$KEY_P2" /api/v1/traces \
  "{\"transactionId\":\"$T_TXN\",\"partitionId\":\"$T_PID\",\"data\":{\"evil\":1},\"traceNames\":[\"evil-$RUN\"]}")
check "pro2 cannot record a trace on pro1's pid" 404 "${TW%%|*}"
TR2=$(req GET pro2 "$KEY_P2" "/api/v1/traces/$T_PID/$T_TXN"); TR2=${TR2#*|}
if [ "$(j '.traces|length' "$TR2")" = "0" ]; then
  ok "pid-addressed trace read returns pro2 nothing of pro1's"
else bad "pid-addressed trace read leaked to pro2: $(short "$TR2")"; fi

# ============================================================================
# 11. api-key SCOPES (narrow keys on cluster dev)
# ============================================================================
SCQ="scope-$RUN"
scope_case() { # desc key method path [body] expected-code
  local d=$1 k=$2 m=$3 p=$4 b=$5 want=$6
  local r; r=$(req "$m" dev "$k" "$p" "$b")
  if [ "${r%%|*}" = "$want" ]; then
    if [ "$want" = "403" ] && ! has '"code":"forbidden"' "${r#*|}"; then
      bad "$d (403 but wrong code: $(short "${r#*|}"))"; return
    fi
    ok "$d"
  else bad "$d (want $want, got ${r%%|*}: $(short "${r#*|}"))"; fi
}
scope_case "produce key: push allowed"   "$KEY_PROD" POST   /api/v1/push "{\"items\":[{\"queue\":\"$SCQ\",\"payload\":1}]}" 201
scope_case "produce key: pop refused"    "$KEY_PROD" GET    "/api/v1/pop/queue/$SCQ?batch=1" "" 403
scope_case "produce key: read refused"   "$KEY_PROD" GET    /api/v1/resources/queues "" 403
scope_case "produce key: admin refused"  "$KEY_PROD" DELETE "/api/v1/resources/queues/$SCQ" "" 403
scope_case "consume key: push refused"   "$KEY_CONS" POST   /api/v1/push "{\"items\":[{\"queue\":\"$SCQ\",\"payload\":1}]}" 403
scope_case "consume key: read refused"   "$KEY_CONS" GET    /api/v1/resources/queues "" 403
scope_case "consume key: admin refused"  "$KEY_CONS" DELETE "/api/v1/resources/queues/$SCQ" "" 403
CP=$(req GET dev "$KEY_CONS" "/api/v1/pop/queue/$SCQ?batch=1")
if [ "${CP%%|*}" = "200" ] || [ "${CP%%|*}" = "204" ]; then
  ok "consume key: pop allowed"
else bad "consume key: pop refused (${CP%%|*}: $(short "${CP#*|}"))"; fi
scope_case "read key: read allowed"      "$KEY_READ" GET    /api/v1/resources/queues "" 200
scope_case "read key: push refused"      "$KEY_READ" POST   /api/v1/push "{\"items\":[{\"queue\":\"$SCQ\",\"payload\":1}]}" 403
scope_case "read key: pop refused"       "$KEY_READ" GET    "/api/v1/pop/queue/$SCQ?batch=1" "" 403
scope_case "read key: admin refused"     "$KEY_READ" DELETE "/api/v1/resources/queues/$SCQ" "" 403

# ============================================================================
# 12. cross-tenant RETENTION: the most aggressive config among tenants sharing
#     a queue NAME must not delete the others' data (server/src/retention.rs
#     used to join queues -> log_queues on name alone).
# ============================================================================
WAIT=$((RET_T0 + 20 - SECONDS))
if [ "$WAIT" -gt 0 ]; then
  say "  ...  waiting ${WAIT}s for retention cycles (RETENTION_INTERVAL_MS=5000)"
  sleep "$WAIT"
fi
RPA=$(req GET dev "$KEY_A" "/api/v1/pop/queue/$RETQ?batch=10")
RPB=$(req GET two "$KEY_B" "/api/v1/pop/queue/$RETQ?batch=10")
want_in "A's message survives B's 1s retention on the same queue name" '"who":"A-keep"' "${RPA#*|}"
if [ "${RPB%%|*}" = "204" ] || [ "$(j '.messages|length' "${RPB#*|}")" = "0" ]; then
  ok "B's own 1s retention did sweep B's message (test is not vacuous)"
else bad "B's message was NOT swept, so the retention check proves nothing: $(short "$RPB")"; fi

# ============================================================================
# 13. api key REVOCATION (queen_proxy.revoke_api_key)
# ============================================================================
RK=$(req GET dev "$KEY_TMP" /api/v1/resources/queues)
check "temp key works before revocation" 200 "${RK%%|*}"
px -c "SELECT queen_proxy.revoke_api_key('$KEY_TMP_ID'::uuid)" >/dev/null
say "  ...  waiting for key revocation to propagate (NOTIFY, worst case the 30s key TTL)"
DEADLINE=$((SECONDS+45)); RCODE=""
while [ $SECONDS -lt $DEADLINE ]; do
  R=$(req GET dev "$KEY_TMP" /api/v1/resources/queues); RCODE=${R%%|*}
  [ "$RCODE" != "200" ] && break
  sleep 2
done
check "revoked key stops working" 401 "$RCODE"

# ============================================================================
# 14. session REVOCATION (logout deny-lists the jti)
# ============================================================================
LOGIN=$(curl -s -o /dev/null -D "$HDRF" -w '%{http_code}' -H "Host: dev" -X POST \
        -d 'email=dev@localhost&password=devpass' "$P/auth/login")
COOKIE=$(hdr set-cookie | cut -d';' -f1)
if [ "$LOGIN" = "303" ] && has 'queen_session=' "$COOKIE"; then
  ok "local login issues a session cookie"
else bad "local login failed (code $LOGIN, cookie '$COOKIE')"; fi
CO=$(creq GET dev "$COOKIE" /api/console/overview)
check "console accepts the live session" 200 "${CO%%|*}"
LO=$(creq POST dev "$COOKIE" /auth/logout)
check "logout succeeds" 200 "${LO%%|*}"
CO2=$(creq GET dev "$COOKIE" /api/console/overview)
check "console rejects the logged-out session" 401 "${CO2%%|*}"

# ============================================================================
# 15. live 429 -- only meaningful when the proxy enforces; counted SKIPs
#     otherwise, so the tally has the same denominator in both modes.
#     Cluster `rl` keeps the stock free plan (5 req/s, burst 25).
# ============================================================================
if [ "$ENFORCING" = "yes" ]; then
  N429=0; LAST=""
  for _ in $(seq 1 60); do
    R=$(req GET rl "$KEY_RL" /api/v1/resources/queues)
    if [ "${R%%|*}" = "429" ]; then N429=$((N429+1)); LAST=${R#*|}; RETRY=$(hdr retry-after); fi
  done
  if [ "$N429" -ge 1 ]; then ok "burst over the plan rate returns 429 ($N429/60)"
  else bad "no 429 in 60 requests against a 5 req/s plan"; fi
  want_in "429 body carries code rate_limited" '"code":"rate_limited"' "$LAST"
  if [ -n "${RETRY:-}" ]; then ok "429 carries Retry-After (${RETRY})"
  else bad "429 without a Retry-After header"; fi
else
  skip "burst over the plan rate returns 429 (proxy enforcing=$ENFORCING)"
  skip "429 body carries code rate_limited (proxy enforcing=$ENFORCING)"
  skip "429 carries Retry-After (proxy enforcing=$ENFORCING)"
fi

# ============================================================================
# 16. storage quota e2e: tiny override -> storage_quota_exceeded -> clear ->
#     unblocked. Cadence: broker stats refresh (~10s) + proxy reconcile (10s in
#     the dev cell) + pump (10s) => allow up to ~90s per transition.
# ============================================================================
QUOTAQ="quota-$RUN"
set_ovr "$CID_A" "$WIDE_Q"
say "  ...  waiting for the storage quota to trip (override 64 bytes)"
DEADLINE=$((SECONDS+90)); TRIPPED=no; LASTQ=""
while [ $SECONDS -lt $DEADLINE ]; do
  R=$(req POST dev "$KEY_A" /api/v1/push "{\"items\":[{\"queue\":\"$QUOTAQ\",\"payload\":{\"fill\":\"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\"}}]}")
  LASTQ=${R#*|}
  if [ "${R%%|*}" = "403" ]; then TRIPPED=yes; break; fi
  sleep 3
done
if [ "$TRIPPED" = "yes" ]; then ok "storage quota blocks pushes (403)"
else bad "storage quota never tripped (last: $(short "$LASTQ"))"; fi
want_in "  ... with code storage_quota_exceeded" '"code":"storage_quota_exceeded"' "$LASTQ"
PR=$(req GET dev "$KEY_A" "/api/v1/pop/queue/$QUOTAQ?batch=1")
if [ "${PR%%|*}" = "200" ] || [ "${PR%%|*}" = "204" ]; then
  ok "consume still allowed while pushes are blocked"
else bad "consume blocked too: ${PR%%|*}"; fi
set_ovr "$CID_A" "$WIDE"
say "  ...  waiting for release after clearing the override"
DEADLINE=$((SECONDS+90)); RELEASED=no
while [ $SECONDS -lt $DEADLINE ]; do
  R=$(req POST dev "$KEY_A" /api/v1/push "{\"items\":[{\"queue\":\"$QUOTAQ\",\"payload\":{\"ok\":1}}]}")
  if [ "${R%%|*}" = "201" ]; then RELEASED=yes; break; fi
  sleep 3
done
if [ "$RELEASED" = "yes" ]; then ok "push unblocked after clearing the override"
else bad "release never happened"; fi

# ============================================================================
# 17. queue-lag content. Runs last: queen.queue_lag_metrics is written on the
#     broker's metrics flush (METRICS_FLUSH_MS, 60s), so by now A's own queue
#     must be in the series -- which is what makes the negative assertion
#     below non-vacuous.
# ============================================================================
DEADLINE=$((SECONDS+90)); QL=""
while [ $SECONDS -lt $DEADLINE ]; do
  QL=$(req GET dev "$KEY_A" /api/v1/analytics/queue-lag); QL=${QL#*|}
  has "\"$QA\"" "$QL" && break
  sleep 5
done
want_in  "queue-lag carries A's own queue (control)" "\"$QA\"" "$QL"
want_out "queue-lag hides B's queue from A"          "\"$QB\"" "$QL"

# ============================================================================
# 18. meters: usage_minutes must carry rows for both tenants' clusters. Only
#     CLOSED minutes are flushed (meter.rs), on QUEEN_PROXY_METER_FLUSH_MS
#     (15s default), so this can need up to ~75s after the first push.
# ============================================================================
say "  ...  waiting for the usage_minutes flush"
DEADLINE=$((SECONDS+120)); ROWS=0
while [ $SECONDS -lt $DEADLINE ]; do
  ROWS=$(px -c "SELECT count(DISTINCT cluster_id) FROM queen_proxy.usage_minutes WHERE msgs > 0")
  [ "${ROWS:-0}" -ge 2 ] 2>/dev/null && break
  sleep 5
done
if [ "${ROWS:-0}" -ge 2 ] 2>/dev/null; then
  ok "usage_minutes has metered rows for both clusters ($ROWS)"
else bad "usage_minutes only has msgs>0 for ${ROWS:-0} cluster(s)"; fi

# ============================================================================
# 19. HOT-LIST + WAKE isolation: ONE queue name and ONE consumer-group name,
#     both held by both tenants -- the shared-cell default, where `orders` and
#     `workers` collide constantly.
#
#     QUEEN_HOTLIST defaults ON, so every pop in the sections above already ran
#     through the broker-side candidate ring; 19a asserts that instead of
#     assuming it. A ring/gate keyed by the BARE queue name is not a data leak
#     (queen.log_pop_list_v1 still resolves the queue under p_tenant), it is a
#     LOST WAKE: the foreign tenant's pop checks the owner's candidates out of
#     the shared ring, the SP reports them 'empty' for the caller, and the
#     checkin epoch-CAS then CLEARS marks that were never the caller's. The
#     owner's own long-poll afterwards finds a cold ring and blocks until the
#     30s reseed floor -- precisely when it is already waiting.
#
#     The two pops are therefore ORDERED (foreign first, owner second), never
#     concurrent: the steal has to be deterministic or the assertion means
#     nothing. Everything here runs inside one reseed interval of the priming
#     below, so a cleared ring cannot quietly heal itself through the floor.
# ============================================================================
HQ="hot-$RUN"; HG="hg-$RUN"

HL_ON=no-log
if [ -r "$BROKER_LOG" ]; then
  if grep -aq "QUEEN_HOTLIST on" "$BROKER_LOG"; then HL_ON=yes; else HL_ON=no; fi
fi
check "broker runs with the hot-list ON (the ring IS the pop path)" yes "$HL_ON"

# autoAck throughout: the ring's leased-Took arm parks a claimed partition on the
# lease wheel, which would make the second half of this section depend on a 300s
# lease expiring. autoAck takes the un-leased arm and leaves no residue.
hq_pop()  { treq GET "$1" "$2" "/api/v1/pop/queue/$HQ?batch=20&partitions=10&autoAck=true&consumerGroup=$HG${3:-}"; }
hq_park() { bgreq GET "$1" "$2" "/api/v1/pop/queue/$HQ?batch=20&partitions=10&autoAck=true&consumerGroup=$HG&wait=true&timeout=9000" "$3"; }
hq_push() { req POST "$1" "$2" /api/v1/push \
              "{\"items\":[{\"queue\":\"$HQ\",\"partition\":\"$3\",\"payload\":$4}]}" >/dev/null; }
parts_of(){ j '[.messages[].partition]|unique|join(",")' "$1"; }
# desc <code|secs|body> expected-partitions expected-payload
served_promptly() {
  local d=$1 r=$2 wp=$3 wb=$4 c s b
  c=${r%%|*}; r=${r#*|}; s=${r%%|*}; b=${r#*|}
  check "$d" 200 "$c"
  if lt "$s" 3; then ok "  ... promptly (${s}s into a 6s long-poll)"
  else bad "  ... but only after ${s}s -- the other tenant consumed the wake"; fi
  want_in "  ... carrying its own payload" "$wb" "$b"
  check "  ... from its own partitions only" "$wp" "$(parts_of "$b")"
}

# 19a. BOTH tenants must own a queue with this name before the experiment: when
# the queue does not resolve for the caller, log_pop_list_v1 returns an empty
# state list, every foreign candidate becomes a Requeue rather than a clear, and
# the steal would be invisible. This is also the shared-cell reality.
hq_push dev "$KEY_A" pa0 '{"hp":"a0"}'
hq_push two "$KEY_B" pb0 '{"hp":"b0"}'
# Prime both rings. The FIRST pop of a (tenant, queue, group) takes the
# first-contact bootstrap (the wildcard SP, which carries the group seed) and
# never touches the ring; a later one finds a cold ring, runs the keyset reseed
# and STAMPS the ring's reseed clock. Without that stamp the checks below would
# be healed by the reseed floor instead of by the ring, and would prove nothing.
for _ in 1 2 3 4; do
  PRA=$(hq_pop dev "$KEY_A"); PRB=$(hq_pop two "$KEY_B")
done
check "tenant A starts from a quiet, primed ring" 204 "${PRA%%|*}"
check "tenant B starts from a quiet, primed ring" 204 "${PRB%%|*}"

# 19b. A's push marks the ring; B pops the SAME (queue, group) FIRST.
hq_push dev "$KEY_A" pa1 '{"hp":"a1"}'
hq_push dev "$KEY_A" pa2 '{"hp":"a2"}'
hq_push dev "$KEY_A" pa3 '{"hp":"a3"}'
SB=$(hq_pop two "$KEY_B")
check "B's pop on A's freshly-marked shared-name queue returns nothing" 204 "${SB%%|*}"
served_promptly "A is still served after B popped the same (queue, group)" \
  "$(hq_pop dev "$KEY_A" '&wait=true&timeout=6000')" "pa1,pa2,pa3" '"hp":"a1"'

# 19c. mirror image, so neither direction is privileged.
hq_push two "$KEY_B" pb1 '{"hp":"b1"}'
hq_push two "$KEY_B" pb2 '{"hp":"b2"}'
hq_push two "$KEY_B" pb3 '{"hp":"b3"}'
SA2=$(hq_pop dev "$KEY_A")
check "A's pop on B's freshly-marked shared-name queue returns nothing" 204 "${SA2%%|*}"
served_promptly "B is still served after A popped the same (queue, group)" \
  "$(hq_pop two "$KEY_B" '&wait=true&timeout=6000')" "pb1,pb2,pb3" '"hp":"b1"'

# 19d. The same defect from the notifier side: B is PARKED on the shared
# (queue, group) when A's push lands. One gate per bare name wakes B, B drains
# the ring, and A -- whose push it was -- is the one left waiting.
hq_park two "$KEY_B" "$TMP/parkb"
sleep 1
hq_push dev "$KEY_A" pa4 '{"hp":"a4"}'
sleep 2
served_promptly "A is served while B sits parked on the same (queue, group)" \
  "$(hq_pop dev "$KEY_A" '&wait=true&timeout=6000')" "pa4" '"hp":"a4"'
wait
PARKB=$(cat "$TMP/parkb")
check "B's parked long-poll is never satisfied by A's push" 204 "${PARKB%%|*}"

# ============================================================================
# 19e. The wake for a tenant whose ONLY consumer is PARTITION-TARGETED.
#      handle_pop_partition parks on the queue GATE and registers no group ring,
#      so this queue's group map stays empty for its whole life. A wake raised
#      only on a ring transition therefore never fires for it. Nothing is wrong
#      with the DATA -- the pop still returns its own rows -- what is lost is the
#      WAKE, so the tenant is served on the pop_wait backoff floor instead. By
#      the time the push lands that floor has already climbed to its
#      POP_WAIT_MAX_INTERVAL_MS cap (1s default), so the assertion measures the
#      PUSH -> RESPONSE delta and not the poll's own wall clock: the wall clock is
#      dominated by the deliberate park below and reads the same either way.
#
#      THREE trials, asserted on the WORST one. The floor is a 1s timer whose
#      phase relative to the push is arbitrary, so a single trial on a broker with
#      the defect still lands under the threshold about a quarter of the time --
#      i.e. a one-shot version of this check would silently pass on broken code
#      once in four runs. A real wake is bounded (it is the ~5ms tick), so the
#      max over three trials separates the two cases and the run stays ~9s.
# ============================================================================
R4Q="hot4-$RUN"; R4P=r4p0
r4_ms()   { perl -MTime::HiRes=time -e 'printf "%.0f", time*1000'; }
r4_push() { req POST dev "$KEY_A" /api/v1/push \
              "{\"items\":[{\"queue\":\"$R4Q\",\"partition\":\"$R4P\",\"payload\":$1}]}" >/dev/null; }
r4_pop()  { req GET dev "$KEY_A" "/api/v1/pop/queue/$R4Q/partition/$R4P?batch=10&autoAck=true"; }
# Parks in the background and records the instant the response actually landed;
# `req`/`treq` only report a duration relative to their own start, which here is
# the park, not the push.
r4_park() { # outfile
  ( c=$(curl -s -o "$1.body" -w '%{http_code}' -H "Host: dev" -H "Authorization: Bearer $KEY_A" \
        "$P/api/v1/pop/queue/$R4Q/partition/$R4P?batch=10&autoAck=true&wait=true&timeout=9000")
    printf '%s|%s|%s' "$c" "$(r4_ms)" "$(cat "$1.body")" >"$1" ) &
}

# The queue must exist AND be empty before the park: a partition pop that finds a
# row returns on its first query and never parks, which would pass vacuously.
r4_push '{"r4":"prime"}'
sleep 0.5
r4_pop >/dev/null
R4DR=$(r4_pop)
check "R4: the partition-targeted queue is empty before the park" 204 "${R4DR%%|*}"

R4MAX=0; R4DELTAS=""; R4BADCODE=""; R4BADBODY=""
for t in 1 2 3; do
  r4_park "$TMP/r4park$t"
  sleep 2.6  # past the three short backoffs, so the floor is already at its 1s cap
  R4T0=$(r4_ms)
  r4_push "{\"r4\":\"wake$t\"}"
  wait
  R4R=$(cat "$TMP/r4park$t"); R4C=${R4R%%|*}; R4R=${R4R#*|}
  R4END=${R4R%%|*}; R4B=${R4R#*|}
  R4D=$((R4END-R4T0))
  [ "$R4D" -gt "$R4MAX" ] && R4MAX=$R4D
  R4DELTAS="$R4DELTAS ${R4D}ms"
  [ "$R4C" = 200 ] || R4BADCODE="$R4BADCODE t$t=$R4C"
  has "\"r4\":\"wake$t\"" "$R4B" || R4BADBODY="$R4BADBODY t$t"
done
if [ -z "$R4BADCODE" ]; then
  ok "R4: a partition-targeted long-poll is served by its own tenant's push (3/3)"
else bad "R4: a partition-targeted long-poll was not served ($R4BADCODE)"; fi
if [ -z "$R4BADBODY" ]; then ok "  ... each carrying that push's own payload"
else bad "  ... wrong/absent payload on trial(s):$R4BADBODY"; fi
if [ "$R4MAX" -lt 250 ]; then
  ok "  ... on the wake, not the backoff floor (worst of 3:$R4DELTAS)"
else
  bad "  ... only on the backoff floor (worst of 3:$R4DELTAS; a wake lands in <250ms)"
fi

# ============================================================================
# 20. SHARED-HOST KEY ROUTING (decision z)
#
#     On a host listed in QUEEN_PROXY_SHARED_HOSTS the cluster is resolved from
#     the CREDENTIAL, not from the Host label: an api key names its own cluster,
#     a human session names one with x-queen-act-cluster and is checked against
#     cluster_roles. dev-cell.sh configures `shared.local` for this.
#
#     Self-detecting, and the detector is itself the load-bearing assertion:
#     WITHOUT the feature, dev-cell.sh's QUEEN_PROXY_DEFAULT_CLUSTER=dev turns
#     any unresolvable Host into cluster `dev`, so B's key on `shared.local`
#     would be a 403 key/cluster mismatch. A 200 means the shared path ran AND
#     that the default cluster did not absorb the host -- the two knobs are
#     different features and must not interact (decision z).
# ============================================================================
SH=shared.local
SHQ="sh20-$RUN"          # the SAME queue name on both clusters
SHB="sh20b-$RUN"         # exists on cluster `two` only
cellpx() { docker exec -i qcell-pg psql -qtA -U postgres -d queen "$@"; }

SHPROBE=$(req GET $SH "$KEY_B" /api/v1/resources/queues)
if [ "${SHPROBE%%|*}" != "200" ]; then
  skip "shared-host routing: QUEEN_PROXY_SHARED_HOSTS is not set on this proxy (got ${SHPROBE%%|*}; expected 200 for a shared host, 403 for the default-cluster fallback)"
else
  ok "a foreign cluster's key is accepted on the shared host (and DEFAULT_CLUSTER did not absorb it)"

  # --- the data plane: the key decides which cluster, on ONE hostname --------
  SHPA=$(req POST $SH "$KEY_A" /api/v1/push \
    "{\"items\":[{\"queue\":\"$SHQ\",\"partition\":\"p0\",\"payload\":{\"sh\":\"A\"}}]}")
  check "shared host + A's key: push accepted" 201 "${SHPA%%|*}"
  SHPB=$(req POST $SH "$KEY_B" /api/v1/push \
    "{\"items\":[{\"queue\":\"$SHQ\",\"partition\":\"p0\",\"payload\":{\"sh\":\"B\"}}]}")
  check "shared host + B's key: push accepted into the SAME queue name" 201 "${SHPB%%|*}"

  SHOA=$(req GET $SH "$KEY_A" "/api/v1/pop/queue/$SHQ?batch=10&autoAck=true")
  check "shared host + A's key: pop served" 200 "${SHOA%%|*}"
  want_in  "  ... it is A's own message" '"sh":"A"' "${SHOA#*|}"
  want_out "  ... and never B's" '"sh":"B"' "${SHOA#*|}"
  SHOB=$(req GET $SH "$KEY_B" "/api/v1/pop/queue/$SHQ?batch=10&autoAck=true")
  check "shared host + B's key: pop served" 200 "${SHOB%%|*}"
  want_in  "  ... it is B's own message" '"sh":"B"' "${SHOB#*|}"
  want_out "  ... and never A's" '"sh":"A"' "${SHOB#*|}"

  # The tenant-header injection, asserted where it actually lands: two rows of
  # the same queue NAME in the cell, one per cluster's broker_tenant_uuid --
  # both created through the one shared hostname.
  SHTEN=$(cellpx -c "SELECT string_agg(tenant_id::text, ',' ORDER BY tenant_id::text) FROM queen.queues WHERE name='$SHQ'")
  SHWANT=$(px -c "SELECT string_agg(broker_tenant_uuid::text, ',' ORDER BY broker_tenant_uuid::text) FROM queen_proxy.clusters WHERE slug IN ('dev','two')")
  check "x-queen-tenant followed the key, not the host (broker rows)" "$SHWANT" "$SHTEN"

  # --- scoped listings: content, not just status ----------------------------
  req POST $SH "$KEY_B" /api/v1/push \
    "{\"items\":[{\"queue\":\"$SHB\",\"partition\":\"p0\",\"payload\":{\"sh\":\"B\"}}]}" >/dev/null
  SHLA=$(req GET $SH "$KEY_A" /api/v1/resources/queues)
  want_out "shared host + A's key: B's queue is not in A's listing" "\"$SHB\"" "${SHLA#*|}"
  SHLB=$(req GET $SH "$KEY_B" /api/v1/resources/queues)
  want_in  "shared host + B's key: B's queue IS in B's listing" "\"$SHB\"" "${SHLB#*|}"
  SHLB2=$(req GET two "$KEY_B" /api/v1/resources/queues)
  # Names only: the two reads are back to back, but `retainedBytes`/segment
  # counts are refreshed by a background lane and would make a whole-body
  # comparison flaky for a reason that has nothing to do with routing.
  check "the shared host and B's own hostname list the same queues" \
        "$(j '[.queues[].name]|sort|@csv' "${SHLB#*|}")" \
        "$(j '[.queues[].name]|sort|@csv' "${SHLB2#*|}")"

  # --- the 401 contract: a missing/invalid key is NEVER a 421 ---------------
  SHNO=$(curl -s -o "$BODYF" -w '%{http_code}' -H "Host: $SH" "$P/api/v1/resources/queues")
  check "shared host, no credential at all: 401 (never 421)" 401 "$SHNO"
  want_in "  ... with the unauthorized code" '"code":"unauthorized"' "$(cat "$BODYF")"
  SHBAD=$(req GET $SH "qk_dev_$(printf 'z%.0s' $(seq 43))" /api/v1/resources/queues)
  check "shared host, unknown api key: 401 (never 421, never 403)" 401 "${SHBAD%%|*}"
  SHJUNK=$(req GET $SH "not-a-jwt-at-all" /api/v1/resources/queues)
  check "shared host, garbage session token: 401" 401 "${SHJUNK%%|*}"

  # A revoked key must fall to the same 401, not to a 403 or a 421.
  R=$(issue_key "$CID_B" shrev "'read'"); SHREV=${R%%|*}; SHREV_ID=${R#*|}
  SHRV=$(req GET $SH "$SHREV" /api/v1/resources/queues)
  check "shared host, a fresh key of cluster two works" 200 "${SHRV%%|*}"
  px -c "SELECT queen_proxy.revoke_api_key('$SHREV_ID'::uuid)" >/dev/null
  say "  ...  waiting for the revocation to propagate (NOTIFY, worst case the 30s key TTL)"
  DEADLINE=$((SECONDS+45)); SHRCODE=""
  while [ $SECONDS -lt $DEADLINE ]; do
    R=$(req GET $SH "$SHREV" /api/v1/resources/queues); SHRCODE=${R%%|*}
    [ "$SHRCODE" != "200" ] && break
    sleep 2
  done
  check "shared host, revoked key: 401 (never 421)" 401 "$SHRCODE"

  # --- the same host, fully qualified --------------------------------------
  # `shared.local.` is the SAME name (the trailing dot is the DNS root label,
  # legal in a Host header and emitted by some clients). It used to miss
  # is_shared_host, fall through to cache::resolve_host, and be absorbed by
  # this cell's QUEEN_PROXY_DEFAULT_CLUSTER=dev -- so B's key answered 403
  # key/cluster mismatch, silently routed to the WRONG cluster's ctx. One
  # character defeating the classification the whole feature rests on.
  SHFQ=$(curl -s -o "$BODYF" -w '%{http_code}' -H "Host: $SH." -H "Authorization: Bearer $KEY_B" \
         "$P/api/v1/resources/queues")
  check "the fully-qualified shared host routes by credential too" 200 "$SHFQ"
  want_in "  ... to B's own cluster, not the default one" "\"$SHB\"" "$(cat "$BODYF")"
  SHFQN=$(curl -s -o /dev/null -w '%{http_code}' -H "Host: $SH." "$P/api/v1/resources/queues")
  check "  ... and it is a 401 with no credential, like the portless form" 401 "$SHFQN"

  # --- a key is never RETARGETED, on a shared host either -------------------
  SHACT=$(curl -s -o "$BODYF" -w '%{http_code}' -H "Host: $SH" -H "Authorization: Bearer $KEY_B" \
          -H 'x-queen-act-cluster: dev' "$P/api/v1/resources/queues")
  check "shared host + key + act-as-cluster: 403, a key stays on its own cluster" 403 "$SHACT"

  # --- non-shared hosts are untouched --------------------------------------
  SHX=$(req GET two "$KEY_A" /api/v1/resources/queues)
  check "non-shared host: A's key on cluster two is still a mismatch" 403 "${SHX%%|*}"
  SHY=$(req GET dev "$KEY_A" /api/v1/resources/queues)
  check "non-shared host: A's key on its own host still works" 200 "${SHY%%|*}"

  # --- the console/webapp half: session identity + cluster_roles ------------
  SHLOGIN=$(curl -s -o /dev/null -D "$HDRF" -w '%{http_code}' -H "Host: $SH" -X POST \
            -d 'email=dev@localhost&password=devpass' "$P/auth/login")
  SHCOOK=$(hdr set-cookie | cut -d';' -f1)
  if [ "$SHLOGIN" = "303" ] && has 'queen_session=' "$SHCOOK"; then
    ok "shared host: local login issues a session cookie"
    SHME=$(creq GET $SH "$SHCOOK" /auth/me)
    check "  ... /auth/me answers on the shared host" 200 "${SHME%%|*}"
    check "  ... naming no acting cluster yet (the SPA's selector case)" "null" \
          "$(j '.acting_cluster' "${SHME#*|}")"
    want_in "  ... but listing the clusters the session may pick" '"slug":"dev"' "${SHME#*|}"
    SHC0=$(creq GET $SH "$SHCOOK" /api/console/overview)
    check "shared host: a session that names no cluster is 403 (not 401, not 421)" 403 "${SHC0%%|*}"
    SHC1=$(curl -s -o "$BODYF" -w '%{http_code}' -H "Host: $SH" -H "Cookie: $SHCOOK" \
           -H 'x-queen-act-cluster: dev' "$P/api/console/overview")
    check "shared host + act-as-cluster: the console opens the named cluster" 200 "$SHC1"
    want_in "  ... and it is that cluster's overview" '"slug":"dev"' "$(cat "$BODYF")"
    SHC2=$(curl -s -o "$TMP/shc2" -w '%{http_code}' -H "Host: $SH" -H "Cookie: $SHCOOK" \
           -H 'x-queen-act-cluster: two' "$P/api/console/overview")
    check "shared host: a cluster the session holds no role on is 403" 403 "$SHC2"
    SHC3=$(curl -s -o "$TMP/shc3" -w '%{http_code}' -H "Host: $SH" -H "Cookie: $SHCOOK" \
           -H 'x-queen-act-cluster: nosuchcluster' "$P/api/console/overview")
    check "  ... and a cluster that does not exist is the SAME 403" 403 "$SHC3"
    check "  ... byte-identical, so the header cannot enumerate slugs" \
          "$(cat "$TMP/shc2")" "$(cat "$TMP/shc3")"
    SHC4=$(req GET $SH "$KEY_B" /api/console/overview)
    check "shared host: the console still refuses an api key" 403 "${SHC4%%|*}"
  else
    skip "shared host: local login did not issue a cookie (code $SHLOGIN) -- console half not exercised"
    skip "  ... (console act-as-cluster checks)"
  fi

  # --- limits still bucket PER CLUSTER on one hostname ----------------------
  if [ "$ENFORCING" = "yes" ]; then
    set_ovr "$CID_B" '{"max_req_per_sec":1,"req_burst":3,"max_queues":500}'
    sleep 1
    SH429=0; SHA200=0
    for _ in $(seq 1 12); do
      R=$(req GET $SH "$KEY_B" /api/v1/resources/queues)
      [ "${R%%|*}" = "429" ] && SH429=$((SH429+1))
    done
    for _ in $(seq 1 12); do
      R=$(req GET $SH "$KEY_A" /api/v1/resources/queues)
      [ "${R%%|*}" = "200" ] && SHA200=$((SHA200+1))
    done
    if [ "$SH429" -gt 0 ]; then
      ok "shared host: the narrowed cluster's own bucket still 429s ($SH429/12)"
    else bad "shared host: the narrowed cluster never 429'd -- the bucket did not follow the key"; fi
    check "  ... while the OTHER cluster on the same hostname is untouched" 12 "$SHA200"
    set_ovr "$CID_B" "$WIDE"
    sleep 1
  else
    skip "shared host: per-cluster rate limiting (proxy is in shadow mode)"
    skip "  ... (the other cluster on the same hostname stays unthrottled)"
  fi
fi

TOTAL=$((PASS+FAIL+SKIP))
say "== result: $TOTAL checks -- $PASS ok, $FAIL fail, $SKIP skipped =="
[ "$FAIL" -eq 0 ]
