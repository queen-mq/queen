#!/usr/bin/env bash
#
# The queen-kafka CLOUD acceptance rig: a whole Queen Cloud cell in one script,
# and the franz-go suite in compat/cloud run against it.
#
#   queen-kafka/compat/cloud/rig-cloud.sh              # the whole suite
#   queen-kafka/compat/cloud/rig-cloud.sh -run TestMetering -v
#   queen-kafka/compat/cloud/rig-cloud.sh --keep       # leave the stack up
#
# Every argument that is not --keep goes through to `go test`.
#
# ## What a "cell" is here, and why the facade points at the PROXY
#
#          Kafka client ──TCP :33044──► queen-kafka (facade)
#                                            │  QUEEN_URL
#                                            ▼
#                                       queen-proxy :33043   ← auth, tenant
#                                            │                  scoping, quotas,
#                                            ▼                  metering
#                                        queen (broker) :33042
#
# In OSS embedded mode the facade is handed a LOOPBACK url and talks to the
# broker directly. In Cloud that is exactly wrong: every Kafka request has to
# cross the proxy, or a Kafka client is a tenant with no quota, no metering and
# no isolation. `QUEEN_URL` here is the proxy, which is what
# `server/src/kafka_facade.rs::child_queen_url` exists to make possible in the
# embedded shape too.
#
# ## Routing: a SHARED host, and why not per-cluster SNI
#
# The proxy resolves a cluster either from the `Host` header (a per-cluster
# hostname) or, on a host listed in `QUEEN_PROXY_SHARED_HOSTS`, FROM THE
# CREDENTIAL (decision z). This rig uses the shared-host arm, and that is a
# deliberate call rather than a shortcut:
#
#   `queen-kafka/src/lib.rs` keeps `advertised_host` per PROCESS, not per SNI
#   lane. One facade therefore hands every client the SAME bootstrap address
#   whatever name they dialled, so a second tenant's connections would come
#   back carrying the first tenant's SNI and be routed to the first tenant's
#   cluster. Per-cluster SNI needs one facade process per cluster; one facade
#   fronting many tenants needs the credential to be the authority. The second
#   is the shape a Kafka listener actually has in Cloud, so it is the one
#   proven here.
#
# Consequence, and it is the point: the tenant of a Kafka connection is the
# tenant of the SASL password. Nothing else.
#
# ## Ports and containers: 33040-33059, prefix qkc-t2-
#
# Deliberately not the defaults, and deliberately not any port this repository's
# other rigs use. 5432 is a LIVE stack on a developer machine and is never
# touched here.
#
#   33040  pxdb        docker  qkc-t2-pxdb     (proxy control plane)
#   33041  cell PG     docker  qkc-t2-cellpg   (the broker's database)
#   33042  broker      host process
#   33043  proxy       host process
#   33044  facade      host process (Kafka wire, plaintext + SASL/PLAIN)
#
# ## Teardown
#
# `docker rm -f` on THIS rig's two container names and nothing else. Host
# processes only by the pids recorded at spawn. Never a pid resolved from a
# port, and never `pkill -f` on a shared binary path: both of those reach into
# somebody else's stack on a machine that is running more than one.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"

PXDB_PORT="${PXDB_PORT:-33040}"
CELLPG_PORT="${CELLPG_PORT:-33041}"
BROKER_PORT="${BROKER_PORT:-33042}"
PROXY_PORT="${PROXY_PORT:-33043}"
KAFKA_PORT="${KAFKA_PORT:-33044}"
PARTITIONS="${PARTITIONS:-4}"

PXDB="${PXDB:-qkc-t2-pxdb}"
CELLPG="${CELLPG:-qkc-t2-cellpg}"

# The proxy's own upstream budget, echoed to the suite so the long-poll margin
# is MEASURED against the number in force rather than against the default
# somebody assumed. 35000 is proxy/src/config.rs's own default.
UPSTREAM_TIMEOUT_MS="${QUEEN_PROXY_UPSTREAM_TIMEOUT_MS:-35000}"

KEEP=0
GO_TEST_ARGS=()
for arg in "$@"; do
  case "$arg" in
    --keep) KEEP=1;;
    -h|--help) sed -n '2,70p' "$0"; exit 0;;
    *) GO_TEST_ARGS+=("$arg");;
  esac
done

RUN_DIR="$(mktemp -d -t queen-kafka-cloud.XXXXXX)"
BROKER_LOG="$RUN_DIR/broker.log"
PROXY_LOG="$RUN_DIR/proxy.log"
FACADE_LOG="$RUN_DIR/facade.log"
BROKER_PID=""
PROXY_PID=""
FACADE_PID=""

say() { printf '\n=== %s\n' "$*"; }
die() { echo "$*" >&2; exit 1; }

cleanup() {
  local code=$?
  if [ "$KEEP" = 1 ]; then
    echo
    echo "--keep: the stack is still up."
    echo "  pxdb    : container $PXDB on 127.0.0.1:$PXDB_PORT"
    echo "  cell PG : container $CELLPG on 127.0.0.1:$CELLPG_PORT"
    echo "  broker  : pid ${BROKER_PID:-none}  http://127.0.0.1:$BROKER_PORT  log $BROKER_LOG"
    echo "  proxy   : pid ${PROXY_PID:-none}   http://127.0.0.1:$PROXY_PORT   log $PROXY_LOG"
    echo "  facade  : pid ${FACADE_PID:-none}  127.0.0.1:$KAFKA_PORT          log $FACADE_LOG"
    echo "  env     : $RUN_DIR/rig.env  (source it, then ./run.sh)"
    echo "  tear down: kill ${FACADE_PID:-} ${PROXY_PID:-} ${BROKER_PID:-}; docker rm -f $PXDB $CELLPG"
    exit $code
  fi
  say "tearing down"
  # By the pids recorded AT SPAWN, in reverse dependency order. Never a pid
  # resolved from a port, and never a pattern match on a binary path.
  for pid in "$FACADE_PID" "$PROXY_PID" "$BROKER_PID"; do
    [ -n "$pid" ] && kill "$pid" 2>/dev/null
  done
  sleep 1
  for pid in "$FACADE_PID" "$PROXY_PID" "$BROKER_PID"; do
    [ -n "$pid" ] && kill -9 "$pid" 2>/dev/null
  done
  docker rm -f "$PXDB" "$CELLPG" >/dev/null 2>&1
  echo "logs kept at $RUN_DIR"
  exit $code
}
trap cleanup EXIT INT TERM

command -v docker >/dev/null || die "docker not found"
command -v go >/dev/null     || die "go not found"
command -v cargo >/dev/null  || die "cargo not found"
command -v openssl >/dev/null || die "openssl not found"

px()   { docker exec -i "$PXDB"   psql -qtA -U postgres -d queen_proxy "$@"; }
cell() { docker exec -i "$CELLPG" psql -qtA -U postgres -d queen "$@"; }

# pg_isready is NOT enough: the postgres entrypoint runs initdb against a
# temporary server and RESTARTS it, and pg_isready answers "ready" during that
# window. A broker that connects there dies while applying its schema. Require a
# real query on the TARGET database, which only succeeds once the final server
# is serving. (proxy/scripts/dev-cell.sh learned this the hard way.)
wait_pg() { # container db
  for _ in $(seq 1 90); do
    docker exec "$1" psql -U postgres -d "$2" -qtAc 'SELECT 1' >/dev/null 2>&1 && return 0
    sleep 0.5
  done
  echo "PG $1 (db $2) never answered a query" >&2
  docker logs "$1" 2>&1 | tail -20 >&2
  return 1
}

# ------------------------------------------------------------------ databases
say "postgres: pxdb :$PXDB_PORT ($PXDB) and cell PG :$CELLPG_PORT ($CELLPG)"
docker rm -f "$PXDB" "$CELLPG" >/dev/null 2>&1
docker run -d --name "$PXDB" -p "$PXDB_PORT":5432 \
  -e POSTGRES_PASSWORD=postgres -e POSTGRES_USER=postgres -e POSTGRES_DB=queen_proxy \
  -e PGDATA=/var/lib/postgresql/data/pgdata \
  --tmpfs /var/lib/postgresql/data:rw,size=1g postgres:16 >/dev/null || exit 1
docker run -d --name "$CELLPG" -p "$CELLPG_PORT":5432 \
  -e POSTGRES_PASSWORD=postgres -e POSTGRES_USER=postgres -e POSTGRES_DB=queen \
  -e PGDATA=/var/lib/postgresql/data/pgdata \
  --tmpfs /var/lib/postgresql/data:rw,size=2g postgres:16 -c max_connections=300 >/dev/null || exit 1
wait_pg "$PXDB" queen_proxy || exit 1
wait_pg "$CELLPG" queen || exit 1

# --------------------------------------------------------------------- builds
say "building broker, proxy and facade (debug)"
( cd "$REPO_ROOT/server" && cargo build ) || exit 1
( cd "$REPO_ROOT/proxy" && cargo build ) || exit 1
( cd "$REPO_ROOT/queen-kafka" && cargo build ) || exit 1

# --------------------------------------------------------------------- broker
# QUEEN_TENANCY_HEADER=true is what makes the broker honour the X-Queen-Tenant
# the proxy injects; without it two clusters share one namespace and every
# isolation assertion below is vacuous.
# QUEEN_KV_TRUSTED_PROXY=true is the interlock that lets the proxy speak for a
# tenant on the KV surface, which is where every Kafka offset lives.
# RETAINED_BYTES_INTERVAL_MS: the storage lane is TEN MINUTES by default, so the
# blocked-tenant scenario would never see a byte measured inside one run.
say "broker on 127.0.0.1:$BROKER_PORT (tenancy header ON)"
PG_HOST=127.0.0.1 PG_PORT="$CELLPG_PORT" PG_USER=postgres PG_PASSWORD=postgres \
PG_DATABASE=queen PORT="$BROKER_PORT" QUEEN_BIND_ADDR=127.0.0.1 \
QUEEN_APPLY_SCHEMA=true DB_POOL_SIZE=24 LOG_LEVEL=info \
QUEEN_TENANCY_HEADER=true QUEEN_KV_TRUSTED_PROXY=true \
RETAINED_BYTES_INTERVAL_MS=5000 \
QUEEN_KV_QUOTA_REFRESH_MS=2000 \
  "$REPO_ROOT/server/target/debug/queen" > "$BROKER_LOG" 2>&1 &
BROKER_PID=$!
for _ in $(seq 1 120); do
  curl -fsS -m 2 "http://127.0.0.1:$BROKER_PORT/health" >/dev/null 2>&1 && break
  kill -0 "$BROKER_PID" 2>/dev/null || { echo "the broker died at boot:" >&2; tail -30 "$BROKER_LOG" >&2; exit 1; }
  sleep 1
done
curl -fsS -m 2 "http://127.0.0.1:$BROKER_PORT/health" >/dev/null 2>&1 || {
  echo "the broker never answered /health" >&2; tail -30 "$BROKER_LOG" >&2; exit 1; }

# ---------------------------------------------------------------------- proxy
# QUEEN_PROXY_ENFORCE=true, not the default shadow mode: a rig that only shadows
# proves nothing about a 403 or a 429, and three of the assertions below are
# exactly those.
# QUEEN_PROXY_SHARED_HOSTS=127.0.0.1: the facade's calls carry `Host:
# 127.0.0.1:33043`, and `Config::is_shared_host` strips the port before
# matching, so this is the one entry that makes the whole listener
# credential-routed. NO QUEEN_PROXY_DEFAULT_CLUSTER: a default cluster that
# absorbed the host would route BOTH tenants to one cluster and every isolation
# assertion would pass for the wrong reason.
say "proxy on 127.0.0.1:$PROXY_PORT (enforce ON, 127.0.0.1 is a shared host)"
QUEEN_PROXY_PORT="$PROXY_PORT" QUEEN_PROXY_BIND_ADDR=127.0.0.1 \
PXDB_HOST=127.0.0.1 PXDB_PORT="$PXDB_PORT" PXDB_USER=postgres \
PXDB_PASSWORD=postgres PXDB_DB=queen_proxy \
QUEEN_PROXY_SPOOL_DIR="$RUN_DIR/spool" \
QUEEN_PROXY_JWT_SECRET=cloud-rig-only-hs256-secret \
QUEEN_PROXY_ENFORCE=true \
QUEEN_PROXY_OPERATOR_ENABLED=false \
QUEEN_PROXY_SHARED_HOSTS=127.0.0.1 \
QUEEN_PROXY_RECONCILE_MS=2000 \
QUEEN_PROXY_UPSTREAM_TIMEOUT_MS="$UPSTREAM_TIMEOUT_MS" \
LOG_LEVEL=debug \
  "$REPO_ROOT/proxy/target/debug/queen-proxy" > "$PROXY_LOG" 2>&1 &
PROXY_PID=$!
for _ in $(seq 1 60); do
  curl -fsS -m 2 "http://127.0.0.1:$PROXY_PORT/healthz" >/dev/null 2>&1 && break
  kill -0 "$PROXY_PID" 2>/dev/null || { echo "the proxy died at boot:" >&2; tail -30 "$PROXY_LOG" >&2; exit 1; }
  sleep 0.5
done
curl -fsS -m 2 "http://127.0.0.1:$PROXY_PORT/healthz" >/dev/null 2>&1 || {
  echo "the proxy never answered /healthz" >&2; tail -30 "$PROXY_LOG" >&2; exit 1; }

# --------------------------------------------------------------- control plane
# The proxy applied its own migrations at boot, so the schema is there now and
# not before. The cell row has to name THIS broker, which the shared dev seed
# cannot know, so it is written here rather than in seed-dev.sql.
say "seeding the control plane (two tenants, six credentials)"
px >/dev/null <<SQL || die "could not seed pxdb"
DO \$\$
DECLARE cell uuid;
BEGIN
  SELECT id INTO cell FROM queen_proxy.cells WHERE slug='qkc-t2';
  IF cell IS NULL THEN
    INSERT INTO queen_proxy.cells (slug, region, base_url, class, cell_secret)
    VALUES ('qkc-t2','local','http://127.0.0.1:$BROKER_PORT','shared',NULL);
  ELSE
    UPDATE queen_proxy.cells SET base_url='http://127.0.0.1:$BROKER_PORT' WHERE id=cell;
  END IF;
END \$\$;
SQL

ensure_cluster() { # tenant-slug tenant-name cluster-slug plan -> cluster uuid
  px >/dev/null <<SQL
DO \$\$
DECLARE t uuid; cell uuid;
BEGIN
  SELECT id INTO cell FROM queen_proxy.cells WHERE slug='qkc-t2';
  IF NOT EXISTS (SELECT 1 FROM queen_proxy.clusters WHERE slug='$3') THEN
    SELECT id INTO t FROM queen_proxy.tenants WHERE slug='$1';
    IF t IS NULL THEN t := queen_proxy.create_tenant('$1','$2'); END IF;
    PERFORM queen_proxy.create_cluster(t,'$3','$4',cell);
  END IF;
END \$\$;
SQL
  px -c "SELECT id FROM queen_proxy.clusters WHERE slug='$3'"
}

# A fresh random key per run: nothing outside this rig can come to depend on a
# value, and the plaintext never leaves $RUN_DIR.
issue_key() { # cluster-uuid label scopes-sql -> plaintext
  local k h
  k="qk_dev_$(openssl rand -base64 48 | tr '+/' '-_' | tr -d '=' | cut -c1-43)"
  h=$(printf '%s' "$k" | shasum -a 256 | cut -d' ' -f1)
  px -c "SELECT queen_proxy.issue_api_key('$1'::uuid,'qkc-$2','$h',ARRAY[$3])" >/dev/null
  printf '%s' "$k"
}

# Both tenants on the `free` plan, whose `features` is '{}' — so NEITHER has the
# `kv` feature. That is not an accident of the fixture, it is the point: before
# T3's qk-prefix rule an offset commit on this plan was 403 `not_in_your_plan`,
# and assertion 11 is that it no longer is.
CID_A=$(ensure_cluster qkc-tenant-a 'QKC Tenant A' qkca free)
CID_B=$(ensure_cluster qkc-tenant-b 'QKC Tenant B' qkcb free)
[ -n "$CID_A" ] && [ -n "$CID_B" ] || die "could not create the two clusters"

# Wide enough that the suite's own traffic never trips the free plan's 5 req/s
# while it is proving something else. The rate-cap scenario parks its own tiny
# override and takes it off again.
WIDE='{"max_req_per_sec":500,"req_burst":2000,"max_msgs_per_sec":5000,"msgs_burst":20000,"max_queues":500}'
px -c "SELECT queen_proxy.set_limit_override('$CID_A'::uuid, '$WIDE'::jsonb)" >/dev/null
px -c "SELECT queen_proxy.set_limit_override('$CID_B'::uuid, '$WIDE'::jsonb)" >/dev/null

# The scope matrix of the design's §0.4, which is the single most valuable fact
# a Cloud Kafka user needs: `read` is not optional. Every Kafka client issues
# Metadata before anything else, and Metadata IS the queue listing, which is a
# `read` route. A key without it is refused at SASL and never opens a
# connection at all.
KEY_A_FULL=$(issue_key    "$CID_A" a-full    "'produce','consume','admin','read'")
KEY_A_CONSUME=$(issue_key "$CID_A" a-consume "'consume','read'")
KEY_A_PRODUCE=$(issue_key "$CID_A" a-produce "'produce','read'")
KEY_A_TXN=$(issue_key     "$CID_A" a-txn     "'produce','consume','read'")
KEY_A_NOREAD=$(issue_key  "$CID_A" a-noread  "'consume'")
# The second credential of tenant A, for the /auth/me identity assertion: two
# keys of ONE cluster must file their groups under ONE scope.
KEY_A_FULL2=$(issue_key   "$CID_A" a-full2   "'produce','consume','admin','read'")
KEY_B_FULL=$(issue_key    "$CID_B" b-full    "'produce','consume','admin','read'")

# The broker's OWN kv gate, which is a second one and easy to miss. With
# QUEEN_TENANCY_HEADER=true the broker derives `kv_require_grant`, and the
# ABSENCE of a queen.kv_quota row is a DENIAL, not a permission
# (server/src/config.rs, server/src/quota.rs). Without these two rows every
# offset commit is 403 at the BROKER, past everything the proxy decided.
TEN_A=$(px -c "SELECT broker_tenant_uuid FROM queen_proxy.clusters WHERE id='$CID_A'")
TEN_B=$(px -c "SELECT broker_tenant_uuid FROM queen_proxy.clusters WHERE id='$CID_B'")
for t in "$TEN_A" "$TEN_B"; do
  cell -c "INSERT INTO queen.kv_quota (tenant_id, enabled) VALUES ('$t', TRUE)
           ON CONFLICT (tenant_id) DO UPDATE SET enabled = TRUE" >/dev/null \
    || die "could not grant kv to broker tenant $t"
done

# --------------------------------------------------------------------- facade
# THE line this whole rig exists for: QUEEN_URL is the PROXY.
say "queen-kafka on 127.0.0.1:$KAFKA_PORT (QUEEN_URL = the proxy, SASL/PLAIN)"
QUEEN_URL="http://127.0.0.1:$PROXY_PORT" \
QUEEN_KAFKA_ADDR="127.0.0.1:$KAFKA_PORT" \
QUEEN_KAFKA_ADVERTISED_ADDR="127.0.0.1:$KAFKA_PORT" \
QUEEN_KAFKA_DEFAULT_PARTITIONS="$PARTITIONS" \
QUEEN_KAFKA_SASL=plain \
LOG_LEVEL="${FACADE_LOG_LEVEL:-debug}" \
  "$REPO_ROOT/queen-kafka/target/debug/queen-kafka" > "$FACADE_LOG" 2>&1 &
FACADE_PID=$!
for _ in $(seq 1 60); do
  nc -z 127.0.0.1 "$KAFKA_PORT" >/dev/null 2>&1 && break
  kill -0 "$FACADE_PID" 2>/dev/null || { echo "the facade died at boot:" >&2; tail -30 "$FACADE_LOG" >&2; exit 1; }
  sleep 0.5
done
nc -z 127.0.0.1 "$KAFKA_PORT" >/dev/null 2>&1 || {
  echo "the facade never listened on $KAFKA_PORT" >&2; tail -30 "$FACADE_LOG" >&2; exit 1; }

# ------------------------------------------------------- the kv grant, LANDED
# The grant rows above are rows in a table; what a commit meets is the broker's
# in-memory snapshot of them, refreshed on a timer (QUEEN_KV_QUOTA_REFRESH_MS,
# set to 2s above and THIRTY SECONDS by default). Until that refresh has run,
# every offset commit is 403 `feature_gated` at the broker and a suite that
# started here would report a Cloud gate that is really a rig that jumped the
# gun. So the readiness check is the real thing: one `qk:` put per tenant,
# through the proxy, until it is taken.
say "waiting for the broker to see the kv grants"
probe_kv() { # key -> 0 when a qk: put is accepted
  local code
  code=$(curl -s -o /dev/null -w '%{http_code}' -m 5 -X POST \
    -H "Authorization: Bearer $1" -H 'Content-Type: application/json' \
    -d '{"operations":[{"op":"put","ns":"queen-kafka","key":"qk:node:rig-probe","value":{"probe":true},"ttlSeconds":60}]}' \
    "http://127.0.0.1:$PROXY_PORT/api/v1/kv")
  [ "$code" = "200" ]
}
for key in "$KEY_A_FULL" "$KEY_B_FULL"; do
  ok=0
  for _ in $(seq 1 40); do
    probe_kv "$key" && { ok=1; break; }
    sleep 0.5
  done
  [ "$ok" = 1 ] || die "the broker never accepted a qk: put for one of the tenants; \
check queen.kv_quota in $CELLPG and the proxy's classification of POST /api/v1/kv"
done
# ...and take the probe rows back out, so a listing assertion never sees them.
for key in "$KEY_A_FULL" "$KEY_B_FULL"; do
  curl -s -o /dev/null -m 5 -X POST -H "Authorization: Bearer $key" \
    -H 'Content-Type: application/json' \
    -d '{"operations":[{"op":"delete","ns":"queen-kafka","key":"qk:node:rig-probe"}]}' \
    "http://127.0.0.1:$PROXY_PORT/api/v1/kv"
done

# ------------------------------------------------------------- psql shims
# The Go suite reads pxdb (metering rows, limit overrides) and the cell PG, and
# it does so through these two one-line scripts rather than through a Postgres
# driver: the suite's whole dependency set is franz-go, and it stays that way.
cat > "$RUN_DIR/px.sh" <<SHIM
#!/usr/bin/env bash
exec docker exec -i "$PXDB" psql -qtA -U postgres -d queen_proxy -c "\$1"
SHIM
cat > "$RUN_DIR/cell.sh" <<SHIM
#!/usr/bin/env bash
exec docker exec -i "$CELLPG" psql -qtA -U postgres -d queen -c "\$1"
SHIM
chmod +x "$RUN_DIR/px.sh" "$RUN_DIR/cell.sh"

# --------------------------------------------------------------------- env
cat > "$RUN_DIR/rig.env" <<ENV
export QKC_BOOTSTRAP=127.0.0.1:$KAFKA_PORT
export QKC_PROXY_URL=http://127.0.0.1:$PROXY_PORT
export QKC_BROKER_URL=http://127.0.0.1:$BROKER_PORT
export QKC_PSQL=$RUN_DIR/px.sh
export QKC_PSQL_CELL=$RUN_DIR/cell.sh
export QKC_CLUSTER_A=$CID_A
export QKC_CLUSTER_B=$CID_B
export QKC_TENANT_A=$TEN_A
export QKC_TENANT_B=$TEN_B
export QKC_KEY_A_FULL=$KEY_A_FULL
export QKC_KEY_A_FULL2=$KEY_A_FULL2
export QKC_KEY_A_CONSUME=$KEY_A_CONSUME
export QKC_KEY_A_PRODUCE=$KEY_A_PRODUCE
export QKC_KEY_A_TXN=$KEY_A_TXN
export QKC_KEY_A_NOREAD=$KEY_A_NOREAD
export QKC_KEY_B_FULL=$KEY_B_FULL
export QKC_FACADE_LOG=$FACADE_LOG
export QKC_PROXY_LOG=$PROXY_LOG
export QKC_PARTITIONS=$PARTITIONS
export QKC_UPSTREAM_TIMEOUT_MS=$UPSTREAM_TIMEOUT_MS
ENV

say "the suite"
# shellcheck disable=SC1090
. "$RUN_DIR/rig.env"
"$SCRIPT_DIR/run.sh" "${GO_TEST_ARGS[@]+"${GO_TEST_ARGS[@]}"}"
RESULT=$?

# A panic on any of the three is a failure even when every assertion passed.
for log in "$BROKER_LOG" "$PROXY_LOG" "$FACADE_LOG"; do
  [ -s "$log" ] || continue
  if grep -qi 'panic' "$log"; then
    echo "PANIC in $log:" >&2
    grep -i -m5 -A5 'panic' "$log" >&2
    RESULT=1
  fi
done

say "result: $([ $RESULT -eq 0 ] && echo PASS || echo FAIL)"
exit $RESULT
