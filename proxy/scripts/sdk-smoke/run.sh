#!/usr/bin/env bash
# ============================================================================
# sdk-smoke -- the three published Queen clients (JS / Go / Python) driven
# against a LIVE queen_proxy + broker, with the proxy ENFORCING.
#
# Everything else in this tree proves the proxy with curl, and proves the
# client 429 contract against canned local HTTP servers. This script closes
# that gap: real SDK -> real proxy -> real broker, per language:
#
#   1. roundtrip  push -> pop -> ack with api-key auth
#   2. isolation  two clusters, same queue name, each sees only its own
#   3. ratelimit  a live 429 off the real token bucket + transparent recovery
#   4. blocked /  storage quota tripped via set_limit_override -> terminal
#      unblocked  403 storage_quota_exceeded -> cleared -> push accepted again
#
# Usage: scripts/sdk-smoke/run.sh
#   SDK_SMOKE_SKIP_UP=1   reuse the cell that is already running (it must have
#                         been started with QUEEN_PROXY_ENFORCE=true)
#   SDK_SMOKE_LANGS="js"  restrict to a subset of languages
# ============================================================================
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
PROXY_DIR="$(cd "$HERE/../.." && pwd)"
ROOT="$(cd "$PROXY_DIR/.." && pwd)"
RUN_DIR="$PROXY_DIR/.devcell"
PROXY_LOG="$RUN_DIR/proxy.log"

PXPG=qpx-pg
PROXY_URL="http://127.0.0.1:6711"
# Dedicated clusters, not the seeded `dev` / isolation-smoke's `two`: this smoke
# parks a rate-limit override and a 64-byte storage quota on cluster A, and
# scripts/isolation-smoke.sh parks its own overrides on dev/two/pro1/pro2/rl.
# Two smokes sharing a cluster overwrite each other's limit_overrides (one whole
# jsonb per cluster), which silently turns a limit test into a no-op. Own
# clusters keep the two independent on the same cell.
HOST_A=sdka
HOST_B=sdkb
KEY_A=""
KEY_B=""
RUN_ID="$(date +%Y%m%d%H%M%S)"
LANGS="${SDK_SMOKE_LANGS:-js go py}"

RESULTS="$(mktemp -t sdksmoke)"
trap 'rm -f "$RESULTS"' EXIT

say()  { printf '%s\n' "$*"; }
head1() { say ""; say "=== $* ==="; }
die()  { say "FATAL: $*"; exit 2; }

psqlq() { docker exec -i "$PXPG" psql -qtA -U postgres -d queen_proxy "$@" | tr -d '[:space:]'; }
strip_ansi() { sed $'s/\x1b\\[[0-9;]*m//g'; }
# Enforced (non-shadow) limit denials the proxy has logged so far. The shadow
# variant of the same line says "limit exceeded (shadow)", so excluding it also
# proves the proxy really is enforcing.
count_denies() { strip_ansi <"$PROXY_LOG" | grep 'limit exceeded' | grep -vc 'shadow'; }
# 1 when the proxy currently serving this cell booted enforcing. The LAST boot
# line is the only one that counts: the log survives a restart, so grepping the
# whole file happily matches an enforcing proxy that has since been replaced by
# a shadow-mode one (which then allows every request while logging
# "limit exceeded (shadow)" -- a rate-limit test that passes vacuously).
enforcing_now() { strip_ansi <"$PROXY_LOG" | grep 'queen-proxy up' | tail -1 | grep -c 'enforce=true'; }

record() { printf '%s %s %s\n' "$1" "$2" "$3" >>"$RESULTS"; }

# ---------------------------------------------------------------------------
# 0. dependency preflight -- a language that cannot run here must say so
#    loudly, never be skipped quietly.
# ---------------------------------------------------------------------------
head1 "dependency preflight"
command -v docker >/dev/null || die "docker not found"

WANT_JS=no; WANT_GO=no; WANT_PY=no
PYBIN=""
for l in $LANGS; do
  case "$l" in
    js)
      if ! command -v node >/dev/null; then
        die "language 'js' requested but node is not installed"
      fi
      if [ ! -d "$ROOT/clients/client-js/node_modules/uuid" ] || [ ! -d "$ROOT/clients/client-js/node_modules/undici" ]; then
        die "language 'js' requested but clients/client-js deps are missing -- run: (cd $ROOT/clients/client-js && npm install)"
      fi
      say "js: $(node --version), client deps present"
      WANT_JS=yes
      ;;
    go)
      command -v go >/dev/null || die "language 'go' requested but the go toolchain is not installed"
      say "go: $(go version | cut -d' ' -f3)"
      WANT_GO=yes
      ;;
    py)
      for cand in "$ROOT/clients/client-py/venv/bin/python" python3 python; do
        if command -v "$cand" >/dev/null 2>&1 || [ -x "$cand" ]; then
          if "$cand" -c 'import httpx' >/dev/null 2>&1; then PYBIN="$cand"; break; fi
        fi
      done
      [ -n "$PYBIN" ] || die "language 'py' requested but no python with httpx found -- tried clients/client-py/venv, python3, python"
      say "py: $("$PYBIN" -c 'import sys,httpx;print(sys.version.split()[0], "httpx", httpx.__version__)') ($PYBIN)"
      WANT_PY=yes
      ;;
    *) die "unknown language '$l' in SDK_SMOKE_LANGS" ;;
  esac
done

if [ "$WANT_GO" = yes ]; then
  # GOWORK=off: this module is deliberately outside the repo go.work (it is a
  # test harness, not a shipped module); its own `replace` points at the
  # in-tree client.
  ( cd "$HERE/go" && GOWORK=off GOPROXY=off go build -o sdk-smoke . ) || die "go build failed"
  say "go: built $HERE/go/sdk-smoke"
fi

# ---------------------------------------------------------------------------
# 1. cell, ENFORCING
# ---------------------------------------------------------------------------
if [ "${SDK_SMOKE_SKIP_UP:-0}" = "1" ]; then
  head1 "reusing the running dev cell (SDK_SMOKE_SKIP_UP=1)"
  "$PROXY_DIR/scripts/dev-cell.sh" status
else
  head1 "bringing the dev cell up ENFORCING"
  QUEEN_PROXY_ENFORCE=true "$PROXY_DIR/scripts/dev-cell.sh" up || die "dev-cell.sh up failed"
fi

curl -sf "$PROXY_URL/healthz" >/dev/null || die "proxy not healthy at $PROXY_URL"
[ "$(enforcing_now)" = "1" ] || \
  die "proxy is NOT enforcing -- restart the cell with QUEEN_PROXY_ENFORCE=true"
say "proxy up and enforcing (enforce=true in $PROXY_LOG)"

# ---------------------------------------------------------------------------
# 2. provision the second cluster through the control plane (migration 004)
# ---------------------------------------------------------------------------
head1 "provisioning clusters '$HOST_A' + '$HOST_B' via queen_proxy.bootstrap_tenant"
CELL_ID="$(psqlq -c "SELECT id FROM queen_proxy.cells WHERE slug='local'")"
[ -n "$CELL_ID" ] || die "cell 'local' not found in pxdb (was seed-dev.sql applied?)"

bootstrap() { # tenant-slug tenant-name cluster-slug admin-email -> plaintext api key
  # A per-run key name: bootstrap_tenant is idempotent on the slugs and only
  # returns a plaintext key for a key it actually issues, so a re-run against an
  # existing pxdb still gets a usable credential back.
  psqlq -c "SELECT queen_proxy.bootstrap_tenant(
              '$1', '$2', '$3', 'free', '$CELL_ID'::uuid, '$4', NULL,
              'sdk-smoke-$RUN_ID')->>'api_key'"
}

KEY_A="$(bootstrap sdk-tenant-a 'SDK Smoke Tenant A' "$HOST_A" "sdk-a@localhost")"
KEY_B="$(bootstrap sdk-tenant-b 'SDK Smoke Tenant B' "$HOST_B" "sdk-b@localhost")"
[ -n "$KEY_A" ] || die "bootstrap_tenant returned no api key for $HOST_A"
[ -n "$KEY_B" ] || die "bootstrap_tenant returned no api key for $HOST_B"
CID_A="$(psqlq -c "SELECT id FROM queen_proxy.clusters WHERE slug='$HOST_A'")"
CID_B="$(psqlq -c "SELECT id FROM queen_proxy.clusters WHERE slug='$HOST_B'")"
say "cluster $HOST_A = $CID_A (key ${KEY_A:0:11}...)"
say "cluster $HOST_B = $CID_B (key ${KEY_B:0:11}...)"

set_ovr() { # cluster-uuid  json | NULL
  local v="$2"
  [ "$v" = "NULL" ] || v="'$v'::jsonb"
  psqlq -c "SELECT queen_proxy.set_limit_override('$1'::uuid, $v)" >/dev/null
}

# set_limit_override replaces the whole jsonb, so every override below carries
# the queue headroom too. The free plan's max_queues (20) is not what this smoke
# is testing, and a proxy process that has served several runs has all of their
# queues in its in-process registry.
BASE_OVR='{"max_queues": 500}'
set_ovr "$CID_A" "$BASE_OVR"
set_ovr "$CID_B" "$BASE_OVR"
docker exec -i "$PXPG" psql -U postgres -d queen_proxy -c \
  "SELECT c.slug, p.code AS plan, p.max_req_per_sec, p.req_burst, p.max_msgs_per_sec,
          p.msgs_burst, p.max_retained_bytes, c.limit_overrides
     FROM queen_proxy.clusters c JOIN queen_proxy.plans p ON p.id = c.plan_id
    WHERE c.slug IN ('$HOST_A','$HOST_B')"

# ---------------------------------------------------------------------------
# shared program environment
# ---------------------------------------------------------------------------
export SDK_SMOKE_URL="$PROXY_URL"
export SDK_SMOKE_HOST_A="$HOST_A"
export SDK_SMOKE_KEY_A="$KEY_A"
export SDK_SMOKE_HOST_B="$HOST_B"
export SDK_SMOKE_KEY_B="$KEY_B"
export SDK_SMOKE_RUN_ID="$RUN_ID"
export SDK_SMOKE_DEADLINE_MS="${SDK_SMOKE_DEADLINE_MS:-150000}"
export SDK_SMOKE_BURN_MAX="${SDK_SMOKE_BURN_MAX:-400}"
export SDK_SMOKE_RECOVER_N="${SDK_SMOKE_RECOVER_N:-20}"

want() { # lang -> 0 if requested
  case "$1" in
    js) [ "$WANT_JS" = yes ] ;;
    go) [ "$WANT_GO" = yes ] ;;
    py) [ "$WANT_PY" = yes ] ;;
  esac
}

run_phase() { # lang phase -> program exit code
  local lang=$1 phase=$2 rc
  export SDK_SMOKE_QUEUE="sdk-$lang-$RUN_ID"
  export SDK_SMOKE_ISO_QUEUE="sdk-iso-$lang-$RUN_ID"
  say ""
  say "-- $lang / $phase"
  case "$lang" in
    js) node "$HERE/js/smoke.mjs" "$phase" ;;
    go) "$HERE/go/sdk-smoke" "$phase" ;;
    py) "$PYBIN" "$HERE/py/smoke.py" "$phase" ;;
  esac
  rc=$?
  return $rc
}

# ---------------------------------------------------------------------------
# 3. phases 1 + 2: round trip and tenant isolation
# ---------------------------------------------------------------------------
head1 "phase 1/2: round trip + tenant isolation through the proxy"
for lang in js go py; do
  want "$lang" || continue
  run_phase "$lang" roundtrip; record "$lang" roundtrip "$?"
  run_phase "$lang" isolation; record "$lang" isolation "$?"
done

# ---------------------------------------------------------------------------
# 4. phase 3: live 429 off the real limiter, plus a log-delta guard so a run
#    that never reached the cap cannot pass vacuously.
# ---------------------------------------------------------------------------
# A pinned, deliberately tight request bucket for this phase: the free plan's
# rate (5 req/s) with a smaller burst, so the phase is short and deterministic
# instead of depending on what the pxdb happens to carry. Restored below.
RL_OVR='{"max_queues": 500, "max_req_per_sec": 5, "req_burst": 10}'
head1 "phase 3: live 429 + recovery (pinned bucket: $RL_OVR)"
set_ovr "$CID_A" "$RL_OVR"
for lang in js go py; do
  want "$lang" || continue
  # Re-checked per language: a cell restarted in shadow mode mid-run would
  # answer every request 200 and log "limit exceeded (shadow)" instead.
  [ "$(enforcing_now)" = "1" ] || die "proxy stopped enforcing mid-run (restarted in shadow mode?)"
  before="$(count_denies)"
  run_phase "$lang" ratelimit
  rc=$?
  after="$(count_denies)"
  delta=$((after - before))
  if [ "$delta" -gt 0 ]; then
    say "  ok  - proxy logged $delta ENFORCED limit denial(s) during this phase"
  else
    say "  FAIL- proxy logged no enforced limit denial: the run never reached the cap"
    rc=1
  fi
  record "$lang" ratelimit "$rc"
done
set_ovr "$CID_A" "$BASE_OVR"
say ""
say "rate-limit override lifted on cluster $HOST_A"

# ---------------------------------------------------------------------------
# 5. phase 4: terminal 403 (storage quota) and recovery. The enforcement loop
#    is asynchronous (broker stats refresh -> proxy reconcile -> quota pump),
#    so the programs poll to a deadline; the override is set once for all
#    languages and cleared once, not per language.
# ---------------------------------------------------------------------------
head1 "phase 4a: storage quota -> terminal 403 storage_quota_exceeded"
say "setting max_retained_bytes=64 on cluster $HOST_A"
set_ovr "$CID_A" '{"max_queues": 500, "max_retained_bytes": 64}'
for lang in js go py; do
  want "$lang" || continue
  run_phase "$lang" blocked; record "$lang" blocked "$?"
done

head1 "phase 4b: clearing the override -> push accepted again"
set_ovr "$CID_A" "$BASE_OVR"
say "storage-quota override cleared on cluster $HOST_A"
for lang in js go py; do
  want "$lang" || continue
  run_phase "$lang" unblocked; record "$lang" unblocked "$?"
done

# ---------------------------------------------------------------------------
# 6. per-language summary
# ---------------------------------------------------------------------------
head1 "SDK smoke summary (run $RUN_ID)"
printf '%-6s %-12s %-12s %-12s %-12s %-12s %s\n' \
  lang roundtrip isolation ratelimit blocked unblocked RESULT
TOTAL_FAIL=0
for lang in js go py; do
  want "$lang" || continue
  line=""
  langfail=0
  for phase in roundtrip isolation ratelimit blocked unblocked; do
    rc="$(awk -v l="$lang" -v p="$phase" '$1==l && $2==p {print $3}' "$RESULTS")"
    if [ -z "$rc" ]; then
      cell="-"
    elif [ "$rc" = "0" ]; then
      cell="PASS"
    else
      cell="FAIL($rc)"; langfail=1
    fi
    line="$line$(printf '%-12s ' "$cell")"
  done
  if [ "$langfail" = "0" ]; then verdict="PASS"; else verdict="FAIL"; TOTAL_FAIL=1; fi
  printf '%-6s %s%s\n' "$lang" "$line" "$verdict"
done

say ""
if [ "$TOTAL_FAIL" = "0" ]; then
  say "== sdk-smoke: ALL LANGUAGES PASS =="
else
  say "== sdk-smoke: FAILURES ABOVE =="
fi
exit "$TOTAL_FAIL"
