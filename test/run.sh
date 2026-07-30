#!/usr/bin/env bash
#
# Queen test matrix. Builds the broker image + per-language runner images, then
# runs each (suite x topology) as an isolated docker-compose project in parallel
# and prints a pass/fail matrix.
#
#   suites:  js go py cli cpp   -> run on `single`, `ha` and `tenanted` stacks
#            rust               -> 50 in-process unit tests, no stack (`unit`)
#            mesh               -> HA mesh assertion, `ha` stack only
#            tenancy            -> two-tenant isolation, `ha-tenanted` stack only
#
#   topologies:
#     single       1 PG + 1 broker                       (QUEEN_TENANCY_HEADER off)
#     ha           1 PG + queen-a/queen-b mesh pair      (QUEEN_TENANCY_HEADER off)
#     tenanted     same as `single` but the broker runs QUEEN_TENANCY_HEADER=true
#                  while the client suites send NO x-queen-tenant header — the
#                  DEFAULT-TENANT path, which is what every cloud cell serves.
#                  Its results MUST be identical to `single`; run.sh reports a
#                  TENANCY PARITY line and fails the run on any divergence.
#     ha-tenanted  the HA pair with QUEEN_TENANCY_HEADER=true — the substrate for
#                  the `tenancy` suite (two tenants, one queue name, mesh in play).
#
# Each stack = its own Postgres + broker(s) + runner on a private network, so
# suites never collide (they share test-queue name patterns) and nothing binds
# host ports. Postgres runs on tmpfs and is thrown away after each run.
#
# Usage:
#   test/run.sh                        # full matrix
#   test/run.sh --suite js,go          # subset of suites
#   test/run.sh --suite py --topo single
#   test/run.sh --suite js --topo tenanted     # flag-ON default-tenant lane
#   test/run.sh --suite tenancy        # two-tenant isolation over the HA pair
#   test/run.sh --no-build-broker      # reuse an existing queen:test
#   test/run.sh -j 3                   # cap parallelism (default: 4)
#   test/run.sh --keep                 # leave stacks up for debugging
#
# Env: QUEEN_TEST_MAX_PARALLEL overrides -j.
set -uo pipefail

ALL_SUITES="js go py cli cpp rust mesh tenancy"
CLIENT_SUITES="js go py cli cpp"

SUITES="$ALL_SUITES"
TOPOS="single ha tenanted"
BUILD_BROKER=1
BUILD_RUNNERS=1
KEEP=0
MAXP="${QUEEN_TEST_MAX_PARALLEL:-4}"

# --- locate repo root (this script lives in <repo>/test) --------------------
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
COMPOSE_DIR="$SCRIPT_DIR/compose"

# --- args -------------------------------------------------------------------
while [ $# -gt 0 ]; do
  case "$1" in
    --suite)  SUITES="$(echo "$2" | tr ',' ' ')"; shift 2;;
    --topo)   TOPOS="$(echo "$2" | tr ',' ' ')"; shift 2;;
    --no-build-broker)  BUILD_BROKER=0; shift;;
    --no-build)         BUILD_RUNNERS=0; BUILD_BROKER=0; shift;;
    -j)       MAXP="$2"; shift 2;;
    --keep)   KEEP=1; shift;;
    -h|--help) sed -n '2,37p' "$0"; exit 0;;
    *) echo "unknown arg: $1" >&2; exit 2;;
  esac
done

command -v docker >/dev/null || { echo "docker not found" >&2; exit 2; }
docker compose version >/dev/null 2>&1 || { echo "docker compose v2 required" >&2; exit 2; }

LOGDIR="$(mktemp -d -t queen-test.XXXXXX)"
echo ">> logs: $LOGDIR"

is_client() { case " $CLIENT_SUITES " in *" $1 "*) return 0;; *) return 1;; esac; }
want_suite() { case " $SUITES " in *" $1 "*) return 0;; *) return 1;; esac; }
want_topo()  { case " $TOPOS " in *" $1 "*) return 0;; *) return 1;; esac; }

# A topology names a compose file + the QUEEN_TENANCY_HEADER value the brokers
# start with. `tenanted`/`ha-tenanted` reuse the single/ha compose files: the
# only difference IS the flag, and duplicating the stack definition would let the
# two lanes drift apart, which would defeat the identical-results invariant.
compose_for() {
  case "$1" in
    single|tenanted)  echo "$COMPOSE_DIR/docker-compose.single.yml";;
    ha|ha-tenanted)   echo "$COMPOSE_DIR/docker-compose.ha.yml";;
    *) echo ""; return 1;;
  esac
}
tenancy_for() { case "$1" in tenanted|ha-tenanted) echo true;; *) echo false;; esac; }

# --- build images -----------------------------------------------------------
build_runner() {
  suite="$1"; df="test/runners/$suite/Dockerfile"; ctx="."
  [ "$suite" = "rust" ] && { df="test/runners/rust/Dockerfile"; ctx="server"; }
  echo ">> build queen-test-runner-$suite"
  ( cd "$REPO_ROOT" && DOCKER_BUILDKIT=1 docker build -q -f "$df" -t "queen-test-runner-$suite" "$ctx" ) \
    || { echo "!! build failed: $suite (see above)"; return 1; }
}

needs_broker=0
for s in $SUITES; do want_suite "$s" && [ "$s" != "rust" ] && needs_broker=1; done

if [ "$BUILD_BROKER" = 1 ] && [ "$needs_broker" = 1 ]; then
  echo ">> build queen:test (broker)"
  ( cd "$REPO_ROOT" && DOCKER_BUILDKIT=1 docker build -q -f server/Dockerfile -t queen:test server ) \
    || { echo "!! broker image build failed"; exit 1; }
fi

if [ "$BUILD_RUNNERS" = 1 ]; then
  for s in $ALL_SUITES; do want_suite "$s" && { build_runner "$s" || exit 1; }; done
fi

# --- assemble job list ("suite topo") --------------------------------------
JOBS=""
add_job() { JOBS="$JOBS $1|$2"; }
for s in $SUITES; do
  want_suite "$s" || continue
  if is_client "$s"; then
    want_topo single   && add_job "$s" single
    want_topo ha       && add_job "$s" ha
    want_topo tenanted && add_job "$s" tenanted
  elif [ "$s" = "mesh" ]; then
    add_job mesh ha            # mesh is inherently an HA-stack check
  elif [ "$s" = "tenancy" ]; then
    add_job tenancy ha-tenanted  # two tenants over the mesh pair, flag ON
  elif [ "$s" = "rust" ]; then
    add_job rust unit          # no stack
  fi
done
[ -n "$JOBS" ] || { echo "no jobs selected" >&2; exit 2; }

# --- run one job ------------------------------------------------------------
run_job() {
  suite="$1"; topo="$2"
  base="$LOGDIR/${suite}-${topo}"; log="$base.log"
  start=$(date +%s)
  if [ "$topo" = "unit" ]; then
    docker run --rm "queen-test-runner-$suite" >"$log" 2>&1
    code=$?
  else
    proj="queen-test-${suite}-${topo}"
    compose="$(compose_for "$topo")"
    if [ -z "$compose" ]; then
      echo "unknown topology: $topo (want single|ha|tenanted|ha-tenanted)" >"$log"
      echo 2 >"$base.code"; echo 0 >"$base.dur"
      echo ">> FAIL ${suite}/${topo} rc=2 (unknown topology)"; return
    fi
    tflag="$(tenancy_for "$topo")"
    QUEEN_RUNNER_IMAGE="queen-test-runner-$suite" QUEEN_TEST_TENANCY="$tflag" \
      docker compose -p "$proj" -f "$compose" up \
        --abort-on-container-exit --exit-code-from runner >"$log" 2>&1
    code=$?
    if [ "$KEEP" = 0 ]; then
      QUEEN_RUNNER_IMAGE="queen-test-runner-$suite" QUEEN_TEST_TENANCY="$tflag" \
        docker compose -p "$proj" -f "$compose" down -v --remove-orphans >>"$log" 2>&1 || true
    fi
  fi
  echo "$code" >"$base.code"
  echo "$(( $(date +%s) - start ))" >"$base.dur"
  if [ "$code" = 0 ]; then echo ">> PASS ${suite}/${topo} ($(cat "$base.dur")s)"
  else echo ">> FAIL ${suite}/${topo} rc=$code ($(cat "$base.dur")s)"; fi
}

# --- rolling-window parallelism (portable to bash 3.2: no `wait -n`) --------
echo ">> running $(echo $JOBS | wc -w | tr -d ' ') jobs, up to $MAXP in parallel"
for job in $JOBS; do
  suite="${job%%|*}"; topo="${job##*|}"
  while [ "$(jobs -rp | wc -l | tr -d ' ')" -ge "$MAXP" ]; do sleep 0.3; done
  run_job "$suite" "$topo" &
done
wait

# --- matrix report ----------------------------------------------------------
echo
echo "========================= Queen test matrix ========================="
printf "%-8s %-11s %-11s %-11s %-13s %-8s\n" \
  "suite" "single" "ha" "tenanted" "ha-tenanted" "unit"
overall=0
cell() {  # suite topo width
  f="$LOGDIR/$1-$2.code"; w="$3"
  [ -f "$f" ] || { printf "%-${w}s" "-"; return; }
  c="$(cat "$f")"; d="$(cat "$LOGDIR/$1-$2.dur" 2>/dev/null || echo '?')"
  if [ "$c" = 0 ]; then printf "%-${w}s" "PASS ${d}s"; else printf "%-${w}s" "FAIL:$c"; overall=1; fi
}
for s in js go py cli cpp rust mesh tenancy; do
  want_suite "$s" || continue
  printf "%-8s " "$s"
  cell "$s" single 11; cell "$s" ha 11; cell "$s" tenanted 11
  cell "$s" ha-tenanted 13; cell "$s" unit 8
  printf "\n"
done
echo "===================================================================="

# --- tenancy parity gate ----------------------------------------------------
# The `tenanted` lane runs the SAME suite against a broker with native tenant
# scoping ON and no tenant header — the default-tenant path. Its outcome must be
# byte-for-byte the same verdict as the flag-off `single` lane; a divergence
# means the flag changed behaviour for an untenanted client, which is a
# regression regardless of which side is green.
#
# The exit code is the hard gate. It is also coarse (99/112 and 100/112 are both
# rc=1), so where a suite prints a machine-readable tally we compare that too —
# counts only, never durations, so it cannot flap.
tally() {  # logfile -> comparable pass tally, or "" if the suite prints none
  local f="$1" t
  # JS: "Overall Results: 100/112 tests passed, 12/112 tests failed" (one per bucket)
  t="$(grep -aoE 'Overall Results: [0-9]+/[0-9]+ tests passed' "$f" 2>/dev/null \
       | sed -e 's/Overall Results: //' -e 's/ tests passed//' | paste -sd, -)"
  [ -n "$t" ] && { echo "$t"; return; }
  # pytest: "=== 140 passed, 1 failed, 91 errors in 12.34s ==="
  t="$(grep -aoE '[0-9]+ passed[0-9a-z, ]*' "$f" 2>/dev/null | tail -1 \
       | sed -e 's/ *$//')"
  [ -n "$t" ] && { echo "$t"; return; }
  echo ""
}

parity_checked=0; parity_bad=0
for s in $CLIENT_SUITES; do
  want_suite "$s" || continue
  fo="$LOGDIR/$s-single.code"; fn="$LOGDIR/$s-tenanted.code"
  [ -f "$fo" ] && [ -f "$fn" ] || continue
  parity_checked=$((parity_checked+1))
  co="$(cat "$fo")"; cn="$(cat "$fn")"
  to="$(tally "$LOGDIR/$s-single.log")"; tn="$(tally "$LOGDIR/$s-tenanted.log")"
  diverged=0
  [ "$co" != "$cn" ] && diverged=1
  [ -n "$to" ] && [ -n "$tn" ] && [ "$to" != "$tn" ] && diverged=1
  if [ "$diverged" = 1 ]; then
    parity_bad=$((parity_bad+1))
    echo "!! TENANCY DIVERGENCE $s: single rc=$co [${to:-no tally}] vs tenanted rc=$cn [${tn:-no tally}]"
    echo "   flag-off log: $LOGDIR/$s-single.log"
    echo "   flag-on  log: $LOGDIR/$s-tenanted.log"
  else
    echo "   parity $s: rc=$co both lanes${to:+, tally $to both lanes}"
  fi
done
if [ "$parity_checked" -gt 0 ]; then
  if [ "$parity_bad" = 0 ]; then
    echo "TENANCY PARITY: OK ($parity_checked suite(s) identical with the flag on and off)"
  else
    echo "TENANCY PARITY: FAILED ($parity_bad of $parity_checked suite(s) diverged)"
    overall=1
  fi
fi

# surface failing logs
for job in $JOBS; do
  suite="${job%%|*}"; topo="${job##*|}"
  c="$(cat "$LOGDIR/$suite-$topo.code" 2>/dev/null || echo 1)"
  if [ "$c" != 0 ]; then
    echo
    echo "----- tail $suite/$topo (rc=$c) : $LOGDIR/$suite-$topo.log -----"
    tail -n 30 "$LOGDIR/$suite-$topo.log" 2>/dev/null
  fi
done

echo
[ "$overall" = 0 ] && echo "ALL GREEN" || echo "SOME FAILURES (logs in $LOGDIR)"
exit "$overall"
