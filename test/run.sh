#!/usr/bin/env bash
#
# Queen test matrix. Builds the broker image + per-language runner images, then
# runs each (suite x topology) as an isolated docker-compose project in parallel
# and prints a pass/fail matrix.
#
#   suites:  js go py cli cpp   -> run on `single` and `ha` stacks
#            rust               -> 50 in-process unit tests, no stack (`unit`)
#            mesh               -> HA mesh assertion, `ha` stack only
#
# Each stack = its own Postgres + broker(s) + runner on a private network, so
# suites never collide (they share test-queue name patterns) and nothing binds
# host ports. Postgres runs on tmpfs and is thrown away after each run.
#
# Usage:
#   test/run.sh                        # full matrix
#   test/run.sh --suite js,go          # subset of suites
#   test/run.sh --suite py --topo single
#   test/run.sh --no-build-broker      # reuse an existing queen-seg:test
#   test/run.sh -j 3                   # cap parallelism (default: 4)
#   test/run.sh --keep                 # leave stacks up for debugging
#
# Env: QUEEN_TEST_MAX_PARALLEL overrides -j.
set -uo pipefail

ALL_SUITES="js go py cli cpp rust mesh"
CLIENT_SUITES="js go py cli cpp"

SUITES="$ALL_SUITES"
TOPOS="single ha"
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
    -h|--help) sed -n '2,40p' "$0"; exit 0;;
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
  echo ">> build queen-seg:test (broker)"
  ( cd "$REPO_ROOT" && DOCKER_BUILDKIT=1 docker build -q -f server/Dockerfile -t queen-seg:test server ) \
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
    want_topo single && add_job "$s" single
    want_topo ha     && add_job "$s" ha
  elif [ "$s" = "mesh" ]; then
    add_job mesh ha            # mesh is inherently an HA-stack check
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
    compose="$COMPOSE_DIR/docker-compose.${topo}.yml"
    QUEEN_RUNNER_IMAGE="queen-test-runner-$suite" \
      docker compose -p "$proj" -f "$compose" up \
        --abort-on-container-exit --exit-code-from runner >"$log" 2>&1
    code=$?
    if [ "$KEEP" = 0 ]; then
      QUEEN_RUNNER_IMAGE="queen-test-runner-$suite" \
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
echo "================= Queen test matrix ================="
printf "%-8s %-10s %-10s %-10s\n" "suite" "single" "ha" "unit"
overall=0
cell() {  # suite topo
  f="$LOGDIR/$1-$2.code"
  [ -f "$f" ] || { printf "%-10s" "-"; return; }
  c="$(cat "$f")"; d="$(cat "$LOGDIR/$1-$2.dur" 2>/dev/null || echo '?')"
  if [ "$c" = 0 ]; then printf "%-10s" "PASS ${d}s"; else printf "%-10s" "FAIL:$c"; overall=1; fi
}
for s in js go py cli cpp rust mesh; do
  want_suite "$s" || continue
  printf "%-8s " "$s"
  cell "$s" single; cell "$s" ha; cell "$s" unit
  printf "\n"
done
echo "====================================================="

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
