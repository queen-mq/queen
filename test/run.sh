#!/usr/bin/env bash
#
# Queen test matrix. Builds the broker image + per-language runner images, then
# runs each (suite x topology) as an isolated docker-compose project in parallel
# and prints a pass/fail matrix.
#
#   suites:  js go py cli cpp laravel rust-client
#                               -> run on `single`, `ha` and `tenanted` stacks
#            rust               -> in-process broker unit tests, no stack (`unit`)
#            mesh               -> HA mesh assertion, `ha` stack only
#            tenancy            -> two-tenant isolation, `ha-tenanted` stack only
#            http               -> the kv/timer wire with no SDK in the way
#                                  (PLAN_KV_TIMERS.md §10.2), `single` only
#            conflation         -> the PLAN_CONFLATION.md §7.3 end-to-end
#                                  scenarios at the raw HTTP wire (no SDK),
#                                  `single` only
#            s3sink             -> the PLAN_S3_SINK.md §9 end-to-end scenarios:
#                                  the queen-s3 connector, a versitygw gateway
#                                  and a DuckDB reader, all inside the runner
#                                  image, against the stack's broker.
#                                  `single` + `tenanted` (the parity rule).
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
#   test/run.sh --suite http           # every kv/timer route, no SDK in the way
#   test/run.sh --suite conflation     # PLAN_CONFLATION §7.3 e2e, no SDK in the way
#   test/run.sh --suite s3sink         # PLAN_S3_SINK §9 e2e: sink + versitygw + DuckDB
#   test/run.sh --no-build-broker      # reuse an existing queen:test
#   test/run.sh -j 3                   # cap parallelism (default: 4)
#   test/run.sh --keep                 # leave stacks up for debugging
#
# Env: QUEEN_TEST_MAX_PARALLEL overrides -j.
set -uo pipefail

ALL_SUITES="js go py cli cpp laravel rust-client rust mesh tenancy http conflation s3sink"
CLIENT_SUITES="js go py cli cpp laravel rust-client"
# Suites whose `single` and `tenanted` lanes must agree (the tenancy parity gate
# at the bottom). Every client suite, plus s3sink: the sink reads through fetch
# and partitions/changed and commits through kv, and all three take the tenant
# from the broker's own header handling — so a divergence between the flag-off
# and default-tenant lanes is exactly the regression the gate exists to catch.
PARITY_SUITES="$CLIENT_SUITES s3sink"

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
    -h|--help) sed -n '2,49p' "$0"; exit 0;;
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
  # Every runner builds from the repo root; the heavy trees are denylisted per
  # runner by a sidecar test/runners/<suite>/Dockerfile.dockerignore. (`rust`
  # used to build from server/, but the shared crates/queen-protocol has to be
  # in its context now — see test/runners/rust/Dockerfile.)
  suite="$1"; df="test/runners/$suite/Dockerfile"; ctx="."
  echo ">> build queen-test-runner-$suite"
  ( cd "$REPO_ROOT" && DOCKER_BUILDKIT=1 docker build -q -f "$df" -t "queen-test-runner-$suite" "$ctx" ) \
    || { echo "!! build failed: $suite (see above)"; return 1; }
}

needs_broker=0
for s in $SUITES; do want_suite "$s" && [ "$s" != "rust" ] && needs_broker=1; done

if [ "$BUILD_BROKER" = 1 ] && [ "$needs_broker" = 1 ]; then
  echo ">> build queen:test (broker)"
  # Context is the repo root, not server/: the broker takes queen-protocol by
  # the relative path ../crates/queen-protocol, which has to be inside it.
  ( cd "$REPO_ROOT" && DOCKER_BUILDKIT=1 docker build -q -f server/Dockerfile -t queen:test . ) \
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
  elif [ "$s" = "http" ]; then
    # `single` and nothing else, and the reason is a product rule rather than a
    # cost: with QUEEN_TENANCY_HEADER on, `kv_require_grant` follows it, so the
    # ABSENCE of a per-tenant quota row is a 403 `feature_gated` (§9.4). Granting
    # the default tenant would mean writing to the database, and this suite has
    # no database access on purpose — every assertion it makes is at the HTTP
    # wire. The wire shape it pins is topology-independent anyway: the same
    # bodies, the same routes and the same result layout on any stack.
    add_job http single
  elif [ "$s" = "conflation" ]; then
    # PLAN_CONFLATION.md §7.3: the conflation e2e scenarios, raw HTTP with no
    # SDK in the path (the per-SDK halves belong to §7.2 and land with the
    # clients). `single` only, and like `http` that is a scope statement rather
    # than a cost cut: every §7.3 scenario is a (partition, consumer-group)
    # semantic — the §1.3 guarantee, the retry budget, the stored policy, depth
    # — none of which changes shape across the mesh, and the ha lanes of the
    # client suites already exercise the transport.
    add_job conflation single
  elif [ "$s" = "s3sink" ]; then
    # PLAN_S3_SINK.md §9. `single` and `tenanted` only: the sink is a client of
    # three routes whose tenant handling is the subject of the parity rule, and
    # the mesh adds nothing to it — window commits are per queue and go through
    # one KV key pair, so a second broker exercises the transport and not the
    # protocol. The runner carries the sink binary, the S3 gateway and the
    # reader, so the stack is the same pg + broker every other suite runs.
    want_topo single   && add_job s3sink single
    want_topo tenanted && add_job s3sink tenanted
  elif [ "$s" = "rust" ]; then
    add_job rust unit          # no stack
  fi
done
[ -n "$JOBS" ] || { echo "no jobs selected" >&2; exit 2; }

# --- run one job ------------------------------------------------------------
run_job() {
  suite="$1"; topo="$2"
  base="$LOGDIR/${suite}-${topo}"; log="$base.log"; diag="$base.diag.log"
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
    # No per-suite kv/timer knob here any more. The http suite's whole subject IS
    # the kv/timer surface, and it used to need the two boot flags pinned on for
    # its lane; those flags are gone and every broker this harness starts has both
    # surfaces, so the suite needs nothing special and no other lane can lose them
    # by accident.
    QUEEN_RUNNER_IMAGE="queen-test-runner-$suite" QUEEN_TEST_TENANCY="$tflag" \
      docker compose -p "$proj" -f "$compose" up \
        --abort-on-container-exit --exit-code-from runner >"$log" 2>&1
    code=$?
    if [ "$code" != 0 ]; then
      # Capture the runner before teardown appends container shutdown noise to
      # the combined log. This keeps the actual assertion or timeout visible.
      QUEEN_RUNNER_IMAGE="queen-test-runner-$suite" QUEEN_TEST_TENANCY="$tflag" \
        docker compose -p "$proj" -f "$compose" logs \
          --no-color --tail=200 runner >"$diag" 2>&1 || true
    fi
    if [ "$KEEP" = 0 ]; then
      QUEEN_RUNNER_IMAGE="queen-test-runner-$suite" QUEEN_TEST_TENANCY="$tflag" \
        docker compose -p "$proj" -f "$compose" down -v --remove-orphans >>"$log" 2>&1 || true
    fi
  fi
  echo "$code" >"$base.code"
  echo "$(( $(date +%s) - start ))" >"$base.dur"
  if [ "$code" = 0 ]; then
    echo ">> PASS ${suite}/${topo} ($(cat "$base.dur")s)"
  else
    echo ">> FAIL ${suite}/${topo} rc=$code ($(cat "$base.dur")s)"
    # Surface diagnostics immediately. A later topology or the outer CI
    # deadline must not hide the only useful failure output in a temp file.
    diagnostic_log="$log"
    [ -s "$diag" ] && diagnostic_log="$diag"
    echo "----- immediate diagnostics ${suite}/${topo}: $diagnostic_log -----"
    tail -n 200 "$diagnostic_log" 2>/dev/null
  fi
}

# --- rolling-window parallelism (portable to bash 3.2: no `wait -n`) --------
echo ">> running $(echo "$JOBS" | wc -w | tr -d ' ') jobs, up to $MAXP in parallel"
for job in $JOBS; do
  suite="${job%%|*}"; topo="${job##*|}"
  while [ "$(jobs -rp | wc -l | tr -d ' ')" -ge "$MAXP" ]; do sleep 0.3; done
  run_job "$suite" "$topo" &
done
wait

# --- matrix report ----------------------------------------------------------
echo
echo "========================= Queen test matrix ========================="
printf "%-12s %-11s %-11s %-11s %-13s %-8s\n" \
  "suite" "single" "ha" "tenanted" "ha-tenanted" "unit"
overall=0
cell() {  # suite topo width
  f="$LOGDIR/$1-$2.code"; w="$3"
  [ -f "$f" ] || { printf "%-${w}s" "-"; return; }
  c="$(cat "$f")"; d="$(cat "$LOGDIR/$1-$2.dur" 2>/dev/null || echo '?')"
  if [ "$c" = 0 ]; then printf "%-${w}s" "PASS ${d}s"; else printf "%-${w}s" "FAIL:$c"; overall=1; fi
}
# Driven by ALL_SUITES rather than a second hardcoded list: a suite added above
# and forgotten here would run, be gated on, and never appear in the report.
for s in $ALL_SUITES; do
  want_suite "$s" || continue
  printf "%-12s " "$s"
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
  # pytest: "=== 140 passed, 1 failed, 91 errors in 12.34s ===". The broad
  # character class also swallows the "in 313" duration tail ("in" is lowercase
  # letters), which made parity flap when one lane merely ran slower — chop it.
  # cargo: one "test result: ok. 24 passed; 0 failed; ..." per test binary.
  # Join them in order — counts only, no durations, so it cannot flap. This
  # MUST come before the pytest pattern below: that one also matches the
  # "24 passed" inside a cargo line (it stops at the ';', which is outside its
  # character class), and would report only the last binary's tally.
  t="$(grep -aoE 'test result: [a-zA-Z]+\. [0-9]+ passed; [0-9]+ failed' "$f" 2>/dev/null \
       | sed 's/test result: //' | paste -sd, -)"
  [ -n "$t" ] && { echo "$t"; return; }
  t="$(grep -aoE '[0-9]+ passed[0-9a-z, ]*' "$f" 2>/dev/null | tail -1 \
       | sed -e 's/ in [0-9]*$//' -e 's/ *$//')"
  [ -n "$t" ] && { echo "$t"; return; }
  echo ""
}

parity_checked=0; parity_bad=0
for s in $PARITY_SUITES; do
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
    diagnostic_log="$LOGDIR/$suite-$topo.log"
    [ -s "$LOGDIR/$suite-$topo.diag.log" ] && diagnostic_log="$LOGDIR/$suite-$topo.diag.log"
    echo
    echo "----- diagnostics $suite/$topo (rc=$c) : $diagnostic_log -----"
    tail -n 200 "$diagnostic_log" 2>/dev/null
  fi
done

echo
[ "$overall" = 0 ] && echo "ALL GREEN" || echo "SOME FAILURES (logs in $LOGDIR)"
exit "$overall"
