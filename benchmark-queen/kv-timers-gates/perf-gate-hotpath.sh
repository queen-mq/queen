#!/usr/bin/env bash
#
# PERF GATE — PLAN_KV_TIMERS §15, row "Perf gate", closes F4.
#
#   "bundle senza kv e senza timers, prima e dopo la patch, payload byte-identico: [...] CPU per
#    messaggio entro l'1%, che e' la metrica che ha catturato la regressione push del seg v2 (la
#    latenza e' troppo rumorosa a questi ordini di grandezza). Piu': pg_stat_statements non deve
#    mostrare NESSUNA query nuova con calls > 0."
#
# Two questions, one run each side:
#
#   Q1  Does a bundle that carries no `kv` and no `timers` cost more CPU after the patch?
#       Gate: median CPU microseconds per message, after vs before, within ±1%.
#   Q2  Did the patch add a query that EXECUTES on a bundle that uses neither feature?
#       Gate: no statement in pg_stat_statements with calls > 0 that the before run did not have.
#
# Q2 is not a weaker restatement of Q1. §6.3 promises zero added statements, zero added plan
# nodes and zero added locks when the arrays are absent, and the way that promise dies is a
# `FROM jsonb_array_elements(COALESCE(p->'kv','[]'))` folded into a UNION with the pushes: one
# extra Function Scan and one nested loop inside a statement that runs on EVERY bundle. At the
# rates this thing runs at, that is well under 1% of CPU and completely invisible to Q1 — and it
# is a permanent tax on people who will never turn the feature on. Q2 sees it immediately, and
# names it.
#
# USAGE
#   # build the two images from the two commits, then:
#   GATE_IMAGE=queen:gate-before ./perf-gate-hotpath.sh capture before
#   GATE_IMAGE=queen:gate-after  ./perf-gate-hotpath.sh capture after
#   ./perf-gate-hotpath.sh compare before after
#
# KNOBS (env)
#   GATE_IMAGE        broker image for this capture           (default queen:test)
#   GATE_REPS         repetitions per capture                 (default 3)
#   GATE_BUNDLES      wire bundles per repetition             (default 20000)
#   GATE_ITEMS        messages per bundle                     (default 20)
#   GATE_WORKERS      concurrent closed loops                 (default 8)
#   GATE_PARTITIONS   destination partitions                  (default 16)
#   GATE_TOLERANCE_PCT       the §15 number                   (default 1.0)
#   GATE_RATE_TOLERANCE_PCT  throughput divergence that voids the comparison (default 10)
#   GATE_ALLOW_NEW    extended regex waiving an IDENTIFIED background statement in Q2
#   GATE_BROKER_CPUS / GATE_PG_CPUS / GATE_SYNC_COMMIT / GATE_POOL_SIZE  see compose.yml
#
# GATE_REPS below 3 makes the gate report UNRESOLVED on purpose: a single repetition has a
# spread of 0%, which then satisfies the resolution check by accident.
#
# The capture is destructive to its own stack only: project `kvtgate`, its own volume, its own
# port. It never touches :5432.

. "$(cd "$(dirname "$0")" && pwd)/lib.sh"

REPS="${GATE_REPS:-3}"
BUNDLES="${GATE_BUNDLES:-20000}"
ITEMS="${GATE_ITEMS:-20}"
WORKERS="${GATE_WORKERS:-8}"
PARTITIONS="${GATE_PARTITIONS:-16}"
WARMUP_BUNDLES="${GATE_WARMUP_BUNDLES:-500}"
IDLE_S="${GATE_IDLE_S:-15}"
TOLERANCE_PCT="${GATE_TOLERANCE_PCT:-1.0}"

usage() {
  sed -n '2,47p' "$0" | sed 's/^# \{0,1\}//'
  exit 2
}

# --------------------------------------------------------------------------- capture

capture() {
  local label="${1:-}"
  [ -n "$label" ] || usage
  need docker; need node; need curl; need awk

  local dir; dir="$(label_dir "$label")"
  rm -rf "$dir"; mkdir -p "$dir"
  save_env "$dir"

  step "capture '$label' — image ${GATE_IMAGE:-queen:test}, $REPS reps of $BUNDLES bundles x $ITEMS msgs"

  local rep
  for rep in $(seq 1 "$REPS"); do
    info "rep $rep/$REPS: fresh stack"
    stack_up
    pgss_assert_available

    # Warm-up, outside every measurement: provisions the queue and its partitions, gets each
    # prepared statement planned once, and lets the fusion hysteresis settle. Provisioning is a
    # once-per-queue cost and would otherwise land entirely in whichever run happened to be
    # first.
    loadgen --url "$BROKER_URL" --mode bundle --tag warm \
            --bundles "$WARMUP_BUNDLES" --items "$ITEMS" --workers "$WORKERS" \
            --partitions "$PARTITIONS" >"$dir/warmup-$rep.json" 2>>"$dir/loadgen.err" || \
      die "warm-up failed, see $dir/loadgen.err and $dir/warmup-$rep.json"

    # Idle window: what the two containers burn with no client at all. Not the gate, but the
    # context for it — if the idle rate moved, the CPU-per-message delta may be background work
    # rather than wire work, and the sweeper gate is the place to chase it.
    local ib0 ip0 ib1 ip1
    ib0="$(cpu_usec broker)"; ip0="$(cpu_usec pg)"
    sleep "$IDLE_S"
    ib1="$(cpu_usec broker)"; ip1="$(cpu_usec pg)"
    local idle_mcpu
    idle_mcpu="$(awk -v d="$(( (ib1-ib0) + (ip1-ip0) ))" -v s="$IDLE_S" 'BEGIN{printf "%.1f", d/(s*1000.0)}')"

    # Statements that ran while the broker was IDLE. These are the background loops — retention,
    # the txns purge, stats, reconcile — and they are recorded separately because they fire on
    # their own cadences, not on ours. Folding them into this label's known set is what keeps Q2
    # from reporting "a new query!" when all that happened is that a 60-second loop landed inside
    # one window and outside the other. It costs nothing and removes the only false positive the
    # check has ever produced on this rig.
    pgss_dump >"$dir/pgss-idle-$rep.tsv"

    # The measured window. Reset the statement statistics HERE so the schema apply, the warm-up
    # and the idle window are not in the sample.
    pgss_reset
    local b0 p0 b1 p1
    b0="$(cpu_usec broker)"; p0="$(cpu_usec pg)"
    loadgen --url "$BROKER_URL" --mode bundle --tag gate \
            --bundles "$BUNDLES" --items "$ITEMS" --workers "$WORKERS" \
            --partitions "$PARTITIONS" >"$dir/load-$rep.json" 2>>"$dir/loadgen.err" \
      || die "load failed (see $dir/load-$rep.json). A gate run with errors is not a slower run,
       it is a different run: the failed bundles did not do the work the denominator counts."
    b1="$(cpu_usec broker)"; p1="$(cpu_usec pg)"

    pgss_dump >"$dir/pgss-$rep.tsv"

    local msgs cpu_us cpu_per_msg net_per_msg rate elapsed sum
    msgs="$(awk -F'[,:]' '{for(i=1;i<=NF;i++) if($i ~ /"messages"/) print $(i+1)}' "$dir/load-$rep.json" | tr -d ' }')"
    rate="$(awk -F'[,:]' '{for(i=1;i<=NF;i++) if($i ~ /"msg_per_s"/) print $(i+1)}' "$dir/load-$rep.json" | tr -d ' }')"
    elapsed="$(awk -F'[,:]' '{for(i=1;i<=NF;i++) if($i ~ /"elapsed_ms"/) print $(i+1)}' "$dir/load-$rep.json" | tr -d ' }')"
    sum="$(awk -F'[,:]' '{for(i=1;i<=NF;i++) if($i ~ /"body_checksum"/) print $(i+1)}' "$dir/load-$rep.json" | tr -d ' }')"
    cpu_us=$(( (b1-b0) + (p1-p0) ))
    cpu_per_msg="$(awk -v c="$cpu_us" -v m="$msgs" 'BEGIN{printf "%.3f", c/m}')"
    net_per_msg="$(awk -v c="$cpu_us" -v m="$msgs" -v i="$idle_mcpu" -v e="$elapsed" \
                     'BEGIN{printf "%.3f", (c - i*e)/m}')"

    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
      "$rep" "$cpu_per_msg" "$net_per_msg" "$rate" "$msgs" "$idle_mcpu" "$sum" >>"$dir/reps.tsv"
    info "rep $rep: ${cpu_per_msg} us/msg (net ${net_per_msg}), ${rate} msg/s, idle ${idle_mcpu} mCPU, body_checksum $sum"

    stack_down
  done

  # The union of statements over all reps is the baseline set. Union, not intersection: a
  # background loop with a slow cadence (retention, stats reconcile) may fire in one repetition
  # and not another, and calling that "a new query" in the other capture would be a false
  # positive on the one check that must stay believable.
  cat "$dir"/pgss-*.tsv | cut -f2- | sort -u >"$dir/queries.txt"
  # The load-window set alone, which is what the "new statement" comparison is ABOUT: a
  # statement that runs while bundles are flowing. queries.txt (load + idle) is the tolerant
  # baseline it is compared against.
  cat "$dir"/pgss-[0-9]*.tsv | cut -f2- | sort -u >"$dir/queries-load.txt"

  step "capture '$label' done"
  info "median CPU/msg : $(cut -f2 "$dir/reps.tsv" | median) us"
  info "spread         : $(cut -f2 "$dir/reps.tsv" >"$dir/.c" && spread_pct "$dir/.c")% of median"
  info "distinct statements with calls>0: $(wc -l <"$dir/queries.txt" | tr -d ' ')"
  info "results in $dir"
  rm -f "$dir/.c"
}

# --------------------------------------------------------------------------- compare

compare() {
  local a="${1:-}" b="${2:-}"
  [ -n "$a" ] && [ -n "$b" ] || usage
  local da db; da="$(label_dir "$a")"; db="$(label_dir "$b")"
  [ -f "$da/reps.tsv" ] || die "no capture '$a' in $RESULTS_DIR"
  [ -f "$db/reps.tsv" ] || die "no capture '$b' in $RESULTS_DIR"

  local fail=0

  step "Q0 — are the two captures comparable at all?"
  local sa sb
  sa="$(cut -f7 "$da/reps.tsv" | sort -u | tr '\n' ' ')"
  sb="$(cut -f7 "$db/reps.tsv" | sort -u | tr '\n' ' ')"
  info "body checksum  $a: $sa"
  info "body checksum  $b: $sb"
  if [ "$sa" != "$sb" ]; then
    printf '  \033[31mVOID\033[0m the two captures did not send the same bytes.\n'
    printf '       §15 asks for a byte-identical payload; a CPU-per-message comparison between\n'
    printf '       two different workloads is not a measurement. Fix the load parameters first.\n'
    fail=1
  fi

  cut -f2 "$da/reps.tsv" >"$da/.cpm"; cut -f2 "$db/reps.tsv" >"$db/.cpm"
  local ma mb spa spb delta
  ma="$(median <"$da/.cpm")"; mb="$(median <"$db/.cpm")"
  spa="$(spread_pct "$da/.cpm")"; spb="$(spread_pct "$db/.cpm")"
  delta="$(pct "$ma" "$mb")"

  local ra rb rdelta
  ra="$(cut -f4 "$da/reps.tsv" | median)"; rb="$(cut -f4 "$db/reps.tsv" | median)"
  rdelta="$(pct "$ra" "$rb")"

  step "Q1 — CPU per message (broker + postgres, cgroup counters)"
  printf '  %-10s median %8s us/msg   spread %6s%%   %6s msg/s\n' "$a" "$ma" "$spa" "$ra"
  printf '  %-10s median %8s us/msg   spread %6s%%   %6s msg/s\n' "$b" "$mb" "$spb" "$rb"
  printf '  delta      %s%%   (gate: +/- %s%%)\n' "$delta" "$TOLERANCE_PCT"

  # A gate is only as good as its resolution, and there are three separate ways this one can be
  # unable to answer. Each is reported as UNRESOLVED rather than as a pass, because a green line
  # that means "we could not tell" is the most expensive output this script could produce.
  local na nb resolved=1
  na="$(wc -l <"$da/reps.tsv" | tr -d ' ')"; nb="$(wc -l <"$db/reps.tsv" | tr -d ' ')"
  if [ "$na" -lt 3 ] || [ "$nb" -lt 3 ]; then
    printf '  \033[31mUNRESOLVED\033[0m %s and %s reps: with fewer than 3 the spread is not a\n' "$na" "$nb"
    printf '       spread (one rep reports 0%%, which then silently satisfies the check below).\n'
    printf '       GATE_REPS=3 is the floor for a verdict.\n'
    resolved=0
  elif awk -v x="$spa" -v y="$spb" -v t="$TOLERANCE_PCT" 'BEGIN{exit !(x>t || y>t)}'; then
    printf '  \033[31mUNRESOLVED\033[0m within-capture spread exceeds the tolerance: this rig\n'
    printf '       cannot resolve %s%%. Raise GATE_REPS and/or GATE_BUNDLES and quiesce the host\n' "$TOLERANCE_PCT"
    printf '       — a laptop running builds in another window will not produce this number.\n'
    resolved=0
  fi
  # CPU per message is a point on a cost curve, not a constant: batching, fusion hysteresis and
  # commit amortization all move it with load. Two runs at materially different throughput are
  # two different points, and their ratio is not a regression measurement.
  if awk -v d="$rdelta" -v t="${GATE_RATE_TOLERANCE_PCT:-10}" 'BEGIN{d=(d<0?-d:d); exit !(d>t)}'; then
    printf '  \033[31mUNRESOLVED\033[0m throughput moved %s%% between the captures, so the two\n' "$rdelta"
    printf '       runs sat at different points on the cost curve. Fix the load level first\n'
    printf '       (same host state, same GATE_WORKERS), then re-run.\n'
    resolved=0
  fi
  if [ "$resolved" -eq 0 ]; then
    fail=1
  elif awk -v d="$delta" -v t="$TOLERANCE_PCT" 'BEGIN{exit !(d>t)}'; then
    printf '  \033[31mFAIL\033[0m CPU per message regressed by more than %s%%.\n' "$TOLERANCE_PCT"
    printf '       This is the metric that caught the seg v2 push regression; latency at these\n'
    printf '       rates is too noisy to have caught it, and it is too noisy to clear it now.\n'
    fail=1
  else
    printf '  \033[32mPASS\033[0m\n'
  fi

  step "Q2 — statements executed during the measured window"
  # after's LOAD-window statements minus everything before ever ran (load or idle).
  comm -13 "$da/queries.txt" "$db/queries-load.txt" >"$db/.new" || true

  # Two classes, because they are two different findings and collapsing them is how a real
  # check gets waived wholesale after its third false positive.
  #
  #   feature  a statement naming this feature's own surfaces ran on a bundle that carries
  #            neither array. That is §6.3 broken, full stop, and no cadence explains it.
  #   other    some other statement appeared. Nearly always a background loop whose period
  #            straddled one window and not the other. Still a failure by default — the plan
  #            says NO new query with calls > 0 — but the message says where to look, and
  #            GATE_ALLOW_NEW exists for a culprit that has been identified by name.
  local feature_re='queen\.kv|queen\.log_timers|kv_apply_v1|log_timers_|kv_expire|kv_usage|kv_quota'
  grep -E "$feature_re" "$db/.new" >"$db/.new-feature" 2>/dev/null || true
  grep -Ev "$feature_re" "$db/.new" >"$db/.new-other" 2>/dev/null || true
  if [ -n "${GATE_ALLOW_NEW:-}" ]; then
    grep -Ev "${GATE_ALLOW_NEW}" "$db/.new-other" >"$db/.new-other.f" 2>/dev/null || true
    mv "$db/.new-other.f" "$db/.new-other"
  fi

  if [ -s "$db/.new-feature" ]; then
    printf '  \033[31mFAIL\033[0m the feature executed SQL on a bundle that carries neither array:\n'
    sed 's/^/       /' "$db/.new-feature"
    printf '\n       §6.3: `p->'"'"'kv'"'"'` and `p->'"'"'timers'"'"'` are two binary lookups and two IFs not\n'
    printf '       taken. The work lives in SEPARATE statements behind those IFs and never joins\n'
    printf '       an existing one — no Function Scan folded into the provisioning query, no\n'
    printf '       UNION with the pushes. This is unwaivable.\n'
    fail=1
  fi
  if [ -s "$db/.new-other" ]; then
    printf '  \033[31mFAIL\033[0m %s other statement(s) ran in %s and never in %s:\n' \
      "$(wc -l <"$db/.new-other" | tr -d ' ')" "$b" "$a"
    sed 's/^/       /' "$db/.new-other"
    printf '\n       Usual cause: a background loop (retention, log_txns purge, stats reconcile)\n'
    printf '       whose cadence landed inside one window and not the other — the idle-window\n'
    printf '       dump already absorbs most of these. Confirm by name, then either lengthen the\n'
    printf '       run (GATE_BUNDLES) so both sides cover the cadence, or waive that one\n'
    printf '       statement with GATE_ALLOW_NEW='"'"'<extended regex>'"'"'.\n'
    fail=1
  fi
  if [ ! -s "$db/.new-feature" ] && [ ! -s "$db/.new-other" ]; then
    printf '  \033[32mPASS\033[0m no statement ran under load in %s that %s never ran\n' "$b" "$a"
  fi

  comm -23 "$da/queries-load.txt" "$db/queries-load.txt" >"$db/.gone" || true
  if [ -s "$db/.gone" ]; then
    printf '  note: %s statement(s) present in %s no longer run in %s (informational)\n' \
      "$(wc -l <"$db/.gone" | tr -d ' ')" "$a" "$b"
  fi

  rm -f "$da/.cpm" "$db/.cpm"
  step "$([ "$fail" -eq 0 ] && printf 'GATE PASSED' || printf 'GATE FAILED')"
  return "$fail"
}

case "${1:-}" in
  capture) shift; capture "$@" ;;
  compare) shift; compare "$@" ;;
  *) usage ;;
esac
