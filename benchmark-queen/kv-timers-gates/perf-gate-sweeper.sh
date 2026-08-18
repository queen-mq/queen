#!/usr/bin/env bash
#
# PERF GATE SWEEPER — PLAN_KV_TIMERS §15, row "Perf gate sweeper", closes F3.
#
#   "percorso caldo invariato con lo sweeper acceso e una tabella di timer non vuota; e COSTO DEL
#    CICLO A TABELLA VUOTA su una cella 2-core, che e' il costo che pagherebbero tutti quelli che
#    non useranno mai la feature."
#
# One image, three conditions, run back to back so nothing but the sweeper differs:
#
#   A  QUEEN_SWEEPER=false — the sweeper task is not spawned (§7.1). The baseline. This used to
#      be "both feature flags off"; those flags are gone (kv and timers are part of the engine),
#      and QUEEN_SWEEPER is now the only knob that yields a broker with no sweep loop.
#   B  sweeper ON, timers table EMPTY — the bill on a cell that never schedules a timer. That
#      used to be "a feature nobody uses" and is now simply the DEFAULT cell, which is what
#      makes G1 a harder budget than when it was written: nobody opts into paying it.
#   C  sweeper ON, timers table SEEDED far in the future — the sweeper is awake and probing on
#      every cycle while producers work. This is the "hot path unchanged" condition.
#
# THREE GATES
#
#   G1  idle cost of B over A, in milli-CPU. §7.1 puts a number on why this matters: on a
#       2-core free tier with a measured ceiling around 480 msg/s, a per-second probe (a 64-seek
#       LATERAL plus a count), a Maint slot and a pool connection per cycle is "rumore
#       misurabile che nessun cliente ha chiesto". Budget: GATE_IDLE_BUDGET_MCPU (default 20 —
#       2% of one core, 1% of that 2-core cell).
#   G2  the empty-table backoff actually engages. §7.1 requires the sleep to climb to 30 s after
#       K idle cycles. If it does not, G1 passes today on a fast host and fails on the customer's
#       small one; counting the probes is what makes the backoff a fact rather than an intention.
#       Gate: due-probe calls during the idle window <= GATE_MAX_IDLE_PROBES (default 12 over
#       60 s, i.e. an average sleep of at least 5 s).
#   G3  hot path invariant with the sweeper awake: CPU per message under C vs A within 1%,
#       measured exactly like the F4 gate. This is the one that catches a fire loop competing
#       with producers for the same partition serializer and the same fsync.
#
# USAGE
#   GATE_IMAGE=queen:gate-after ./perf-gate-sweeper.sh run
#
# KNOBS (env), beyond those of compose.yml and perf-gate-hotpath.sh
#   GATE_SEED_TIMERS         rows for condition C            (default 200000)
#   GATE_IDLE_S              idle window per condition       (default 60)
#   GATE_IDLE_BUDGET_MCPU    G1 budget                       (default 20)
#   GATE_MAX_IDLE_PROBES     G2 budget                       (default 12)

. "$(cd "$(dirname "$0")" && pwd)/lib.sh"

REPS="${GATE_REPS:-3}"
BUNDLES="${GATE_BUNDLES:-20000}"
ITEMS="${GATE_ITEMS:-20}"
WORKERS="${GATE_WORKERS:-8}"
PARTITIONS="${GATE_PARTITIONS:-16}"
WARMUP_BUNDLES="${GATE_WARMUP_BUNDLES:-500}"
IDLE_S="${GATE_IDLE_S:-60}"
SEED_TIMERS="${GATE_SEED_TIMERS:-200000}"
IDLE_BUDGET_MCPU="${GATE_IDLE_BUDGET_MCPU:-20}"
MAX_IDLE_PROBES="${GATE_MAX_IDLE_PROBES:-12}"
TOLERANCE_PCT="${GATE_TOLERANCE_PCT:-1.0}"

usage() { sed -n '2,43p' "$0" | sed 's/^# \{0,1\}//'; exit 2; }

# Seed the staging table directly. Not through POST /api/v1/timers on purpose: 200k HTTP round
# trips would take longer than the whole gate, and what condition C needs is a table with rows
# in it, not a test of the schedule path. `visible_at` is GENERATED from deliver_at (§3.3), so
# pushing deliver_at far out is enough to guarantee nothing fires during the run — the sweeper
# still probes every cycle, which is the cost being measured.
seed_timers() {
  local n="$1"
  psql_q "
    INSERT INTO queen.log_timers
           (tenant_id, queue, timer_key, partition, deliver_at, txn, message_id, payload)
    SELECT '00000000-0000-0000-0000-000000000001'::uuid,
           'gate-timers', 'k-'||g, 'Default',
           now() + interval '30 days' + (g || ' milliseconds')::interval,
           'gate-timer-txn-'||g, gen_random_uuid(),
           convert_to('{\"gate\":true}', 'UTF8')
      FROM generate_series(1, $n) g
    ON CONFLICT DO NOTHING" >/dev/null \
    || die "could not seed queen.log_timers. If the columns have moved, fix the INSERT above
       against sql/procedures/025_log_timers.sql — the gate seeds the table directly and is the
       one place in the repo that knows its shape outside the SQL itself."
  local got; got="$(psql_q "SELECT count(*) FROM queen.log_timers")"
  info "seeded $got timer rows (all due in ~30 days)"
  [ "$got" -ge "$n" ] || die "expected >= $n timer rows, found $got"
}

timers_table_exists() { [ "$(psql_q "SELECT to_regclass('queen.log_timers') IS NOT NULL")" = "t" ]; }

# Sweeper statements that ran during the window, by call count.
sweeper_calls() {
  psql_q "/*gate*/ SELECT coalesce(sum(calls),0) FROM pg_stat_statements
           WHERE query LIKE '%log_timers_due%' AND query NOT LIKE '%/*gate*/%'"
}
sweeper_all_calls() {
  psql_q "/*gate*/ SELECT coalesce(sum(calls),0), coalesce(string_agg(DISTINCT left(regexp_replace(query,'\s+',' ','g'),60), ' | '),'-')
            FROM pg_stat_statements
           WHERE (query LIKE '%log_timers%' OR query LIKE '%kv_expire%' OR query LIKE '%kv_usage%')
             AND query NOT LIKE '%/*gate*/%'"
}

# One condition: fresh stack, warm up, optionally seed, measure an idle window, then measure a
# fixed-work load window. Appends one line per rep to <dir>/reps.tsv.
run_condition() {
  local name="$1" sweeper="$2" seed="$3"
  local dir; dir="$(label_dir "sweeper-$name")"
  rm -rf "$dir"; mkdir -p "$dir"

  step "condition $name — SWEEPER=$sweeper seed=$seed"

  # Exported, not prefixed onto stack_up: compose reads it, and so does save_env, which is the
  # only record of which condition a results directory belongs to.
  export GATE_SWEEPER="$sweeper"

  local rep
  for rep in $(seq 1 "$REPS"); do
    stack_up
    save_env "$dir"
    pgss_assert_available

    if [ "$seed" -gt 0 ]; then
      timers_table_exists || die "condition $name needs queen.log_timers, and this image's
       schema does not create it. Build an image that includes 025_log_timers.sql (F1) before
       running the sweeper gate."
      seed_timers "$seed"
    elif [ "$sweeper" = "true" ] && ! timers_table_exists; then
      die "the sweeper is on but queen.log_timers does not exist — this image predates F1, so
       conditions B and C would measure a broker that cannot have a sweeper. Only condition A
       is meaningful here."
    fi

    loadgen --url "$BROKER_URL" --mode bundle --tag warm \
            --bundles "$WARMUP_BUNDLES" --items "$ITEMS" --workers "$WORKERS" \
            --partitions "$PARTITIONS" >"$dir/warmup-$rep.json" 2>>"$dir/loadgen.err" \
      || die "warm-up failed, see $dir/loadgen.err"

    # ---- idle window: no client at all. This is the number a customer who never enables the
    # feature pays, every second, forever.
    pgss_reset
    local ib0 ip0 ib1 ip1
    ib0="$(cpu_usec broker)"; ip0="$(cpu_usec pg)"
    sleep "$IDLE_S"
    ib1="$(cpu_usec broker)"; ip1="$(cpu_usec pg)"
    local idle_mcpu probes
    idle_mcpu="$(awk -v d="$(( (ib1-ib0) + (ip1-ip0) ))" -v s="$IDLE_S" 'BEGIN{printf "%.2f", d/(s*1000.0)}')"
    probes="$(sweeper_calls)"
    sweeper_all_calls >"$dir/sweeper-statements-$rep.tsv"

    # ---- load window: identical fixed work in every condition.
    pgss_reset
    local b0 p0 b1 p1
    b0="$(cpu_usec broker)"; p0="$(cpu_usec pg)"
    loadgen --url "$BROKER_URL" --mode bundle --tag gate \
            --bundles "$BUNDLES" --items "$ITEMS" --workers "$WORKERS" \
            --partitions "$PARTITIONS" >"$dir/load-$rep.json" 2>>"$dir/loadgen.err" \
      || die "load failed, see $dir/load-$rep.json"
    b1="$(cpu_usec broker)"; p1="$(cpu_usec pg)"

    local msgs rate cpu_us cpu_per_msg
    msgs="$(awk -F'[,:]' '{for(i=1;i<=NF;i++) if($i ~ /"messages"/) print $(i+1)}' "$dir/load-$rep.json" | tr -d ' }')"
    rate="$(awk -F'[,:]' '{for(i=1;i<=NF;i++) if($i ~ /"msg_per_s"/) print $(i+1)}' "$dir/load-$rep.json" | tr -d ' }')"
    cpu_us=$(( (b1-b0) + (p1-p0) ))
    cpu_per_msg="$(awk -v c="$cpu_us" -v m="$msgs" 'BEGIN{printf "%.3f", c/m}')"

    printf '%s\t%s\t%s\t%s\t%s\n' "$rep" "$cpu_per_msg" "$rate" "$idle_mcpu" "$probes" >>"$dir/reps.tsv"
    info "rep $rep: ${cpu_per_msg} us/msg, ${rate} msg/s, idle ${idle_mcpu} mCPU, ${probes} due-probes/${IDLE_S}s"

    stack_down
  done
}

col_median() { cut -f"$2" "$(label_dir "sweeper-$1")/reps.tsv" | median; }

run_all() {
  need docker; need node; need curl; need awk
  mkdir -p "$RESULTS_DIR"

  run_condition A false 0
  run_condition B true  0
  run_condition C true  "$SEED_TIMERS"

  local a_cpu b_cpu c_cpu a_idle b_idle c_idle b_probes c_probes a_rate c_rate
  a_cpu="$(col_median A 2)";  b_cpu="$(col_median B 2)";  c_cpu="$(col_median C 2)"
  a_idle="$(col_median A 4)"; b_idle="$(col_median B 4)"; c_idle="$(col_median C 4)"
  b_probes="$(col_median B 5)"; c_probes="$(col_median C 5)"
  a_rate="$(col_median A 3)"; c_rate="$(col_median C 3)"

  local fail=0

  step "results (medians of $REPS reps)"
  printf '  %-38s %10s %10s %10s\n' '' 'A off' 'B empty' 'C seeded'
  printf '  %-38s %10s %10s %10s\n' 'idle CPU, broker+pg (mCPU)' "$a_idle" "$b_idle" "$c_idle"
  printf '  %-38s %10s %10s %10s\n' "due probes per ${IDLE_S}s idle" '-' "$b_probes" "$c_probes"
  printf '  %-38s %10s %10s %10s\n' 'CPU per message (us)' "$a_cpu" "$b_cpu" "$c_cpu"
  printf '  %-38s %10s %10s %10s\n' 'throughput (msg/s)' "$a_rate" '-' "$c_rate"

  step "G1 — what an installation that never schedules a timer pays"
  local idle_delta
  idle_delta="$(awk -v a="$a_idle" -v b="$b_idle" 'BEGIN{printf "%.2f", b-a}')"
  printf '  B - A = %s mCPU   (budget %s mCPU)\n' "$idle_delta" "$IDLE_BUDGET_MCPU"
  if awk -v d="$idle_delta" -v t="$IDLE_BUDGET_MCPU" 'BEGIN{exit !(d>t)}'; then
    printf '  \033[31mFAIL\033[0m the empty-table cycle costs more than the budget.\n'
    printf '       §7.1 is explicit that this is the cost nobody asked for, and every cell now\n'
    printf '       pays it by default. The levers, in order: is the task spawned under\n'
    printf '       QUEEN_SWEEPER=false (it must not be)? does the backoff engage (G2)? is the\n'
    printf '       due probe one call or several?\n'
    fail=1
  else
    printf '  \033[32mPASS\033[0m\n'
  fi
  printf '  context: %s mCPU is %s%% of one core, %s%% of a 2-core cell\n' \
    "$idle_delta" \
    "$(awk -v d="$idle_delta" 'BEGIN{printf "%.2f", d/10.0}')" \
    "$(awk -v d="$idle_delta" 'BEGIN{printf "%.2f", d/20.0}')"

  step "G2 — the empty-table backoff engages"
  printf '  %s due probes in %ss of idle   (max %s)\n' "$b_probes" "$IDLE_S" "$MAX_IDLE_PROBES"
  if [ "${b_probes:-0}" -gt "$MAX_IDLE_PROBES" ] 2>/dev/null; then
    printf '  \033[31mFAIL\033[0m the sleep is not climbing: %s probes means an average sleep of\n' "$b_probes"
    printf '       about %ss, not the 30s §7.1 asks for after K empty cycles. G1 may still be\n' \
      "$(awk -v p="$b_probes" -v s="$IDLE_S" 'BEGIN{printf "%.1f", (p>0? s/p : 0)}')"
    printf '       green on this host and red on a small one.\n'
    fail=1
  else
    printf '  \033[32mPASS\033[0m (average sleep ~%ss)\n' \
      "$(awk -v p="$b_probes" -v s="$IDLE_S" 'BEGIN{printf "%.1f", (p>0? s/p : s)}')"
  fi
  printf '  note: condition C probes %s times — a NON-empty table is due-driven and does not\n' "$c_probes"
  printf '        back off, by design (§1.7). That is the cost of having pending timers, and it\n'
  printf '        is the number to quote when someone asks what the feature costs at rest.\n'

  step "G3 — hot path with the sweeper awake and the table non-empty"
  local d_ac
  d_ac="$(pct "$a_cpu" "$c_cpu")"
  printf '  CPU/msg  A %s us  ->  C %s us   delta %s%%   (gate +/- %s%%)\n' \
    "$a_cpu" "$c_cpu" "$d_ac" "$TOLERANCE_PCT"
  local spa spc
  cut -f2 "$(label_dir sweeper-A)/reps.tsv" >"$RESULTS_DIR/.a"; spa="$(spread_pct "$RESULTS_DIR/.a")"
  cut -f2 "$(label_dir sweeper-C)/reps.tsv" >"$RESULTS_DIR/.c"; spc="$(spread_pct "$RESULTS_DIR/.c")"
  printf '  within-condition spread: A %s%%  C %s%%\n' "$spa" "$spc"
  if [ "$REPS" -lt 3 ]; then
    printf '  \033[31mUNRESOLVED\033[0m %s rep(s): below 3 the spread is not a spread and a single\n' "$REPS"
    printf '       measurement reports 0%%, which satisfies the check by accident.\n'
    fail=1
  elif awk -v x="$spa" -v y="$spc" -v t="$TOLERANCE_PCT" 'BEGIN{exit !(x>t || y>t)}'; then
    printf '  \033[31mUNRESOLVED\033[0m spread exceeds the tolerance; raise GATE_REPS/GATE_BUNDLES\n'
    printf '       and quiesce the host. This rig cannot resolve %s%% today.\n' "$TOLERANCE_PCT"
    fail=1
  elif awk -v d="$d_ac" -v t="$TOLERANCE_PCT" 'BEGIN{exit !(d>t)}'; then
    printf '  \033[31mFAIL\033[0m the sweeper is taxing the message path.\n'
    printf '       Most likely: the fire (or the probe) is competing with producers for the same\n'
    printf '       partition serializer, or the Maint lane is not holding it back. §7.3 keeps\n'
    printf '       PARALLELISM at 1 for exactly this reason — check it was not raised.\n'
    fail=1
  else
    printf '  \033[32mPASS\033[0m\n'
  fi
  rm -f "$RESULTS_DIR/.a" "$RESULTS_DIR/.c"

  step "$([ "$fail" -eq 0 ] && printf 'GATE PASSED' || printf 'GATE FAILED')"
  return "$fail"
}

case "${1:-}" in
  run) shift; run_all "$@" ;;
  *) usage ;;
esac
