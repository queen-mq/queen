#!/usr/bin/env bash
#
# Analyze a captured gperftools CPU profile and attribute broker CPU to the
# JSON, UUID, lock and allocator buckets. Runs pprof INSIDE the broker
# container (which holds the symbolized binary + google-pprof).
#
# Env:
#   BROKER_CONTAINER  (default qjup-broker)
#   PROFILE           (path inside the container; default: newest /profiles/queen.prof*)
#
# NOTE: intentionally NOT using `set -e -o pipefail` — several sections pipe
# pprof through `head`, whose SIGPIPE would otherwise abort the report. Error
# handling is done with explicit checks below.
set -u

C="${BROKER_CONTAINER:-qjup-broker}"
PROF="${PROFILE:-}"
BIN="server/bin/queen-server"

# Pick whichever pprof is present in the image.
PPROF="$(docker exec "$C" bash -lc 'command -v google-pprof || command -v pprof || true')"
if [[ -z "$PPROF" ]]; then
  echo "ERROR: no pprof binary found inside $C" >&2
  exit 1
fi

# In signal-toggle mode gperftools writes CPUPROFILE with a per-cycle suffix
# (queen.prof.0, .1, ...). If no explicit PROFILE was given (or it's missing),
# resolve to the newest /profiles/queen.prof* inside the container.
if [[ -z "$PROF" ]] || ! docker exec "$C" test -s "$PROF"; then
  PROF="$(docker exec "$C" bash -lc 'ls -1t /profiles/queen.prof* 2>/dev/null | head -n1')"
fi
if [[ -z "$PROF" ]] || ! docker exec "$C" test -s "$PROF"; then
  echo "ERROR: no non-empty profile found in /profiles inside $C (was profiling toggled on?)" >&2
  exit 1
fi

echo "=================================================================="
echo " Profile: $PROF   (pprof: $PPROF)"
echo "=================================================================="
echo
echo "### Top 40 functions by self (flat) CPU"
echo "------------------------------------------------------------------"
docker exec "$C" "$PPROF" --text "$BIN" "$PROF" 2>/dev/null | head -n 42

echo
echo "### Bucketed CPU attribution (sum of flat %)"
echo "------------------------------------------------------------------"
docker exec "$C" "$PPROF" --text "$BIN" "$PROF" 2>/dev/null \
| awk '
  # Only process data rows: "<flat> <flat%> <sum%> <cum> <cum%> <symbol...>".
  $2 ~ /%$/ {
    pct=$2; gsub(/%/,"",pct); p=pct+0;
    low=tolower($0);
    matched=0;
    if (low ~ /nlohmann|basic_json|json_sax|sax_parse|::parse|::dump|_rb_tree|serializer|lexer/) { json+=p; matched=1 }
    else if (low ~ /generate_uuid|uuidv7|stringstream|num_put|money_put|__ostream_insert|ostream:|ostream_|do_put|setfill|setw/) { uuid+=p; matched=1 }
    else if (low ~ /mutex|futex|__lll|pthread_mutex|spin_lock|lock_wa/) { lock+=p; matched=1 }
    else if (low ~ /operator new|operator delete|_int_malloc|_int_free|morecore|tcmalloc|cfree|[^a-z]malloc|[^a-z]free/) { alloc+=p; matched=1 }
    else if (low ~ /__send|__recv|__write|__read|epoll|timer_settime|pq[a-z]|libpq/) { io+=p; matched=1 }
    if (!matched) other+=p;
  }
  END {
    printf "  JSON    (nlohmann parse/dump/lexer/serialize) : %6.1f %%\n", json;
    printf "  UUID    (generate_uuid + stringstream fmt)    : %6.1f %%\n", uuid;
    printf "  LOCK    (mutex/futex contention)              : %6.1f %%\n", lock;
    printf "  ALLOC   (new/delete/malloc, mostly JSON-driven): %6.1f %%\n", alloc;
    printf "  IO/sys  (send/recv/epoll/timer/libpq)         : %6.1f %%\n", io;
    printf "  other                                         : %6.1f %%\n", other;
    printf "\n  NOTE: buckets are regex heuristics over flat%%. ALLOC is largely\n";
    printf "        JSON-DOM-driven, so JSON+ALLOC approximates the realistic\n";
    printf "        upper bound on JSON-attributable CPU.\n";
  }'

echo
echo "### Line-level cost inside the UUID generator (stringstream hotspot)"
echo "------------------------------------------------------------------"
if [[ -n "${SKIP_LINE_LEVEL:-}" ]]; then
  echo "(skipped; unset SKIP_LINE_LEVEL to enable the slow addr2line listing)"
else
  docker exec "$C" "$PPROF" --list='generate_uuid' "$BIN" "$PROF" 2>/dev/null | head -n 70 || true
fi

echo
echo "### Collapsed stacks (for flamegraph)"
echo "------------------------------------------------------------------"
# Only emit collapsed stacks when an output file is requested — they are large
# (one line per unique stack) and would otherwise flood the report.
if [[ -n "${COLLAPSED_OUT:-}" ]]; then
  if docker exec "$C" "$PPROF" --collapsed "$BIN" "$PROF" 2>/dev/null > "$COLLAPSED_OUT"; then
    echo "wrote collapsed stacks to $COLLAPSED_OUT ($(wc -l < "$COLLAPSED_OUT") stacks)"
    echo "render with e.g.:  flamegraph.pl $COLLAPSED_OUT > flame.svg"
  else
    echo "(--collapsed not supported by this pprof build; use --svg with graphviz instead)"
  fi
else
  echo "(set COLLAPSED_OUT=path to emit collapsed stacks for a flamegraph)"
fi
