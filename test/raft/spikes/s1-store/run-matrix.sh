#!/usr/bin/env bash
# S1 store spike: engines x rates, one table.
#
#   ./run-matrix.sh [duration_s]
#
# env:
#   ENGINES   default "redb fjall heed"
#   RATES     default "20000 50000 100000"   (msg/s, batch 10 -> 2k/5k/10k entries/s)
#   DATA      where the store dirs go (big; default /var/tmp/s1-store)
#   OUT       where the logs and the table go (default ./results)
#   EXTRA     extra flags passed to every run (e.g. --fsync-threads 8)
#
# Each run is a fresh directory. Nothing here is killed: use kill-loop.sh for that.
set -u
cd "$(dirname "$0")" || exit 2

DUR=${1:-60}
ENGINES=${ENGINES:-"redb fjall heed"}
RATES=${RATES:-"20000 50000 100000"}
DATA=${DATA:-/var/tmp/s1-store}
OUT=${OUT:-./results}
EXTRA=${EXTRA:-}
BIN=./target/release/s1-store

[ -x "$BIN" ] || { echo "build first: cargo build --release"; exit 2; }
mkdir -p "$OUT" "$DATA"
TSV="$OUT/matrix-$(date +%Y%m%d-%H%M%S).tsv"
: > "$TSV"

for e in $ENGINES; do
  for r in $RATES; do
    d="$DATA/$e-$r"
    rm -rf "$d"
    log="$OUT/$e-$r.log"
    echo "=== $e @ $r msg/s for ${DUR}s -> $log"
    # shellcheck disable=SC2086
    "$BIN" run --engine "$e" --dir "$d" --rate "$r" --duration "$DUR" $EXTRA >"$log" 2>&1
    rc=$?
    if [ $rc -ne 0 ]; then
      echo "  FAILED rc=$rc (see $log)"
      echo -e "$e\t$r\tFAILED rc=$rc" >> "$TSV"
      continue
    fi
    grep '^RESULT ' "$log" >> "$TSV"
    tail -1 "$TSV" | tr ' ' '\n' | grep -E '^(msgs_per_s|nondur_p99_ms|dur_p99_ms|wa_io|rss_max_mib)=' | tr '\n' ' '
    echo
    # keep the dir only if asked: these get large
    [ "${KEEP:-0}" = "1" ] || rm -rf "$d"
  done
done

echo
echo "=== matrix ($TSV) ==="
awk '
function g(k,   i,n,a) { for (i=1;i<=NF;i++) { n=index($i,"="); if (substr($i,1,n-1)==k) return substr($i,n+1) } return "-" }
BEGIN { printf "%-7s %8s %10s %9s %9s %9s %9s %8s %8s %9s %9s\n", \
  "engine","rate","msg/s","nd_p50","nd_p99","dur_p50","dur_p99","WA_io","WA_disk","rssMaxMiB","reopen_ms" }
/^RESULT/ { printf "%-7s %8s %10s %9s %9s %9s %9s %8s %8s %9s %9s\n", \
  g("engine"),g("rate"),g("msgs_per_s"),g("nondur_p50_ms"),g("nondur_p99_ms"),g("dur_p50_ms"),g("dur_p99_ms"), \
  g("wa_io"),g("wa_files"),g("rss_max_mib"),g("reopen_ms") }
' "$TSV"
echo
echo "full lines: $TSV ; logs: $OUT/<engine>-<rate>.log"
