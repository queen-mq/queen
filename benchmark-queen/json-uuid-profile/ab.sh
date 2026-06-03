#!/usr/bin/env bash
#
# Stage 1 A/B: legacy nlohmann push path (QUEEN_PUSH_SIMD=0) vs simdjson + raw
# payload pass-through (QUEEN_PUSH_SIMD=1), across payload sizes.
#
# For each (flag, payload) it resets the stack to an empty DB, runs the profiled
# harness, and collects: PG-ground-truth push msg/s, mean broker CPU, and the
# JSON CPU bucket from the profile. Prints a comparison table at the end.
set -uo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

PAYLOADS="${PAYLOADS:-1024 4096}"
export PROFILE_WINDOW="${PROFILE_WINDOW:-25}"
export WARMUP_DURATION="${WARMUP_DURATION:-10}"
export DURATION="${DURATION:-45}"
export RAMP="${RAMP:-6}"
export SKIP_LINE_LEVEL=1   # skip the slow addr2line listing during A/B

SUMMARY="results/ab-$(date +%Y%m%d-%H%M%S).tsv"
printf "flag\tpayload_B\tmsg_per_s\tbroker_cpu_mean%%\tJSON%%\tUUID%%\tnon2xx\n" | tee "$SUMMARY"

mean_broker_cpu() { # $1 = docker-stats.txt
  grep 'qjup-broker' "$1" 2>/dev/null | sed -E 's/.*cpu=([0-9.]+)%.*/\1/' \
    | awk '{s+=$1;n++} END{ if(n) printf "%.0f", s/n; else print "?" }'
}
bucket() { # $1 = analysis.txt  $2 = label (JSON|UUID)
  grep -E "^\s+$2\s" "$1" 2>/dev/null | sed -E 's/.*: *([0-9.]+) *%.*/\1/' | head -n1
}

for simd in 0 1; do
  for pl in $PAYLOADS; do
    echo "######################################################################"
    echo "### RUN: QUEEN_PUSH_SIMD=$simd  PAYLOAD_SIZE_BYTES=$pl"
    echo "######################################################################"
    docker compose down -v >/dev/null 2>&1 || true
    QUEEN_PUSH_SIMD=$simd PAYLOAD_SIZE_BYTES=$pl ./run.sh >/dev/null 2>&1 || { echo "run failed (simd=$simd pl=$pl)"; continue; }

    # newest results dir
    rd="$(ls -1dt results/2026*/ 2>/dev/null | head -n1)"
    msg="$(grep -E '^push_msg_per_s_ground_truth=' "$rd/run-meta.txt" 2>/dev/null | cut -d= -f2)"
    cpu="$(mean_broker_cpu "$rd/docker-stats.txt")"
    js="$(bucket "$rd/analysis.txt" JSON)"
    uu="$(bucket "$rd/analysis.txt" UUID)"
    non2xx="$(grep -oE 'non2xx=[0-9]+' "$rd/loadgen.txt" 2>/dev/null | head -n1 | cut -d= -f2)"
    printf "%s\t%s\t%s\t%s\t%s\t%s\t%s\n" "$simd" "$pl" "${msg:-?}" "${cpu:-?}" "${js:-?}" "${uu:-?}" "${non2xx:-?}" | tee -a "$SUMMARY"
  done
done

echo
echo "================= A/B SUMMARY (flag 0=nlohmann, 1=simd) ================="
column -t -s $'\t' "$SUMMARY"
echo "raw: $SUMMARY"
