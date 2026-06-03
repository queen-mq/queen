#!/usr/bin/env bash
#
# Stage 1 + Stage 2 A/B matrix. Each combo is "SIMD:RAWRESULT".
#   0:0 = baseline (nlohmann ingest + parse/reserialize result)
#   1:0 = Stage 1 only (simd ingest + raw payload pass-through)
#   0:1 = Stage 2 only (raw result pass-through)
#   1:1 = both
#
# Resets to a fresh DB per run, runs the profiled harness, and reports
# PG-ground-truth msg/s, mean broker CPU, and CPU/msg (the bottleneck-independent
# efficiency metric).
set -uo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

COMBOS="${COMBOS:-0:0 1:0 0:1 1:1}"
PAYLOADS="${PAYLOADS:-1024}"
export PROFILE_WINDOW="${PROFILE_WINDOW:-25}"
export WARMUP_DURATION="${WARMUP_DURATION:-10}"
export DURATION="${DURATION:-45}"
export RAMP="${RAMP:-6}"
export SKIP_LINE_LEVEL=1

SUMMARY="results/ab2-$(date +%Y%m%d-%H%M%S).tsv"
printf "simd\traw\tpayload_B\tmsg_per_s\tbroker_cpu%%\tcpu_per_1k_msg\tnon2xx\n" | tee "$SUMMARY"

mean_broker_cpu() {
  grep 'qjup-broker' "$1" 2>/dev/null | sed -E 's/.*cpu=([0-9.]+)%.*/\1/' \
    | awk '{s+=$1;n++} END{ if(n) printf "%.0f", s/n; else print "0" }'
}

for combo in $COMBOS; do
  simd="${combo%%:*}"; raw="${combo##*:}"
  for pl in $PAYLOADS; do
    echo "### RUN simd=$simd raw=$raw payload=$pl"
    docker compose down -v >/dev/null 2>&1 || true
    QUEEN_PUSH_SIMD=$simd QUEEN_PUSH_RAW_RESULT=$raw PAYLOAD_SIZE_BYTES=$pl ./run.sh >/dev/null 2>&1 \
      || { echo "run failed (simd=$simd raw=$raw pl=$pl)"; continue; }
    rd="$(ls -1dt results/2026*/ 2>/dev/null | head -n1)"
    msg="$(grep -E '^push_msg_per_s_ground_truth=' "$rd/run-meta.txt" 2>/dev/null | cut -d= -f2)"
    cpu="$(mean_broker_cpu "$rd/docker-stats.txt")"
    non2xx="$(grep -oE 'non2xx=[0-9]+' "$rd/loadgen.txt" 2>/dev/null | head -n1 | cut -d= -f2)"
    cpm="$(awk -v c="${cpu:-0}" -v m="${msg:-0}" 'BEGIN{ if(m>0) printf "%.2f", (c*1000.0)/m; else print "?" }')"
    printf "%s\t%s\t%s\t%s\t%s\t%s\t%s\n" "$simd" "$raw" "$pl" "${msg:-?}" "${cpu:-?}" "$cpm" "${non2xx:-?}" | tee -a "$SUMMARY"
  done
done

echo
echo "============ STAGE 1+2 A/B (simd=ingest, raw=result; cpu_per_1k_msg lower=better) ============"
column -t -s $'\t' "$SUMMARY"
echo "raw: $SUMMARY"
