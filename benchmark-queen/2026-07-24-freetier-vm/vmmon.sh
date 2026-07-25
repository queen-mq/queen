#!/usr/bin/env bash
# vmmon.sh <outfile.csv> <step_s> <n_samples> — free-tier co-located sampler.
# broker + PG CPU come from `docker stats` (per-cgroup, so goload's host CPU is
# NOT counted in them); goload CPU from ps; PG xact rate from pg_stat_database.
# 2 vCPU box => 200% = both cores fully busy.
set -uo pipefail
OUT="${1:-/root/vmmon.csv}"; STEP="${2:-5}"; N="${3:-60}"
PG=qbench-pg; RN="${RN:-r6682}"
q(){ docker exec "$PG" psql -U postgres -tAc "$1" </dev/null 2>/dev/null; }
echo "t_s,broker_cpu,broker_mem_mb,pg_cpu,pg_mem_mb,goload_cpu,commit_s,rss_free_mb" > "$OUT"
echo "  t   | brCPU  pgCPU glCPU | sum  | brMem pgMem | commit/s | memFree"
pC=0; first=1
for i in $(seq 1 "$N"); do
  sleep "$STEP"
  stats=$(docker stats --no-stream --format '{{.Name}};{{.CPUPerc}};{{.MemUsage}}' "$RN" "$PG" 2>/dev/null)
  brc=$(echo "$stats" | awk -F';' -v n="$RN" '$1==n{gsub("%","",$2);print $2}')
  pgc=$(echo "$stats" | awk -F';' -v n="$PG" '$1==n{gsub("%","",$2);print $2}')
  brm=$(echo "$stats" | awk -F';' -v n="$RN" '$1==n{split($3,a,"/");gsub(/[A-Za-z ]/,"",a[1]);print a[1]}')
  pgm=$(echo "$stats" | awk -F';' -v n="$PG" '$1==n{split($3,a,"/");gsub(/[A-Za-z ]/,"",a[1]);print a[1]}')
  glc=$(ps -C goload-linux-am -o %cpu= 2>/dev/null | awk '{s+=$1}END{print s+0}')  # comm truncates to 15 chars
  C=$(q "select xact_commit from pg_stat_database where datname='postgres';"); C=${C:-0}
  mf=$(free -m | awk '/^Mem:/{print $7}')
  brc=${brc:-0}; pgc=${pgc:-0}; glc=${glc:-0}; brm=${brm:-0}; pgm=${pgm:-0}
  if [ "$first" = 1 ]; then first=0; else
    cps=$(awk -v a="$C" -v b="$pC" -v s="$STEP" 'BEGIN{printf "%.0f",(a-b)/s}')
    sum=$(awk -v a="$brc" -v b="$pgc" -v c="$glc" 'BEGIN{printf "%.0f",a+b+c}')
    printf "  %-4s| %5.0f %5.0f %5.0f | %4s | %5s %5s | %8s | %sMB\n" "$((i*STEP))s" "$brc" "$pgc" "$glc" "$sum" "$brm" "$pgm" "$cps" "$mf"
    echo "$((i*STEP)),$brc,$brm,$pgc,$pgm,$glc,$cps,$mf" >> "$OUT"
  fi
  pC=$C
done
