#!/bin/bash
# PERF-1 — scrape the raft broker's /metrics/prometheus every N seconds into a
# CSV of the queen_raft_* timing families (PLAN_RAFT.md O18). Run it against a
# broker that is already up (e.g. one measure-raft1.sh or profile.sh started),
# for the length of a regime, so the histogram quantiles and the stage counters
# can be plotted over the run next to the perf profile.
#
# usage: timing.sh <out.csv> [seconds] [interval]
#   out.csv:  where to write            (required)
#   seconds:  how long to scrape        (default 120)
#   interval: seconds between scrapes   (default 5)
# env:
#   URL   broker base URL   (default http://127.0.0.1:6699)
set -u

OUT=${1:?usage: timing.sh <out.csv> [seconds] [interval]}
SECONDS_TOTAL=${2:-120}
INTERVAL=${3:-5}
URL=${URL:-http://127.0.0.1:6699}

# The families scraped, and the samples pulled from each. Latency summaries
# report seconds; size summaries and counters report raw values. One CSV row
# per scrape, wide: ts plus <metric>_<sample> columns.
# We pull, for every queen_raft_*_seconds summary, its q0.5/q0.99/q1/_count, and
# for the counters their totals.

# A metric name pulled with an optional label match. Prints the value or "".
val() { # $1 body  $2 grep-pattern
  echo "$1" | grep -E "$2" | grep -vE '^#' | awk '{print $NF}' | head -1
}

# Header: enumerate the columns once from the first scrape.
LAT="plan arrival_to_proposed propose_roundtrip proposed_to_committed log_fsync \
apply_entry apply_segment apply_other store_commit durable_point durable_seg_fsync \
durable_dir_fsync pop_read"
SIZE="drain_commands drain_messages group_entries group_bytes apply_channel_depth"

hdr="ts"
for m in $LAT; do hdr="$hdr,${m}_p50,${m}_p99,${m}_max,${m}_count"; done
for m in $SIZE; do hdr="$hdr,${m}_p50,${m}_p99,${m}_count"; done
hdr="$hdr,slow_commands_total,apply_receives_total,apply_entries,apply_appends,apply_messages,apply_commits,apply_durable_points"
echo "$hdr" > "$OUT"

end=$(( $(date +%s) + SECONDS_TOTAL ))
while [ "$(date +%s)" -lt "$end" ]; do
  body=$(curl -s "$URL/metrics/prometheus" 2>/dev/null)
  ts=$(date +%s)
  if [ -z "$body" ]; then echo "$ts,SCRAPE_FAILED" >> "$OUT"; sleep "$INTERVAL"; continue; fi
  row="$ts"
  for m in $LAT; do
    n="queen_raft_${m}_seconds"
    p50=$(val "$body" "^${n}\{quantile=\"0.5\"\}")
    p99=$(val "$body" "^${n}\{quantile=\"0.99\"\}")
    mx=$(val "$body" "^${n}\{quantile=\"1\"\}")
    ct=$(val "$body" "^${n}_count")
    row="$row,${p50:-},${p99:-},${mx:-},${ct:-}"
  done
  for m in $SIZE; do
    n="queen_raft_${m}"
    p50=$(val "$body" "^${n}\{quantile=\"0.5\"\}")
    p99=$(val "$body" "^${n}\{quantile=\"0.99\"\}")
    ct=$(val "$body" "^${n}_count")
    row="$row,${p50:-},${p99:-},${ct:-}"
  done
  slow=$(val "$body" "^queen_raft_slow_commands_total ")
  recv=$(val "$body" "^queen_raft_apply_receives_total ")
  ent=$(val "$body" "^queen_raft_apply_stats\{field=\"entries\"\}")
  app=$(val "$body" "^queen_raft_apply_stats\{field=\"appends\"\}")
  msg=$(val "$body" "^queen_raft_apply_stats\{field=\"messages\"\}")
  cmt=$(val "$body" "^queen_raft_apply_stats\{field=\"commits\"\}")
  dp=$(val "$body" "^queen_raft_apply_stats\{field=\"durable_points\"\}")
  row="$row,${slow:-},${recv:-},${ent:-},${app:-},${msg:-},${cmt:-},${dp:-}"
  echo "$row" >> "$OUT"
  sleep "$INTERVAL"
done
echo "wrote $(( $(wc -l < "$OUT") - 1 )) scrapes to $OUT"
