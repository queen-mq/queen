#!/bin/sh
# Download the GitHub Archive hours the demo reads: 2025-10-01 and the next
# morning (2025-10-02 00:00-05:59, so the issues and pull requests of the
# day's last hours get the time to be answered). About 2.9 GB.
#   ./fetch.sh [data dir]   (default ~/Work/queen-data/gharchive)
set -e
DATA=${1:-${GH_DATA:-$HOME/Work/queen-data/gharchive}}
mkdir -p "$DATA/raw"
cd "$DATA/raw"
hours=""
for h in $(seq 0 23); do hours="$hours 2025-10-01-$h"; done
for h in 0 1 2 3 4 5; do hours="$hours 2025-10-02-$h"; done
for f in $hours; do
  [ -s "$f.json.gz" ] && continue
  echo "$f"
done | xargs -P 4 -I{} sh -c 'curl -sS --retry 3 --max-time 900 -o {}.json.gz.part https://data.gharchive.org/{}.json.gz && mv {}.json.gz.part {}.json.gz && gzip -t {}.json.gz && echo "got {}"'
ls "$DATA/raw" | wc -l
