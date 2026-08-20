#!/bin/bash
# run.sh <name> <duration_s> <goload-args...>
#
# Drives one scenario of the 1.0.4 tenant-density rerun:
#   queen box  -> reset pg_stat_statements, 1 Hz bench-sampler
#   loader box -> goload -mode tenants
#   queen box  -> pg_stat_statements attribution + broker log capture
#
# Artifacts land in ./out/<name>.{csv,load.txt,pgss.txt,brk.txt}
set -uo pipefail
Q=46.101.193.166
L=209.38.206.19
QPRIV=10.114.0.2
OUT="$(dirname "$0")/out"; mkdir -p "$OUT"

NAME="${1:?usage: run.sh <name> <duration_s> <goload args...>}"; shift
DUR="${1:?duration}"; shift

SSH="ssh -o BatchMode=yes -o ServerAliveInterval=30 -o ServerAliveCountMax=10"

# NB: stop the previous sampler BY PID FILE, never `pkill -f bench-sampler.sh`
# — that pattern matches this very ssh command line and kills the shell that is
# about to start the new sampler (silently: goload then runs unmonitored).
echo "=== [$NAME] reset counters + start sampler (${DUR}s) ==="
$SSH root@$Q "psql -h 127.0.0.1 -U postgres -d queen -qc 'SELECT pg_stat_statements_reset();' >/dev/null
[ -f /root/sampler.pid ] && kill \$(cat /root/sampler.pid) 2>/dev/null
rm -f /root/$NAME.csv
setsid nohup bash /root/bench-sampler.sh /root/$NAME.csv $((DUR + 90)) </dev/null > /root/$NAME.sampler.log 2>&1 &
echo \$! > /root/sampler.pid
sleep 3
kill -0 \$(cat /root/sampler.pid) 2>/dev/null && echo \"sampler-started pid=\$(cat /root/sampler.pid) rows=\$(wc -l < /root/$NAME.csv)\" || echo SAMPLER-FAILED"

BRK_SINCE=$($SSH root@$Q "date -u +%FT%TZ")

echo "=== [$NAME] goload (duration=${DUR}s) ==="
$SSH root@$L "ulimit -n 200000; /root/goload -mode tenants -url http://$QPRIV:6632 -duration $DUR $*" \
  2>&1 | tee "$OUT/$NAME.load.txt" | tail -25

echo "=== [$NAME] capture ==="
$SSH root@$Q "[ -f /root/sampler.pid ] && kill \$(cat /root/sampler.pid) 2>/dev/null
psql -h 127.0.0.1 -U postgres -d queen -qXc \"
SELECT round(total_exec_time)::bigint AS total_ms, calls,
       round(mean_exec_time::numeric,3) AS mean_ms,
       round(100*total_exec_time/NULLIF(sum(total_exec_time) OVER (),0))::int AS pct,
       left(regexp_replace(query,'\s+',' ','g'),68) AS query
FROM pg_stat_statements
WHERE dbid=(SELECT oid FROM pg_database WHERE datname='queen')
ORDER BY total_exec_time DESC LIMIT 18;\"
echo '--- xact/s over the window ---'
psql -h 127.0.0.1 -U postgres -d queen -qXtAc \"SELECT xact_commit FROM pg_stat_database WHERE datname='queen';\"
" > "$OUT/$NAME.pgss.txt" 2>&1

$SSH root@$Q "docker logs queen --since '$BRK_SINCE' 2>&1 | grep -vE 'refresh \(idle\)' | tail -40" > "$OUT/$NAME.brk.txt" 2>&1
scp -o BatchMode=yes -q root@$Q:/root/$NAME.csv "$OUT/$NAME.csv" 2>/dev/null

echo "=== [$NAME] resource summary ==="
python3 "$(dirname "$0")/summarize.py" "$OUT/$NAME.csv" 2>/dev/null || echo "(no csv)"
echo "=== [$NAME] done -> $OUT/$NAME.* ==="
