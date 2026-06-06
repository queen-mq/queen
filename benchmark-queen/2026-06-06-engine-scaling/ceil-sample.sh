#!/usr/bin/env bash
# Broker-side: sample broker+PG saturation signals for DUR seconds, then print a
# one-line-per-metric summary for this ceiling cell. Run this CONCURRENTLY with
# goload (started on the loader VM). args: DUR C
set -u
DUR="${1:-60}"; C="${2:-0}"
TSV="/root/ceil_${C}.tsv"; : > "$TSV"
end=$(( $(date +%s) + DUR ))
pgq() {
  docker exec postgres psql -U postgres -d postgres -tAF'|' -c \
"SELECT (SELECT count(*) FROM pg_stat_activity WHERE state='active' AND pid<>pg_backend_pid()),
        (SELECT count(*) FROM pg_stat_activity WHERE wait_event_type='Lock'),
        (SELECT count(*) FROM pg_stat_activity WHERE wait_event_type IN ('IO','IPC')),
        COALESCE((SELECT n_tup_ins FROM pg_stat_user_tables WHERE schemaname='queen' AND relname='messages'),0),
        pg_wal_lsn_diff(pg_current_wal_lsn(),'0/0')::bigint" 2>/dev/null
}
while [ "$(date +%s)" -lt "$end" ]; do
  ts=$(date +%s)
  cpu=$(docker stats --no-stream --format '{{.Name}}={{.CPUPerc}}' queen postgres 2>/dev/null | tr '\n' ' ' | tr -d '%')
  row=$(pgq)
  echo "$ts|$cpu|$row" >> "$TSV"
done
awk -F'|' -v C="$C" '{
  n++; ts[n]=$1; act[n]=$3+0; lock[n]=$4+0; io[n]=$5+0; ins[n]=$6+0; wal[n]=$7+0;
  split($2,c," "); for(i in c){split(c[i],kv,"="); if(kv[1]=="queen")bq+=kv[2]; else if(kv[1]=="postgres")bp+=kv[2]}
}
END{
  if(n<2){print "C="C" insufficient samples ("n")"; exit}
  el=ts[n]-ts[1]; if(el<=0)el=1;
  sa=0;sl=0;sio=0; for(i=1;i<=n;i++){sa+=act[i];sl+=lock[i];sio+=io[i]}
  printf "C=%s n=%d dur=%ds | push/s=%d wal_MB/s=%.0f | brk_vcpu=%.2f pg_vcpu=%.2f | pg_active=%.1f lockwait=%.1f io=%.1f\n",
    C, n, el, (ins[n]-ins[1])/el, (wal[n]-wal[1])/el/1e6, bq/100/n, bp/100/n, sa/n, sl/n, sio/n;
}' "$TSV"
docker logs --since "${DUR}s" queen 2>&1 | grep -oE 'push\(q=[0-9]+ f=[0-9]+/[0-9]+[^)]*\).*evl=[0-9]+ms' | tail -2 | sed 's/^/    /'
