#!/bin/bash
PSQL(){ docker exec postgres psql -U postgres -d postgres -tAc "$1" 2>/dev/null; }
echo "=== PG client connections by state (idle = free) ==="
PSQL "SELECT state, count(*) FROM pg_stat_activity WHERE backend_type='client backend' GROUP BY state ORDER BY count(*) DESC"
echo "=== broker pool env ==="
docker inspect queen --format '{{range .Config.Env}}{{println .}}{{end}}' | grep -E "DB_POOL_SIZE|SIDECAR|NUM_WORKERS|CUSTOM"
echo "=== fire /status, then snapshot active queries (does it reach PG? wait on what?) ==="
( curl -s --max-time 25 -o /dev/null -w "STATUS returned HTTP %{http_code} in %{time_total}s\n" http://localhost:6632/api/v1/status > /tmp/statresult.txt 2>&1 & )
sleep 2
PSQL "SELECT 'pid '||pid||' | '||state||' | '||coalesce(wait_event_type,'-')||'/'||coalesce(wait_event,'-')||' | age='||round(extract(epoch from now()-query_start),2)||'s | '||left(regexp_replace(query,'\s+',' ','g'),58) FROM pg_stat_activity WHERE backend_type='client backend' AND state<>'idle' ORDER BY query_start LIMIT 20"
echo "--- which SP families are active right now ---"
PSQL "SELECT 'get_status='||count(*) FILTER (WHERE query ILIKE '%get_status%')||' stats_agg='||count(*) FILTER (WHERE query ILIKE '%compute_partition%' OR query ILIKE '%aggregate_%' OR query ILIKE '%increment_message%')||' push='||count(*) FILTER (WHERE query ILIKE '%push_messages%')||' pop='||count(*) FILTER (WHERE query ILIKE '%pop_unified%')||' delete='||count(*) FILTER (WHERE query ILIKE '%DELETE FROM queen.messages%') FROM pg_stat_activity WHERE state='active'"
sleep 24
echo "=== /status result ==="; cat /tmp/statresult.txt 2>/dev/null
echo "=== CURRENT TEST status ==="
A=$(PSQL "SELECT n_tup_ins FROM pg_stat_user_tables WHERE relname='messages'"); sleep 5; B=$(PSQL "SELECT n_tup_ins FROM pg_stat_user_tables WHERE relname='messages'"); echo "push ins/s = $(( (B-A)/5 ))"
PSQL "SELECT 'msgs table='||pg_size_pretty(pg_total_relation_size('queen.messages'))||'  live='||(SELECT n_live_tup FROM pg_stat_user_tables WHERE relname='messages')||'  dead='||(SELECT n_dead_tup FROM pg_stat_user_tables WHERE relname='messages')"
docker stats --no-stream --format "{{.Name}}={{.CPUPerc}}" queen postgres | tr "\n" " "; echo
