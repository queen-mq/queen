#!/bin/bash
PSQL(){ docker exec postgres psql -U postgres -d postgres -tAc "$1" 2>/dev/null; }
echo "=== soak state ==="
echo "node clients: $(pgrep -xc node || echo 0)   SUSTAINED-DONE: $(grep -c SUSTAINED-DONE /root/bench-runs/sustained.log)"
grep "min " /root/bench-runs/sustained.log | tail -1
echo "=== table vs shared_buffers (24GB) ==="
PSQL "SELECT 'messages total=' || pg_size_pretty(pg_total_relation_size('queen.messages')) || '  live=' || (SELECT n_live_tup FROM pg_stat_user_tables WHERE relname='messages') || '  dead=' || (SELECT n_dead_tup FROM pg_stat_user_tables WHERE relname='messages')"
echo "=== active queries + wait events (who is reading?) ==="
PSQL "SELECT pid || ' | ' || state || ' | ' || coalesce(wait_event_type,'-') || '/' || coalesce(wait_event,'-') || ' | age=' || round(extract(epoch from now()-query_start)) || 's | ' || left(regexp_replace(query,'\s+',' ','g'),68) FROM pg_stat_activity WHERE state <> 'idle' AND pid <> pg_backend_pid() ORDER BY query_start LIMIT 12"
echo "=== messages disk-read source over 15s (heap vs index) + cache hit ==="
H0=$(PSQL "SELECT heap_blks_read FROM pg_statio_user_tables WHERE relname='messages'"); I0=$(PSQL "SELECT idx_blks_read FROM pg_statio_user_tables WHERE relname='messages'"); HH0=$(PSQL "SELECT heap_blks_hit+idx_blks_hit FROM pg_statio_user_tables WHERE relname='messages'")
sleep 15
H1=$(PSQL "SELECT heap_blks_read FROM pg_statio_user_tables WHERE relname='messages'"); I1=$(PSQL "SELECT idx_blks_read FROM pg_statio_user_tables WHERE relname='messages'"); HH1=$(PSQL "SELECT heap_blks_hit+idx_blks_hit FROM pg_statio_user_tables WHERE relname='messages'")
python3 - "$H0" "$H1" "$I0" "$I1" "$HH0" "$HH1" <<'PY'
import sys
H0,H1,I0,I1,HH0,HH1=[int(x) for x in sys.argv[1:7]]
dt=15.0; h=H1-H0; i=I1-I0; hh=HH1-HH0; reads=h+i; tot=reads+hh
print(f"heap disk reads : {h*8192/dt/1e6:6.1f} MB/s")
print(f"index disk reads: {i*8192/dt/1e6:6.1f} MB/s")
print(f"cache-hit (msgs, last 15s): {100.0*hh/tot:.2f}%   (disk read blks/s={reads/dt:,.0f})" if tot else "no activity")
PY
echo "=== autovacuum on messages ==="
PSQL "SELECT 'last_autovacuum=' || coalesce(last_autovacuum::text,'never') || '  count=' || autovacuum_count FROM pg_stat_user_tables WHERE relname='messages'"
