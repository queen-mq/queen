#!/bin/bash
# Read-only soak diagnostics: WAL/msg, index write-amplification, disk saturation.
PSQL(){ docker exec postgres psql -U postgres -d postgres -tAc "$1" 2>/dev/null; }

echo "=== messages: heap vs indexes (write amplification) ==="
PSQL "SELECT 'heap=' || pg_size_pretty(pg_table_size('queen.messages')) || '  indexes=' || pg_size_pretty(pg_indexes_size('queen.messages')) || '  ratio_idx/heap=' || round(pg_indexes_size('queen.messages')::numeric / nullif(pg_table_size('queen.messages'),0), 2)"
PSQL "SELECT '  ' || indexrelname || ' = ' || pg_size_pretty(pg_relation_size(indexrelid)) FROM pg_stat_user_indexes WHERE schemaname='queen' AND relname='messages' ORDER BY pg_relation_size(indexrelid) DESC"

echo "=== checkpointer ==="
PSQL "SELECT num_timed, num_requested, buffers_written, round(write_time/1000.0,1), round(sync_time/1000.0,1) FROM pg_stat_checkpointer"
echo "    (num_timed | num_requested(WAL-triggered) | buffers_written | write_s | sync_s)"

echo "=== sampling 30s: WAL/msg (PG insert counter) + retention + disk ==="
W0=$(PSQL "SELECT wal_bytes FROM pg_stat_wal"); R0=$(PSQL "SELECT wal_records FROM pg_stat_wal")
WBF0=$(PSQL "SELECT wal_buffers_full FROM pg_stat_wal")
INS0=$(PSQL "SELECT n_tup_ins FROM pg_stat_user_tables WHERE schemaname='queen' AND relname='messages'")
DEL0=$(PSQL "SELECT n_tup_del FROM pg_stat_user_tables WHERE schemaname='queen' AND relname='messages'")
read -r WSEC0 IOMS0 < <(awk '/ vda /{print $10, $13}' /proc/diskstats)
sleep 30
W1=$(PSQL "SELECT wal_bytes FROM pg_stat_wal"); R1=$(PSQL "SELECT wal_records FROM pg_stat_wal")
WBF1=$(PSQL "SELECT wal_buffers_full FROM pg_stat_wal")
INS1=$(PSQL "SELECT n_tup_ins FROM pg_stat_user_tables WHERE schemaname='queen' AND relname='messages'")
DEL1=$(PSQL "SELECT n_tup_del FROM pg_stat_user_tables WHERE schemaname='queen' AND relname='messages'")
read -r WSEC1 IOMS1 < <(awk '/ vda /{print $10, $13}' /proc/diskstats)

python3 - "$W0" "$W1" "$R0" "$R1" "$INS0" "$INS1" "$DEL0" "$DEL1" "$WSEC0" "$WSEC1" "$IOMS0" "$IOMS1" "$WBF0" "$WBF1" <<'PY'
import sys
a=[int(x) for x in sys.argv[1:15]]
W0,W1,R0,R1,I0,I1,D0,D1,S0,S1,T0,T1,B0,B1=a
dt=30.0
wal=W1-W0; recs=R1-R0; ins=I1-I0; dele=D1-D0; disk=(S1-S0)*512; util=(T1-T0)/(dt*1000)*100
print(f"INSERTS/s (push)      : {ins/dt:,.0f}")
print(f"DELETES/s (retention) : {dele/dt:,.0f}   -> retention {'KEEPS UP' if dele>=ins*0.95 else 'BEHIND by %.0f/s'%((ins-dele)/dt)}")
print(f"WAL bytes/s           : {wal/dt/1e6:,.1f} MB/s")
print(f"WAL per INSERT        : {wal/ins:,.0f} bytes  ({recs/ins:,.1f} records/insert)" if ins else "no ins")
print(f"wal_buffers_full delta: {B1-B0}")
print(f"disk WRITE throughput : {disk/dt/1e6:,.1f} MB/s   util(io_time): {util:,.1f} %")
print(f"non-WAL disk (flush)  : {(disk/dt/1e6)-(wal/dt/1e6):,.1f} MB/s")
PY
