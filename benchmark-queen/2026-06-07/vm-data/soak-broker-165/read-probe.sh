#!/bin/bash
# Instantaneous disk-READ analysis for the soak (not cumulative).
PSQL(){ docker exec postgres psql -U postgres -d postgres -tAc "$1" 2>/dev/null; }
echo "messages table = $(PSQL "SELECT pg_size_pretty(pg_total_relation_size('queen.messages'))")  | shared_buffers = 24GB"
echo "live_tup = $(PSQL "SELECT n_live_tup FROM pg_stat_user_tables WHERE schemaname='queen' AND relname='messages'")"
HR0=$(PSQL "SELECT heap_blks_read FROM pg_statio_user_tables WHERE schemaname='queen' AND relname='messages'")
IR0=$(PSQL "SELECT idx_blks_read FROM pg_statio_user_tables WHERE schemaname='queen' AND relname='messages'")
BH0=$(PSQL "SELECT sum(blks_hit) FROM pg_stat_database"); BR0=$(PSQL "SELECT sum(blks_read) FROM pg_stat_database")
INS0=$(PSQL "SELECT n_tup_ins FROM pg_stat_user_tables WHERE schemaname='queen' AND relname='messages'")
DEL0=$(PSQL "SELECT n_tup_del FROM pg_stat_user_tables WHERE schemaname='queen' AND relname='messages'")
RS0=$(awk '/ vda /{print $6}' /proc/diskstats)
sleep 30
HR1=$(PSQL "SELECT heap_blks_read FROM pg_statio_user_tables WHERE schemaname='queen' AND relname='messages'")
IR1=$(PSQL "SELECT idx_blks_read FROM pg_statio_user_tables WHERE schemaname='queen' AND relname='messages'")
BH1=$(PSQL "SELECT sum(blks_hit) FROM pg_stat_database"); BR1=$(PSQL "SELECT sum(blks_read) FROM pg_stat_database")
INS1=$(PSQL "SELECT n_tup_ins FROM pg_stat_user_tables WHERE schemaname='queen' AND relname='messages'")
DEL1=$(PSQL "SELECT n_tup_del FROM pg_stat_user_tables WHERE schemaname='queen' AND relname='messages'")
RS1=$(awk '/ vda /{print $6}' /proc/diskstats)
python3 - "$HR0" "$HR1" "$IR0" "$IR1" "$BH0" "$BH1" "$BR0" "$BR1" "$INS0" "$INS1" "$DEL0" "$DEL1" "$RS0" "$RS1" <<'PY'
import sys
HR0,HR1,IR0,IR1,BH0,BH1,BR0,BR1,I0,I1,D0,D1,RS0,RS1=[int(x) for x in sys.argv[1:15]]
dt=30.0
mread=((HR1-HR0)+(IR1-IR0))*8192/dt/1e6
bh=BH1-BH0; br=BR1-BR0
hit=100.0*bh/(bh+br) if (bh+br) else 100
diskr=(RS1-RS0)*512/dt/1e6
print(f"messages disk reads    : {mread:,.1f} MB/s  (heap {(HR1-HR0)*8/dt/1e3:,.1f}k blks/s + idx {(IR1-IR0)*8/dt/1e3:,.1f}k blks/s)")
print(f"INSTANTANEOUS hit ratio: {hit:.3f} %   (vs cumulative 99.9%)")
print(f"disk READ throughput   : {diskr:,.1f} MB/s")
print(f"INSERTs/s={(I1-I0)/dt:,.0f}   DELETEs/s={(D1-D0)/dt:,.0f}   retention behind by {((I1-I0)-(D1-D0))/dt:,.0f}/s")
PY
