#!/usr/bin/env bash
# Broker-side: full Postgres-side measurement for ONE load window. Run
# CONCURRENTLY with goload (started on the loader). Captures the things that
# distinguish the real write bottleneck:
#   - wait-event breakdown (active backends) -> Lock vs LWLock:WALWrite/WALInsert
#     vs IO:WALSync vs IO:DataFileWrite vs BufferContent ...
#   - pg_stat_wal deltas: wal_bytes/s, fpi, buffers_full, wal_sync/s,
#     wal_sync_time, wal_write_time (needs track_wal_io_timing=on; we enable it)
#   - checkpointer/bgwriter deltas (checkpoint pressure)
#   - commits/s, messages inserts/s, partition_lookup upserts/s
#   - broker/PG container CPU + disk MB/s (/proc/diskstats)
# args: DUR LABEL
set -u
DUR="${1:-40}"; LABEL="${2:-run}"
PSQL(){ docker exec postgres psql -U postgres -d postgres -tAF'|' -c "$1" 2>/dev/null; }
PSQLX(){ docker exec postgres psql -U postgres -d postgres -tA -c "$1" 2>/dev/null; }

# Enable WAL/IO timing (reloadable) so pg_stat_io write_time/fsync_time populate.
PSQLX "ALTER SYSTEM SET track_io_timing=on; ALTER SYSTEM SET track_wal_io_timing=on; SELECT pg_reload_conf();" >/dev/null

# Host disk sectors-written (vda root + vdb data volume), *512 -> KB.
disk_kb(){ awk '$3=="vda"||$3=="vdb"{w+=$10} END{print w*512/1024}' /proc/diskstats 2>/dev/null; }

snap(){ # epoch|xact_commit|msg_ins|pl_upd|wal_lsn_bytes|io_writes|io_fsyncs|io_write_ms|io_fsync_ms|wal_fpi|wal_buf_full|ckpt|disk_kb
  local row wal io ck
  row=$(PSQL "SELECT (SELECT xact_commit FROM pg_stat_database WHERE datname='postgres'),
        COALESCE((SELECT n_tup_ins FROM pg_stat_user_tables WHERE schemaname='queen' AND relname='messages'),0),
        COALESCE((SELECT n_tup_upd FROM pg_stat_user_tables WHERE schemaname='queen' AND relname='partition_lookup'),0)")
  # WAL volume from the LSN (version-independent); fpi/buffers_full from pg_stat_wal.
  wal=$(PSQL "SELECT pg_wal_lsn_diff(pg_current_wal_lsn(),'0/0')::bigint, wal_fpi, wal_buffers_full FROM pg_stat_wal")
  # WAL fsync/write counts+time from pg_stat_io (PG16+: object='wal').
  io=$(PSQL "SELECT COALESCE(sum(writes),0), COALESCE(sum(fsyncs),0), COALESCE(round(sum(write_time)::numeric,1),0), COALESCE(round(sum(fsync_time)::numeric,1),0) FROM pg_stat_io WHERE object='wal'")
  [ -z "$io" ] && io="0|0|0|0"
  ck=$(PSQL "SELECT num_timed+num_requested FROM pg_stat_checkpointer" 2>/dev/null)
  [ -z "$ck" ] && ck=0
  # columns: ts | xc msg pl | wal_bytes fpi buffull | io_w io_fs io_wt io_ft | ck | disk
  local wb=$(echo "$wal"|cut -d'|' -f1) fpi=$(echo "$wal"|cut -d'|' -f2) bf=$(echo "$wal"|cut -d'|' -f3)
  local iw=$(echo "$io"|cut -d'|' -f1) ifs=$(echo "$io"|cut -d'|' -f2) iwt=$(echo "$io"|cut -d'|' -f3) ift=$(echo "$io"|cut -d'|' -f4)
  echo "$(date +%s)|$row|$wb|$iw|$ifs|$iwt|$ift|$fpi|$bf|$ck|$(disk_kb)"
}

A=$(snap)
WAITS=/tmp/waits_$LABEL.txt; : > "$WAITS"
end=$(( $(date +%s) + DUR ))
while [ "$(date +%s)" -lt "$end" ]; do
  PSQL "SELECT coalesce(wait_event_type,'CPU')||':'||coalesce(wait_event,'-') FROM pg_stat_activity WHERE state='active' AND pid<>pg_backend_pid()" >> "$WAITS"
  sleep 1
done
B=$(snap)
CPU=$(docker stats --no-stream --format '{{.Name}}={{.CPUPerc}}' queen postgres 2>/dev/null | tr '\n' ' ')

awk -F'|' -v L="$LABEL" -v a="$A" -v b="$B" -v cpu="$CPU" 'BEGIN{
  split(a,A,"|"); split(b,B,"|");
  el=B[1]-A[1]; if(el<=0)el=1;
  printf "=== PG measure [%s] dur=%ds | %s ===\n", L, el, cpu;
  printf "  commits/s      = %d\n", (B[2]-A[2])/el;
  printf "  msg inserts/s  = %d\n", (B[3]-A[3])/el;
  printf "  pl upserts/s   = %d\n", (B[4]-A[4])/el;
  printf "  WAL MB/s       = %.1f\n", (B[5]-A[5])/el/1048576;
  fs=(B[7]-A[7]);
  printf "  WAL fsync/s    = %d   WAL write/s = %d\n", fs/el, (B[6]-A[6])/el;
  printf "  WAL fsync_time/s = %.0f ms (%.3f ms/fsync)   write_time/s = %.0f ms\n",
         (B[9]-A[9])/el, (fs>0?(B[9]-A[9])/fs:0), (B[8]-A[8])/el;
  printf "  WAL fpi/s      = %d   buffers_full/s = %d\n", (B[10]-A[10])/el, (B[11]-A[11])/el;
  printf "  checkpoints    = %d (in window)\n", (B[12]-A[12]);
  printf "  disk write MB/s= %.1f\n", (B[13]-A[13])/el/1024;
}'
echo "  -- top active wait events (sampled $(wc -l < "$WAITS") times) --"
sort "$WAITS" | uniq -c | sort -rn | head -12 | sed 's/^/    /'
