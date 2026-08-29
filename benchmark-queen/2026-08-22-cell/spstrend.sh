#!/bin/bash
# Windowed cost of the maintenance SPs against segment count.
#
# pg_stat_statements reports mean_exec_time CUMULATIVELY since reset, which
# blurs exactly the transition we are hunting: a cliff at 1.2-1.7M segments
# shows up in the cumulative mean only long after it starts. Recording raw
# calls + total_exec_time lets the mean be differenced per window:
#     window_mean = (total2 - total1) / (calls2 - calls1)
PGU=queenadm; PGP=54987
Q="SELECT count(*) FROM queen.log_segments"
S="SELECT COALESCE(sum(calls),0)||','||COALESCE(round(sum(total_exec_time)::numeric,1),0)
   FROM pg_stat_statements WHERE query ILIKE"
echo "ts,segments,reseed_calls,reseed_ms,qstats_calls,qstats_ms,refresh_calls,refresh_ms,pop_calls,pop_ms"
while true; do
  SEG=$(docker exec cell-pg psql -U $PGU -p $PGP -d queen -qtAX -c "$Q" 2>/dev/null)
  # Track the statement the broker ACTUALLY calls. Since 2026-08-23 both the
  # full and windowed walks run log_hotlist_reseed_window_v1 (full pins p_cutoff
  # to '-infinity'); log_hotlist_reseed_v1 is dead code on this path, so grepping
  # for it silently records zeros.
  R=$(docker exec cell-pg psql -U $PGU -p $PGP -d queen -qtAX -c "$S '%log_hotlist_reseed_window_v1%'" 2>/dev/null)
  G=$(docker exec cell-pg psql -U $PGU -p $PGP -d queen -qtAX -c "$S '%log_queue_stats_all_v1%'" 2>/dev/null)
  F=$(docker exec cell-pg psql -U $PGU -p $PGP -d queen -qtAX -c "$S '%log_refresh_all_stats_v1%'" 2>/dev/null)
  P=$(docker exec cell-pg psql -U $PGU -p $PGP -d queen -qtAX -c "$S '%t.blobs, (t.states)%'" 2>/dev/null)
  [ -n "$SEG" ] && echo "$(date -u +%FT%TZ),$SEG,$R,$G,$F,$P"
  sleep 120
done
