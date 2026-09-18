#!/usr/bin/env bash
# S2 (WP-0.4) refutation re-checks, VM half (2026-09-18). Runs on
# queenpgless-01 under /root/raft/s2-dedup. Five 120 s cells, ~10 minutes.
#
# Campaign windows throughout (dedup 360 s, txns 432 s, retention 72 s), so
# every figure is comparable with the 600 s cells of RESULTS-vm.md §1 minute
# by minute. 120 s is inside the growth phase: no cell reaches the prune.
#
#   1-3  a-heed at three durable-point cadences (250 / 1000 / 4000 ms). The
#        memo blamed 1227 ms mean `mdb_env_sync` on "a 3.1 GB store". heed is
#        opened MDB_NOSYNC, so a sync flushes the pages dirtied SINCE THE LAST
#        ONE: if that is the mechanism, the cost per sync tracks the cadence
#        and the cost per second does not. This is the measurement that says
#        which, and §11.4 explicitly allows bounding the durable point.
#   4    a-lean at the reference cadence: option (a) with the
#        (created_at,pid,hash) expiry index replaced by one sequential txns
#        row per Append. The review's "leaner (a), never run".
#   5    b-heed with 005's batching (--ack-batch 10) and a rebuild, which is
#        also option (b)'s first restart-exactness check with the corrected
#        sample selection on this host.
set -u
. ~/.cargo/env
cd /root/raft/s2-dedup || exit 2
echo "=== build $(date -u +%FT%TZ)"
cargo build --release > build-refutation.log 2>&1 || { echo "BUILD FAILED"; tail -20 build-refutation.log; exit 2; }
BIN=./target/release/s2-dedup
DATA=/root/raft/s2-dedup/data; OUT=/root/raft/s2-dedup/results/vm; mkdir -p "$DATA" "$OUT"
DUR=${DUR:-120}
WIN="--window-s 360 --txns-s 432 --retention-s 72 --dup-max-age-s 420"
run() { local name=$1; shift; local d="$DATA/$name"; rm -rf "$d"
  echo "=== run $name $(date -u +%FT%TZ)"
  # shellcheck disable=SC2086
  "$BIN" run --dir "$d" --engine heed --rate 50000 --batch 10 --entry-appends 10 --payload 96 \
    --partitions 4096 --segment-bytes 2097152 --fsync-mode data --fsync-threads 8 --duration "$DUR" \
    $WIN "$@" --label "$name" > "$OUT/$name.log" 2>&1
  echo "    rc=$?"; grep -E '^RESULT' "$OUT/$name.log" || true; }
rebuild() { local name=$1; shift; echo "=== rebuild $name $(date -u +%FT%TZ)"
  # shellcheck disable=SC2086
  "$BIN" rebuild --dir "$DATA/$name" --engine heed --payload 96 --segment-bytes 2097152 \
    --mode blooms --window-s 360 "$@" > "$OUT/$name-rebuild-blooms.log" 2>&1
  echo "    rc=$?"; grep -E '^RESULT|samples checked' "$OUT/$name-rebuild-blooms.log" || true; }

run v-a-dms250      --option a --durable-ms 250
rm -rf "$DATA/v-a-dms250"
run v-a-dms1000     --option a --durable-ms 1000
rm -rf "$DATA/v-a-dms1000"
run v-a-dms4000     --option a --durable-ms 4000
rm -rf "$DATA/v-a-dms4000"
run v-alean-dms1000 --option a-lean --durable-ms 1000
rebuild v-alean-dms1000 --option a-lean
rm -rf "$DATA/v-alean-dms1000"
run v-b-ack10       --option b --cache-mb 64 --ack-batch 10 --durable-ms 1000
rebuild v-b-ack10   --option b --cache-mb 64
rm -rf "$DATA/v-b-ack10"

echo "=== done $(date -u +%FT%TZ)"
pgrep -af s2-dedup/target | grep -v pgrep || echo "nothing left running"
