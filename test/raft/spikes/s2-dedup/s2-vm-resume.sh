#!/usr/bin/env bash
# WP-0.4 part 2, continuation (2026-09-18). Two things the 2026-09-17 campaign
# (s2-vm.sh) could not deliver, with its parameters unchanged:
#
#   1. the a-heed cell, which died at once on MDB_BAD_VALSIZE (option (a)'s
#      prune walks its expiry index from the empty key, and LMDB rejects a
#      zero-length key; fixed in src/engines/heed_eng.rs);
#   2. a rebuild that does not report wrong=76..302 samples. The pre-fix
#      harness saved the FIRST 4096 ack-probe samples and never noticed when
#      the run itself re-pushed one of those hashes out of window, which gives
#      the hash a second, younger occurrence and makes the saved expectation
#      stale. `--legacy-samples true` restores the old behaviour so the
#      artefact can be shown, then the fixed selection shows it gone.
#
# Log: /root/raft/s2-dedup/vm-resume.log
set -u
. ~/.cargo/env
cd /root/raft/s2-dedup || exit 2
echo "=== build $(date -u +%FT%TZ)"
cargo build --release > build-resume.log 2>&1 || { echo "BUILD FAILED (see build-resume.log)"; exit 2; }
BIN=./target/release/s2-dedup
DATA=/root/raft/s2-dedup/data; OUT=/root/raft/s2-dedup/results/vm; mkdir -p "$DATA" "$OUT"
DUR=${DUR:-600}
# identical to s2-vm.sh
WIN="--window-s 360 --txns-s 432 --retention-s 72 --dup-max-age-s 420"
run() { local name=$1 eng=$2; shift 2; local d="$DATA/$name"; rm -rf "$d"
  echo "=== run $name engine=$eng $(date -u +%FT%TZ)"
  "$BIN" run --dir "$d" --engine "$eng" --rate 50000 --batch 10 --entry-appends 10 --payload 96 \
    --partitions 4096 --segment-bytes 2097152 --fsync-mode data --fsync-threads 8 --duration "$DUR" \
    $WIN "$@" --label "$name" > "$OUT/$name.log" 2>&1
  echo "    rc=$?"; grep -E '^RESULT' "$OUT/$name.log" || true; }
rebuild() { local name=$1 eng=$2 mode=$3; shift 3
  echo "=== rebuild $name mode=$mode $(date -u +%FT%TZ)"
  "$BIN" rebuild --dir "$DATA/$name" --engine "$eng" --payload 96 --segment-bytes 2097152 --mode "$mode" --window-s 360 "$@" > "$OUT/$name-rebuild-$mode.log" 2>&1
  echo "    rc=$?"; grep -E '^RESULT' "$OUT/$name-rebuild-$mode.log" || true; }

# (1) the missing cell
run a-heed heed --option a
rebuild a-heed heed blooms --option a
rm -rf "$DATA/a-heed"

# (2) the rebuild check itself, on scaled windows so a 2-minute run has the same
# shape as a 10-minute one at the campaign's windows: the duplicate ages (90 s)
# reach far past the dedup window (40 s), and the run outlives window + txns, so
# a hash re-pushed mid-run still has a YOUNG occurrence at the end while the
# sample's own occurrence is long expired.
SWIN="--window-s 40 --txns-s 50 --retention-s 10 --dup-max-age-s 90"
sample_cell() { local name=$1 legacy=$2; local d="$DATA/$name"; rm -rf "$d"
  echo "=== run $name legacy-samples=$legacy $(date -u +%FT%TZ)"
  "$BIN" run --dir "$d" --engine fjall --option a --rate 50000 --batch 10 --entry-appends 10 \
    --payload 96 --partitions 4096 --segment-bytes 2097152 --fsync-mode data --fsync-threads 8 \
    --duration 120 $SWIN --legacy-samples "$legacy" --label "$name" > "$OUT/$name.log" 2>&1
  echo "    rc=$?"
  echo "=== rebuild $name $(date -u +%FT%TZ)"
  "$BIN" rebuild --dir "$d" --engine fjall --option a --payload 96 --segment-bytes 2097152 \
    --mode blooms --window-s 40 > "$OUT/$name-rebuild-blooms.log" 2>&1
  echo "    rc=$?"; grep -E '^RESULT|samples checked' "$OUT/$name-rebuild-blooms.log" || true
  rm -rf "$d"; }
sample_cell samples-legacy true
sample_cell samples-fixed  false

echo "=== done $(date -u +%FT%TZ)"; pgrep -af s2-dedup/target | grep -v pgrep || echo "nothing left running"
