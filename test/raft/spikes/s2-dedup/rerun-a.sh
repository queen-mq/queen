#!/usr/bin/env bash
# The two option (a) runs of run-laptop.sh, repeated after the file-GC fix
# (the harness counted a hash reference into every segment file for BOTH
# options, so option (a)'s files were never unlinked). Kept so the repeat is
# reproducible; run-laptop.sh is the full matrix.
set -u
cd "$(dirname "$0")" || exit 2
DATA=/var/tmp/s2-dedup; OUT=./results; BIN=./target/release/s2-dedup
COMMON="--engine fjall --rate 50000 --batch 10 --entry-appends 10 --payload 96 --partitions 4096 --segment-bytes 2097152 --fsync-mode data --fsync-threads 8 --duration 300"
rm -rf $DATA/a-short
echo "=== run a-short (file GC fixed) $(date +%H:%M:%S)"
$BIN run --dir $DATA/a-short $COMMON --option a --window-s 60 --txns-s 72 --retention-s 20 --dup-max-age-s 70 --label a-short > $OUT/a-short.log 2>&1
grep -E '^RESULT' $OUT/a-short.log
$BIN rebuild --dir $DATA/a-short --engine fjall --payload 96 --segment-bytes 2097152 --mode blooms --option a --window-s 60 > $OUT/a-short-rebuild-blooms.log 2>&1
grep -E '^RESULT' $OUT/a-short-rebuild-blooms.log
rm -rf $DATA/a-short
echo "=== run a-long $(date +%H:%M:%S)"
rm -rf $DATA/a-long
$BIN run --dir $DATA/a-long $COMMON --option a --window-s 300 --txns-s 360 --retention-s 60 --dup-max-age-s 350 --label a-long > $OUT/a-long.log 2>&1
grep -E '^RESULT' $OUT/a-long.log
$BIN rebuild --dir $DATA/a-long --engine fjall --payload 96 --segment-bytes 2097152 --mode blooms --option a --window-s 300 > $OUT/a-long-rebuild-blooms.log 2>&1
grep -E '^RESULT' $OUT/a-long-rebuild-blooms.log
rm -rf $DATA/a-long
echo "=== done $(date +%H:%M:%S)"
