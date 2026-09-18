#!/usr/bin/env bash
# S2 (WP-0.4) refutation re-checks, laptop half (2026-09-18).
#
# Three questions the adversarial review of MEMO.md raised that a laptop cell
# can answer in minutes, all at the SHORT window scale of run-laptop.sh (60 s
# dedup / 72 s txns / 20 s retention) so a 120 s run spends 40 % of its life in
# the pruning regime, on heed (D9's engine):
#
#   1. option (a) with the (created_at,pid,hash) expiry index REMOVED
#      (`--option a-lean`: one sequential txns row per Append instead), which
#      the review says was the obvious middle ground and was never run;
#   2. option (b)'s ack-by-hash resolved the way 005 resolves it — the whole
#      p_hashes array of ONE ack in ONE pass (`--ack-batch 10`) — against the
#      campaign's one-hash-per-call workload (`--ack-batch 1`);
#   3. option (b)'s restart exactness with the corrected sample selection,
#      which no option-(b) cell ever ran (the campaign's b cells used the
#      pre-fix selection and their data dirs are gone).
#
# macOS numbers are for the design-vs-design comparison only (§0.3): every
# absolute figure to quote comes from the VM (refutation-vm.sh).
set -u
cd "$(dirname "$0")" || exit 2
DUR=${DUR:-120}
DATA=${DATA:-/private/tmp/claude-502/-Users-alice-Work-queen/e561891e-3c5f-4589-8141-dd6b961a2d37/scratchpad/s2-memo/data}
OUT=${OUT:-./results/refutation}
BIN=./target/release/s2-dedup
[ -x "$BIN" ] || { echo "build first: cargo build --release"; exit 2; }
mkdir -p "$OUT" "$DATA"

COMMON="--engine heed --rate 50000 --batch 10 --entry-appends 10 --payload 96 \
 --partitions 4096 --segment-bytes 2097152 --fsync-mode data --fsync-threads 8 --duration $DUR"
SHORT="--window-s 60 --txns-s 72 --retention-s 20 --dup-max-age-s 70"

run() { local name=$1; shift; local d="$DATA/$name"; rm -rf "$d"
  echo "=== run $name $(date +%H:%M:%S)"
  # shellcheck disable=SC2086
  "$BIN" run --dir "$d" $COMMON $SHORT "$@" --label "$name" >"$OUT/$name.log" 2>&1
  echo "    rc=$?"; grep -E '^RESULT' "$OUT/$name.log" || true; }
rebuild() { local name=$1; shift; echo "=== rebuild $name $(date +%H:%M:%S)"
  # shellcheck disable=SC2086
  "$BIN" rebuild --dir "$DATA/$name" --engine heed --payload 96 --segment-bytes 2097152 \
    --mode blooms --window-s 60 "$@" >"$OUT/$name-rebuild-blooms.log" 2>&1
  echo "    rc=$?"; grep -E '^RESULT' "$OUT/$name-rebuild-blooms.log" || true; }
drop() { rm -rf "$DATA/$1"; }

run  r-a       --option a
rebuild r-a    --option a
drop r-a
run  r-alean   --option a-lean
rebuild r-alean --option a-lean
drop r-alean
run  r-b-ack1  --option b --cache-mb 64 --ack-batch 1
rebuild r-b-ack1 --option b --cache-mb 64
drop r-b-ack1
run  r-b-ack10 --option b --cache-mb 64 --ack-batch 10
rebuild r-b-ack10 --option b --cache-mb 64
drop r-b-ack10
run  r-alean-ack10 --option a-lean --ack-batch 10
drop r-alean-ack10
echo "=== done $(date +%H:%M:%S)"
