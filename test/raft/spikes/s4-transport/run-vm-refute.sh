#!/usr/bin/env bash
# Spike S4, refutation round (2026-09-18): the cells passes A and B did not
# have. Runs on the Linux VM (§13.6 host), ~9 minutes, two phases.
#
#   ssh root@164.90.215.224 'mkdir -p /root/raft/wp06-refute'
#   rsync -az --exclude target --exclude results \
#       test/raft/spikes/s4-transport/ \
#       root@164.90.215.224:/root/raft/wp06-refute/s4-transport/
#   ssh root@164.90.215.224 'nohup bash /root/raft/wp06-refute/s4-transport/run-vm-refute.sh \
#       > /root/raft/wp06-refute/vm.log 2>&1 &'
#
# Phase 1 (results/vm-c/) runs on the **pass A binary**
# (/root/raft/wp-0.6/s4-transport/target/release/s4, built 2026-09-17 15:08),
# unchanged, so its rows are directly comparable with results/vm{,-b}/:
#   - HTTPS *without* the per-request MAC — the like-for-like partner of
#     tcp+tls that the matrix never had (the "0.32x" headline);
#   - HTTP/1.1 capped at 2 pooled connections — socket-matched against framed;
#   - framed TCP @50k/2conn again, as a control on the day.
# Phase 2 (results/vm-pace/) rebuilds with the added `--pace spin` receiver
# option and runs framed and HTTP under both pacers, to separate the framed
# transport's write coalescing from the harness's ~1 ms timer bursts.
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
OLDBIN="${OLDBIN:-/root/raft/wp-0.6/s4-transport/target/release/s4}"
. "$HOME/.cargo/env" 2>/dev/null || true
say() { echo "$(date -u +%FT%TZ) $*"; }

say "host"; uptime; grep -c sha_ni /proc/cpuinfo | sed 's/^/sha_ni_lines=/'

OUT1="$HERE/results/vm-c"; mkdir -p "$OUT1"
{ uname -a; nproc; grep -m1 'model name' /proc/cpuinfo; uptime
  echo "binary=$OLDBIN"; ls -l --time-style=full-iso "$OLDBIN"; md5sum "$OLDBIN"; } > "$OUT1/host.txt" 2>&1
cat "$OUT1/host.txt"
say "phase 1: missing cells on the pass A binary (SECS=60)"
( cd "$HERE" && BIN="$OLDBIN" OUT="$OUT1" SECS=60 PORT="${PORT:-6754}" PART=extra bash run.sh ) > "$OUT1/run.log" 2>&1
say "phase 1 rc=$? rows=$(wc -l < "$OUT1/results.jsonl" 2>/dev/null)"

say "phase 2: build the pacer binary"
( cd "$HERE" && cargo build --release ) > "$HERE/build.log" 2>&1 || { say "BUILD FAILED"; tail -20 "$HERE/build.log"; exit 2; }
( cd "$HERE" && cargo test --release ) > "$HERE/test.log" 2>&1; say "unit tests rc=$? ($(grep -c 'test result: ok' "$HERE/test.log") ok lines)"
OUT2="$HERE/results/vm-pace"; mkdir -p "$OUT2"
say "phase 2: timer vs spin pacer (SECS=40)"
( cd "$HERE" && OUT="$OUT2" SECS=40 PORT="${PORT:-6754}" PART=pace bash run.sh ) > "$OUT2/run.log" 2>&1
say "phase 2 rc=$? rows=$(wc -l < "$OUT2/results.jsonl" 2>/dev/null)"
pgrep -af 's4 leader' || say "no leader left running"
say "done"
