#!/usr/bin/env bash
# Spike S4 on the Linux VM (PLAN_RAFT.md §13.6 host, WP-0.6 part 2).
#
# Runs on the VM, not on the laptop. Copy the harness over first:
#
#   ssh root@164.90.215.224 'mkdir -p /root/raft/<task>/s4-transport'
#   rsync -az --exclude target --exclude node_modules --exclude results \
#       test/raft/spikes/s4-transport/ root@164.90.215.224:/root/raft/<task>/s4-transport/
#   ssh root@164.90.215.224 'nohup bash /root/raft/<task>/s4-transport/run-vm.sh \
#       > /root/raft/<task>/vm.log 2>&1 &'
#
# WAIT_PID=<pid>  first waits for that process to exit (the VM is serialized:
#                 never measure next to another phase-0 job).
# OUT=<dir>       results directory (default results/vm).
# SECS/ROUNDS     as in run.sh (default: one contiguous 60 s per configuration).
#
# It builds --release, records the host facts that the MAC-vs-TLS decision
# needs (CPU model, sha_ni, aes), runs the matrix, and kills nothing it did
# not start (run.sh kills its own leader by PID).
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
OUT="${OUT:-$HERE/results/vm}"
SECS="${SECS:-60}"
ROUNDS="${ROUNDS:-1}"
WAIT_PID="${WAIT_PID:-}"
. "$HOME/.cargo/env" 2>/dev/null || true

mkdir -p "$OUT"
say() { echo "$(date -u +%FT%TZ) $*"; }

if [ -n "$WAIT_PID" ]; then
  say "waiting for pid $WAIT_PID to exit (max 60 min)"
  for _ in $(seq 1 360); do kill -0 "$WAIT_PID" 2>/dev/null || break; sleep 10; done
  kill -0 "$WAIT_PID" 2>/dev/null && { say "pid $WAIT_PID still alive after 60 min: refusing to measure next to it"; exit 3; }
  say "pid $WAIT_PID gone; settling 30 s"; sleep 30
fi

{ say "host facts"; uname -a; nproc; grep -m1 'model name' /proc/cpuinfo
  echo "sha_ni_lines=$(grep -c sha_ni /proc/cpuinfo) aes_lines=$(grep -c ' aes ' /proc/cpuinfo)"
  rustc -V; uptime; } > "$OUT/host.txt" 2>&1
cat "$OUT/host.txt"

say "building"
( cd "$HERE" && cargo build --release ) > "$OUT/build.log" 2>&1 || { say "BUILD FAILED"; tail -20 "$OUT/build.log"; exit 2; }
say "unit tests"
( cd "$HERE" && cargo test --release ) > "$OUT/test.log" 2>&1; say "tests rc=$? ($(grep -c 'test result: ok' "$OUT/test.log") ok lines)"

say "matrix SECS=$SECS ROUNDS=$ROUNDS -> $OUT"
( cd "$HERE" && OUT="$OUT" SECS="$SECS" ROUNDS="$ROUNDS" PORT="${PORT:-6744}" bash run.sh ) > "$OUT/matrix.log" 2>&1
say "matrix rc=$? rows=$(wc -l < "$OUT/results.jsonl" 2>/dev/null)"
pgrep -af 's4 leader' || say "no leader left running"
say "done"
