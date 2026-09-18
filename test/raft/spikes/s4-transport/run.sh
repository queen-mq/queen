#!/usr/bin/env bash
# Spike S4 (PLAN_RAFT.md WP-0.6) — the whole configuration matrix.
#
#   bash run.sh                        # 60 s per configuration, one pass
#   SECS=10 ROUNDS=6 bash run.sh       # interleaved: 6 rounds of 10 s per config,
#                                      # so a noisy neighbour hits every config alike
#   SECS=10 bash run.sh                # short shakedown
#   OUT=/some/dir bash run.sh          # results elsewhere (default: results/<host tag>)
#
# NEVER edit this file while it is running: bash re-reads a script from a byte
# offset and a mid-run edit kills the run (it happened once, 2026-09-17).
#
# One leader process per configuration, started and killed by PID (never by
# port). Every run appends one JSON line to $OUT/results.jsonl and one line
# with the load averages to $OUT/runlog.txt.
set -u

HERE="$(cd "$(dirname "$0")" && pwd)"
BIN="${BIN:-$HERE/target/release/s4}"
TAG="${TAG:-laptop}"
OUT="${OUT:-$HERE/results/$TAG}"
SECS="${SECS:-60}"
ROUNDS="${ROUNDS:-1}"
WARMUP="${WARMUP:-5}"
PORT="${PORT:-6734}"
THREADS="${THREADS:-4}"
BATCH="${BATCH:-10}"
PAYLOAD="${PAYLOAD:-256}"
POOL="${POOL:-64}"
SECRET="${SECRET:-s4-spike-secret-not-a-real-key}"
# all | matrix (the six transport configurations) | sweep (connection count)
# | extra (the 2026-09-18 refutation cells) | pace (timer vs jitter-free pacer)
PART="${PART:-all}"

[ -x "$BIN" ] || { echo "build first: cargo build --release"; exit 1; }
mkdir -p "$OUT"
touch "$OUT/runlog.txt"

echo "# s4 matrix  host=$(hostname)  secs=$SECS  rounds=$ROUNDS  threads=$THREADS  batch=$BATCH  payload=$PAYLOAD" | tee -a "$OUT/runlog.txt"
uname -a >> "$OUT/runlog.txt"

one() { # transport tls mac rate conns label [pool] [pace]
  local T="$1" TLS="$2" MAC="$3" RATE="$4" CONNS="$5" LABEL="$6"
  local P="${7:-$POOL}" PACE="${8:-timer}"
  echo "--- $LABEL"
  "$BIN" leader --transport "$T" --port "$PORT" --secret "$SECRET" --mac "$MAC" \
      --tls "$TLS" --cert-out "$OUT/cert.der" --threads "$THREADS" \
      > "$OUT/leader-$LABEL.log" 2>&1 &
  local LP=$!
  sleep 1
  local L0; L0="$(uptime | sed 's/.*load averages*: //')"
  "$BIN" receiver --transport "$T" --addr "127.0.0.1:$PORT" --secret "$SECRET" --mac "$MAC" \
      --tls "$TLS" --cert "$OUT/cert.der" --rate "$RATE" --batch "$BATCH" --payload "$PAYLOAD" \
      --secs "$SECS" --warmup "$WARMUP" --conns "$CONNS" --pool "$P" --pace "$PACE" --threads "$THREADS" \
      2>> "$OUT/receiver-$LABEL.log" | grep '^RESULT ' | sed 's/^RESULT //' >> "$OUT/results.jsonl"
  local RC=$?
  local L1; L1="$(uptime | sed 's/.*load averages*: //')"
  kill "$LP" 2>/dev/null
  wait "$LP" 2>/dev/null
  echo "$LABEL rc=$RC load_before=[$L0] load_after=[$L1]" >> "$OUT/runlog.txt"
  tail -1 "$OUT/receiver-$LABEL.log"
  sleep 2
}

# PART=extra: the cells the 2026-09-18 refutation round needed and pass A/B did
# not have. Run with BIN pointing at the pass A binary to stay comparable.
if [ "$PART" = "extra" ]; then
  for RATE in 50000 20000; do
    K=$((RATE / 1000))
    # HTTPS *without* the per-request MAC: the like-for-like partner of
    # tcp+tls (pass A/B only ever ran http+tls WITH mac=1).
    one http 1 0 "$RATE" 2 "https-plain-${K}k" 64
  done
  # HTTP/1.1 held to the same socket count as the framed transport (2), so the
  # comparison is not framed-on-2 against HTTP-on-15-to-49.
  one http 0 0 50000 2 "http-pool2-50k" 2
  # control: the pass A/B configuration re-run today, same binary
  one tcp 0 0 50000 2 "tcp-control-50k"
  echo "extra done -> $OUT/results.jsonl"
  exit 0
fi

# PART=pace: does the framed CPU advantage survive a jitter-free pacer?
# Same four minutes, one binary, timer against spin.
if [ "$PART" = "pace" ]; then
  one tcp  0 0 50000 2 "pace-timer-tcp-50k"  64 timer
  one tcp  0 0 50000 2 "pace-spin-tcp-50k"   64 spin
  one http 0 0 50000 2 "pace-timer-http-50k" 64 timer
  one http 0 0 50000 2 "pace-spin-http-50k"  64 spin
  echo "pace done -> $OUT/results.jsonl"
  exit 0
fi

for ROUND in $(seq 1 "$ROUNDS"); do
  R=""
  [ "$ROUNDS" -gt 1 ] && R="-r$ROUND"
  if [ "$PART" != "sweep" ]; then
  for RATE in 20000 50000; do
    K=$((RATE / 1000))
    # (a) framed TCP, 2 persistent connections
    one tcp  0 0 "$RATE" 2 "tcp-plain-${K}k$R"
    one tcp  0 1 "$RATE" 2 "tcp-mac-${K}k$R"
    one tcp  1 0 "$RATE" 2 "tcp-tls-${K}k$R"
    # (b) HTTP/1.1 binary bodies over a hyper keep-alive pool
    one http 0 0 "$RATE" 2 "http-plain-${K}k$R"
    one http 0 1 "$RATE" 2 "http-mac-${K}k$R"
    one http 1 1 "$RATE" 2 "http-tls-mac-${K}k$R"
  done
  fi
  # connection-count sweep for the framed transport at the high rate
  if [ "$PART" != "matrix" ]; then
    one tcp 0 0 50000 1 "tcp-plain-50k-conns1$R"
    one tcp 0 0 50000 4 "tcp-plain-50k-conns4$R"
    one tcp 0 0 50000 8 "tcp-plain-50k-conns8$R"
  fi
done

echo "done -> $OUT/results.jsonl"
