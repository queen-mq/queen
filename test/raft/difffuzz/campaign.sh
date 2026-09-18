#!/usr/bin/env bash
#
# campaign.sh — run a seeded differential campaign of difffuzz against two
# brokers and collect every failing seed as a regression fixture (PLAN_RAFT.md
# §13.4: "a failing seed becomes a regression fixture").
#
# It does NOT start the brokers — that is deliberately the caller's job, because
# where the two brokers come from differs (a laptop with a throwaway Postgres
# container, the Linux VM, a CI lane). Bring up:
#
#   * side A, a POSTGRES broker (the oracle, D22): QUEEN_STORAGE=postgres,
#     pointed at a throwaway Postgres (a container on a port above 5480), on
#     e.g. :6632 — it applies its own schema at boot;
#   * side B, a RAFT broker: QUEEN_STORAGE=raft, QUEEN_RAFT_DIR=<dir>, no
#     Postgres, on e.g. :7632.
#
# Set DEFAULT_SUBSCRIPTION_MODE=new on BOTH (the shipped default; the raft facade
# forces "new" for a named group in phase 1, so matching it keeps the compare
# clean), JWT_ENABLED=false and a bind on 127.0.0.1. Then:
#
#   ./campaign.sh -a http://127.0.0.1:6632 -b http://127.0.0.1:7632 -n 2000
#
# Each seed runs with a FRESH run-id (fresh queue/group names) so a retry can
# never collide with the broker's hour-long transaction-id dedup window. A seed
# that diverges (exit 1) has its report written to the fixtures directory; that
# report carries the exact replay command. A seed that could-not-run (exit 2, a
# broker down) is counted and retried once. Standard tools only.
set -u

DF_DIR="$(cd "$(dirname "$0")" && pwd)"
A=""; B=""; N=2000; OPS=250; START=1; OUT="$DF_DIR/fixtures"; MIX=""; BIN=""
while [ $# -gt 0 ]; do
  case "$1" in
    -a) A="$2"; shift 2;;
    -b) B="$2"; shift 2;;
    -n) N="$2"; shift 2;;
    -ops) OPS="$2"; shift 2;;
    -start) START="$2"; shift 2;;
    -out) OUT="$2"; shift 2;;
    -mix) MIX="$2"; shift 2;;
    -bin) BIN="$2"; shift 2;;   # a prebuilt difffuzz binary; else `go run .`
    -h|--help) sed -n '2,30p' "$0"; exit 0;;
    *) echo "campaign.sh: unknown flag $1" >&2; exit 2;;
  esac
done
[ -z "$A" ] || [ -z "$B" ] && { echo "campaign.sh: -a and -b are required" >&2; exit 2; }
mkdir -p "$OUT"

run() {
  if [ -n "$BIN" ]; then "$BIN" "$@"; else ( cd "$DF_DIR" && GOWORK=off go run . "$@" ); fi
}
mixarg=(); [ -n "$MIX" ] && mixarg=(-mix "$MIX")

clean=0; div=0; cnr=0; s=$START; last=$((START + N - 1))
while [ $s -le $last ]; do
  out=$(run -a "$A" -b "$B" -seed "$s" -ops "$OPS" -stop-on-diff=true "${mixarg[@]}" 2>&1); rc=$?
  if [ $rc -eq 2 ]; then
    sleep 1
    out=$(run -a "$A" -b "$B" -seed "$s" -ops "$OPS" -stop-on-diff=true "${mixarg[@]}" 2>&1); rc=$?
  fi
  case $rc in
    0) clean=$((clean+1));;
    1) div=$((div+1)); echo "$out" > "$OUT/seed-$s.txt"
       echo "seed $s DIVERGENCE: $(echo "$out" | grep DIVERGENCE | head -1 | sed 's/^DIVERGENCE //')";;
    *) cnr=$((cnr+1)); echo "seed $s: could not run (broker down?)";;
  esac
  [ $(( (s - START + 1) % 50 )) -eq 0 ] && echo "… progress: attempted=$((s-START+1)) clean=$clean diverged=$div could-not-run=$cnr"
  s=$((s+1))
done
echo "=================================================================="
echo "campaign done: attempted=$((last-START+1)) clean=$clean diverged=$div could-not-run=$cnr"
echo "fixtures (failing seeds) in: $OUT"
[ $div -gt 0 ] && exit 1 || exit 0
