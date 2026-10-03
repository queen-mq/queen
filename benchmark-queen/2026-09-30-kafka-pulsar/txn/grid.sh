#!/usr/bin/env bash
# txn/grid.sh (Mac) — the TRANSACTIONAL matrix as txn/run.sh calls, partition-first, one system at a time (only one may
# listen on the brokers). A point whose runs/txn/<system>/<tag>/DONE exists is skipped, so re-running resumes after a
# failure or an interruption; a FAILED point simply runs again. Tags name the shape: 1x<partitions>-t<txn size>-<rate>k,
# so a point shared by two series (B's 200 partitions = S's 10 per transaction) runs once.
#   B  partitions B_PARTS (200, 10k, 100k; in AND out: 2 x P in the cluster), 10 messages per transaction, RATE msg/s
#   S  messages per transaction 1, 10, 100 at 200 partitions, RATE msg/s offered
#   C  ceiling ramp at 200 partitions, 10 per transaction: CEILING rates, ascending; a system's ramp stops after the
#      first point it did not carry (out/s < 90% of offered, or more than 1% of the inputs still unprocessed at the end)
#   W  more workers at 200 partitions (W_CONS per process, W_RATES): C's ceiling is one open transaction per partition,
#      not the 198 workers (10-01: Queen leases a partition to one consumer of a group at a time, Pulsar's failover
#      slice gives a partition one active consumer), so W measures what workers beyond the partitions add. Queen
#      only: a pload worker beyond the partitions owns none (txn.go), so a Pulsar W point is the C point again
#   L  the ceiling with more lanes: L_PARTS partitions and L_CONS workers per process (one partition per worker when
#      L_PARTS = L_CONS x 9), L_TXN (10) per transaction, L_RATES ascending, stops at the first miss as C. L_TAGX is appended
#      to the tags (e.g. -pw1-b100 for a run with POP_WIDTH=1 BATCH=100 in the environment)
# Env:
#   SYSTEMS="kafka redpanda pulsar queen"  in this order (default)      POINTS="B S C" (default)
#   RATE=9000                  the fixed offered rate of B and S (msg/s in total: 1000 per process). Why (10-01 smokes, 200
#                              partitions, 10 per transaction, 20k offered): Kafka committed 16-18k (classic groups; KIP-848
#                              15k), Redpanda 19k, Pulsar 20k, Queen 20k. At 200 partitions 198 workers hold them, so two
#                              workers own two partitions each and were the first behind on Kafka and Redpanda (e2e p99
#                              26-46 s from those two). A Kafka worker that is behind starts each transaction right after
#                              the previous commit, and transactions v2 holds that first produce until the previous
#                              transaction's markers are written (commit p50 31 -> 115 ms at 20k): Kafka is bistable near
#                              its ceiling. 9k is half of the weakest system's carry and a two-partition worker then gets
#                              90 msg/s; Kafka at 9k (smoke3): commit p50/p99 31/54 ms, e2e p99 0.5 s, verifier PASS.
#   CEILING="9000 18000 36000 72000 144000 288000 576000"   the C ramp, doubling (each system stops at its first miss)
#   B_PARTS="200 10000 100000" the partition counts of B (each of in and out). 100k = 200k partitions in the cluster:
#                              Redpanda measured ~155-180 KB per replica at rest (redpanda/mkconf.sh) = ~34 GB of the
#                              ~29 GiB it holds, and the 09-30 Pulsar 100k point already FAILED; B_PARTS="200 10000 50000"
#                              is the fallback
#   S100_LINGER=3s             TXN_LINGER of the 100-per-transaction point: at 9k msg/s a worker gets ~45 msg/s, so 100
#                              messages take ~2.2 s to arrive (the other points keep txn/run.sh's 1 s)
#   DRY=1                      print the txn/run.sh calls, run nothing
# Every other variable (HOSTS_ENV, TXN_LINGER, LEASE, ...) passes through to txn/run.sh. After the redpanda series the
# Redpanda OS tuning is undone (redpanda/cluster.sh untune), as SPEC §0 requires before another system runs.
# Duration: a 200-partition point took 3-5 min on 10-01 (Queen 172 s, Kafka 189-232 s, Redpanda 247 s, Pulsar 309 s:
# reset, create + warm, READY, 70 s, drain, verify, collect, stop); 10k points add the creation of 20k partitions, 100k
# points of 200k (Kafka's 09-30 100k took ~6 min to READY; Pulsar 15+ min). The whole grid: ~4.5-5.5 h.
set -u
H=$(cd "$(dirname "$0")/.." && pwd)
RUNS=$H/runs/txn
RATE=${RATE:-9000}
CEILING=${CEILING:-"9000 18000 36000 72000 144000 288000 576000"}
B_PARTS=${B_PARTS:-"200 10000 100000"}
S100_LINGER=${S100_LINGER:-3s}
W_CONS=${W_CONS:-44}; W_RATES=${W_RATES:-"144000"}
L_PARTS=${L_PARTS:-900}; L_CONS=${L_CONS:-100}; L_RATES=${L_RATES:-"144000 288000 576000"}; L_TXN=${L_TXN:-10}
log() { echo "[$(date -u +%FT%TZ)] txn grid: $*"; }
OKS=""; FAILS=""; SKIPS=""; LISTED=""

pt() {  # pt <system> <tag> <total rate> [KEY=VAL ...]
  local sys=$1 tag=$2 rc; shift 2
  if [ -f "$RUNS/$sys/$tag/DONE" ]; then log "skip $sys/$tag (DONE)"; SKIPS="$SKIPS $sys/$tag"; return 0; fi
  if [ "${DRY:-0}" = 1 ]; then
    case " $LISTED " in *" $sys/$tag "*) echo "  (txn/run.sh $sys $tag: listed above, runs once)";; *) echo "  txn/run.sh $sys $tag $*"; LISTED="$LISTED $sys/$tag";; esac
    return 0
  fi
  log "point $sys/$tag: $*"
  bash "$H/txn/run.sh" "$sys" "$tag" "$@"; rc=$?
  if [ -f "$RUNS/$sys/$tag/DONE" ]; then OKS="$OKS $sys/$tag"; log "point $sys/$tag DONE: $(grep -E '^verifier:' "$RUNS/$sys/$tag/summary.txt" 2>/dev/null)"
  else FAILS="$FAILS $sys/$tag"; log "point $sys/$tag FAILED rc=$rc: $(tail -1 "$RUNS/$sys/$tag/FAILED" 2>/dev/null)"; fi
}

k() { echo "$(($1 / 1000))k"; }

big() {  # big <system> <partitions>: the 09-30 matrix's budgets for 50k+ partitions (here in + out = 2 x partitions)
  [ "$2" -ge 50000 ] || return 0
  case $1 in
    kafka|redpanda) echo "READY_TIMEOUT=900 LX=-group-timeout=600s" ;;
    *) echo "READY_TIMEOUT=900" ;;
  esac
}
series_B() {  # partitions, 10 per transaction, the fixed rate
  local s=$1 p
  for p in $B_PARTS; do
    pt "$s" "1x$p-t10-$(k "$RATE")" "$RATE" PARTITIONS=$p TXN_SIZE=10 $(big "$s" "$p")
  done
}
series_S() {  # transaction size at 200 partitions, the fixed rate; 100 per transaction waits up to S100_LINGER to fill
  local s=$1 t
  for t in 1 10 100; do
    if [ "$t" = 100 ]; then
      pt "$s" "1x200-t$t-$(k "$RATE")" "$RATE" PARTITIONS=200 TXN_SIZE=$t TXN_LINGER="$S100_LINGER"
    else
      pt "$s" "1x200-t$t-$(k "$RATE")" "$RATE" PARTITIONS=200 TXN_SIZE=$t
    fi
  done
}
series_C() {  # ceiling ramp at 200 partitions, 10 per transaction; stops after the first point the system did not carry
  local s=$1 r d
  for r in $CEILING; do
    pt "$s" "1x200-t10-$(k "$r")" "$r" PARTITIONS=200 TXN_SIZE=10
    [ "${DRY:-0}" = 1 ] && continue
    d=$RUNS/$s/1x200-t10-$(k "$r")
    [ -f "$d/DONE" ] || { log "C: $s at $r msg/s did not finish: ramp stopped"; break; }
    if ! carried "$d" "$r"; then log "C: $s did not carry $r msg/s: ramp stopped"; break; fi
  done
}

series_W() {  # more workers than partitions at 200 partitions (Queen only, see the header)
  local s=$1 r
  [ "$s" = queen ] || { log "W: $s skipped (a worker beyond the partitions owns none)"; return 0; }
  for r in $W_RATES; do
    pt "$s" "1x200-t10-$(k "$r")-w$((W_CONS * 9))" "$r" PARTITIONS=200 TXN_SIZE=10 CONSUMERS=$W_CONS
  done
}
series_L() {  # the ceiling with L_PARTS lanes and L_CONS workers per process; stops after the first miss
  local s=$1 r d tag
  for r in $L_RATES; do
    tag="1x$L_PARTS-t$L_TXN-$(k "$r")-w$((L_CONS * 9))${L_TAGX:-}"
    pt "$s" "$tag" "$r" PARTITIONS=$L_PARTS TXN_SIZE=$L_TXN CONSUMERS=$L_CONS READY_TIMEOUT=900
    [ "${DRY:-0}" = 1 ] && continue
    d=$RUNS/$s/$tag
    [ -f "$d/DONE" ] || { log "L: $s at $r msg/s did not finish: ramp stopped"; break; }
    if ! carried "$d" "$r"; then log "L: $s did not carry $r msg/s: ramp stopped"; break; fi
  done
}

carried() {  # carried <run dir> <offered>: out/s >= 90% of offered and <= 1% of the inputs left unprocessed
  python3 - "$H" "$1" "$2" <<'PY'
import json, os, subprocess, sys
H, d, offered = sys.argv[1], sys.argv[2], float(sys.argv[3])
out = subprocess.run([sys.executable, os.path.join(H, "report", "report.py"), "--txn", "--csv", d], capture_output=True, text=True).stdout.splitlines()
ok = True
if len(out) >= 2:
    cols, row = out[0].split(","), out[1].split(",")
    v = row[cols.index("out/s")]
    outs = float(v[:-1]) * 1000 if v.endswith("k") else float(v)
    ok = outs >= 0.9 * offered
try:
    vj = json.load(open(os.path.join(d, "verify.json")))
    if vj.get("produced", 0) and vj.get("pending_in", 0) > 0.01 * vj["produced"]:
        ok = False
except (OSError, ValueError):
    pass
sys.exit(0 if ok else 1)
PY
}

main() {
  local s p T0; T0=$(date +%s)
  mkdir -p "$RUNS"
  log "systems ${SYSTEMS:-kafka redpanda pulsar queen}, series ${POINTS:-B S C}, RATE=$RATE, B_PARTS=$B_PARTS, CEILING=$CEILING, W=${W_CONS}x9 at $W_RATES, L=${L_PARTS}p ${L_CONS}x9 at $L_RATES$([ "${DRY:-0}" = 1 ] && echo ' DRY')"
  for s in ${SYSTEMS:-kafka redpanda pulsar queen}; do
    case $s in kafka|redpanda|pulsar|queen) ;; *) log "unknown system $s"; return 2;; esac
    for p in ${POINTS:-B S C}; do
      case $p in B|S|C|W|L) log "== $s series $p"; "series_$p" "$s" ;; *) log "unknown series $p (B S C W L)"; return 2;; esac
    done
    if [ "$s" = redpanda ] && [ "${DRY:-0}" != 1 ]; then
      log "redpanda series over: stop + untune (the OS back to the SPEC os-tune state)"
      bash "$H/redpanda/cluster.sh" stop > /dev/null 2>&1
      bash "$H/redpanda/cluster.sh" untune 2>&1 | tail -3
    fi
  done
  log "end after $(( ($(date +%s) - T0) / 60 )) min | done:${OKS:- none} | skipped:${SKIPS:- none} | FAILED:${FAILS:- none}"
  if [ "${DRY:-0}" != 1 ] && ls -d "$RUNS"/*/*/DONE > /dev/null 2>&1; then
    python3 "$H/report/report.py" --txn $(ls -d "$RUNS"/*/*/ | while read -r d; do [ -f "$d/DONE" ] && echo "$d"; done)
  fi
  [ -z "$FAILS" ]
}
main "$@"; exit $?
