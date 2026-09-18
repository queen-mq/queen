#!/usr/bin/env bash
# S1 refutation round (2026-09-18): re-measure the DECISION cells against the
# store shape the G0 amendments actually ratified, not the one the 2026-09-17
# campaign measured.
#
# What changed in the design after that campaign:
#   * PLAN_RAFT.md §11.3 (G0 amendment): NOT one store transaction per applied
#     entry — the apply thread commits every QUEEN_RAFT_STORE_COMMIT_MS (4) or
#     QUEEN_RAFT_STORE_COMMIT_ENTRIES (256). redb was dropped on a ~10k msg/s
#     ceiling and 29x write amplification that RESULTS-vm.md §1 attributes to
#     exactly the per-commit COW cost a 256:1 batch amortizes.
#   * §6.1 (G0 amendment): `segments` is NOT in the store (one immutable .qidx
#     per sealed file). The memo's ordered-scan pillar was measured on it.
#   * D10 ratified option (a): a dedup index (pid, hash) -> (offset, created_at),
#     uniformly random 16-byte keys, one row per message — the keyspace that
#     replaces `segments` as the largest, and that the harness never had.
#
# Budget: this is written to fit inside ~8 minutes of VM wall clock.
# Everything lives under /root/raft/s1-store; nothing is left running.
set -u
OUT=${OUT:-./results-vm}
DATA=${DATA:-/root/raft/s1-store/data-refute}
DUR=${DUR:-40}
SEGB=${SEGB:-262144}
RATES=${RATES:-"20000 50000"}
ENGINES=${ENGINES:-"redb fjall heed"}
BIN=./target/release/s1-store
mkdir -p "$OUT"
STAMP=$(date -u +%Y%m%d-%H%M%S)
LOG="$OUT/refute-$STAMP.log"
say() { echo "[$(date -u +%H:%M:%S)] $*" | tee -a "$LOG"; }

say "=== S1 refutation round, ratified store shape ==="
say "host $(hostname), $(uptime | tr -s ' ')"

# 1. the ratified shape: batched store commits, no `segments` rows, dedup rows
for e in $ENGINES; do
  for r in $RATES; do
    say "matrix ratified $e $r"
    rm -rf "$DATA/$e-$r"
    $BIN run --engine "$e" --dir "$DATA/$e-$r" --shape ratified \
      --store-commit-ms 4 --store-commit-entries 256 \
      --rate "$r" --duration "$DUR" --segment-bytes "$SEGB" \
      --export 0 --reclaim 0 >"$OUT/ratified-$e-$r.log" 2>&1
    grep -E '^RESULT ' "$OUT/ratified-$e-$r.log" | tee -a "$LOG"
  done
done

# 2. the LMDB flag the spike never measured. lmdb.h on MDB_NOMETASYNC:
#    "maintains database integrity, but a system crash may undo the last
#    committed transaction" — unconditional, unlike the MDB_NOSYNC paragraph.
say "heed MDB_NOMETASYNC, ratified shape, 20000"
rm -rf "$DATA/heed-nometasync"
$BIN run --engine heed --dir "$DATA/heed-nometasync" --shape ratified \
  --store-commit-ms 4 --store-commit-entries 256 --heed-flags nometasync \
  --rate 20000 --duration "$DUR" --segment-bytes "$SEGB" \
  --export 0 --reclaim 0 >"$OUT/ratified-heed-nometasync-20000.log" 2>&1
grep -E '^RESULT ' "$OUT/ratified-heed-nometasync-20000.log" | tee -a "$LOG"

# 3. the D15 read path, from more than one thread, in both reader modes.
#    heed's default EnvOpenOptions::new() is WithTls: RoTxn is not Send, it
#    cannot cross an .await, and a nested read txn on one thread is illegal.
#    read_txn_without_tls() (MDB_NOTLS) lifts both — at a cost mdb.c predicts:
#    every read txn begin then takes env->me_rmutex and scans the reader table.
if [ -d "$DATA/heed-20000" ]; then
  say "reads, heed, TLS vs NOTLS"
  for t in 1 4 8; do
    for tls in 0 1; do
      $BIN reads --engine heed --dir "$DATA/heed-20000" --read-threads "$t" \
        --duration 5 --heed-no-tls "$tls" --heed-max-readers 512 \
        >>"$OUT/reads-heed.log" 2>&1
    done
  done
  $BIN reads --engine heed --dir "$DATA/heed-20000" --read-threads 1 \
    --duration 1 --readers 200 >>"$OUT/reads-heed.log" 2>&1
  grep -E '^RESULT-READS |^READERS ' "$OUT/reads-heed.log" | tee -a "$LOG"
fi

say "=== done ==="
say "disk: $(df -h / | tail -1)"
