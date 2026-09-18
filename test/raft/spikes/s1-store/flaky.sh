#!/usr/bin/env bash
# S1 store spike, the second half of the WP-0.3 PASS/FAIL: dropped unflushed
# writes. LINUX ONLY, NEEDS ROOT (losetup + device-mapper + mount).
#
# What it does, per run:
#   1. create a file-backed loop device under $BASE,
#   2. put a dm-flakey device on it that is fully "up" (no drops),
#   3. mkfs.ext4 + mount it,
#   4. run the harness on the mounted filesystem for $RUN_S seconds,
#   5. switch the dm table to `drop_writes` (suspend --noflush --nolockfs,
#      load, resume): from that instant every write the filesystem issues is
#      silently thrown away, which is what a power loss does to everything that
#      was never fsynced,
#   6. kill -9 the harness, unmount (its writeback is dropped too),
#   7. put the clean table back, mount again, and run `verify`.
#
# The verdict is the same as kill-loop.sh: does the applied index the reopened
# store reports agree with the segment bytes it references (I11), and did the
# engine reopen past its last DURABLE commit — which after dropped writes means
# state referring to bytes that never reached the disk.
#
#   sudo ./flaky.sh [runs]
#
# env: ENGINE (redb|fjall|heed), RATE, RUN_S, SIZE_MB, BASE, OUT, EXTRA
#
# Cleanup: the EXIT trap unmounts, removes the dm device, detaches the loop
# device and deletes the backing file, whatever happens. It touches ONLY the
# device it created (name: s1-flakey-$$) and never any other dm/loop device.
set -u
cd "$(dirname "$0")" || exit 2

RUNS=${1:-5}
ENGINE=${ENGINE:-redb}
RATE=${RATE:-20000}
RUN_S=${RUN_S:-20}
SIZE_MB=${SIZE_MB:-4096}
BASE=${BASE:-/var/tmp/s1-flaky}
OUT=${OUT:-./results}
EXTRA=${EXTRA:-}
BIN=$(cd "$(dirname "$0")" && pwd)/target/release/s1-store

[ "$(uname -s)" = "Linux" ] || { echo "flaky.sh is Linux only (dm-flakey)"; exit 2; }
[ "$(id -u)" = "0" ] || { echo "flaky.sh needs root (losetup, dmsetup, mount)"; exit 2; }
for t in losetup dmsetup mkfs.ext4 blockdev; do
  command -v $t >/dev/null || { echo "missing $t"; exit 2; }
done
modprobe dm-flakey 2>/dev/null || true
[ -x "$BIN" ] || { echo "build first: cargo build --release"; exit 2; }

NAME=s1-flakey-$$
IMG=$BASE/$NAME.img
MNT=$BASE/$NAME.mnt
LOOP=""
mkdir -p "$BASE" "$MNT" "$OUT"
LOG="$OUT/flaky-$ENGINE-$(date +%Y%m%d-%H%M%S).log"

cleanup() {
  set +e
  mountpoint -q "$MNT" && umount -l "$MNT"
  dmsetup remove "$NAME" 2>/dev/null
  [ -n "$LOOP" ] && losetup -d "$LOOP" 2>/dev/null
  rm -f "$IMG"
  rmdir "$MNT" 2>/dev/null
}
trap cleanup EXIT

SECTORS=$(( SIZE_MB * 2048 ))
table_up="0 $SECTORS flakey %s 0 60 0"          # 60 s up, 0 s down: no drops
table_drop="0 $SECTORS flakey %s 0 0 60 1 drop_writes"  # always down, writes dropped

pass=0; fail=0; past=0; nostate=0
echo "flaky: engine=$ENGINE runs=$RUNS rate=$RATE run_s=$RUN_S size=${SIZE_MB}MiB" | tee "$LOG"

for i in $(seq 1 "$RUNS"); do
  # ---- fresh device ----
  mountpoint -q "$MNT" && umount "$MNT"
  dmsetup remove "$NAME" 2>/dev/null
  [ -n "$LOOP" ] && losetup -d "$LOOP" 2>/dev/null
  rm -f "$IMG"
  truncate -s "${SIZE_MB}M" "$IMG"
  LOOP=$(losetup --find --show "$IMG") || exit 2
  # shellcheck disable=SC2059
  printf "$table_up\n" "$LOOP" | dmsetup create "$NAME" || exit 2
  mkfs.ext4 -q -F "/dev/mapper/$NAME"
  mount -o noatime "/dev/mapper/$NAME" "$MNT"
  DIR="$MNT/store"

  # ---- load ----
  # shellcheck disable=SC2086
  "$BIN" run --engine "$ENGINE" --dir "$DIR" --rate "$RATE" --duration 600 \
    --segment-bytes 262144 $EXTRA >"$OUT/.flaky-run.log" 2>&1 &
  pid=$!
  sleep "$RUN_S"

  # ---- drop every write from here on, then crash ----
  dmsetup suspend --noflush --nolockfs "$NAME"
  # shellcheck disable=SC2059
  printf "$table_drop\n" "$LOOP" | dmsetup load "$NAME"
  dmsetup resume "$NAME"
  kill -9 "$pid" 2>/dev/null
  wait "$pid" 2>/dev/null
  umount -l "$MNT"

  # ---- back to a clean device and reopen ----
  dmsetup suspend "$NAME"
  # shellcheck disable=SC2059
  printf "$table_up\n" "$LOOP" | dmsetup load "$NAME"
  dmsetup resume "$NAME"
  mount -o noatime "/dev/mapper/$NAME" "$MNT" || { echo "run $i: FILESYSTEM WOULD NOT MOUNT" | tee -a "$LOG"; fail=$((fail+1)); continue; }

  out=$("$BIN" verify --engine "$ENGINE" --dir "$DIR" --verify-all $EXTRA 2>&1)
  vrc=$?
  line=$(printf '%s\n' "$out" | grep '^VERIFY')
  if [ -z "$line" ]; then
    nostate=$((nostate+1))
    sig=""
    [ "$vrc" -gt 128 ] && sig=" killed by signal $((vrc-128))"
    echo "run $i -> VERIFY DID NOT RUN (store would not reopen) rc=$vrc$sig" | tee -a "$LOG"
    printf '%s\n' "$out" | tail -5 | sed 's/^/    /' | tee -a "$LOG"
    continue
  fi
  case "$line" in
    "VERIFY PASS"*) pass=$((pass+1)) ;;
    *) fail=$((fail+1)) ;;
  esac
  case "$line" in *past_durable=true*) past=$((past+1)) ;; esac
  echo "run $i -> $line" | tee -a "$LOG"
done

echo | tee -a "$LOG"
echo "SUMMARY engine=$ENGINE runs=$RUNS pass=$pass fail=$fail reopened_past_durable=$past no_reopen=$nostate" | tee -a "$LOG"
[ "$fail" -eq 0 ] && [ "$nostate" -eq 0 ]
