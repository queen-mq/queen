#!/usr/bin/env bash
# WP-1.11 deliverable 4 — dropped unflushed writes against the raft1 broker
# (PLAN_RAFT.md §13.6, checker §13.7). The proof owed since WP-1.2/1.3 by
# R-106 (and restated by R-111 for apply and R-114 for the LocalReplicator WAL):
# a `kill -9` keeps the page cache, so it falsifies the bookkeeping but NOT the
# unsynced bytes. dm-flakey `drop_writes` throws unsynced (and "fsync'd") bytes
# away, which is what a power cut does.
#
# LINUX ONLY, NEEDS ROOT (losetup + device-mapper + mount).
#
# How one round works (load-to-completion, THEN drop, THEN kill):
#   1. a fresh queue; the broker's data dir is on a dm-flakey ext4 filesystem;
#   2. `flakechk load` pushes K messages and records every ACKNOWLEDGED
#      transactionId to a ledger on the NORMAL disk. Because the log fsyncs
#      BEFORE a push is answered (local.rs), every ledger entry is durable in
#      the log; because the store commit and the segment writes between durable
#      points are NOT fsynced (MDB_NOSYNC, §11.3/§11.4), a large slice of the
#      round's applied state sits only in the page cache;
#   3. `drop` — from here every write the victim's filesystem issues is thrown
#      away (an fsync that returns success and loses the data);
#   4. `kill -9` the broker; `up` unmounts (dropping the page cache) and brings
#      a clean table back; restart the broker;
#   5. `flakechk verify` drains the queue and asserts EVERY acknowledged push is
#      delivered (§13.7). All the acks are strictly before the drop, so they were
#      log-fsynced for real and MUST come back; the dropped page cache is exactly
#      the store/segment state recovery has to rebuild from the log (I11, §11.5).
#
# The durable cadence and the pre-drop settle are varied per round so the kill
# lands both where nothing of the round was durable (the whole log replays) and
# where most of it was (only the tail replays) — the §13.6 "cover every periodic
# boundary" lesson, applied to the durable point.
#
# The broker runs with QUEEN_RAFT_PIPELINE=1: at the ratified default of 4 the
# raft1 planner violates I5 under concurrent load (WP-1.11 finding F-1) and the
# node stops before any durability question can be asked. Pipeline depth is a
# planner-time concern and is ORTHOGONAL to log/store durability, which is what
# this proof exercises; the finding is recorded in RESULTS.md.
#
# Cleanup: the EXIT trap unmounts, removes the dm device, detaches the loop
# device and deletes the backing file. It touches ONLY the device it created
# (raft1-flakey-$$) and never any other dm or loop device. It never kills by
# name/port — only the broker PID it started.
set -u
ulimit -n 262144 2>/dev/null || true   # raft broker holds many segment + log fds (WP-1.11 F-2)

here="$(cd "$(dirname "$0")" && pwd)"
BIN=${QUEEN_BIN:-/root/raft/wp111/queen/server/target/release/queen}
FLAKECHK=${FLAKECHK:-$here/flakechk}
PORT=${PORT:-6698}
URL=http://127.0.0.1:$PORT

ROUNDS=${ROUNDS:-20}
K=${K:-4000}                      # messages per round (varied ±)
PARTS=${PARTS:-8}
SIZE_MB=${SIZE_MB:-3072}
BASE=${BASE:-/root/raft/wp111/flaky}
STATE=$BASE/state
OUT=${OUT:-/root/raft/wp111/raft1-flaky}
BROKER_PORT=$PORT

need_linux() {
    [ "$(uname -s)" = "Linux" ] || { echo "flaky-raft1.sh is Linux only (dm-flakey)"; exit 2; }
    [ "$(id -u)" = "0" ] || { echo "flaky-raft1.sh needs root (losetup, dmsetup, mount)"; exit 2; }
    for t in losetup dmsetup mkfs.ext4 mountpoint; do command -v $t >/dev/null || { echo "missing $t"; exit 2; }; done
    [ -x "$BIN" ] || { echo "no broker at $BIN"; exit 2; }
    [ -x "$FLAKECHK" ] || { echo "no flakechk at $FLAKECHK (build it: GOWORK=off go build -o $FLAKECHK $here/flakechk.go)"; exit 2; }
}

NAME=raft1-flakey-$$
IMG=$BASE/$NAME.img
MNT=$BASE/$NAME.mnt
SECTORS=$((SIZE_MB * 2048))
QPID=""

setup() {
    modprobe dm-flakey 2>/dev/null || true
    mkdir -p "$BASE" "$MNT" "$OUT"
    truncate -s "${SIZE_MB}M" "$IMG"
    LOOP=$(losetup --find --show "$IMG") || exit 2
    # up table: pass all writes through for 600 s, then 0 s down.
    printf '0 %s flakey %s 0 600 0\n' "$SECTORS" "$LOOP" | dmsetup create "$NAME" || exit 2
    mkfs.ext4 -q -F "/dev/mapper/$NAME"
    mount -o noatime "/dev/mapper/$NAME" "$MNT" || exit 2
    cat >"$STATE" <<EOS
NAME=$NAME
LOOP=$LOOP
SECTORS=$SECTORS
EOS
    echo "flaky-raft1: $NAME on $LOOP, mounted $MNT (${SIZE_MB} MiB)"
}

drop_writes() { # every subsequent write silently discarded
    dmsetup suspend --noflush --nolockfs "$NAME" || exit 2
    printf '0 %s flakey %s 0 0 600 1 drop_writes\n' "$SECTORS" "$LOOP" | dmsetup load "$NAME" || exit 2
    dmsetup resume "$NAME" || exit 2
}

remount_clean() { # throw the page cache away and bring a healthy fs back
    mountpoint -q "$MNT" && umount -l "$MNT"
    dmsetup suspend "$NAME" || exit 2
    printf '0 %s flakey %s 0 600 0\n' "$SECTORS" "$LOOP" | dmsetup load "$NAME" || exit 2
    dmsetup resume "$NAME" || exit 2
    if ! mount -o noatime "/dev/mapper/$NAME" "$MNT" 2>/dev/null; then
        echo "  fs needed a repair after the dropped writes"
        e2fsck -p -f "/dev/mapper/$NAME" >/dev/null 2>&1
        mount -o noatime "/dev/mapper/$NAME" "$MNT" || { echo "FILESYSTEM WOULD NOT MOUNT"; exit 2; }
    fi
}

kill_broker() {
    [ -n "$QPID" ] || return 0
    kill -9 "$QPID" 2>/dev/null
    # wait for the process to actually be gone before touching its fs
    for _ in $(seq 1 50); do kill -0 "$QPID" 2>/dev/null || break; sleep 0.1; done
    QPID=""
}

start_broker() { # $1 = data dir, $2 = durable_every_ms
    env QUEEN_STORAGE=raft QUEEN_RAFT_DIR="$1" QUEEN_BIND_ADDR=127.0.0.1 PORT=$BROKER_PORT \
        JWT_ENABLED=false QUEEN_TENANCY_HEADER=false QUEEN_RAFT_PIPELINE=1 \
        QUEEN_RAFT_DURABLE_EVERY_MS="$2" FILE_BUFFER_DIR="$OUT/buffers" LOG_LEVEL=warn \
        "$BIN" >>"$OUT/broker.log" 2>&1 &
    QPID=$!
    for _ in $(seq 1 90); do
        curl -s $URL/health 2>/dev/null | grep -q '"storageReady":true' && return 0
        kill -0 "$QPID" 2>/dev/null || { echo "  broker exited before healthy"; return 1; }
        sleep 0.3
    done
    echo "  broker not storageReady"; return 1
}

cleanup() {
    set +e
    kill_broker
    if [ -f "$STATE" ]; then
        . "$STATE"
        mountpoint -q "$MNT" && umount -l "$MNT"
        dmsetup remove "$NAME" 2>/dev/null
        [ -n "${LOOP:-}" ] && losetup -d "$LOOP" 2>/dev/null
        rm -f "$IMG"
    fi
    rm -rf "$MNT"
}
trap cleanup EXIT

# ------------------------------------------------------------------- run
need_linux
(pkill -x queen 2>/dev/null; true); sleep 1
rm -rf "$OUT"; mkdir -p "$OUT"
setup

RES="$OUT/RESULTS.tsv"
echo -e "round\tdurable_ms\tsettle_s\tK\tacked\tpusherr\tledger\tdelivered\tmissing\tverdict" > "$RES"
pass=0; fail=0; DATA="$MNT/data"

for r in $(seq 1 "$ROUNDS"); do
    # vary the unsynced window: some rounds keep NOTHING durable during load
    # (whole log replays), some flush often (only the tail replays).
    case $((r % 4)) in
        0) DUR=60000 ;;   # nothing of the round is durable-pointed
        1) DUR=100 ;;     # frequent durable points
        2) DUR=500 ;;
        3) DUR=1000 ;;    # the Appendix-H default
    esac
    SETTLE=$(awk -v s=$RANDOM 'BEGIN{srand(s); printf "%.1f", rand()*1.5}')  # 0..1.5s
    KK=$(( K + (RANDOM % 2000) - 1000 ))                                     # K ± 1000
    Q="flakyR$r"
    LED="$OUT/round$r.ledger"

    rm -rf "$DATA"; mkdir -p "$DATA"
    echo "== round $r/$ROUNDS  queue=$Q  durable_ms=$DUR  settle=${SETTLE}s  K=$KK"
    if ! start_broker "$DATA" "$DUR"; then
        echo -e "$r\t$DUR\t$SETTLE\t$KK\t-\t-\t-\t-\t-\tSTART_FAIL" >> "$RES"; fail=$((fail+1)); continue
    fi

    LOADOUT=$("$FLAKECHK" load -url $URL -queue "$Q" -partitions $PARTS -count $KK -batch 20 -prefix "$Q" -ledger "$LED" 2>&1)
    acked=$(echo "$LOADOUT" | sed -n 's/.*acked=\([0-9]*\).*/\1/p')
    pusherr=$(echo "$LOADOUT" | sed -n 's/.*pusherr=\([0-9]*\).*/\1/p')
    [ -n "$acked" ] || { acked=0; }

    # let (or don't let) a durable point fire, then drop and cut power.
    sleep "$SETTLE"
    drop_writes
    kill_broker
    remount_clean

    if ! start_broker "$DATA" 1000; then
        echo -e "$r\t$DUR\t$SETTLE\t$KK\t$acked\t${pusherr:-?}\t-\t-\t-\tREOPEN_FAIL" >> "$RES"; fail=$((fail+1)); continue
    fi
    VOUT=$("$FLAKECHK" verify -url $URL -queue "$Q" -ledger "$LED" 2>&1)
    vrc=$?
    ledger=$(echo "$VOUT" | sed -n 's/.*ledger=\([0-9]*\).*/\1/p')
    delivered=$(echo "$VOUT" | sed -n 's/.*delivered=\([0-9]*\).*/\1/p')
    missing=$(echo "$VOUT" | sed -n 's/.*missing=\([0-9]*\).*/\1/p')
    kill_broker

    if [ "$vrc" = "0" ] && [ "${missing:-1}" = "0" ]; then
        verdict=PASS; pass=$((pass+1))
    else
        verdict=FAIL; fail=$((fail+1))
        echo "  !! $VOUT"
    fi
    echo -e "$r\t$DUR\t$SETTLE\t$KK\t$acked\t${pusherr:-?}\t${ledger:-?}\t${delivered:-?}\t${missing:-?}\t$verdict" >> "$RES"
    echo "  $verdict  acked=$acked ledger=${ledger:-?} delivered=${delivered:-?} missing=${missing:-?}"
    rm -f "$LED"
done

echo "== dropped-writes proof: $pass PASS / $fail FAIL over $ROUNDS rounds"
echo "== broker error lines: $(grep -ciE ' error |panic|poison' "$OUT/broker.log" 2>/dev/null)"
column -t -s $'\t' "$RES" | tee "$OUT/RESULTS.txt"
[ "$fail" = "0" ]
