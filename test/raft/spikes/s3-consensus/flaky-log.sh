#!/usr/bin/env bash
# WP-0.5 / spike S3 — dropped unflushed writes against the RAFT LOG.
#
# This is the second half of the bar WP-0.3 set for the store engine (S1's
# flaky.sh), applied to `raft-log` 0.4.6, which is what the refutation of
# 2026-09-18 said was missing: `kill -9` alone never asks whether the log can
# reopen AHEAD of its durable point.
#
# LINUX ONLY, NEEDS ROOT (losetup + device-mapper + mount).
#
#   ./flaky-log.sh run [rounds]     one full experiment (setup, scenario 8 with
#                                   dropped writes, teardown)
#   ./flaky-log.sh setup|drop|up|teardown      the verbs, used by the scenario
#
# How one round works (scenario 8 drives it, this script only moves the dm
# table): the victim FOLLOWER's data directory lives on a dm-flakey
# filesystem; the two other nodes are on the normal disk, so the cluster keeps
# committing throughout. Before each `kill -9` the scenario calls `drop`, and
# from that instant every write the victim's filesystem issues is silently
# thrown away — an fsync that returns success and loses the data, which is what
# a power cut does. After the kill the scenario calls `up`, which throws the
# page cache away with the unmount and brings a clean table back, and then
# restarts the victim.
#
# What must hold, and what scenario 8 prints:
#   * the log reopens (no corruption, no refusal to start);
#   * it never claims entries whose bytes were dropped: `reopen_last_log` is
#     allowed to be BELOW what the leader had counted as matched (the leader
#     re-sends them), never above what really reached the disk;
#   * after the node rejoins, no acknowledged write is missing anywhere and
#     every node's digest agrees.
#
# env: ROUNDS, RATE, ENTRY_BYTES, SECS_PER_ROUND, SECS_DOWN, SIZE_MB, BASE,
#      ROOT, OUT, STATE
#
# Cleanup: the EXIT trap of `run` unmounts, removes the dm device, detaches the
# loop device and deletes the backing file. It touches ONLY the device it
# created (name: s3-flakey-log-$$ recorded in the state file) and never any
# other dm or loop device.
set -u
here="$(cd "$(dirname "$0")" && pwd)"
bin="$here/target/release/s3-consensus"

STATE=${STATE:-/var/tmp/s3-flakey-log.state}
SIZE_MB=${SIZE_MB:-4096}
BASE=${BASE:-/var/tmp/s3-flakey-log}
ROOT=${ROOT:-/root/raft/s3-consensus/data-flakey}
OUT=${OUT:-$here/results}
ROUNDS=${ROUNDS:-10}
RATE=${RATE:-200}
ENTRY_BYTES=${ENTRY_BYTES:-65536}
SECS_PER_ROUND=${SECS_PER_ROUND:-2}
SECS_DOWN=${SECS_DOWN:-1}

need_linux() {
    [ "$(uname -s)" = "Linux" ] || { echo "flaky-log.sh is Linux only (dm-flakey)"; exit 2; }
    [ "$(id -u)" = "0" ] || { echo "flaky-log.sh needs root (losetup, dmsetup, mount)"; exit 2; }
}

load_state() {
    # shellcheck disable=SC1090
    . "$STATE"
}

do_setup() {
    need_linux
    for t in losetup dmsetup mkfs.ext4; do
        command -v $t >/dev/null || { echo "missing $t"; exit 2; }
    done
    modprobe dm-flakey 2>/dev/null || true
    NAME=s3-flakey-log-$$
    IMG=$BASE/$NAME.img
    MNT=$BASE/$NAME.mnt
    SECTORS=$((SIZE_MB * 2048))
    mkdir -p "$BASE" "$MNT"
    truncate -s "${SIZE_MB}M" "$IMG"
    LOOP=$(losetup --find --show "$IMG") || exit 2
    printf '0 %s flakey %s 0 600 0\n' "$SECTORS" "$LOOP" | dmsetup create "$NAME" || exit 2
    mkfs.ext4 -q -F "/dev/mapper/$NAME"
    mount -o noatime "/dev/mapper/$NAME" "$MNT" || exit 2
    mkdir -p "$MNT/victim"
    cat >"$STATE" <<EOS
NAME=$NAME
IMG=$IMG
MNT=$MNT
LOOP=$LOOP
SECTORS=$SECTORS
EOS
    echo "flaky-log: $NAME on $LOOP, mounted at $MNT (${SIZE_MB} MiB)"
}

do_drop() {
    load_state
    dmsetup suspend --noflush --nolockfs "$NAME" || exit 2
    printf '0 %s flakey %s 0 0 600 1 drop_writes\n' "$SECTORS" "$LOOP" | dmsetup load "$NAME" || exit 2
    dmsetup resume "$NAME" || exit 2
}

do_up() {
    load_state
    mountpoint -q "$MNT" && umount -l "$MNT"
    dmsetup suspend "$NAME" || exit 2
    printf '0 %s flakey %s 0 600 0\n' "$SECTORS" "$LOOP" | dmsetup load "$NAME" || exit 2
    dmsetup resume "$NAME" || exit 2
    if ! mount -o noatime "/dev/mapper/$NAME" "$MNT" 2>/dev/null; then
        echo "flaky-log: filesystem needed a repair after the dropped writes"
        e2fsck -p -f "/dev/mapper/$NAME" >/dev/null 2>&1
        mount -o noatime "/dev/mapper/$NAME" "$MNT" || { echo "FILESYSTEM WOULD NOT MOUNT"; exit 2; }
    fi
    mkdir -p "$MNT/victim"
}

do_teardown() {
    [ -f "$STATE" ] || return 0
    load_state
    set +e
    mountpoint -q "$MNT" && umount -l "$MNT"
    dmsetup remove "$NAME" 2>/dev/null
    [ -n "${LOOP:-}" ] && losetup -d "$LOOP" 2>/dev/null
    rm -f "$IMG"
    rmdir "$MNT" 2>/dev/null
    rm -f "$STATE"
    echo "flaky-log: cleaned up $NAME"
}

case "${1:-run}" in
selftest)
    # Proof that the fault injector really injects: one buffered file (never
    # fsynced) and one fsynced file, then the drop + crash + remount path. The
    # buffered one must lose its bytes, the fsynced one must keep them.
    need_linux
    trap do_teardown EXIT
    do_setup
    load_state
    dd if=/dev/urandom of="$MNT/buffered" bs=1M count=16 status=none
    dd if=/dev/urandom of="$MNT/fsynced" bs=1M count=16 conv=fsync status=none
    before_b=$(stat -c %s "$MNT/buffered")
    before_f=$(stat -c %s "$MNT/fsynced")
    do_drop
    do_up
    after_b=$(stat -c %s "$MNT/buffered" 2>/dev/null || echo 0)
    after_f=$(stat -c %s "$MNT/fsynced" 2>/dev/null || echo 0)
    echo "selftest: buffered $before_b -> $after_b bytes, fsynced $before_f -> $after_f bytes"
    [ "$after_f" = "$before_f" ] || { echo "SELFTEST FAIL: an fsynced file lost data"; exit 1; }
    [ "$after_b" != "$before_b" ] || echo "NOTE: the buffered file survived (writeback had already run)"
    ;;
setup) do_setup ;;
drop) do_drop ;;
up) do_up ;;
teardown) do_teardown ;;
run)
    need_linux
    [ -x "$bin" ] || { echo "build first: cargo build --release"; exit 2; }
    rounds=${2:-$ROUNDS}
    trap do_teardown EXIT
    do_setup
    load_state
    mkdir -p "$OUT"
    rm -rf "$ROOT"
    log="$OUT/s8-log-crash-flakey.txt"
    "$bin" --root "$ROOT" log-crash \
        --rounds "$rounds" --rate "$RATE" --entry-bytes "$ENTRY_BYTES" \
        --secs-per-round "$SECS_PER_ROUND" --secs-down "$SECS_DOWN" \
        --victim-dir "$MNT/victim" --flakey-cmd "$here/flaky-log.sh" 2>&1 | tee "$log"
    rc=${PIPESTATUS[0]}
    mkdir -p "$OUT/s8-log-crash-flakey-logs"
    cp "$ROOT"/*.log "$OUT/s8-log-crash-flakey-logs/" 2>/dev/null
    rm -rf "$ROOT"
    exit "$rc"
    ;;
*) echo "usage: $0 {run [rounds]|setup|drop|up|teardown}"; exit 2 ;;
esac
