#!/usr/bin/env bash
# WP-0.5 / spike S3 driver: runs the seven scenarios and tees the output.
#
#   ./run.sh laptop [outdir]   small scale, macOS smoke (PLAN_RAFT.md §0.3:
#                              laptop numbers are smoke only)
#   ./run.sh vm     [outdir]   full scale, meant for the Linux VM (§13.6)
#   ./run.sh vm-today [outdir] the budgeted VM pass of 2026-09-17: 1 GiB
#                              snapshot instead of 10 GiB, 3 kill repetitions
#   ./run.sh vm-refute [outdir] the refutation pass of 2026-09-18: the raft-log
#                              crash qualification (scenario 8), scenario 7 with
#                              kill -9 and a stale durable point, the cost of
#                              each save_committed setting, and the one-way
#                              partition of GH#2080 (scenario 9)
#
# Environment:
#   ROOT=<dir>   where the node data directories go (default under /private/tmp
#                on macOS, /root/raft/wp-0.5/run on Linux). Every scenario wipes
#                its own root at boot and this script removes it afterwards
#                unless KEEP=1.
#   NODE_LOG=<filter>  RUST_LOG for the node processes (default info).
#
# Each scenario is a fresh 3-process cluster on 127.0.0.1:26101..26104.

set -u

profile="${1:-laptop}"
out="${2:-$(pwd)/out-$profile-$(date +%Y%m%d-%H%M%S)}"
here="$(cd "$(dirname "$0")" && pwd)"
bin="$here/target/release/s3-consensus"

case "$(uname -s)" in
Darwin) default_root=/private/tmp/s3-consensus-run ;;
*) default_root=/root/raft/wp-0.5/run ;;
esac
ROOT="${ROOT:-$default_root}"
KEEP="${KEEP:-0}"

mkdir -p "$out"
echo "profile=$profile out=$out root=$ROOT"

(cd "$here" && cargo build --release) || exit 1

# The state machine keeps every entry AND the raft log keeps a copy, so a run
# needs roughly 2 x entries x 3 nodes of free space.
avail=$(df -k "$(dirname "$ROOT")" | awk 'NR==2 {print $4}')
echo "free space at $(dirname "$ROOT"): $((avail / 1024)) MiB"

run() {
    local name="$1"
    shift
    local root="$ROOT/$name"
    echo "=== $name: $* ==="
    "$bin" --root "$root" "$@" 2>&1 | tee "$out/$name.txt"
    # Keep the node logs, drop the data.
    mkdir -p "$out/$name-logs"
    cp "$root"/*.log "$out/$name-logs/" 2>/dev/null
    [ "$KEEP" = "1" ] || rm -rf "$root"
}

if [ "$profile" = "laptop" ]; then
    run s1-latency latency --rates 200,500,1000 --secs 5 --entry-bytes 65536 --writers 1
    run s1-latency-16 latency --rates 200,500,1000 --secs 5 --entry-bytes 65536 --writers 16
    run s3-transfer transfer --rounds 1
    run s4-linearizable linearizable --concurrency 1,8,32 --secs 5
    run s7-restart restart --entries 50
    run s2-kill-leader kill-leader --rate 50 --entry-bytes 16384 --secs-before 4 --secs-after 4
    run s6-wiped-voter wiped-voter
    run s5-snapshot --file-bytes 8388608 --keep-logs 0 \
        snapshot --total-bytes 134217728 --entry-bytes 1048576 --kill-after-ms 0
    run s5-snapshot-resume --file-bytes 8388608 --keep-logs 0 \
        snapshot --total-bytes 268435456 --entry-bytes 1048576 --kill-after-ms 150
elif [ "$profile" = "laptop-refute" ]; then
    # Small-scale smoke of the refutation pass (macOS: behaviour only, §0.3).
    run s8-log-crash log-crash --rounds 4 --rate 100 --entry-bytes 16384 \
        --secs-per-round 1 --secs-down 0.5
    run s7-restart-kill9-none --durable-ms 60000 --wait-recovery true \
        restart --entries 300 --kill9 true
    run s7-restart-kill9-fsync --durable-ms 60000 --committed fsync --wait-recovery true \
        restart --entries 300 --kill9 true
    run s1-committed-none --committed none \
        latency --rates 200 --secs 8 --entry-bytes 65536 --writers 16
    run s1-committed-buffered --committed buffered \
        latency --rates 200 --secs 8 --entry-bytes 65536 --writers 16
    run s1-committed-fsync --committed fsync \
        latency --rates 200 --secs 8 --entry-bytes 65536 --writers 16
    run s9-one-way-partition one-way-partition --secs-blocked 8
elif [ "$profile" = "vm-refute" ]; then
    # The refutation pass of 2026-09-18 (budget: 10 minutes of VM wall clock
    # beyond the build). It answers the two evidence gaps the refuters found:
    # the raft log was never crash-tested, and `save_committed` diverges from
    # §12.3. Everything here is new since the 2026-09-17 pass.
    run s8-log-crash log-crash --rounds 10 --rate 200 --entry-bytes 65536 \
        --secs-per-round 2 --secs-down 1
    # Scenario 7 the way it should have been run: kill -9, with the state
    # machine's periodic durable point deliberately stale (60 s), so recovery
    # really has to replay the log. Once with openraft's own `save_committed`
    # (no flush) and once with §12.3's (fsync), both with wait_for_recovery
    # timed.
    run s7-restart-kill9-none --durable-ms 60000 --wait-recovery true \
        restart --entries 2000 --kill9 true
    run s7-restart-kill9-fsync --durable-ms 60000 --committed fsync --wait-recovery true \
        restart --entries 2000 --kill9 true
    # What each `save_committed` setting costs on the write path.
    run s1-committed-none --committed none \
        latency --rates 500 --secs 20 --entry-bytes 65536 --writers 16
    run s1-committed-buffered --committed buffered \
        latency --rates 500 --secs 20 --entry-bytes 65536 --writers 16
    run s1-committed-fsync --committed fsync \
        latency --rates 500 --secs 20 --entry-bytes 65536 --writers 16
    # The liveness shape of GH#2080 on the pinned rev, with both operator
    # actions timed.
    run s9-one-way-partition one-way-partition --secs-blocked 12
elif [ "$profile" = "vm-today" ]; then
    # The budgeted VM pass of 2026-09-17 (Alice: at most 30 minutes of VM
    # wall-clock beyond the build). Same scenarios as `vm`, with the sizes of
    # the WP-0.5 row except the snapshot, which is 1 GiB instead of 10 GiB, and
    # the kill repetitions, which are 3 instead of 5. Deferred: the 10 GiB
    # manifest, 5 kill repetitions, dm-delay.
    run s1-latency latency --rates 200,500,1000 --secs 60 --entry-bytes 65536 --writers 1
    run s1-latency-16 latency --rates 200,500,1000 --secs 20 --entry-bytes 65536 --writers 16
    run s3-transfer transfer --rounds 5
    run s4-linearizable linearizable --rates 1000,5000 --secs 30
    run s7-restart restart --entries 2000
    run s2-kill-leader-1 kill-leader --rate 200 --entry-bytes 65536 --secs-before 10 --secs-after 10
    run s2-kill-leader-2 kill-leader --rate 200 --entry-bytes 65536 --secs-before 10 --secs-after 10
    run s2-kill-leader-3 kill-leader --rate 200 --entry-bytes 65536 --secs-before 10 --secs-after 10
    run s2-kill-leader-sigstop kill-leader --rate 200 --entry-bytes 65536 \
        --secs-before 10 --secs-after 10 --stop-follower-secs 5
    run s6-wiped-voter wiped-voter
    run s5-snapshot --file-bytes 16777216 --keep-logs 0 \
        snapshot --total-bytes 1073741824 --entry-bytes 4194304 --kill-after-ms 0
    run s5-snapshot-resume --file-bytes 16777216 --keep-logs 0 \
        snapshot --total-bytes 1073741824 --entry-bytes 4194304 --kill-after-ms 400
else
    run s1-latency latency --rates 200,500,1000 --secs 30 --entry-bytes 65536 --writers 1
    run s1-latency-16 latency --rates 200,500,1000 --secs 30 --entry-bytes 65536 --writers 16
    run s1-latency-batch --fsync batch latency --rates 200,500 --secs 30 --entry-bytes 65536 --writers 1
    run s3-transfer transfer --rounds 5
    run s4-linearizable linearizable --concurrency 1,8,32,128 --secs 10
    run s7-restart restart --entries 2000
    run s2-kill-leader kill-leader --rate 200 --entry-bytes 65536 --secs-before 20 --secs-after 20
    run s6-wiped-voter wiped-voter
    # The 10 GiB manifest of the WP-0.5 row: 64 MiB files, so 160 of them.
    run s5-snapshot --file-bytes 67108864 --keep-logs 0 \
        snapshot --total-bytes 10737418240 --entry-bytes 4194304 --kill-after-ms 0
    run s5-snapshot-resume --file-bytes 67108864 --keep-logs 0 \
        snapshot --total-bytes 10737418240 --entry-bytes 4194304 --kill-after-ms 15000
fi

echo "done; output in $out"
