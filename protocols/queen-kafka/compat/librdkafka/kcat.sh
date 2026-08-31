#!/usr/bin/env bash
#
# The kcat half of the librdkafka row in the M6 client matrix. kcat 1.7 links
# librdkafka, which is also the engine under Confluent's Python, .NET, Go and
# (via node-rdkafka) JS clients: whatever this proves holds for all of them.
#
#   protocols/queen-kafka/compat/librdkafka/kcat.sh [bootstrap] [runid]
#
# Nothing here is a framework: every step prints what it sent and what came
# back, and the negotiated request versions are read out of librdkafka's own
# `debug=protocol` log rather than assumed.
set -uo pipefail

BOOTSTRAP="${1:-127.0.0.1:19092}"
RUN="${2:-$(date +%s)}"
TOPIC="rdk-$RUN"
GROUP="rdk-g-$RUN"
WORK="$(mktemp -d -t kcat-compat.XXXXXX)"
FAIL=0

ok()   { printf '  ok   %s\n' "$*"; }
bad()  { printf '  FAIL %s\n' "$*"; FAIL=1; }
info() { printf '  ..   %s\n' "$*"; }
check() { if [ "$1" = 0 ]; then ok "$2"; else bad "$2"; fi; }

say() { printf '\n=== %s\n' "$*"; }

# A hang is a result too, and a script that waits forever for one reports
# nothing. Every kcat call that could block goes through this: it returns 124
# on the deadline, the way timeout(1) would on a machine that has it.
limit() {
  local secs="$1"; shift
  "$@" &
  local pid=$! i=0
  while [ $i -lt $((secs * 10)) ]; do
    kill -0 "$pid" 2>/dev/null || { wait "$pid"; return $?; }
    sleep 0.1; i=$((i + 1))
  done
  echo "  !!   TIMED OUT after ${secs}s: $*" >&2
  kill -9 "$pid" 2>/dev/null
  wait "$pid" 2>/dev/null
  return 124
}

# ------------------------------------------------------------------- metadata
say "kcat -L (metadata), with the protocol trace kept for the version table"
limit 30 kcat -b "$BOOTSTRAP" -L -t "$TOPIC" -X debug=protocol > "$WORK/meta.out" 2> "$WORK/meta.err"
rc=$?
check $rc "kcat -L exited 0"
grep -E "^  (broker|topic)" "$WORK/meta.out" | head -5
grep -q "1 brokers:" "$WORK/meta.out"
check $? "exactly one broker advertised"
grep -q "topic \"$TOPIC\" with 8 partitions" "$WORK/meta.out"
check $? "the topic was auto-created at 8 partitions by the metadata request alone"
grep -q "Err" <<< "$(grep -o 'Err[a-zA-Z_]*' "$WORK/meta.out")" && bad "metadata reported an error"

# ------------------------------------------------------------------- produce
say "kcat -P (produce, acks=all, keys and headers)"
printf 'one\ntwo\nthree\n' | kcat -b "$BOOTSTRAP" -t "$TOPIC" -p 0 -P -K: \
  -H "trace=abc" -H "empty=" -X request.required.acks=-1 \
  -X debug=protocol 2> "$WORK/prod.err"
check $? "kcat -P exited 0"
printf 'k4:four\nk5:five\n' | kcat -b "$BOOTSTRAP" -t "$TOPIC" -p 1 -P -K: \
  -X request.required.acks=-1 2>> "$WORK/prod.err"
check $? "kcat -P with keys on partition 1 exited 0"
printf 'g1\ng2\ng3\n' | kcat -b "$BOOTSTRAP" -t "$TOPIC" -p 2 -P \
  -X compression.codec=gzip -X request.required.acks=-1 2>> "$WORK/prod.err"
check $? "kcat -P with gzip exited 0"
printf 'z1\nz2\n' | kcat -b "$BOOTSTRAP" -t "$TOPIC" -p 3 -P \
  -X compression.codec=snappy -X request.required.acks=-1 2>> "$WORK/prod.err"
check $? "kcat -P with snappy exited 0"
printf 'l1\nl2\n' | kcat -b "$BOOTSTRAP" -t "$TOPIC" -p 4 -P \
  -X compression.codec=lz4 -X request.required.acks=-1 2>> "$WORK/prod.err"
check $? "kcat -P with lz4 exited 0"
# zstd is asked for and NOT what goes on the wire: librdkafka gates the codec on
# BOTH Produce v7 and FETCH v10 (rdkafka_feature.c), and this facade caps Fetch
# at v6 on purpose, so librdkafka logs "Broker does not support compression type
# zstd" and sends the batch uncompressed. The records still land, which is what
# is checked below; the fallback is reported at the end of this script.
printf 'zs1\nzs2\n' | kcat -b "$BOOTSTRAP" -t "$TOPIC" -p 5 -P \
  -X compression.codec=zstd -X request.required.acks=-1 2>> "$WORK/prod.err"
check $? "kcat -P with zstd exited 0 (see the codec note at the end)"

# ------------------------------------------------------------------- consume
say "kcat -C (consume to the end of the log)"
limit 60 kcat -b "$BOOTSTRAP" -t "$TOPIC" -C -e -o beginning -q \
  -f '%p|%o|%k|%s|%h\n' > "$WORK/cons.out" 2> "$WORK/cons.err"
check $? "kcat -C exited 0"
cat "$WORK/cons.out"
n=$(wc -l < "$WORK/cons.out" | tr -d ' ')
[ "$n" = 14 ]
check $? "every one of the 14 produced records came back ($n)"
grep -q '^0|0||one|trace=abc,empty=$' "$WORK/cons.out"
check $? "partition 0 offset 0 has the headers kcat sent, empty header value included"
grep -q '^1|0|k4|four|$' "$WORK/cons.out"
check $? "the key round-trips on partition 1"
grep -q '^2|0||g1|$' "$WORK/cons.out" && grep -q '^3|0||z1|$' "$WORK/cons.out" &&
  grep -q '^4|0||l1|$' "$WORK/cons.out" && grep -q '^5|0||zs1|$' "$WORK/cons.out"
check $? "the gzip, snappy, lz4 and zstd-requested batches all decoded"

say "kcat -C on one partition from an explicit offset"
limit 60 kcat -b "$BOOTSTRAP" -t "$TOPIC" -p 0 -C -e -o 1 -q -f '%o:%s\n' > "$WORK/off.out" 2>&1
check $? "kcat -C -o 1 exited 0"
cat "$WORK/off.out"
[ "$(cat "$WORK/off.out")" = "$(printf '1:two\n2:three')" ]
check $? "an explicit start offset skipped exactly the first record"

say "kcat -Q (offset by timestamp — a concrete ListOffsets timestamp)"
limit 30 kcat -b "$BOOTSTRAP" -Q -t "$TOPIC:0:$(( ($(date +%s) - 3600) * 1000 ))" > "$WORK/q.out" 2>&1
rc=$?
info "exit $rc: $(cat "$WORK/q.out")"

# --------------------------------------------------------------------- group
# EVERY option has to come BEFORE -G: in group mode kcat treats every remaining
# argument as a topic name, so a flag written after it is silently subscribed to
# instead of parsed, and the consumer then waits forever for a topic that does
# not exist. That is kcat's CLI, not the facade.
say "kcat -G (consumer group: join, assign, fetch, commit)"
# No -q on this one: kcat's quiet flag silences librdkafka's debug stream too,
# and the commit this step is about is only visible in it.
limit 120 kcat -b "$BOOTSTRAP" -e -o beginning -f '%p|%o|%s\n' -X debug=cgrp,protocol \
  -G "$GROUP" "$TOPIC" > "$WORK/grp.out" 2> "$WORK/grp.err"
check $? "kcat -G exited 0"
gn=$(wc -l < "$WORK/grp.out" | tr -d ' ')
[ "$gn" = 14 ]
check $? "the group read all 14 records ($gn)"
grep -q "OffsetCommit for .* returned: Success" "$WORK/grp.err"
check $? "librdkafka committed the group's offsets on close and the facade accepted it"

# -o beginning is a kcat SEEK and not an auto.offset.reset: give it again and
# kcat starts at offset 0 whatever the group committed, on any broker. The
# resume is therefore asked for the way a real consumer asks for it — a reset
# policy, no explicit offset — and the two new records go on a partition the
# first run committed, so "resume" and "reset" cannot be confused for each other.
say "kcat -G again in the same group: the committed offsets must hold it back"
printf 'late1\nlate2\n' | kcat -b "$BOOTSTRAP" -t "$TOPIC" -p 0 -P -X request.required.acks=-1
limit 120 kcat -b "$BOOTSTRAP" -e -q -f '%p|%o|%s\n' -X topic.auto.offset.reset=earliest \
  -G "$GROUP" "$TOPIC" > "$WORK/grp2.out" 2>&1
check $? "the second kcat -G exited 0"
cat "$WORK/grp2.out"
g2=$(wc -l < "$WORK/grp2.out" | tr -d ' ')
[ "$g2" = 2 ]
check $? "the restarted group read only the 2 new records ($g2), not the whole topic"
grep -q '^0|3|late1$' "$WORK/grp2.out"
check $? "it resumed at the committed offset (partition 0, offset 3)"

# ------------------------------------------------------ negotiated versions
say "request versions librdkafka actually negotiated"
cat "$WORK"/*.err | grep -oE 'Sent [A-Za-z]+Request \(v[0-9]+' | sort -u | sed 's/^/  /'
say "codec fallbacks librdkafka decided on"
cat "$WORK"/*.err | grep -i "does not support compression" | sort -u | sed 's/^/  /'
say "anything librdkafka called an error"
cat "$WORK"/*.err | grep -iE '%[0-9]|error|fail' | grep -v 'debug' | sort -u | head -20

printf '\nlogs: %s\n' "$WORK"
printf 'RESULT: %s\n' "$([ $FAIL = 0 ] && echo PASS || echo FAIL)"
exit $FAIL
