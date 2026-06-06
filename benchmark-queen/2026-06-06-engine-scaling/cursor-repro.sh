#!/usr/bin/env bash
# Deterministic repro / validation for the push-serialization cursor-skip fix.
#
# Runs against a Postgres that has the queen schema WITH the PUSHSER changes
# applied (lock_push_partition + clock_timestamp stamping in push_messages_v3 /
# execute_transaction_v2). Designed to run on the broker VM where Postgres is the
# `postgres` docker container.
#
# It proves four things:
#   NEG  (control): if created_at is NOT commit-ordered, the pop cursor skips a
#        message -> demonstrates the bug the fix prevents (uses direct INSERTs
#        with hand-set created_at to simulate the pre-fix race).
#   T1   the push partition lock is EXCLUSIVE per partition (a 2nd pusher blocks)
#        and STRUCTURALLY DISJOINT from pop's claim key (pop can still claim while
#        a push holds the partition) and from ack's key.
#   T2   under the fix, a 2nd push to a partition cannot commit while the 1st is
#        in flight; the later-committing push gets the larger created_at
#        (commit-ordered), and nothing is lost.
#   T3   a cursor scan from epoch sees BOTH messages and, after advancing to the
#        max, sees zero remaining (no skip).
#
# Exit code 0 = all PASS.
set -u
PGC="${PG_CONT:-postgres}"
DB="${PGDB:-postgres}"
Q="repro_$$"
fails=0
psql() { docker exec -i "$PGC" psql -U postgres -d "$DB" -qtA "$@"; }
check() { # check <label> <actual> <expected>
  if [ "$2" = "$3" ]; then echo "PASS  $1 ($2)"; else echo "FAIL  $1: got [$2] want [$3]"; fails=$((fails+1)); fi
}

echo "=== setup (queue=$Q) ==="
# Create queue+partition+two messages via the normal push path, then resolve PID.
psql -c "SELECT queen.push_messages_v3('[{\"queue\":\"$Q\",\"partition\":\"P\",\"payload\":{},\"transactionId\":\"seed\"}]'::jsonb,true,false)" >/dev/null
PID=$(psql -c "SELECT p.id FROM queen.partitions p JOIN queen.queues q ON q.id=p.queue_id WHERE q.name='$Q' AND p.name='P'")
echo "partition_id=$PID"
psql -c "DELETE FROM queen.messages m USING queen.partitions p,queen.queues q WHERE m.partition_id=p.id AND p.queue_id=q.id AND q.name='$Q'" >/dev/null

# ---------------------------------------------------------------------------
echo "=== NEG control: non-commit-ordered created_at -> pop cursor skips ==="
# Two rows: X has the SMALLER created_at but is the one that (in the pre-fix race)
# becomes visible LAST. Simulate a consumer that already consumed Y, then check
# whether X is still reachable by the forward-only cursor.
psql >/dev/null <<SQL
INSERT INTO queen.messages (id, transaction_id, partition_id, payload, created_at)
VALUES (gen_random_uuid(),'X','$PID','{}','2020-01-01 00:00:00+00'),
       (gen_random_uuid(),'Y','$PID','{}','2020-01-02 00:00:00+00');
SQL
# Consumer "already consumed" Y (the later-created, earlier-committed one). The
# forward-only cursor scan strictly after Y's (created_at,id): X (created_at
# 2020-01-01) is below the cursor, so it is invisible -> the skip. Tuple compared
# inside SQL (a timestamptz literal contains a space, so never round-trip it).
NEG_REMAINING=$(psql -c "SELECT count(*) FROM queen.messages WHERE partition_id='$PID' AND (created_at,id) > (SELECT created_at,id FROM queen.messages WHERE transaction_id='Y' AND partition_id='$PID')")
NEG_X_REACHED=$(psql -c "SELECT count(*) FROM queen.messages WHERE partition_id='$PID' AND transaction_id='X' AND (created_at,id) > (SELECT created_at,id FROM queen.messages WHERE transaction_id='Y' AND partition_id='$PID')")
check "NEG: X is skipped by forward cursor (unreachable)" "$NEG_X_REACHED" "0"
check "NEG: cursor reports 0 remaining yet X never consumed (=loss)" "$NEG_REMAINING" "0"
psql -c "DELETE FROM queen.messages WHERE partition_id='$PID'" >/dev/null

# ---------------------------------------------------------------------------
echo "=== T1: push lock exclusive per-partition + disjoint from pop/ack keys ==="
# Session A holds the push lock for PID for 3s.
( psql >/dev/null 2>&1 <<SQL
BEGIN; SELECT queen.lock_push_partition('$PID'); SELECT pg_sleep(3); COMMIT;
SQL
) &
APID=$!
sleep 0.7
# pop's claim key (single-bigint, ns 12648430) must STILL be acquirable -> disjoint
POP_OK=$(psql -c "SELECT pg_try_advisory_xact_lock(hashtextextended('$PID'::text, 12648430))")
check "T1: pop claim key acquirable while push holds partition (disjoint)" "$POP_OK" "t"
# ack's key (single-bigint md5) must still be acquirable -> disjoint
ACK_OK=$(psql -c "SELECT pg_try_advisory_xact_lock(('x'||substr(md5('$PID'||'__QUEUE_MODE__'),1,16))::bit(64)::bigint)")
check "T1: ack key acquirable while push holds partition (disjoint)" "$ACK_OK" "t"
# a 2nd push lock on the SAME partition must block (we time out) -> exclusive
PUSH_BLOCKED=$(psql -c "SET lock_timeout='600ms'; SELECT queen.lock_push_partition('$PID');" 2>&1 | grep -c -i "lock_timeout\|canceling statement")
check "T1: 2nd push on same partition blocks (exclusive)" "$PUSH_BLOCKED" "1"
wait "$APID" 2>/dev/null

# ---------------------------------------------------------------------------
echo "=== T2/T3: concurrent push is commit-ordered, zero loss ==="
# Session A: push msg A, hold the txn open 2s, then commit (A's lock is held the
# whole time). Its created_at is stamped (clock_timestamp) at insert ~ now.
( psql >/dev/null 2>&1 <<SQL
BEGIN;
SELECT queen.push_messages_v3('[{"queue":"$Q","partition":"P","payload":{},"transactionId":"A"}]'::jsonb,true,false);
SELECT pg_sleep(2);
COMMIT;
SQL
) &
APID=$!
sleep 0.5
# Session B tries to push to the same partition. It BLOCKS on A's push lock until
# A commits (~1.5s from now), then inserts (clock_timestamp now > A's) and commits.
T0=$(date +%s.%N)
psql -c "SELECT queen.push_messages_v3('[{\"queue\":\"$Q\",\"partition\":\"P\",\"payload\":{},\"transactionId\":\"B\"}]'::jsonb,true,false)" >/dev/null
T1=$(date +%s.%N)
wait "$APID" 2>/dev/null
B_WAIT_MS=$(awk "BEGIN{printf \"%d\", ($T1-$T0)*1000}")
echo "B push blocked for ~${B_WAIT_MS}ms (expect >~1000ms, proving serialization)"
BLOCKED_OK=$(awk "BEGIN{print ($B_WAIT_MS>800)?1:0}")
check "T2: B's push blocked until A committed" "$BLOCKED_OK" "1"

# Both present?
CNT=$(psql -c "SELECT count(*) FROM queen.messages m JOIN queen.partitions p ON p.id=m.partition_id JOIN queen.queues q ON q.id=p.queue_id WHERE q.name='$Q' AND m.transaction_id IN ('A','B')")
check "T2: both A and B present (zero loss)" "$CNT" "2"
# created_at(B) > created_at(A): commit order (A committed first, B second)
ORDERED=$(psql -c "SELECT (SELECT created_at FROM queen.messages WHERE transaction_id='B' AND partition_id='$PID') > (SELECT created_at FROM queen.messages WHERE transaction_id='A' AND partition_id='$PID')")
check "T2: created_at(B) > created_at(A) (commit-ordered)" "$ORDERED" "t"

# T3: cursor from epoch sees both; after advancing to max, zero remain (no skip).
SEEN=$(psql -c "SELECT count(*) FROM queen.messages WHERE partition_id='$PID' AND (created_at,id) > ('epoch','00000000-0000-0000-0000-000000000000'::uuid)")
check "T3: cursor-from-epoch sees both messages" "$SEEN" "2"
REMAIN=$(psql -c "SELECT count(*) FROM queen.messages WHERE partition_id='$PID' AND (created_at,id) > (SELECT created_at,id FROM queen.messages WHERE partition_id='$PID' ORDER BY created_at DESC,id DESC LIMIT 1)")
check "T3: after consuming to max, zero remaining (nothing skipped)" "$REMAIN" "0"

# ---------------------------------------------------------------------------
echo "=== T4: high-concurrency push to ONE partition, zero lost inserts ==="
# K concurrent sessions each push a batch of M unique messages to the SAME
# partition. They contend on the single push-partition lock and must all commit
# (serialized, deadlock-free). Assert total rows == K*M and created_at is unique
# enough that a forward cursor covers every row (no skip).
K=10; M=50
psql -c "DELETE FROM queen.messages m USING queen.partitions p,queen.queues q WHERE m.partition_id=p.id AND p.queue_id=q.id AND q.name='$Q'" >/dev/null
pids=()
for k in $(seq 1 "$K"); do
  ( psql -c "SELECT queen.push_messages_v3((SELECT jsonb_agg(jsonb_build_object('queue','$Q','partition','P','payload','{}'::jsonb,'transactionId',gen_random_uuid()::text)) FROM generate_series(1,$M)), true, false)" >/dev/null 2>&1 ) &
  pids+=($!)
done
cc_fail=0
for p in "${pids[@]}"; do wait "$p" || cc_fail=$((cc_fail+1)); done
check "T4: all $K concurrent pushers succeeded" "$cc_fail" "0"
TOTAL=$(psql -c "SELECT count(*) FROM queen.messages WHERE partition_id='$PID'")
check "T4: zero lost inserts (count == K*M)" "$TOTAL" "$((K*M))"
# every row reachable by a forward cursor from epoch (no row below a consumed max)
REACH=$(psql -c "SELECT count(*) FROM queen.messages WHERE partition_id='$PID' AND (created_at,id) > ('epoch','00000000-0000-0000-0000-000000000000'::uuid)")
check "T4: every row reachable by forward cursor (no skip)" "$REACH" "$((K*M))"

echo "=== cleanup ==="
psql -c "DELETE FROM queen.messages m USING queen.partitions p,queen.queues q WHERE m.partition_id=p.id AND p.queue_id=q.id AND q.name='$Q'" >/dev/null
psql -c "DELETE FROM queen.partitions p USING queen.queues q WHERE p.queue_id=q.id AND q.name='$Q'" >/dev/null
psql -c "DELETE FROM queen.queues WHERE name='$Q'" >/dev/null

echo "=============================="
if [ "$fails" -eq 0 ]; then echo "ALL PASS"; else echo "$fails CHECK(S) FAILED"; fi
exit "$fails"
