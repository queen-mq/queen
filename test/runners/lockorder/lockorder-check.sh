#!/usr/bin/env bash
# =============================================================================
#  LOCK-ORDER CONCURRENCY GATE
#  PLAN_KV_TIMERS.md §2 (ordine di lock), §15 row "Concorrenza sull'ordine di
#  lock", phase F4. MERGE CRITERION, not a recommendation.
#
#  ---------------------------------------------------------------------------
#  READ THIS BEFORE YOU DELETE, SHORTEN OR "SPEED UP" THIS TEST.
#
#  THE PURPOSE OF THIS TEST IS THE LOCK ORDER. That is the whole of it. It is
#  slow ON PURPOSE and it is contended ON PURPOSE.
#
#  PLAN_KV_TIMERS.md declares a total order over six lock spaces:
#
#     queen.kv -> queen.log_timers -> ADV -> queen.queues
#              -> queen.log_partitions -> queen.log_consumers -> leaves
#
#  and proves deadlock-freedom from it (§2.2). Today that proof lives ONLY in a
#  prose header inside 024_kv.sql / 025_log_timers.sql — §18.7 accepts that as a
#  risk. This file is the only mechanical check that the code still obeys the
#  header. Five of the six cycles catalogued in §2.4 (C1, C3, C4, C5, C6) are the
#  OBVIOUS implementation of their own piece: the natural way to write the fire
#  path, the natural ORDER BY, the natural pop rider, the natural partition_id
#  column, the natural lazy provisioning. Every one of them reintroduces a cycle
#  that no unit test and no review checklist catches, because each fragment looks
#  correct on its own and the failure is a 40P01 that appears only under
#  concurrency, only on some collations, and only on some installations.
#
#  So: it drives N concurrent bundles over a DELIBERATELY CROSSED set of kv keys,
#  timer keys and partitions, with the sweeper's fire path and a cancel storm
#  running against the same rows, and it FAILS ON ANY 40P01 — from any actor, on
#  any statement, observed anywhere.
#
#  If you find it slow: lower LO_ROUNDS. Do not lower the contention (LO_KV_KEYS,
#  LO_TIMER_KEYS, LO_PARTS) — the small key sets ARE the test. Do not delete the
#  psql check either: pg_stat_database.deadlocks is the only detector that sees a
#  40P01 the broker swallowed or retried.
#  ---------------------------------------------------------------------------
#
#  THE CROSS, actor by actor (§2.3 table; the numbers are that table's).
#
#    lane 0  kv + push, kv keys ASCENDING, partitions DESCENDING   actor 1, 2
#    lane 1  kv + push, kv keys DESCENDING, partitions ASCENDING   actor 1, 2
#    lane 2  kv + timer + ack in one bundle (pops for a lease)     actor 1, 3
#    lane 3  timers only, through the wire, crossed key order      actor 1
#    lane 4  standalone kv on the path routes (PUT then DELETE)    actor 12
#    lane 5  standalone timer schedule + a CANCEL STORM            actor 13
#    lane 6  plain push storm (fusion path) + pop/ack, no kv       actor 2, 3, 4
#    ambient the SWEEPER fires the maturing timers throughout      actor 6, 7
#
#  The crossing that matters is NOT the order of ops inside one body — §2.4 C3
#  point 3 forbids the broker from pre-sorting, so the SP owns the order and the
#  permuted input is exactly the adversarial case. What matters is which SPACES
#  a bundle touches and which two actors hold them at the same instant:
#
#    * lane 0/1 hold kv rows while waiting on partitions held by lane 6's fusion
#      flush (§2.3 "the case that looks like a cycle and is not" — a chain, and
#      the accepted cost of §18.2). If that chain ever closes, it closes here.
#    * the sweeper fire is T -> Q -> P while the wire is KV -> T -> Q -> P. C1 is
#      the fire that pushes before deleting; it deadlocks against lane 0/1 only
#      when both are in flight, which is why the timer delays below are SHORT.
#    * lane 5's cancel races the claim on rows the fire is about to take: C2's
#      cancelled "pre-lock before DELETE" would make the fire a waiter in T here.
#    * lane 2 reaches log_consumers (space 6) while still holding kv rows, which
#      is the C4 shape from the other side.
#
#  Timers are scheduled with delays of 0..LO_TIMER_MAX_DELAY_MS so they mature
#  DURING the storm. A run in which nothing fired has not tested actor 6, so the
#  fire participation is asserted at the end and a non-participating run FAILS.
# =============================================================================
set -uo pipefail

URL="${QUEEN_HTTP_URL:-http://localhost:16633}"

# Contention knobs. Small on purpose: few keys, many workers.
WORKERS="${LO_WORKERS:-14}"
ROUNDS="${LO_ROUNDS:-40}"
KV_KEYS="${LO_KV_KEYS:-6}"
TIMER_KEYS="${LO_TIMER_KEYS:-8}"
PARTS="${LO_PARTS:-4}"
TIMER_MAX_DELAY_MS="${LO_TIMER_MAX_DELAY_MS:-400}"
DRAIN_DEADLINE_S="${LO_DRAIN_DEADLINE_S:-45}"

RID="lo$$"
NS="lockorder"
Q0="lo-q0-$RID"
Q1="lo-q1-$RID"
TQ="lo-timers-$RID"      # timer destination queue; created lazily at fire
GRP="lo-grp"

PASS=0; FAIL=0
say()  { printf '%s\n' "$*"; }
ok()   { PASS=$((PASS+1)); say "  ok   - $1"; }
bad()  { FAIL=$((FAIL+1)); say "  FAIL - $1"; }

TMP="$(mktemp -d -t queen-lockorder.XXXXXX)"
trap 'rm -rf "$TMP"' EXIT

# --- psql -------------------------------------------------------------------
# Required, never optional: pg_stat_database.deadlocks is the authoritative
# detector. A 40P01 that the broker retried internally, or that hit a background
# task instead of a request, leaves NO trace in any HTTP body. Without this the
# whole file degrades to a smoke test, so a missing psql is a FAIL, not a skip.
#
# Standalone use against a container-only Postgres:
#   QUEEN_PSQL="docker exec -i kvt-pg-1 psql -U postgres -d postgres" \
#     test/runners/lockorder/lockorder-check.sh
PSQL="${QUEEN_PSQL:-}"
if [ -z "$PSQL" ]; then
  PSQL="psql postgresql://${QUEEN_PG_USER:-postgres}:${QUEEN_PG_PASSWORD:-postgres}@${QUEEN_PG_HOST:-localhost}:${QUEEN_PG_PORT:-5432}/${QUEEN_PG_DB:-postgres}"
fi
# shellcheck disable=SC2086  # $PSQL is a command line and must word-split
sql() { printf '%s\n' "$1" | $PSQL -qtAX -v ON_ERROR_STOP=1 -f - 2>&1 | tr -d ' \r'; }

# --- http -------------------------------------------------------------------
# Every worker appends its findings to $OUT. Any line there is a failure; the
# parent aggregates after the join. Bodies are scanned for the SQLSTATE and for
# the message text, because the broker surfaces DB errors both ways (raw
# db_error message on the transaction path, JSON error field elsewhere).
# BODYF is per-worker: the storm runs the same function in N background
# subshells, and $$ does NOT change in a bash subshell, so a single shared body
# file would let lane 2 read another lane's leaseId.
BODYF="$TMP/body.main"
req() { # method path [body]  -> RC, BODY
  local m=$1 p=$2 b=${3:-}
  local args=(-s -o "$BODYF" -w '%{http_code}' --max-time 30 -X "$m")
  [ -n "$b" ] && args+=(-H 'Content-Type: application/json' --data-binary "$b")
  RC=$(curl "${args[@]}" "$URL$p" 2>/dev/null) || RC="000"
  BODY=$(cat "$BODYF" 2>/dev/null)
  case "$BODY" in
    *40P01*|*"deadlock detected"*|*"deadlock_detected"*)
      printf 'DEADLOCK-IN-BODY %s %s :: %s\n' "$m" "$p" "$BODY" >> "$OUT" ;;
  esac
  case "$RC" in
    000) printf 'NO-RESPONSE %s %s\n' "$m" "$p" >> "$OUT" ;;
    5??) printf 'HTTP-5XX %s %s %s :: %s\n' "$RC" "$m" "$p" "$BODY" >> "$OUT" ;;
  esac
  case "$RC" in 2??) printf 'req2xx\n' >> "$CNT" ;; esac
}

jv() { printf '%s' "${BODY:-}" | jq -r "($1) | tostring" 2>/dev/null || echo "?"; }

# --- op builders ------------------------------------------------------------
# The wire shapes these build ARE the contract this gate pins (PLAN §6.3, §8.2,
# §10.4):
#
#   {"operations":[ push/ack, ... ],          <- unchanged, `type`-discriminated
#    "kv":      [ {"op":...,"ns":...,"key":...}, ... ],
#    "timers":  [ {"op":"schedule"|"cancel","queue":...,"timerKey":...}, ... ]}
#
# KV and timer ops are TOP-LEVEL SIBLING ARRAYS, never elements of operations[]
# with a `type` field. §10.4 decides it and the reason is Go: two struct fields
# sharing one JSON key at one level are BOTH silently dropped by encoding/json,
# so an inline shape would ship bundles whose gate simply is not there, with no
# error anywhere. 005_log_ack.sql agrees from the other end — it reads `p->'kv'`
# and `p->'timers'` off the payload root.
#
# A timer `payload` is BASE64 TEXT, not a JSON object: it is opaque bytes end to
# end (it may be zstd-compressed, it may be encrypted §13.4), so there is no
# point on the wire at which it is a JSON value.
#
# delayMs (not delaySeconds) per §20.6 as ratified: sub-second durations are ms.
# ttlSeconds (never ttlMillis) per §20.1.
#
# base64 of {"lo":1} — a constant, so the storm does not fork a `base64` per op.
LO_PAYLOAD_B64='eyJsbyI6MX0='
kv_put() { # ns key value ttl
  printf '{"op":"put","ns":"%s","key":"%s","value":%s,"ttlSeconds":%s}' "$1" "$2" "$3" "$4"
}
kv_incr() { # ns key delta ttl
  printf '{"op":"incr","ns":"%s","key":"%s","delta":%s,"ttlSeconds":%s}' "$1" "$2" "$3" "$4"
}
timer_sched() { # queue timerKey partition delayMs txn
  printf '{"op":"schedule","queue":"%s","timerKey":"%s","partition":"%s","delayMs":%s,"txn":"%s","payload":"%s"}' \
    "$1" "$2" "$3" "$4" "$5" "$LO_PAYLOAD_B64"
}
timer_cancel() { # queue timerKey
  printf '{"op":"cancel","queue":"%s","timerKey":"%s"}' "$1" "$2"
}
push_op() { # queue partition txn
  printf '{"type":"push","queue":"%s","partition":"%s","payload":{"lo":1},"transactionId":"%s"}' "$1" "$2" "$3"
}

# --- preflight --------------------------------------------------------------
# Every broker of this build has the kv and timer routes; no flag can withhold
# them. What this preflight still catches is a URL that reaches something else —
# the proxy, an older image — against which every bundle below would be a 400
# from the demux fallthrough, a green run that tested nothing. Refuse to run.
say "== preflight =="
export QUEEN_WAIT_URLS="${QUEEN_WAIT_URLS:-$URL/health}"
if command -v wait-for-broker >/dev/null 2>&1; then wait-for-broker; fi

RC=$(curl -s -o /dev/null -w '%{http_code}' --max-time 10 "$URL/health" 2>/dev/null) || RC=000
if [ "$RC" != "200" ]; then
  say "!! broker not healthy at $URL (health -> $RC)"
  say "LOCKORDER: FAIL"; exit 1
fi
ok "broker healthy at $URL"

DL0=$(sql "SELECT deadlocks FROM pg_stat_database WHERE datname = current_database();")
case "$DL0" in
  ''|*[!0-9]*)
    say "!! cannot read pg_stat_database.deadlocks — psql is NOT optional here."
    say "!! got: $DL0"
    say "!! set QUEEN_PSQL, or QUEEN_PG_HOST/PORT/USER/PASSWORD/DB."
    say "LOCKORDER: FAIL"; exit 1 ;;
esac
ok "postgres reachable; deadlocks counter starts at $DL0"

OUT="$TMP/findings"; : > "$OUT"
CNT="$TMP/counts";   : > "$CNT"

# KV surface present?
req POST /api/v1/kv "{\"operations\":[$(kv_put "$NS" "preflight:$RID" '{"p":1}' 60)]}"
KVRC="$RC"
# Timer surface present?
req POST /api/v1/timers "{\"operations\":[$(timer_sched "$TQ" "preflight:$RID" Default 60000 "pre-$RID")]}"
TMRC="$RC"
# And the RIDER shape on the transaction wire, which is what the storm actually
# drives. A 2xx on the two standalone routes above does not imply the wire
# accepts the sibling arrays — that is a different code path (§6.3's graft into
# 005_log_ack.sql plus §8.2's demux), and if it 400s every lane below degenerates
# into a plain push storm that tests nothing about the kv/timer lock spaces.
req POST /api/v1/transaction "{\"operations\":[],\
\"kv\":[$(kv_put "$NS" "preflight-wire:$RID" '{"p":1}' 60)],\
\"timers\":[$(timer_sched "$TQ" "preflight-wire:$RID" Default 60000 "prew-$RID")]}"
WIRERC="$RC"; WIREOK=$(jv '.success')
if [ "$KVRC" = "404" ] || [ "$TMRC" = "404" ]; then
  say "!! kv=$KVRC timers=$TMRC — these routes exist on every broker of this"
  say "!! build, so a 404 means \$QUEEN_HTTP_URL is not one: check for the proxy"
  say "!! or an older image before looking at the broker."
  say "!! Refusing to report a vacuous pass."
  say "LOCKORDER: FAIL"; exit 1
fi
case "$KVRC" in 2??) ok "kv surface answers ($KVRC)";; *) bad "kv surface answered $KVRC: $(head -c 300 "$BODYF" 2>/dev/null)";; esac
case "$TMRC" in 2??) ok "timers surface answers ($TMRC)";; *) bad "timers surface answered $TMRC";; esac
if [ "$WIRERC" = "200" ] && [ "$WIREOK" = "true" ]; then
  ok "the transaction wire accepts the kv/timers rider arrays"
else
  bad "the wire rider was refused (HTTP $WIRERC, success=$WIREOK): $(head -c 300 "$BODYF" 2>/dev/null)"
fi
if [ "$FAIL" != "0" ]; then
  say "!! the kv/timer surfaces do not answer; the storm below would be vacuous."
  say "LOCKORDER: FAIL"; exit 1
fi

# Seed both storm queues + the shared partitions so the storm contends on
# EXISTING partition rows from its first request (lazy provisioning on the first
# hit would otherwise serialize the opening rounds on queen.queues and hide the
# window this test is looking for).
for q in "$Q0" "$Q1"; do
  req POST /api/v1/configure "{\"queue\":\"$q\",\"leaseTime\":30,\"retryLimit\":5}"
done
SEED='['
i=0
while [ "$i" -lt "$PARTS" ]; do
  for q in "$Q0" "$Q1"; do
    [ "$SEED" = '[' ] || SEED="$SEED,"
    SEED="$SEED{\"queue\":\"$q\",\"partition\":\"p$i\",\"payload\":{\"seed\":$i}}"
  done
  i=$((i+1))
done
SEED="$SEED]"
req POST /api/v1/push "{\"items\":$SEED}"
ok "seeded $PARTS partitions on $Q0 and $Q1"
# Open the group cursor before the storm so lane 2/6 pops see the seeded frames.
req GET "/api/v1/pop/queue/$Q0?batch=1&consumerGroup=$GRP&partitions=$PARTS&subscriptionMode=all&wait=false" ""

# =============================================================================
#  THE STORM
# =============================================================================
say ""
say "== storm: $WORKERS workers x $ROUNDS rounds over $KV_KEYS kv keys / $TIMER_KEYS timer keys / $PARTS partitions =="
say "   (this is the slow part, and it is the point of the file)"

worker() {
  # NB: three statements, not `local w=$1 lane=$((w % 7))`. `local` is a builtin,
  # so bash expands ALL of its arguments before any assignment takes effect —
  # $((w % 7)) would read an unset w and abort every worker under `set -u`.
  local w=$1
  local lane=$((w % 7))
  local r=0
  BODYF="$TMP/body.w$w"
  while [ "$r" -lt "$ROUNDS" ]; do
    local a=$(( (w + r) % KV_KEYS ))
    local b=$(( (w * 3 + r * 5 + 1) % KV_KEYS ))
    local t1=$(( (w + r) % TIMER_KEYS ))
    local t2=$(( (w * 5 + r * 3 + 2) % TIMER_KEYS ))
    local pa=$(( (w + r) % PARTS ))
    local pb=$(( (w * 2 + r + 1) % PARTS ))
    local d=$(( (r * 137 + w * 31) % (TIMER_MAX_DELAY_MS + 1) ))
    local tag="$RID-$w-$r"
    local q_a="$Q0" q_b="$Q1"

    case "$lane" in
      0)  # kv ascending, partitions descending. The SP must sort; we do not.
          # The crossing lives INSIDE each array (kv keys a,b vs b,a and
          # partitions pb,pa vs pa,pb), which is where it has to be now that the
          # riders are their own arrays: §2.4 C3 point 3 forbids the broker from
          # pre-sorting, so the permuted array IS the adversarial input.
          req POST /api/v1/transaction "{\"operations\":[\
$(push_op "$q_b" "p$pb" "$tag-x"),\
$(push_op "$q_a" "p$pa" "$tag-y")],\
\"kv\":[$(kv_put "$NS" "k$a" "{\"w\":$w}" 120),$(kv_put "$NS" "k$b" "{\"w\":$w}" 120)]}"
          ;;
      1)  # mirror of lane 0: the inverted input order on the same key set.
          req POST /api/v1/transaction "{\"operations\":[\
$(push_op "$q_a" "p$pa" "$tag-y"),\
$(push_op "$q_b" "p$pb" "$tag-x")],\
\"kv\":[$(kv_put "$NS" "k$b" "{\"w\":$w}" 120),$(kv_put "$NS" "k$a" "{\"w\":$w}" 120)]}"
          ;;
      2)  # kv + timer + ack: the only lane that reaches log_consumers (space 6)
          # while holding kv rows. C4 seen from the wire side.
          req GET "/api/v1/pop/queue/$q_a?batch=2&consumerGroup=$GRP&partitions=$PARTS&leaseSeconds=30&wait=false"
          local pid txn lease
          pid=$(jv '.messages[0].partitionId'); txn=$(jv '.messages[0].transactionId'); lease=$(jv '.messages[0].leaseId')
          if [ "$pid" != "?" ] && [ -n "$pid" ] && [ "$pid" != "null" ]; then
            req POST /api/v1/transaction "{\"operations\":[\
{\"type\":\"ack\",\"transactionId\":\"$txn\",\"partitionId\":\"$pid\",\"consumerGroup\":\"$GRP\",\"leaseId\":\"$lease\",\"status\":\"completed\"}],\
\"kv\":[$(kv_incr "$NS" "k$a" 1 120)],\
\"timers\":[$(timer_sched "$TQ" "t$t1" "p$pa" "$d" "$tag-t1")],\
\"requiredLeases\":[\"$lease\"]}"
          else
            req POST /api/v1/transaction "{\"operations\":[],\
\"kv\":[$(kv_incr "$NS" "k$a" 1 120)],\
\"timers\":[$(timer_sched "$TQ" "t$t1" "p$pa" "$d" "$tag-t1")]}"
          fi
          ;;
      3)  # timers only, two keys per bundle in crossed order. The bundle holds
          # T rows across the whole commit while the fire wants the same rows.
          # NB: a timers-only bundle is NOT routed off the wire (only KV-only is,
          # §2.5), so this really does exercise the wire's sequence.
          req POST /api/v1/transaction "{\"operations\":[],\
\"timers\":[$(timer_sched "$TQ" "t$t2" "p$pb" "$d" "$tag-t2"),\
$(timer_sched "$TQ" "t$t1" "p$pa" "$d" "$tag-t1")]}"
          ;;
      4)  # standalone kv on the fully specified path routes (§8.1). Actor 12:
          # a singleton in KV, but it is the actor that proves a singleton can
          # never be an edge of the graph.
          req PUT "/api/v1/kv/$NS/k$a" "{\"value\":{\"standalone\":$w},\"ttlSeconds\":90}"
          req DELETE "/api/v1/kv/$NS/k$b" '{}'
          ;;
      5)  # standalone schedule + CANCEL STORM. The cancel is the actor §2.4 C2
          # is about: if the fire ever waits in T, a cancel held by a slow bundle
          # is what freezes it.
          req POST /api/v1/timers "{\"operations\":[$(timer_sched "$TQ" "t$t2" "p$pb" "$d" "$tag-s")]}"
          req DELETE "/api/v1/timers/$TQ/t$t1" '{}'
          ;;
      6)  # plain push storm (the fusion path, actor 2) + pop/ack. No kv, no
          # timers: this is the lane whose partition locks the kv-holding lanes
          # must wait on without ever closing a cycle.
          req POST /api/v1/push "{\"items\":[\
{\"queue\":\"$q_a\",\"partition\":\"p$pa\",\"payload\":{\"f\":$w}},\
{\"queue\":\"$q_b\",\"partition\":\"p$pb\",\"payload\":{\"f\":$w}},\
{\"queue\":\"$q_a\",\"partition\":\"p$pb\",\"payload\":{\"f\":$w}}]}"
          req GET "/api/v1/pop/queue/$q_b?batch=4&consumerGroup=$GRP&partitions=$PARTS&autoAck=true&wait=false"
          ;;
    esac
    r=$((r+1))
  done
}

W=0
while [ "$W" -lt "$WORKERS" ]; do
  worker "$W" &
  W=$((W+1))
done
wait
say "   storm done"

# =============================================================================
#  DRAIN + ASSERTIONS
# =============================================================================
say ""
say "== drain: the fire path must have participated =="
# A run where nothing fired has not exercised actor 6 (T -> Q -> P), i.e. it has
# not tested C1 at all. Waiting for the table to drain is how we know it did.
#
# The preflight rows are EXCLUDED, and that exclusion is load-bearing rather than
# cosmetic: the preflight schedules its probes 60 s out (deliberately far enough
# that they cannot fire and confuse the fired-count), while this deadline is 45 s.
# Counting them makes the drain assertion fail on a perfectly drained table, and
# the failure text ("still holds 2 rows") points at the fire path instead of at
# the clock. Only the STORM's rows — timer_key 't<N>' — are the population under
# test; they are scheduled 0..LO_TIMER_MAX_DELAY_MS out precisely so they mature
# during the storm.
DEADLINE=$(( $(date +%s) + DRAIN_DEADLINE_S ))
PENDING=-1
while [ "$(date +%s)" -lt "$DEADLINE" ]; do
  PENDING=$(sql "SELECT count(*) FROM queen.log_timers WHERE queue = '$TQ' AND timer_key NOT LIKE 'preflight%';")
  case "$PENDING" in ''|*[!0-9]*) PENDING=-1; break;; esac
  [ "$PENDING" = "0" ] && break
  sleep 1
done
# last_offset is the allocator watermark: last_offset+1 frames were allocated
# into that partition. There is no total_messages column on queen.log_partitions.
FIRED=$(sql "SELECT COALESCE(sum(lp.last_offset + 1),0) FROM queen.log_partitions lp JOIN queen.queues q ON q.id = lp.queue_id WHERE q.name = '$TQ';")
case "$FIRED" in ''|*[!0-9]*) bad "could not count fired timer messages: $FIRED"; FIRED=0;; esac
if [ "$FIRED" -gt 0 ]; then
  ok "the sweeper fired $FIRED timer message(s) into $TQ during the storm (actor 6 participated)"
else
  bad "NOTHING FIRED into $TQ: actor 6 never ran, so this run did not test the fire/wire cross"
fi
if [ "$PENDING" = "0" ]; then
  ok "queen.log_timers drained for $TQ"
elif [ "$PENDING" = "-1" ]; then
  bad "queen.log_timers could not be queried (does the table exist?)"
else
  bad "queen.log_timers still holds $PENDING storm row(s) for $TQ after ${DRAIN_DEADLINE_S}s"
fi

KVROWS=$(sql "SELECT count(*) FROM queen.kv WHERE namespace = '$NS';")
case "$KVROWS" in
  ''|*[!0-9]*) bad "queen.kv could not be queried (does the table exist?): $KVROWS";;
  0)           bad "queen.kv is EMPTY in namespace $NS: no kv row was ever written, the cross had no kv leg";;
  *)           ok "queen.kv holds $KVROWS row(s) in namespace $NS (the kv space was really locked)";;
esac

REQ2XX=$(grep -c 'req2xx' "$CNT" 2>/dev/null); REQ2XX=${REQ2XX:-0}
MINREQ=$(( WORKERS * ROUNDS / 2 ))
if [ "$REQ2XX" -ge "$MINREQ" ]; then
  ok "$REQ2XX successful requests (>= $MINREQ): the storm really ran"
else
  bad "only $REQ2XX successful requests (< $MINREQ): the storm mostly errored, findings below are not trustworthy"
fi

say ""
say "== THE ASSERTION: no 40P01, from anybody, anywhere =="
DL1=$(sql "SELECT deadlocks FROM pg_stat_database WHERE datname = current_database();")
case "$DL1" in ''|*[!0-9]*) DL1="-1";; esac
if [ "$DL1" = "$DL0" ]; then
  ok "pg_stat_database.deadlocks unchanged at $DL1"
else
  bad "pg_stat_database.deadlocks went $DL0 -> $DL1: the declared total order of §2 is VIOLATED"
fi

NDL=$(grep -c 'DEADLOCK-IN-BODY' "$OUT" 2>/dev/null); NDL=${NDL:-0}
if [ "$NDL" = "0" ]; then
  ok "no 40P01 surfaced in any response body"
else
  bad "$NDL response(s) carried a deadlock error"
  grep 'DEADLOCK-IN-BODY' "$OUT" | head -5 | while IFS= read -r l; do say "        $l"; done
fi

N5=$(grep -c 'HTTP-5XX' "$OUT" 2>/dev/null); N5=${N5:-0}
NNR=$(grep -c 'NO-RESPONSE' "$OUT" 2>/dev/null); NNR=${NNR:-0}
if [ "$N5" = "0" ] && [ "$NNR" = "0" ]; then
  ok "no 5xx and no dropped connection during the storm"
else
  bad "$N5 x 5xx and $NNR x no-response during the storm"
  grep -E 'HTTP-5XX|NO-RESPONSE' "$OUT" | head -5 | while IFS= read -r l; do say "        $l"; done
fi

say ""
say "passed: $PASS   failed: $FAIL"
if [ "$FAIL" = "0" ]; then say "LOCKORDER: PASS"; exit 0; fi
say "LOCKORDER: FAIL"
exit 1
