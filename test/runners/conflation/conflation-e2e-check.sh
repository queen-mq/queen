#!/usr/bin/env bash
# =============================================================================
#  CONFLATION E2E GATE — PLAN_CONFLATION.md §7.3, at the raw HTTP wire.
#
#  RED-FIRST: this suite was written BEFORE the feature (TDD's red half). On a
#  broker without conflation every scenario below must FAIL on its assertions
#  (never on transport errors): the `conflation` query param is an unknown key
#  serde ignores, so pops answer 200/204 and simply deliver full batches — which
#  is exactly the behaviour these tests exist to rule out.
#
#  WHY RAW HTTP AND NOT AN SDK. Same reason as test/runners/http: the wire IS
#  the contract, and the SDK halves of this feature land later (§4). curl is the
#  client, jq is the assertion. Nothing here depends on any clients/ change.
#
#  WHAT IS COVERED (one section per scenario, names from the plan):
#    E2E-1  the §1.3 guarantee: a push during processing STAYS PENDING; the
#           follow-up pop delivers the tail; adversarial continuous-producer run.
#    E2E-2  DLQ under a hot producer (the M2 pin): retry budget survives
#           supersession, poison = tail at last attempt, no livelock (deadline).
#    E2E-3  mixed groups on one queue: `audit` reads everything, `workers`
#           conflates, newest in both.
#    E2E-4  declaration conflict: stored policy wins loudly — the
#           `conflationConflict` response echo (§3.1/§3.3), both directions —
#           plus the two §3.3 refusals (no group / autoAck) as 400s.
#           TWO CLAUSES OF §7.3 E2E-4 ARE DELIBERATELY NOT ASSERTED HERE, and
#           neither is an oversight. "the conflict counter increments" is
#           `add_conflation_conflict` (§3.3 item 2), which surfaces only on the
#           windowed `rates` LOG line as `cfl_conflict_s` (§6.1) — §6.3 adds no
#           Prometheus family for it, so it has no HTTP surface to assert
#           against. "the disagreeing consumer warns exactly once" is SDK
#           behaviour (§4 item 3), and there is no SDK in this path by design.
#           The response echo asserted below is the wire-observable half, and it
#           is the half the SDK warning is derived FROM.
#    MODES  §1.5 composition, e2e shape of §7.1(9): all+conflation delivers 1
#           over a backlog; new+conflation delivers 0 until the next push.
#    DEPTH  §2.5/§5.3: partitionsPending / conflation / effectivePending on
#           GET /api/v1/resources/queues/:queue/depth.
#    COUNTER §6.3: queen_queue_conflated_per_minute surfaces on
#           /metrics/prometheus (fed by ack's `conflated` through syscollect's
#           METRICS_FLUSH_MS lane, default 60 s — hence this section's deadline).
#
#  NOT COVERED HERE — E2E-5 (new SDK against an old broker). The behaviour under
#  test there is the SDK's degrade-loudly error (§4): "conflation requested but
#  not applied". A suite with no SDK in the path cannot express it, and the
#  harness has no old-broker topology. It belongs with the §7.2 client suites
#  when the SDK halves land.
#
#  HYGIENE (the §10.4 rules, adapted): queue names, groups and transaction ids
#  are all PER-RUN — a consumer group carries a cursor and the dedup ring
#  carries txn ids, neither of which any DELETE here can reset — and the queues
#  are dropped from an EXIT trap so a run that dies half way leaves nothing.
#
#  Requires: bash, curl, jq, a broker. Usage:
#    QUEEN_HTTP_URL=http://localhost:6632 test/runners/conflation/conflation-e2e-check.sh
# =============================================================================
set -uo pipefail

QUEEN_HTTP_URL="${QUEEN_HTTP_URL:-http://localhost:6632}"
RUN="$(date +%s)-$$"
TMPD="$(mktemp -d)"

# Per-run names (see HYGIENE above). One queue per scenario so leases, cursors
# and retry budgets can never bleed between sections.
Q_GUAR="cfl1-$RUN"       # E2E-1
Q_DLQ="cfl2-$RUN"        # E2E-2
Q_MIX="cfl3-$RUN"        # E2E-3
Q_CONF="cflx-$RUN"       # E2E-4
Q_MODE="cflm-$RUN"       # MODES
Q_DEPTH="cfld-$RUN"      # DEPTH
Q_CNT="cflc-$RUN"        # COUNTER
ALL_QUEUES="$Q_GUAR $Q_DLQ $Q_MIX $Q_CONF $Q_MODE $Q_DEPTH $Q_CNT"

PASS=0; FAIL=0
SCEN_REPORT=""
say()  { printf '%s\n' "$*"; }
ok()   { PASS=$((PASS+1)); say "  ok   - $1"; }
bad()  { FAIL=$((FAIL+1)); say "  FAIL - $1"; }
eq()   { if [ "$2" = "$3" ]; then ok "$1"; else bad "$1 (want '$2', got '$3')"; fi; }
# numeric comparisons that survive a non-numeric got-value
num_le() { case "$3" in ''|*[!0-9-]*) bad "$1 (want <= $2, got '$3')"; return;; esac
           if [ "$3" -le "$2" ]; then ok "$1"; else bad "$1 (want <= $2, got $3)"; fi; }
num_ge() { case "$3" in ''|*[!0-9-]*) bad "$1 (want >= $2, got '$3')"; return;; esac
           if [ "$3" -ge "$2" ]; then ok "$1"; else bad "$1 (want >= $2, got $3)"; fi; }

CUR_SCEN=""; SCEN_FAIL0=0
scenario_begin() { CUR_SCEN="$1"; SCEN_FAIL0=$FAIL; say ""; say "== $1 =="; }
scenario_end() {
  local verdict="PASS"
  [ "$FAIL" -gt "$SCEN_FAIL0" ] && verdict="FAIL"
  say "-- SCENARIO $CUR_SCEN: $verdict"
  SCEN_REPORT="${SCEN_REPORT}${verdict}  ${CUR_SCEN}
"
}

# ---------------------------------------------------------------- HTTP helpers
QW_RC=""; QW_BODY=""
req() { # method path [body] -> QW_RC, QW_BODY
  local m="$1" p="$2" b="${3:-}" f="$TMPD/body.${BASHPID:-$$}" rq="$TMPD/req.${BASHPID:-$$}"
  if [ -n "$b" ]; then
    # Body via FILE, not argv: a 1000-message batch ack is ~180 KB of JSON and
    # Linux caps a single exec argument at 128 KB (MAX_ARG_STRLEN) — passed
    # inline, curl never even starts and the call reads as HTTP 000.
    printf '%s' "$b" > "$rq"
    QW_RC="$(curl -sS -X "$m" -H 'Content-Type: application/json' --data-binary "@$rq" \
             -o "$f" -w '%{http_code}' --max-time 30 "$QUEEN_HTTP_URL$p" 2>/dev/null)" || QW_RC=000
  else
    QW_RC="$(curl -sS -X "$m" -o "$f" -w '%{http_code}' --max-time 30 \
             "$QUEEN_HTTP_URL$p" 2>/dev/null)" || QW_RC=000
  fi
  QW_BODY="$(cat "$f" 2>/dev/null || true)"
}
jv() { printf '%s' "$QW_BODY" | jq -r "$1" 2>/dev/null; }

# configure_queue: options top-level (the raw-caller form handle_configure accepts)
configure_queue() { # queue extra-options-json (e.g. '{"retryLimit":3}')
  local body o="${2:-}"
  [ -n "$o" ] || o='{}'
  body="$(jq -cn --arg q "$1" --argjson o "$o" '{queue:$q} + $o')"
  req POST /api/v1/configure "$body"
  case "$QW_RC" in 2??) : ;; *) bad "configure $1 (HTTP $QW_RC: $(printf '%s' "$QW_BODY" | head -c 160))";; esac
}

# push_range QUEUE PARTITION START COUNT -> pushes payload {"n":i}, txn per-run
PUSH_RC=""
push_range() {
  local body
  body="$(jq -cn --arg q "$1" --arg p "$2" --argjson s "$3" --argjson c "$4" --arg r "$RUN" \
    '{items:[range($s; $s+$c) | {queue:$q, partition:$p, payload:{n:.}, transactionId:("t-\($r)-\($q)-\($p)-\(.)")}]}')"
  req POST /api/v1/push "$body"
  PUSH_RC="$QW_RC"
  if [ "$QW_RC" != "201" ]; then
    bad "push $1/$2 n=$3+$4 (HTTP $QW_RC: $(printf '%s' "$QW_BODY" | head -c 160))"
  fi
}

# pop QUEUE QUERYSTRING -> POP_RC, POP_BODY, POP_N (0 on 204, -1 on error),
# POP_LEASE, and messages[0]'s txn/pid/n for the single-message conflating shape.
POP_RC=""; POP_BODY=""; POP_N=0; POP_LEASE=""; POP_TXN=""; POP_PID=""; POP_N0=""
pop() {
  req GET "/api/v1/pop/queue/$1?$2"
  POP_RC="$QW_RC"; POP_BODY="$QW_BODY"; POP_LEASE=""; POP_TXN=""; POP_PID=""; POP_N0=""
  case "$QW_RC" in
    200) POP_N="$(printf '%s' "$QW_BODY" | jq -r '.messages | length' 2>/dev/null)"
         case "$POP_N" in ''|*[!0-9]*) POP_N=-1;; esac
         if [ "$POP_N" -gt 0 ] 2>/dev/null; then
           POP_LEASE="$(printf '%s' "$QW_BODY" | jq -r '.leaseId // ""')"
           POP_TXN="$(printf '%s' "$QW_BODY" | jq -r '.messages[0].transactionId')"
           POP_PID="$(printf '%s' "$QW_BODY" | jq -r '.messages[0].partitionId')"
           POP_N0="$(printf '%s' "$QW_BODY" | jq -r '.messages[0].data.n')"
         fi;;
    204) POP_N=0; POP_BODY="";;
    *)   POP_N=-1;;
  esac
}

# pop_until QUEUE QUERYSTRING [deadline_s]: retry until a non-empty pop.
# LONG-POLLS (wait=true) on purpose — that is the product's consumer shape, and
# a parked pop rides the push wake instead of racing the pending gate's and
# hot-list's eventual consistency, which is a seam and not the subject here.
# Returns 1 on deadline; caller asserts.
pop_until() {
  local dl=$(( $(date +%s) + ${3:-8} ))
  while :; do
    pop "$1" "$2&wait=true&timeout=1500"
    [ "$POP_N" -gt 0 ] 2>/dev/null && return 0
    [ "$(date +%s)" -ge "$dl" ] && return 1
    sleep 0.2
  done
}

expect_empty() { # label queue querystring — a single shot: emptiness must be steady-state
  pop "$2" "$3"
  if [ "$POP_N" = 0 ]; then ok "$1"; else bad "$1 (HTTP $POP_RC, $POP_N msgs)"; fi
}

# ack_one GROUP TXN PID LEASE STATUS [ERROR] -> ACK_OK, ACK_DLQ
ACK_OK=""; ACK_DLQ=""
ack_one() {
  local body
  body="$(jq -cn --arg g "$1" --arg t "$2" --arg p "$3" --arg l "$4" --arg s "$5" --arg e "${6:-}" \
    '{transactionId:$t, partitionId:$p, status:$s, consumerGroup:$g, leaseId:$l}
     + (if $e != "" then {error:$e} else {} end)')"
  req POST /api/v1/ack "$body"
  ACK_OK="$(jv '.[0].success')"
  ACK_DLQ="$(jv '.[0].dlq')"
}

# ack_msgs GROUP STATUS FILE: FILE holds one message JSON object per line
# (accumulated from pops); acks them all in one /ack/batch. ACKB_ALL="true"
# when every element succeeded.
#
# A partial ack is announced immediately, and that is not decoration: an ack
# that does not land leaves the (partition, group) lease HELD for the whole
# leaseTime, and a held lease makes every later pop of that queue answer 204.
# The scenario after it then reports "delivered nothing" instead of the content
# mismatch it exists to report — one silent ack failure strands a whole section.
# Saying so here names the root cause where it happens.
ACKB_ALL=""
ack_msgs() {
  local body
  body="$(jq -cs --arg g "$1" --arg s "$2" \
    '{consumerGroup:$g, acknowledgments: map({transactionId, partitionId, status:$s, leaseId})}' "$3")"
  req POST /api/v1/ack/batch "$body"
  # length > 0 guard: `all` of an EMPTY array is vacuously true, so a missing/
  # empty input file would otherwise report a successful ack of nothing.
  ACKB_ALL="$(jv 'if (type=="array" and length > 0) then (map(.success)|all|tostring) else "false" end')"
  [ "$ACKB_ALL" = "true" ] || \
    say "  !! ack/batch did not fully succeed (group $1, HTTP $QW_RC) — a held lease will blind the pops below: $(printf '%s' "$QW_BODY" | head -c 200)"
}

# ack_delivered GROUP [STATUS]: ack EVERY message of the last pop, one batch.
# For a conflating delivery that is exactly one frame, so post-implementation
# this is byte-equivalent to a single ack. It is written this way so that the
# red run against a pre-conflation broker (full-batch deliveries) closes the
# whole lease: acking only messages[0] of a batch leaves the (partition, group)
# lease open for the rest of leaseTime, which blinds every later pop of that
# partition and turns honest reds into empty-pop noise (measured on run 1).
ACKD_OK=""
ack_delivered() {
  # The path is expanded ONCE, into a local — the idiom every temp file in this
  # script uses. It used to be spelled "$TMPD/ackd.${BASHPID:-$$}" on BOTH
  # lines, which is a trap: bash 5 gives every subshell its own BASHPID, and the
  # WRITE happens inside the pipeline's subshell while the READ happens in this
  # function's shell. Measured in the alpine runner: written as ackd.9, read
  # back as ackd.1, so every ack_delivered silently acked nothing, held its
  # lease for the full leaseTime, and turned the next scenario's honest content
  # reds into 204s. (macOS bash 3.2 has no BASHPID at all, so ${BASHPID:-$$}
  # collapses to $$ there and the bug is invisible outside the container.)
  local f="$TMPD/ackd.${BASHPID:-$$}"
  printf '%s\n' "$POP_BODY" | jq -c '.messages[]' > "$f"
  ack_msgs "$1" "${2:-completed}" "$f"
  ACKD_OK="$ACKB_ALL"
}

# depth QUEUE [GROUP] -> QW_BODY is the depth JSON
depth() {
  if [ -n "${2:-}" ]; then req GET "/api/v1/resources/queues/$1/depth?group=$2"
  else req GET "/api/v1/resources/queues/$1/depth"; fi
}

# Background producer: pushes batches until FLAG file disappears.
#   producer_loop QUEUE PARTITION START BATCH SLEEP FLAG NEXTN_FILE
# NEXTN_FILE always holds the next unpushed n, so pushed = next - START and the
# last pushed payload is next-1. Writes ONLY after a 201 (a failed push is not
# "pushed").
producer_loop() {
  local q="$1" p="$2" n="$3" c="$4" slp="$5" flag="$6" nf="$7" body rc
  while [ -e "$flag" ]; do
    body="$(jq -cn --arg q "$q" --arg p "$p" --argjson s "$n" --argjson c "$c" --arg r "$RUN" \
      '{items:[range($s; $s+$c) | {queue:$q, partition:$p, payload:{n:.}, transactionId:("t-\($r)-\($q)-\($p)-\(.)")}]}')"
    rc="$(curl -sS -o /dev/null -w '%{http_code}' -X POST -H 'Content-Type: application/json' \
          --data-binary "$body" --max-time 15 "$QUEEN_HTTP_URL/api/v1/push" 2>/dev/null)" || rc=000
    if [ "$rc" = "201" ]; then n=$((n+c)); echo "$n" > "$nf"; fi
    [ "$slp" != "0" ] && sleep "$slp"
  done
  # Last act: a done-marker, so a consumer never declares "drained" while this
  # loop's FINAL push is still in flight (removing the flag alone races it).
  : > "$flag.done"
}

cleanup() {
  local q
  for q in $ALL_QUEUES; do
    curl -sS -o /dev/null -X DELETE --max-time 10 "$QUEEN_HTTP_URL/api/v1/resources/queues/$q" 2>/dev/null || true
  done
  rm -rf "$TMPD" 2>/dev/null || true
}
trap cleanup EXIT

# =============================================================================
say "== preflight =="
# =============================================================================
export QUEEN_WAIT_URLS="${QUEEN_WAIT_URLS:-$QUEEN_HTTP_URL/health}"
if command -v wait-for-broker >/dev/null 2>&1; then wait-for-broker; fi
RC="$(curl -s -o /dev/null -w '%{http_code}' --max-time 10 "$QUEEN_HTTP_URL/health" 2>/dev/null)" || RC=000
[ "$RC" = "200" ] || { say "!! broker not healthy at $QUEEN_HTTP_URL (health -> $RC)"; say "CONFLATION E2E: FAIL"; exit 1; }
ok "broker healthy at $QUEEN_HTTP_URL"

# =============================================================================
scenario_begin "E2E-1 the guarantee (§1.3, §7.3 E2E-1)"
# =============================================================================
# One partition. The "handler" is this script holding the lease between pop and
# ack; "processing time" is the span in which n2..n5 are pushed.
configure_queue "$Q_GUAR" '{"leaseTime":120,"retryLimit":3}'
GG="workers-$RUN"

push_range "$Q_GUAR" main 1 1
if pop_until "$Q_GUAR" "consumerGroup=$GG&conflation=true&subscriptionMode=all&batch=200"; then
  eq "(a) the first pop delivers exactly the one visible message" "1" "$POP_N"
  eq "(a) and it is n=1" "1" "$POP_N0"
else
  bad "(a) first pop delivered nothing (HTTP $POP_RC)"
fi
T1_TXN="$POP_TXN"; T1_PID="$POP_PID"; T1_LEASE="$POP_LEASE"

# While the handler "runs", four more pushes commit. They were NOT visible at
# pop time, so the §1.3 corollary says the coming ack MUST NOT retire them.
for i in 2 3 4 5; do push_range "$Q_GUAR" main "$i" 1; done

ack_one "$GG" "$T1_TXN" "$T1_PID" "$T1_LEASE" completed
eq "(b) ack of n=1 succeeds" "true" "$ACK_OK"
depth "$Q_GUAR" "$GG"
# THE HEADLINE ASSERTION: committed = the offset observed at pop time, so the
# four messages pushed during processing are still pending. A broker that
# commits to "current tail at ack time" answers 0 here and loses n2..n5.
eq "(b) pushes during processing STAY PENDING (depth.pending)" "4" "$(jv '.pending')"

# (c) a second pop follows with no new push and delivers the tail, n=5, alone.
if pop_until "$Q_GUAR" "consumerGroup=$GG&conflation=true&subscriptionMode=all&batch=200"; then
  eq "(c) redelivery is ONE message" "1" "$POP_N"
  eq "(c) and it is the newest, n=5" "5" "$POP_N0"
  # (d) e2e form: this pop request was issued strictly after n=5's push call
  # returned (sequential script), so the handler invocation for the last payload
  # started after the last push committed — the §1.3 property.
  ack_delivered "$GG"
  eq "(c) ack of the tail succeeds" "true" "$ACKD_OK"
else
  bad "(c) no redelivery after ack while n2..n5 pending (HTTP $POP_RC)"
fi
depth "$Q_GUAR" "$GG"
eq "(c) the episode retired the whole span (pending 0)" "0" "$(jv '.pending')"
expect_empty "(c) and a further pop is empty" "$Q_GUAR" "consumerGroup=$GG&conflation=true&subscriptionMode=all"

# --- adversarial variant: continuous producer, then the LAST payload wins ----
say "  -- adversarial: continuous producer vs conflating consumer"
AFLAG="$TMPD/adv.flag"; ANEXT="$TMPD/adv.next"
: > "$AFLAG"; echo 100 > "$ANEXT"
producer_loop "$Q_GUAR" main 100 25 0.05 "$AFLAG" "$ANEXT" &
APID=$!
( sleep 8; rm -f "$AFLAG" ) &
STOPPER=$!

PROCESSED=0; LAST_SEEN=-1; EMPTIES=0
ADL=$(( $(date +%s) + 45 ))
while [ "$(date +%s)" -lt "$ADL" ]; do
  # Long-poll like a real consumer: the parked pop rides the push wake.
  pop "$Q_GUAR" "consumerGroup=$GG&conflation=true&subscriptionMode=all&batch=200&wait=true&timeout=1500"
  if [ "$POP_N" -gt 0 ] 2>/dev/null; then
    EMPTIES=0
    PROCESSED=$((PROCESSED+POP_N))
    LAST_SEEN="$(printf '%s' "$POP_BODY" | jq -r '[.messages[].data.n] | max')"
    printf '%s\n' "$POP_BODY" | jq -c '.messages[]' > "$TMPD/adv.msgs"
    ack_msgs "$GG" completed "$TMPD/adv.msgs"
    [ "$ACKB_ALL" = "true" ] || bad "adversarial: an ack failed (HTTP $QW_RC)"
  else
    if [ -e "$AFLAG.done" ]; then
      EMPTIES=$((EMPTIES+1))
      [ "$EMPTIES" -ge 2 ] && break
    fi
    sleep 0.1
  fi
done
wait "$APID" 2>/dev/null; wait "$STOPPER" 2>/dev/null
ANEXTV="$(cat "$ANEXT")"; APUSHED=$((ANEXTV-100)); ALAST=$((ANEXTV-1))
say "  -- adversarial: pushed=$APUSHED processed=$PROCESSED last_pushed=$ALAST last_processed=$LAST_SEEN"
num_ge "adversarial: the producer actually produced a stream" 300 "$APUSHED"
eq "adversarial: the FINAL processed payload is the LAST pushed one" "$ALAST" "$LAST_SEEN"
# The collapse bound below is an UPPER bound, and on its own it is satisfied by
# processed=0 — i.e. by a consumer that was served nothing at all, which is the
# exact opposite of conflation and is what a stranded lease looks like. The
# floor rules that reading out, so the pair means "it worked, and it collapsed".
num_ge "adversarial: the conflating consumer actually processed messages" 1 "$PROCESSED"
# Conflation is what makes processed << pushed; a full-batch broker processes
# every one of them.
num_le "adversarial: conflation collapsed the backlog (processed <= pushed/2)" $((APUSHED/2)) "$PROCESSED"
depth "$Q_GUAR" "$GG"
eq "adversarial: nothing left pending" "0" "$(jv '.pending')"
scenario_end

# =============================================================================
scenario_begin "MODES all/new composition (§1.5, §7.1-9 at e2e)"
# =============================================================================
configure_queue "$Q_MODE" '{"leaseTime":120}'
push_range "$Q_MODE" main 0 500
push_range "$Q_MODE" main 500 500
GA="g-all-$RUN"; GN="g-new-$RUN"

# all + conflation: the whole retained history conflates to ONE message.
if pop_until "$Q_MODE" "consumerGroup=$GA&conflation=true&subscriptionMode=all&batch=200"; then
  eq "all+conflation over a 1000 backlog delivers 1" "1" "$POP_N"
  eq "and it is the newest (n=999)" "999" "$POP_N0"
  ack_delivered "$GA"
  eq "ack succeeds" "true" "$ACKD_OK"
else
  bad "all+conflation delivered nothing (HTTP $POP_RC)"
fi
expect_empty "all+conflation: backlog fully retired by one ack" "$Q_MODE" "consumerGroup=$GA&conflation=true&subscriptionMode=all"

# new + conflation: history is skipped by the SEED, not by conflation.
expect_empty "new+conflation over the same backlog delivers 0" "$Q_MODE" "consumerGroup=$GN&conflation=true&subscriptionMode=new"
push_range "$Q_MODE" main 1000 3
if pop_until "$Q_MODE" "consumerGroup=$GN&conflation=true&subscriptionMode=new"; then
  eq "new+conflation delivers 1 after the next push" "1" "$POP_N"
  eq "and it is the newest of the new (n=1002)" "1002" "$POP_N0"
  ack_delivered "$GN"
  eq "ack succeeds" "true" "$ACKD_OK"
else
  bad "new+conflation delivered nothing after a fresh push (HTTP $POP_RC)"
fi
scenario_end

# =============================================================================
scenario_begin "DEPTH partitionsPending / conflation / effectivePending (§2.5, §5.3)"
# =============================================================================
configure_queue "$Q_DEPTH" '{"leaseTime":120}'
push_range "$Q_DEPTH" p0 0 5
push_range "$Q_DEPTH" p1 0 3
push_range "$Q_DEPTH" p2 0 7
GW="workers-$RUN"; GAU="audit-$RUN"

# Register + lease the conflating group. Accumulate pops until all 3 partitions
# served (one wildcard claim MAY serve fewer than max_parts per round).
: > "$TMPD/dw.msgs"
DW_TOTAL=0; DDL=$(( $(date +%s) + 15 ))
while [ "$DW_TOTAL" -lt 3 ] && [ "$(date +%s)" -lt "$DDL" ]; do
  pop "$Q_DEPTH" "consumerGroup=$GW&conflation=true&subscriptionMode=all&partitions=8&batch=50"
  if [ "$POP_N" -gt 0 ] 2>/dev/null; then
    DW_TOTAL=$((DW_TOTAL+POP_N))
    printf '%s\n' "$POP_BODY" | jq -c '.messages[]' >> "$TMPD/dw.msgs"
  else sleep 0.2; fi
done
eq "conflating pop serves ONE message per partition (3 partitions -> 3)" "3" "$DW_TOTAL"

# Leased but NOT acked: the log depth must be untouched, and the three new
# fields must be present with the conflating reading.
depth "$Q_DEPTH" "$GW"
eq "pending is LOG depth (positions to retire)" "15" "$(jv '.pending')"
eq "partitionsPending counts partitions with pending > 0" "3" "$(jv '.partitionsPending')"
eq "conflation echoes the group's stored policy" "true" "$(jv '.conflation')"
eq "effectivePending is WORK depth for a conflating group" "3" "$(jv '.effectivePending')"

ack_msgs "$GW" completed "$TMPD/dw.msgs"
eq "batch ack of the three tails succeeds" "true" "$ACKB_ALL"
depth "$Q_DEPTH" "$GW"
eq "after the acks pending is 0" "0" "$(jv '.pending')"
eq "and partitionsPending is 0" "0" "$(jv '.partitionsPending')"
eq "and effectivePending is 0" "0" "$(jv '.effectivePending')"
eq "and conflation still reads true" "true" "$(jv '.conflation')"

# A NON-conflating group on the same queue: effectivePending == pending.
pop "$Q_DEPTH" "consumerGroup=$GAU&subscriptionMode=all&batch=1"   # register; lease not acked
depth "$Q_DEPTH" "$GAU"
eq "non-conflating group: pending unchanged by an unacked lease" "15" "$(jv '.pending')"
eq "non-conflating group: conflation false" "false" "$(jv '.conflation')"
eq "non-conflating group: effectivePending == pending" "15" "$(jv '.effectivePending')"
eq "non-conflating group: partitionsPending" "3" "$(jv '.partitionsPending')"

# Queue-level (no group): worst named cursor wins; no group => conflation false.
depth "$Q_DEPTH"
eq "queue-level pending is the worst named cursor's" "15" "$(jv '.pending')"
eq "queue-level conflation is false" "false" "$(jv '.conflation')"
eq "queue-level effectivePending == pending" "15" "$(jv '.effectivePending')"
scenario_end

# =============================================================================
scenario_begin "E2E-4 declaration conflict echo (§3.1, §3.3, §7.3 E2E-4)"
# =============================================================================
configure_queue "$Q_CONF" '{"leaseTime":120}'
CG1="cg1-$RUN"; CG2="cg2-$RUN"
push_range "$Q_CONF" main 0 10

# First registration persists conflation=true; no conflict on the register.
if pop_until "$Q_CONF" "consumerGroup=$CG1&conflation=true&subscriptionMode=all"; then
  eq "registering pop echoes conflation:true" "true" "$(printf '%s' "$POP_BODY" | jq -r '.conflation')"
  CFT="$(printf '%s' "$POP_BODY" | jq -r '.conflationConflict // false')"
  eq "no conflict echo on the registering pop" "false" "$CFT"
  eq "and it conflated (1 message)" "1" "$POP_N"
  ack_delivered "$CG1"
else
  bad "registering conflating pop delivered nothing (HTTP $POP_RC)"
fi

# A consumer of the SAME group declaring conflation=false: stored wins, loudly.
push_range "$Q_CONF" main 10 3
if pop_until "$Q_CONF" "consumerGroup=$CG1&conflation=false&subscriptionMode=all"; then
  eq "disagreeing pop carries conflationConflict:true" "true" "$(printf '%s' "$POP_BODY" | jq -r '.conflationConflict')"
  eq "and the EFFECTIVE flag is the stored one (conflation:true)" "true" "$(printf '%s' "$POP_BODY" | jq -r '.conflation')"
  eq "and behaviour follows the store: 1 message, not 3" "1" "$POP_N"
  eq "which is the newest (n=12)" "12" "$POP_N0"
  ack_delivered "$CG1"
else
  bad "disagreeing pop delivered nothing (HTTP $POP_RC)"
fi

# Absent flag is NOT a conflict (§3.3: conflict iff R is Some and R != stored).
push_range "$Q_CONF" main 13 2
if pop_until "$Q_CONF" "consumerGroup=$CG1&subscriptionMode=all"; then
  CFT="$(printf '%s' "$POP_BODY" | jq -r '.conflationConflict // false')"
  eq "absent flag raises no conflict" "false" "$CFT"
  eq "and still conflates (stored policy)" "1" "$POP_N"
  ack_delivered "$CG1"
else
  bad "flag-absent pop delivered nothing (HTTP $POP_RC)"
fi

# The other direction: group stored NON-conflating, request says true.
push_range "$Q_CONF" main 15 5
if pop_until "$Q_CONF" "consumerGroup=$CG2&conflation=false&subscriptionMode=all&batch=200"; then
  eq "cg2 registers non-conflating and reads the whole backlog (20)" "20" "$POP_N"
  CFT="$(printf '%s' "$POP_BODY" | jq -r '.conflation // false')"
  eq "no conflation echo for a non-conflating group" "false" "$CFT"
  printf '%s\n' "$POP_BODY" | jq -c '.messages[]' > "$TMPD/cg2.msgs"
  ack_msgs "$CG2" completed "$TMPD/cg2.msgs"
  eq "cg2 batch ack succeeds" "true" "$ACKB_ALL"
else
  bad "cg2 registering pop delivered nothing (HTTP $POP_RC)"
fi
push_range "$Q_CONF" main 20 2
if pop_until "$Q_CONF" "consumerGroup=$CG2&conflation=true&subscriptionMode=all&batch=200"; then
  eq "conflation=true against a stored-false group: conflict echo" "true" "$(printf '%s' "$POP_BODY" | jq -r '.conflationConflict')"
  CFT="$(printf '%s' "$POP_BODY" | jq -r '.conflation // false')"
  eq "and NO conflation echo (stored false wins)" "false" "$CFT"
  eq "and behaviour follows the store: both pending delivered" "2" "$POP_N"
  printf '%s\n' "$POP_BODY" | jq -c '.messages[]' > "$TMPD/cg2b.msgs"
  ack_msgs "$CG2" completed "$TMPD/cg2b.msgs"
else
  bad "cg2 conflicting pop delivered nothing (HTTP $POP_RC)"
fi

# THE STEADY STATE OF A DISAGREEING CONSUMER: an EMPTY conflicting pop.
#
# Every assertion above pushes messages first, and that is exactly what hid the
# hole: for a conflicting request the EFFECTIVE flag is the stored one, so a
# status keyed only on "is conflation on" answers a bodiless 204 and the conflict
# echo never reaches the wire. An SDK cannot tell that from a pre-1.1.0 broker, so
# it raises "requires broker >= 1.1.0" and stops the consumer — on its FIRST poll
# of an idle queue, which is what a long-poll consumer does all day. Both
# directions of the disagreement are asserted, on a drained queue.
# cg2 is drained (it just acked everything), so this pop delivers nothing.
req GET "/api/v1/pop/queue/$Q_CONF?consumerGroup=$CG2&conflation=true&subscriptionMode=all"
eq "empty conflicting pop keeps the 200 (cg2 stored false, asks true)" "200" "$QW_RC"
eq "and still carries conflationConflict" "true" "$(jv '.conflationConflict')"
eq "with no conflation echo (the stored policy is off)" "false" "$(jv '.conflation // false')"
eq "and no messages" "0" "$(jv '.messages | length')"
# The other direction, on a group seeded at the tail so it is empty by
# construction: stored TRUE, request says false.
CG3="cg3-$RUN"
req GET "/api/v1/pop/queue/$Q_CONF?consumerGroup=$CG3&conflation=true&subscriptionMode=new"
eq "cg3 registers conflating at the tail (empty, echoed)" "200" "$QW_RC"
eq "empty registering pop carries the echo" "true" "$(jv '.conflation')"
req GET "/api/v1/pop/queue/$Q_CONF?consumerGroup=$CG3&conflation=false"
eq "empty conflicting pop, other direction (cg3 stored true, asks false)" "200" "$QW_RC"
eq "carries conflationConflict" "true" "$(jv '.conflationConflict')"
eq "and the EFFECTIVE (stored) flag" "true" "$(jv '.conflation')"
# Control: a consumer that never mentioned conflation keeps the pre-1.1.0
# bodiless 204 on the same drained queue.
req GET "/api/v1/pop/queue/$Q_CONF?consumerGroup=$CG2&subscriptionMode=all"
eq "a flag-less empty pop stays a bodiless 204" "204" "$QW_RC"

# §3.3 refusals — the two consumer bugs that must be a 400, not a warning.
# (Pins the plan's recommendation on §10 Q2; if the author resolves Q2 the
# other way, THIS assertion is the one to change.)
req GET "/api/v1/pop/queue/$Q_CONF?conflation=true"
eq "conflation without a consumerGroup is refused (400)" "400" "$QW_RC"
req GET "/api/v1/pop/queue/$Q_CONF?consumerGroup=refuse-$RUN&conflation=true&autoAck=true"
eq "conflation + autoAck is refused (400)" "400" "$QW_RC"
scenario_end

# =============================================================================
scenario_begin "E2E-3 mixed groups on one queue (§7.3 E2E-3)"
# =============================================================================
configure_queue "$Q_MIX" '{"leaseTime":120}'
GMW="workers-$RUN"; GMA="audit-$RUN"
# 10 000 messages over 4 partitions: pk gets n = k*2500 .. k*2500+2499.
for k in 0 1 2 3; do
  base=$((k*2500))
  for b in 0 500 1000 1500 2000; do
    push_range "$Q_MIX" "p$k" $((base+b)) 500
  done
done

# audit reads EVERYTHING (non-conflating, autoAck to keep the drain cheap).
: > "$TMPD/audit.seen"
AUD_TOTAL=0; EMPTIES=0; MDL=$(( $(date +%s) + 90 ))
while [ "$(date +%s)" -lt "$MDL" ]; do
  pop "$Q_MIX" "consumerGroup=$GMA&subscriptionMode=all&batch=2000&autoAck=true"
  if [ "$POP_N" -gt 0 ] 2>/dev/null; then
    EMPTIES=0
    AUD_TOTAL=$((AUD_TOTAL+POP_N))
    printf '%s\n' "$POP_BODY" | jq -r '.messages[] | "\(.partition) \(.data.n)"' >> "$TMPD/audit.seen"
  else
    EMPTIES=$((EMPTIES+1)); [ "$EMPTIES" -ge 2 ] && break; sleep 0.2
  fi
done
eq "audit received all 10000" "10000" "$AUD_TOTAL"
AUD_NEWEST_OK="$(awk '{ if ($2 > m[$1]) m[$1] = $2 } END {
  okc = 0
  if (m["p0"] == 2499) okc++
  if (m["p1"] == 4999) okc++
  if (m["p2"] == 7499) okc++
  if (m["p3"] == 9999) okc++
  print okc }' "$TMPD/audit.seen")"
eq "audit saw the newest of every partition" "4" "$AUD_NEWEST_OK"

# workers conflates the same backlog to one message per partition.
: > "$TMPD/mw.msgs"
MW_TOTAL=0; EMPTIES=0; MDL=$(( $(date +%s) + 60 ))
while [ "$(date +%s)" -lt "$MDL" ]; do
  pop "$Q_MIX" "consumerGroup=$GMW&conflation=true&subscriptionMode=all&partitions=8&batch=1000"
  if [ "$POP_N" -gt 0 ] 2>/dev/null; then
    EMPTIES=0
    MW_TOTAL=$((MW_TOTAL+POP_N))
    printf '%s\n' "$POP_BODY" | jq -c '.messages[]' >> "$TMPD/mw.msgs"
    # ack exactly what THIS pop delivered (the accumulator is for the newest check)
    printf '%s\n' "$POP_BODY" | jq -c '.messages[]' > "$TMPD/mw.msgs.last"
    ack_msgs "$GMW" completed "$TMPD/mw.msgs.last"
    [ "$ACKB_ALL" = "true" ] || bad "workers ack failed (HTTP $QW_RC)"
  else
    EMPTIES=$((EMPTIES+1)); [ "$EMPTIES" -ge 2 ] && break; sleep 0.2
  fi
done
say "  -- mixed: audit=$AUD_TOTAL workers=$MW_TOTAL"
eq "workers received one per partition (4), far fewer than audit" "4" "$MW_TOTAL"
MW_NEWEST_OK="$(jq -rs '[ .[] | "\(.partition) \(.data.n)" ] | join("\n")' "$TMPD/mw.msgs" | \
  awk '{ if ($2 > m[$1]) m[$1] = $2 } END {
    okc = 0
    if (m["p0"] == 2499) okc++
    if (m["p1"] == 4999) okc++
    if (m["p2"] == 7499) okc++
    if (m["p3"] == 9999) okc++
    print okc }')"
eq "workers saw the newest of every partition" "4" "$MW_NEWEST_OK"
scenario_end

# =============================================================================
scenario_begin "E2E-2 DLQ under a hot producer (§1.4, §7.3 E2E-2, the M2 pin)"
# =============================================================================
# retryLimit=3, DLQ on, retryDelay=0 so the failing loop is snappy. The
# producer keeps pushing THROUGHOUT the retries, so every redelivery is a NEWER
# tail — the budget must survive supersession (M2) and the run must not
# livelock (the deadline IS the assertion).
configure_queue "$Q_DLQ" '{"leaseTime":60,"retryLimit":3,"retryDelay":0,"deadLetterQueue":true}'
GP="poison-$RUN"
PFLAG="$TMPD/dlq.flag"; PNEXT="$TMPD/dlq.next"
: > "$PFLAG"; echo 0 > "$PNEXT"
producer_loop "$Q_DLQ" main 0 5 0.1 "$PFLAG" "$PNEXT" &
PROD_PID=$!   # NOT `PPID`: that name is a readonly bash builtin
sleep 0.6     # let the first batch land

ATTEMPTS=0; DLQED=0; SHAPE_VIOLATIONS=0; LAST_FAIL_TXN=""; LAST_FAIL_N=""
DISTINCT="$TMPD/dlq.txns"; : > "$DISTINCT"
DDL=$(( $(date +%s) + 75 ))
while [ "$(date +%s)" -lt "$DDL" ]; do
  pop "$Q_DLQ" "consumerGroup=$GP&conflation=true&subscriptionMode=all&batch=200&wait=true&timeout=1500"
  if [ "$POP_N" -le 0 ] 2>/dev/null; then sleep 0.2; continue; fi
  [ "$POP_N" = "1" ] || SHAPE_VIOLATIONS=$((SHAPE_VIOLATIONS+1))
  ATTEMPTS=$((ATTEMPTS+1))
  LAST_FAIL_TXN="$POP_TXN"; LAST_FAIL_N="$POP_N0"
  echo "$POP_TXN" >> "$DISTINCT"
  ack_one "$GP" "$POP_TXN" "$POP_PID" "$POP_LEASE" failed "e2e2 poison handler"
  # One line per attempt: this is where the M2 pin is READ. Under conflation the
  # delivered n climbs every attempt (the producer is live, so each redelivery
  # is a newer tail) while the budget keeps counting up to the DLQ — the whole
  # point of §1.4. A non-conflating broker prints the same n four times.
  say "  -- attempt $ATTEMPTS: delivered n=$LAST_FAIL_N (${POP_N} msg) -> failed, dlq=${ACK_DLQ:-false}"
  if [ "$ACK_DLQ" = "true" ]; then DLQED=1; break; fi
  # belt+braces: some paths may file without flagging; ask the DLQ surface
  req GET "/api/v1/dlq?queue=$Q_DLQ&consumerGroup=$GP&limit=5"
  DN="$(jv '.messages | length')"
  [ "$DN" != "" ] && [ "$DN" != "0" ] && [ "$DN" != "null" ] && { DLQED=1; break; }
  sleep 0.3   # give the producer a beat so the next tail is a NEWER message
done
eq "the failing partition dead-letters (no livelock before the deadline)" "1" "$DLQED"
num_le "dead-letters within 4 delivery attempts" 4 "$ATTEMPTS"
eq "every conflating delivery was a single message" "0" "$SHAPE_VIOLATIONS"
DISTINCT_N="$(sort -u "$DISTINCT" | wc -l | tr -d ' ')"
num_ge "the delivered tail superseded across attempts (M2: budget survives it)" 2 "$DISTINCT_N"

req GET "/api/v1/dlq?queue=$Q_DLQ&consumerGroup=$GP&limit=5"
eq "exactly one DLQ row" "1" "$(jv '.messages | length')"
eq "the DLQ row spent the whole budget (retryCount 3)" "3" "$(jv '.messages[0].retryCount')"
eq "the poison IS the tail of the last attempt (§1.4)" "$LAST_FAIL_TXN" "$(jv '.messages[0].transactionId')"

# Stop the producer, then prove the group RESUMES past the poison.
rm -f "$PFLAG"; wait "$PROD_PID" 2>/dev/null
push_range "$Q_DLQ" main 900001 3
if pop_until "$Q_DLQ" "consumerGroup=$GP&conflation=true&subscriptionMode=all"; then
  eq "the group resumes on the next tail" "1" "$POP_N"
  eq "which is the newest post-poison message" "900003" "$POP_N0"
  ack_delivered "$GP"
  eq "and its ack lands" "true" "$ACKD_OK"
else
  bad "no delivery after DLQ — the cursor did not jump past the poison (HTTP $POP_RC)"
fi
depth "$Q_DLQ" "$GP"
eq "episode fully retired after resume (pending 0)" "0" "$(jv '.pending')"
scenario_end

# =============================================================================
scenario_begin "COUNTER queen_queue_conflated_per_minute (§6.2, §6.3)"
# =============================================================================
# The ack's `conflated` field feeds metrics.per_queue, syscollect flushes it to
# queen.queue_lag_metrics on the METRICS_FLUSH_MS cadence (default 60 s), and
# /metrics/prometheus exposes the most recent bucket. So: feed a fresh burst,
# then poll across one full flush window. The deadline exists because the lane
# is slow, not because the value is uncertain.
configure_queue "$Q_CNT" '{"leaseTime":120}'
GC="meter-$RUN"
feed_counter() {
  push_range "$Q_CNT" main "$1" 40
  if pop_until "$Q_CNT" "consumerGroup=$GC&conflation=true&subscriptionMode=all"; then
    ack_delivered "$GC"   # one conflating frame; its ack reports conflated=39
  fi
}
feed_counter 0
CNT_VAL=""; CDL=$(( $(date +%s) + 100 )); REFED=0
while [ "$(date +%s)" -lt "$CDL" ]; do
  req GET /metrics/prometheus
  CNT_VAL="$(printf '%s' "$QW_BODY" | grep -E "^queen_queue_conflated_per_minute\{queue=\"$Q_CNT\"\}" | awk '{print $2}' | head -1)"
  case "$CNT_VAL" in
    ''|0|0.0) : ;;
    *) break ;;
  esac
  # keep the newest minute bucket non-zero across a flush boundary
  if [ "$REFED" = 0 ] && [ "$(date +%s)" -ge $((CDL-55)) ]; then feed_counter 1000; REFED=1; fi
  sleep 5
done
case "$CNT_VAL" in
  ''|0|0.0) bad "queen_queue_conflated_per_minute{queue=\"$Q_CNT\"} never surfaced > 0 (got '${CNT_VAL:-absent}')";;
  *) ok "queen_queue_conflated_per_minute surfaced for the queue (value $CNT_VAL)";;
esac
scenario_end

# =============================================================================
say ""
say "================== CONFLATION E2E summary =================="
printf '%s' "$SCEN_REPORT"
say "skipped: E2E-5 old-broker degrade-loudly (SDK behaviour, §7.2 lane — see header)"
say "assertions: $PASS passed, $FAIL failed"
if [ "$FAIL" = 0 ]; then say "CONFLATION E2E: PASS"; exit 0
else say "CONFLATION E2E: FAIL"; exit 1; fi
