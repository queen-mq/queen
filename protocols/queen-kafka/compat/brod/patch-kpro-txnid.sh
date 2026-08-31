#!/usr/bin/env bash
#
# THE ONE-LINE OVERLAY, and why it exists.
#
# Stock brod cannot produce to queen-kafka at all. kafka_protocol builds the
# Produce request by hand rather than from its own schema, and the hand-rolled
# encoder types `transactional_id` as `string` instead of `nullable_string`:
#
#   kpro_req_lib.erl:308  [encode(string, transactional_id(TxnCtx)) || Vsn > 2]
#   kpro_req_lib.erl:593  transactional_id(false) -> ?kpro_null.
#   kpro_lib.erl:140      encode(string, ?null) -> encode(string, "").
#
# so a NON-transactional produce puts a zero-length string on the wire where
# the schema -- kafka_protocol's OWN schema, kpro_schema.erl:212, which reads
# {transactional_id, nullable_string} -- says null. queen-kafka refuses any
# present transactional id (src/handlers/produce.rs:195) and answers
# TRANSACTIONAL_ID_AUTHORIZATION_FAILED, which brod treats as not_retriable and
# which kills the producer process on the first send. Apache Kafka 3.9.1 takes
# the same bytes without complaint.
#
# This script applies the fix that kafka_protocol upstream would apply: use the
# nullable type, so null encodes as -1 rather than as "". It edits ONLY the
# fetched dependency inside _build/ (gitignored, recreated by any clean build)
# and never the facade.
#
# It is OPT-IN -- run.sh applies it only when BROD_PATCH_TXNID=1 -- because the
# DEFAULT run must show what a real Erlang shop actually gets, which is a dead
# producer. Turn it on to answer the follow-up question: with that one field
# corrected, does everything else about brod work? (It does.)
set -euo pipefail

# `--revert` puts the dependency back to stock. run.sh calls it on EVERY
# unpatched run, and that is not tidiness -- it is a correctness trap being
# closed. _build persists between runs, so without it a plain `./run.sh` after
# a BROD_PATCH_TXNID=1 run would quietly keep using the patched beam and report
# a PASS that stock brod cannot reproduce.
MODE="${1:-apply}"

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SRC="$HERE/_build/default/lib/kafka_protocol/src/kpro_req_lib.erl"

if [ ! -f "$SRC" ]; then
  if [ "$MODE" = "--revert" ]; then exit 0; fi   # nothing fetched yet: nothing to revert
  echo "patch-kpro-txnid: $SRC not there yet; run rebar3 compile first" >&2
  exit 1
fi

PATCHED=no
grep -q 'encode(nullable_string, transactional_id(TxnCtx))' "$SRC" && PATCHED=yes
STOCK=no
grep -q 'encode(string, transactional_id(TxnCtx))' "$SRC" && STOCK=yes

if [ "$PATCHED" = "no" ] && [ "$STOCK" = "no" ]; then
  echo "patch-kpro-txnid: the line this patch targets is gone." >&2
  echo "  Expected: [encode(string, transactional_id(TxnCtx)) || Vsn > 2]" >&2
  echo "  kafka_protocol may have fixed it upstream -- check, then delete" >&2
  echo "  this script and the BROD_PATCH_TXNID knob in run.sh." >&2
  exit 1
fi

if [ "$MODE" = "--revert" ]; then
  if [ "$PATCHED" = "no" ]; then exit 0; fi     # already stock, and so is the beam
  perl -pi -e 's/encode\(nullable_string, transactional_id\(TxnCtx\)\)/encode(string, transactional_id(TxnCtx))/' "$SRC"
  ACTION="reverted to stock (nullable_string -> string)"
else
  if [ "$PATCHED" = "no" ]; then
    perl -pi -e 's/encode\(string, transactional_id\(TxnCtx\)\)/encode(nullable_string, transactional_id(TxnCtx))/' "$SRC"
  fi
  ACTION="applied (string -> nullable_string)"
fi

# The recompile runs EVERY time, patched-this-run or patched-last-run, because
# rebar3 treats a HEX dependency as immutable: `rebar3 compile` will not notice
# that the .erl under _build changed and will happily leave the old .beam in
# place. Editing the source is therefore only half the patch -- compile it by
# hand, straight into the dep's ebin.
LIB="$HERE/_build/default/lib"
erlc -o "$LIB/kafka_protocol/ebin" \
     -I "$LIB/kafka_protocol/include" \
     -I "$LIB/kafka_protocol/src" \
     -I "$LIB" \
     +debug_info \
     "$SRC"
echo "patch-kpro-txnid: kpro_req_lib.erl $ACTION"
