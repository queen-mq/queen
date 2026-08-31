#!/usr/bin/env bash
#
# The queen-sqs M0 smoke, through the aws CLI.
#
# It is deliberately SMALLER than smoke_m0.py and it is not a duplicate of it.
# boto3 is a library this suite drives; the CLI is a program a person runs, and
# it exercises things the library never touches — credentials resolved from the
# environment the way a shell provides them, a `--endpoint-url` that has to
# survive the CLI's own endpoint machinery, and the CLI's argument shorthand for
# message attributes, which is its own encoder. A facade that satisfied boto3
# and not this has still failed the "change endpoint_url only" promise, because
# `aws sqs` is the first thing anybody tries.
#
#   protocols/queen-sqs/compat/rig.sh up
#   source protocols/queen-sqs/compat/.rig/env.sh
#   protocols/queen-sqs/compat/smoke_m0_cli.sh
#
# The CLI is taken from $AWS_CLI, else from PATH. The rig's own environment is a
# python venv, so:
#
#   AWS_CLI=/path/to/venv/bin/aws protocols/queen-sqs/compat/smoke_m0_cli.sh
#
# Contract, as everywhere in compat/: one `ok NAME` or `FAIL NAME: detail` per
# assertion, a `RESULT:` line last, nonzero exit when anything failed.
#
# NO jq, NO python. Every value is pulled out with the CLI's own `--query`
# (JMESPath) and `--output text`, so the only thing this script needs is the
# thing it is testing.
set -uo pipefail

ENDPOINT="${QUEEN_SQS_ENDPOINT:-http://127.0.0.1:19324}"
REGION="${QUEEN_SQS_REGION:-queen-1}"
ACCOUNT="${QUEEN_SQS_ACCOUNT:-000000000000}"
AWS_CLI="${AWS_CLI:-aws}"

command -v "$AWS_CLI" >/dev/null 2>&1 || {
  echo "FAIL cli.present: no aws CLI at '$AWS_CLI' (set AWS_CLI=/path/to/venv/bin/aws)"
  echo "RESULT: FAIL"; exit 1; }

# The credentials the rig serves. Exported rather than passed per command so
# that this is the same credential resolution any operator's shell would do.
export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-QSQSTEST}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-qsqssecret}"
export AWS_DEFAULT_REGION="$REGION"
export AWS_REGION="$REGION"
# The rig's third credential field is a QUEEN bearer, not an AWS session token:
# signing with it would put an x-amz-security-token in the request that
# verification would reject.
unset AWS_SESSION_TOKEN
# A stray profile in ~/.aws would otherwise win over the variables above, and
# IMDS lookups on a laptop are a 1-second stall per command for nothing.
unset AWS_PROFILE
export AWS_EC2_METADATA_DISABLED=true

# A name nothing else is using: DeleteQueue arms a 60-second tombstone on the
# name, so a fixed one could not be run twice inside a minute.
QUEUE="m0-cli-$$-$(date +%s)"

PASSES=0
FAILURES=0

ok()   { PASSES=$((PASSES + 1)); printf 'ok %s\n' "$1"; }
bad()  { FAILURES=$((FAILURES + 1)); printf 'FAIL %s: %s\n' "$1" "$2"; }
eq()   { # eq NAME GOT WANT
  if [ "$2" = "$3" ]; then ok "$1"; else bad "$1" "got '$2', want '$3'"; fi
}
yes_() { # yes_ NAME CONDITION-RESULT DETAIL
  if [ "$2" = 0 ]; then ok "$1"; else bad "$1" "$3"; fi
}

sqs() { "$AWS_CLI" --endpoint-url "$ENDPOINT" sqs "$@"; }

md5_of() { # the same digest SQS reports for a body
  if command -v md5sum >/dev/null 2>&1; then printf '%s' "$1" | md5sum | cut -d' ' -f1
  else printf '%s' "$1" | md5 -q; fi
}

cleanup() {
  # Best effort, and quiet: the happy path already deleted the queue, so this
  # only ever fires when an assertion above bailed out early.
  [ -n "${URL:-}" ] && sqs delete-queue --queue-url "$URL" >/dev/null 2>&1
  return 0
}
trap cleanup EXIT

echo "# endpoint $ENDPOINT  region $REGION  account $ACCOUNT  queue $QUEUE"
echo "# cli: $("$AWS_CLI" --version 2>&1)"

# --------------------------------------------------- which protocol it spoke
# The CLIENT_MATRIX rule: report the protocol the client ACTUALLY spoke, read
# from its own debug stream, never assumed. `X-Amz-Target: AmazonSQS.<Action>`
# is AWS JSON 1.0; a form-encoded `Action=` body is Query/XML. Both are
# supposed to work, and which one a given CLI major picks is exactly the fact
# that stops being true under a version bump.
DEBUG_OUT="$(sqs list-queues --debug 2>&1)"
if grep -q 'X-Amz-Target' <<<"$DEBUG_OUT"; then
  echo "# protocol: AWS JSON 1.0 ($(grep -o "X-Amz-Target': b\?'\?[^']*" <<<"$DEBUG_OUT" | head -1))"
elif grep -q 'Action=' <<<"$DEBUG_OUT"; then
  echo "# protocol: Query/XML"
else
  echo "# protocol: UNDETERMINED (neither X-Amz-Target nor Action= in the debug stream)"
fi

# ------------------------------------------------------------------ the pass

URL="$(sqs create-queue --queue-name "$QUEUE" \
        --attributes VisibilityTimeout=30,MessageRetentionPeriod=3600 \
        --query QueueUrl --output text 2>&1)"
eq "cli.create-queue" "$URL" "$ENDPOINT/$ACCOUNT/$QUEUE"
if [ "$URL" != "$ENDPOINT/$ACCOUNT/$QUEUE" ]; then
  echo "# create-queue failed, nothing below can run"
  echo "# $PASSES passed, $FAILURES failed"
  echo "RESULT: FAIL"; exit 1
fi

LOOKED_UP="$(sqs get-queue-url --queue-name "$QUEUE" --query QueueUrl --output text)"
eq "cli.get-queue-url" "$LOOKED_UP" "$URL"

sqs list-queues --queue-name-prefix "$QUEUE" --query 'QueueUrls[0]' --output text |
  grep -qxF "$URL"
yes_ "cli.list-queues" $? "the queue is not in a prefix listing of its own name"

VISIBILITY="$(sqs get-queue-attributes --queue-url "$URL" \
               --attribute-names VisibilityTimeout \
               --query 'Attributes.VisibilityTimeout' --output text)"
eq "cli.get-queue-attributes" "$VISIBILITY" "30"

# The CLI's own shorthand for message attributes: a different encoder from
# boto3's, and the reason this is worth sending here rather than a bare body.
BODY="hello from the aws cli"
SENT_MD5="$(sqs send-message --queue-url "$URL" --message-body "$BODY" \
             --message-attributes 'origin={StringValue=cli,DataType=String},attempt={StringValue=1,DataType=Number}' \
             --query MD5OfMessageBody --output text)"
eq "cli.send-message.md5-of-body" "$SENT_MD5" "$(md5_of "$BODY")"

# --output text with a two-element projection gives one TAB-separated line,
# which is all this needs and does not drag in a JSON parser. IFS is set to the
# tab alone and not left at its default: the body has spaces in it on purpose,
# and a default `read` would hand back its first word and call the second one
# the receipt handle.
IFS=$'\t' read -r GOT_BODY HANDLE < <(sqs receive-message --queue-url "$URL" \
  --max-number-of-messages 1 --wait-time-seconds 5 \
  --message-attribute-names All \
  --query 'Messages[0].[Body,ReceiptHandle]' --output text)
eq "cli.receive-message.body" "$GOT_BODY" "$BODY"
[ -n "${HANDLE:-}" ] && [ "$HANDLE" != "None" ]
yes_ "cli.receive-message.receipt-handle" $? "no receipt handle came back"

ORIGIN="$(sqs receive-message --queue-url "$URL" --wait-time-seconds 0 \
           --query 'Messages[0].Body' --output text 2>/dev/null)"
[ -z "$ORIGIN" ] || [ "$ORIGIN" = "None" ]
yes_ "cli.receive-message.in-flight-message-is-hidden" $? "got '$ORIGIN' while it was in flight"

if [ -n "${HANDLE:-}" ] && [ "$HANDLE" != "None" ]; then
  sqs delete-message --queue-url "$URL" --receipt-handle "$HANDLE"
  yes_ "cli.delete-message" $? "the delete was refused"
else
  bad "cli.delete-message" "skipped: there was no receipt handle to delete with"
fi

# That the message does not come back proves nothing on its own — an
# undeleted message would also be silent, for the thirty seconds of its
# visibility. The depth attributes are the unambiguous statement: BOTH counters
# at zero says gone, where visible=0/not-visible=1 would say merely hidden.
# They are also what KEDA reads, so they are worth asserting for their own sake.
DEPTH=""
for _ in 1 2 3 4 5 6 7 8 9 10; do
  DEPTH="$(sqs get-queue-attributes --queue-url "$URL" \
            --attribute-names ApproximateNumberOfMessages ApproximateNumberOfMessagesNotVisible \
            --query 'Attributes.[ApproximateNumberOfMessages,ApproximateNumberOfMessagesNotVisible]' \
            --output text | tr '\t' '/')"
  [ "$DEPTH" = "0/0" ] && break
  sleep 1
done
eq "cli.delete-message.queue-is-empty-afterwards" "$DEPTH" "0/0"

sqs delete-queue --queue-url "$URL"
yes_ "cli.delete-queue" $? "the delete was refused"

# The CLI's own view of an error: it must exit nonzero and name the queue, not
# print an empty success. `NonExistentQueue` is SQS's legacy Query code, which
# is what lands in the CLI's stderr.
ERR="$(sqs get-queue-url --queue-name "$QUEUE" 2>&1)"
if [ $? -eq 0 ]; then
  bad "cli.get-queue-url-after-delete" "it succeeded"
elif grep -q 'NonExistentQueue' <<<"$ERR"; then
  ok "cli.get-queue-url-after-delete"
else
  bad "cli.get-queue-url-after-delete" "wrong error: $(head -2 <<<"$ERR" | tr '\n' ' ')"
fi
URL=""

echo "# $PASSES passed, $FAILURES failed"
if [ "$FAILURES" -eq 0 ]; then echo "RESULT: PASS"; exit 0; fi
echo "RESULT: FAIL"; exit 1
