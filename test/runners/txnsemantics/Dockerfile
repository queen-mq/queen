# Transactional semantics gate (PLAN_KV_TIMERS.md §15 "Transazionali" / F4).
# Build context = repo root.
#   docker build -f test/runners/txnsemantics/Dockerfile -t queen-test-runner-txnsemantics .
#
# Every assertion in this suite is made at the HTTP wire, on purpose: the wire
# IS the contract the seven clients depend on. An assertion that read the row
# out of the broker's store would pass on a broker that commits correctly and
# reports the wrong thing.
FROM alpine:3.20

RUN apk add --no-cache bash curl jq

COPY test/runners/common/wait-for-broker.sh /usr/local/bin/wait-for-broker
COPY test/runners/txnsemantics/txn-semantics-check.sh /entrypoint.sh
RUN chmod +x /usr/local/bin/wait-for-broker /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
