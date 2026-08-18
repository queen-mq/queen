# The HTTP wire suite of PLAN_KV_TIMERS.md §10.2: the client row that is not an
# SDK. Build context = repo root.
#   docker build -f test/runners/http/Dockerfile -t queen-test-runner-http .
#
# curl and jq are the client. python3 is NOT part of it: it runs the scripted
# plan server of the broker-free half, which records the request bytes and
# replies with canned answers so the commit contract of §8.3 (200 + verdict,
# versus every other failure) can be exercised without a broken database.
#
# No postgresql-client, deliberately and for the same reason the txnsemantics
# runner has none: every assertion here is made at the HTTP wire, because the
# wire IS the contract the seven clients depend on. An assertion that read the
# row straight out of Postgres would pass on a broker that commits correctly and
# reports the wrong thing, which is exactly the failure this suite exists to
# catch.
FROM alpine:3.20

RUN apk add --no-cache bash curl jq python3

COPY test/runners/common/wait-for-broker.sh /usr/local/bin/wait-for-broker
COPY test/runners/http/kv-timers-wire.sh   /suite/kv-timers-wire.sh
COPY test/runners/http/plan-server.py      /suite/plan-server.py
COPY test/runners/http/http-wire-unit.sh   /suite/http-wire-unit.sh
COPY test/runners/http/http-wire-check.sh  /suite/http-wire-check.sh
COPY test/runners/http/entrypoint.sh       /entrypoint.sh
RUN chmod +x /usr/local/bin/wait-for-broker /entrypoint.sh \
             /suite/http-wire-unit.sh /suite/http-wire-check.sh

ENTRYPOINT ["/entrypoint.sh"]
