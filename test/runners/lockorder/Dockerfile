# Lock-order concurrency gate (PLAN_KV_TIMERS.md §2, §15 / F4). Build context = repo root.
#   docker build -f test/runners/lockorder/Dockerfile -t queen-test-runner-lockorder .
#
# postgresql-client is NOT optional here: pg_stat_database.deadlocks is the only
# detector that sees a 40P01 the broker retried or swallowed, and the check
# script FAILS (never skips) when it cannot reach Postgres.
FROM alpine:3.20

RUN apk add --no-cache bash curl jq postgresql16-client

COPY test/runners/common/wait-for-broker.sh /usr/local/bin/wait-for-broker
COPY test/runners/lockorder/lockorder-check.sh /entrypoint.sh
RUN chmod +x /usr/local/bin/wait-for-broker /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
