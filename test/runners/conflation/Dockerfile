# The conflation e2e suite of PLAN_CONFLATION.md §7.3: raw HTTP, no SDK in the
# path — the scenarios must not depend on client changes that land later (§4
# ships separately, and §7.2 owns the per-SDK halves). Build context = repo root.
#   docker build -f test/runners/conflation/Dockerfile -t queen-test-runner-conflation .
#
# curl and jq are the client, exactly like test/runners/http. No
# postgresql-client, and for the same reason as there: every §7.3 assertion is
# observable at the wire (pop shape, response echoes, depth fields, the DLQ
# surface, /metrics/prometheus), and an assertion that read the cursor straight
# out of Postgres would pass on a broker that commits correctly and REPORTS the
# wrong thing. The cursor-level pins (batch_end, lease_conflated,
# attempt_offset) live in server/tests/conflation_semantics.rs (§7.1), not here.
FROM alpine:3.20

RUN apk add --no-cache bash curl jq

COPY test/runners/common/wait-for-broker.sh          /usr/local/bin/wait-for-broker
COPY test/runners/conflation/conflation-e2e-check.sh /suite/conflation-e2e-check.sh
COPY test/runners/conflation/entrypoint.sh           /entrypoint.sh
RUN chmod +x /usr/local/bin/wait-for-broker /entrypoint.sh /suite/conflation-e2e-check.sh

ENTRYPOINT ["/entrypoint.sh"]
