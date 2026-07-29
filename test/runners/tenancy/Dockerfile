# Two-tenant isolation runner over the HA pair (curl + jq). Build context = repo root.
#   docker build -f test/runners/tenancy/Dockerfile -t queen-test-runner-tenancy .
FROM alpine:3.20

RUN apk add --no-cache bash curl jq

COPY test/runners/common/wait-for-broker.sh /usr/local/bin/wait-for-broker
COPY test/runners/tenancy/tenancy-check.sh /entrypoint.sh
RUN chmod +x /usr/local/bin/wait-for-broker /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
