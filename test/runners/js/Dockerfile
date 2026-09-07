# JS client integration runner. Build context = repo root.
#   docker build -f test/runners/js/Dockerfile -t queen-test-runner-js .
FROM node:24-bookworm-slim

RUN apt-get update \
 && apt-get install -y --no-install-recommends curl ca-certificates \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /suite

# Manifests first for layer caching; clean install from the committed lockfile.
COPY clients/client-js/package.json clients/client-js/package-lock.json ./
RUN npm ci

# Suite sources. run.js imports ../client-v2 and ./* by relative path, so the
# layout under /suite must match clients/client-js.
COPY clients/client-js/client-v2 ./client-v2
COPY clients/client-js/test-v2 ./test-v2

COPY test/runners/common/wait-for-broker.sh /usr/local/bin/wait-for-broker
COPY test/runners/js/entrypoint.sh /entrypoint.sh
RUN chmod +x /usr/local/bin/wait-for-broker /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
