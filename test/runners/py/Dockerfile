# Python client integration runner. Build context = repo root.
#   docker build -f test/runners/py/Dockerfile -t queen-test-runner-py .
FROM python:3.12-slim-bookworm

RUN apt-get update \
 && apt-get install -y --no-install-recommends curl ca-certificates \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /src

# The suite installs editable; asyncpg/httpx ship cp312 wheels, so no compiler
# toolchain is needed. (.venv is excluded from the context by the sidecar ignore.)
COPY clients/client-py/ ./
RUN pip install --no-cache-dir -e ".[dev]"

COPY test/runners/common/wait-for-broker.sh /usr/local/bin/wait-for-broker
COPY test/runners/py/entrypoint.sh /entrypoint.sh
RUN chmod +x /usr/local/bin/wait-for-broker /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
