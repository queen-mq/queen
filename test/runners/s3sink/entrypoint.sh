#!/usr/bin/env bash
# The S3 sink e2e suite (PLAN_S3_SINK.md §9). Every scenario is an end-to-end
# property of a real broker, a real S3 gateway and a real reader, so the wait
# comes first and the suite is the whole job.
set -u

/usr/local/bin/wait-for-broker || exit 97

exec python3 -u /suite/scenarios.py
