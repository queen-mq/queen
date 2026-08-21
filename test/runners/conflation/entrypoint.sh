#!/usr/bin/env bash
# Conflation e2e suite (PLAN_CONFLATION.md §7.3). Unlike the http runner there
# is no broker-free unit half here: every scenario is an end-to-end property of
# a running broker, so the wait comes first and the suite is the whole job.
set -u

/usr/local/bin/wait-for-broker || exit 97

exec /suite/conflation-e2e-check.sh
