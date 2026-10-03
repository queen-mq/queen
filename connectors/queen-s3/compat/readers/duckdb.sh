#!/usr/bin/env bash
# DuckDB, in the shared python container. Prints VERDICT/NOTE lines on stdout.
#   compat/readers/duckdb.sh <samples-dir>
set -uo pipefail
SAMPLES="$(cd "${1:?usage: duckdb.sh <samples-dir>}" && pwd)"
HERE="$(cd "$(dirname "$0")" && pwd)"
exec docker run --rm --name queen-s3-compat-duckdb \
  -v "$SAMPLES":/samples:ro -v "$HERE":/readers:ro \
  queen-s3-compat-python:latest python /readers/duckdb_reader.py
