#!/usr/bin/env bash
# Polars, PyArrow and pandas, in the shared python container — three readers,
# one container start, because a notebook has all three installed anyway.
#   compat/readers/dataframes.sh <samples-dir>
set -uo pipefail
SAMPLES="$(cd "${1:?usage: dataframes.sh <samples-dir>}" && pwd)"
HERE="$(cd "$(dirname "$0")" && pwd)"
exec docker run --rm --name queen-s3-compat-dataframes \
  -v "$SAMPLES":/samples:ro -v "$HERE":/readers:ro \
  queen-s3-compat-python:latest python /readers/polars_pandas_reader.py
