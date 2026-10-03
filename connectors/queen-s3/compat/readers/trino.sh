#!/usr/bin/env bash
# Trino / the Athena shape: NOT RUN. See compat/MATRIX.md for the DDL and for
# the four configuration facts that were measured before the attempt was
# stopped. This script exists so the row is in the matrix as an explicit
# not-run rather than as an absence.
set -uo pipefail
for cell in 'jsonl zstd' 'jsonl gzip' 'jsonl none' 'parquet zstd' 'parquet snappy'; do
  set -- $cell
  printf 'VERDICT {"reader":"trino","version":"483 (not run)","format":"%s","compression":"%s","verdict":"not-run","incantation":"see MATRIX.md: CREATE TABLE ... WITH (external_location, partitioned_by)","detail":"stood up but not measured"}\n' "$1" "$2"
done
