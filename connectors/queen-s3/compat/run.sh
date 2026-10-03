#!/usr/bin/env bash
#
# The queen-s3 reader compatibility lane (PLAN_S3_SINK.md §9).
#
#   connectors/queen-s3/compat/run.sh                # everything, rewrite MATRIX.md
#   connectors/queen-s3/compat/run.sh duckdb spark   # a subset of the readers
#   connectors/queen-s3/compat/run.sh --keep         # leave the sample lake behind
#   connectors/queen-s3/compat/run.sh --samples DIR  # reuse a lake instead of building one
#
# It builds a small sample lake with the sink's OWN writers
# (`cargo run --example gen_samples`), runs every reader in `readers/` against it
# in Docker with the lake mounted READ-ONLY, and writes the measured table to
# MATRIX.md. Nothing it starts outlives it: every container is `--rm` and named
# `queen-s3-compat-*`, and the temporary lake is deleted unless you say --keep.
#
# Exit status: non-zero if any cell came back `fail`. An `unsupported` is a
# documented fact about a reader, not a failure of the sink, and a `not-run` is
# an admission — neither one fails the lane.
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
CRATE="$(cd "$HERE/.." && pwd)"
READERS_DIR="$HERE/readers"
MATRIX="$HERE/MATRIX.md"
IMAGE=queen-s3-compat-python:latest

KEEP=0
SAMPLES=""
ONLY=()
while [ $# -gt 0 ]; do
  case "$1" in
    --keep) KEEP=1;;
    --samples) SAMPLES="$2"; shift;;
    -h|--help) sed -n '2,20p' "$0"; exit 0;;
    -*) echo "unknown flag $1" >&2; exit 2;;
    *) ONLY+=("$1");;
  esac
  shift
done

say() { printf '\n=== %s\n' "$*"; }

OWN_SAMPLES=0
LOGDIR="$(mktemp -d -t queen-s3-compat.XXXXXX)"
cleanup() {
  local code=$?
  # Belt and braces: every reader container is --rm, so this only ever catches
  # one that was killed mid-run.
  docker ps -aq --filter 'name=queen-s3-compat-' | xargs -r docker rm -f >/dev/null 2>&1
  if [ "$OWN_SAMPLES" = 1 ] && [ "$KEEP" = 0 ]; then
    rm -rf "$SAMPLES"
  elif [ "$OWN_SAMPLES" = 1 ]; then
    echo "--keep: the sample lake is at $SAMPLES"
  fi
  echo "logs kept at $LOGDIR"
  exit $code
}
trap cleanup EXIT INT TERM

command -v docker >/dev/null || { echo "docker not found" >&2; exit 2; }
command -v cargo  >/dev/null || { echo "cargo not found" >&2; exit 2; }
command -v python3 >/dev/null || { echo "python3 not found" >&2; exit 2; }

# --- the sample lake -------------------------------------------------------
if [ -z "$SAMPLES" ]; then
  SAMPLES="$(mktemp -d -t queen-s3-samples.XXXXXX)"
  OWN_SAMPLES=1
  say "building the sample lake in $SAMPLES"
  ( cd "$CRATE" && cargo run --quiet --example gen_samples -- "$SAMPLES" ) || exit 1
else
  SAMPLES="$(cd "$SAMPLES" && pwd)"
  say "reusing the sample lake at $SAMPLES"
fi
[ -f "$SAMPLES/expected.json" ] || { echo "no expected.json in $SAMPLES" >&2; exit 1; }

# --- the shared reader image ----------------------------------------------
say "building $IMAGE (DuckDB, Polars, PyArrow, pandas)"
docker build -q -f "$READERS_DIR/python.Dockerfile" -t "$IMAGE" "$READERS_DIR" >/dev/null || exit 1

# --- the readers -----------------------------------------------------------
ALL=(duckdb clickhouse spark dataframes trino)
RUN=("${ALL[@]}")
[ ${#ONLY[@]} -gt 0 ] && RUN=("${ONLY[@]}")

: > "$LOGDIR/verdicts.jsonl"
for reader in "${RUN[@]}"; do
  script="$READERS_DIR/$reader.sh"
  [ -x "$script" ] || { echo "no such reader: $reader" >&2; exit 2; }
  say "$reader"
  "$script" "$SAMPLES" 2>"$LOGDIR/$reader.err" | tee "$LOGDIR/$reader.out" \
    | grep -E '^(VERDICT|NOTE) ' >> "$LOGDIR/verdicts.jsonl"
  if [ ! -s "$LOGDIR/$reader.out" ]; then
    echo "  !! $reader produced nothing; stderr tail:" >&2
    tail -5 "$LOGDIR/$reader.err" >&2
  fi
done

# --- the matrix ------------------------------------------------------------
say "writing $MATRIX"
python3 "$HERE/render_matrix.py" "$LOGDIR/verdicts.jsonl" "$SAMPLES/expected.json" "$MATRIX" || exit 1

fails=$(grep -c '"verdict": *"fail' "$LOGDIR/verdicts.jsonl" || true)
echo
if [ "${fails:-0}" -gt 0 ]; then
  echo "FAIL: $fails cell(s) came back fail — see $MATRIX"
  exit 1
fi
echo "OK: no cell failed."
