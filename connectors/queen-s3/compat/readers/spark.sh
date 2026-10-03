#!/usr/bin/env bash
# Apache Spark, PySpark in local mode, submitted inside the official image.
#   compat/readers/spark.sh <samples-dir>
#
# The image is pinned: `apache/spark:latest` does not exist, and which Spark you
# run decides which Hadoop codecs you get — which is half of what this row
# measures. Override with QUEEN_S3_SPARK_IMAGE.
set -uo pipefail
SAMPLES="$(cd "${1:?usage: spark.sh <samples-dir>}" && pwd)"
HERE="$(cd "$(dirname "$0")" && pwd)"
IMAGE="${QUEEN_S3_SPARK_IMAGE:-apache/spark:4.0.1}"
docker run --rm --name queen-s3-compat-spark \
  -v "$SAMPLES":/samples:ro -v "$HERE":/readers:ro \
  "$IMAGE" /opt/spark/bin/spark-submit --master 'local[2]' /readers/spark_reader.py \
  2>/dev/null | grep -E '^(VERDICT|NOTE) '
