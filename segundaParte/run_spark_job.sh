#!/bin/bash
set -e

# Load SPARK_MASTER_HOST / SPARK_MASTER_PORT from parent .env if present
if [ -f "$(dirname "$0")/../.env" ]; then
  set -a
  # shellcheck disable=SC1090
  source "$(dirname "$0")/../.env"
  set +a
fi

docker run --rm \
  --network host \
  -v "$(pwd)":/app \
  spark-nuevo:latest \
  /opt/spark/bin/spark-submit --master spark://${SPARK_MASTER_HOST:-spark-master}:${SPARK_MASTER_PORT:-7077} /app/spark_example.py
