#!/bin/bash
set -e

NETWORK=kafka-redes-final

docker network inspect "$NETWORK" >/dev/null 2>&1 || docker network create "$NETWORK"

docker run --rm \
  --network "$NETWORK" \
  -v "$(pwd)":/app \
  spark-nuevo:latest \
  /opt/spark/bin/spark-submit --master spark://spark-master:7077 /app/spark_example.py
