#!/bin/bash
set -e

NETWORK=kafka-redes-final

docker network inspect "$NETWORK" >/dev/null 2>&1 || docker network create "$NETWORK"

TOPICS=(ventas logs sensores transacciones eventos)
for topic in "${TOPICS[@]}"; do
  echo "Creando topic: $topic"
  docker exec kafka1 /opt/kafka/bin/kafka-topics.sh \
    --create --topic "$topic" \
    --bootstrap-server kafka1:9092 \
    --partitions 3 \
    --replication-factor 3 \
    --if-not-exists
  echo "  -> $topic creado"
done

echo "Tópicos creados con éxito."
