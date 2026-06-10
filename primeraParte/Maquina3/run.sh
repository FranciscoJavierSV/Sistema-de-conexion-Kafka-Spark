#!/usr/bin/env bash
set -e

# cargar .env si existe (exporta variables para usar en -p)
if [ -f "$(dirname "$0")/../../.env" ]; then
  set -a
  # shellcheck disable=SC1090
  source "$(dirname "$0")/../../.env"
  set +a
fi

# valores por defecto específicos de esta máquina (puedes sobrescribir en .env)
ADVERTISED_IP=${ADVERTISED_IP:-100.100.10.102}
HOST_PORT=${HOST_PORT:-9098}

docker run -it \
  --name kafka3 \
  --hostname kafka3 \
  --env-file "$(dirname "$0")/../../.env" \
  --network kafka-redes-final \
  -p ${HOST_PORT:-9098}:9092 \
  -p $(( ${HOST_PORT:-9098} + 1 )):9093 \
  -e KAFKA_NODE_ID=3 \
  -e KAFKA_PROCESS_ROLES=broker,controller \
  -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  -e KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://${ADVERTISED_IP}:${HOST_PORT} \
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
  -e KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT \
  -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@100.100.10.100:9093,2@100.100.10.101:9093,3@100.100.10.102:9093 \
  -e CLUSTER_ID=5L6g3nShT-eMCtK--X86sw \
  -e KAFKA_AUTO_CREATE_TOPICS_ENABLE=false \
  -e KAFKA_NUM_PARTITIONS=3 \
  -e KAFKA_DEFAULT_REPLICATION_FACTOR=3 \
  -e KAFKA_MIN_INSYNC_REPLICAS=2 \
  -e PATH="/opt/kafka/bin:$PATH" \
  apache/kafka:latest