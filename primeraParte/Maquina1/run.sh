#!/usr/bin/env bash
set -e

# cargar .env si existe (exporta variables para usar en -p)
if [ -f "$(dirname "$0")/../../.env" ]; then
  set -a
  # shellcheck disable=SC1090
  source "$(dirname "$0")/../../.env"
  set +a
fi

# Detectar automáticamente la máquina (Maquina1/2/3) y asignar variables M1_/M2_/M3_
BASEDIR=$(basename "$(dirname "$0")")
case "$BASEDIR" in
  Maquina1) IDX=1 ;;
  Maquina2) IDX=2 ;;
  Maquina3) IDX=3 ;;
  *) IDX=${NODE_ID:-1} ;;
esac

# Cargar valores específicos M${IDX}_* desde .env (si existen)
eval "ADVERTISED_IP=\${M${IDX}_ADVERTISED_IP:-\$ADVERTISED_IP}"
eval "HOST_PORT=\${M${IDX}_HOST_PORT:-\$HOST_PORT}"
eval "NODE_ID=\${M${IDX}_NODE_ID:-\$NODE_ID}"

ADVERTISED_IP=${ADVERTISED_IP:-100.100.10.100}
HOST_PORT=${HOST_PORT:-9094}

docker run -it \
  --name kafka1 \
  --hostname kafka1 \
  --env-file "$(dirname "$0")/../../.env" \
  --network host \
  -e KAFKA_NODE_ID=1 \
  -e KAFKA_PROCESS_ROLES=broker,controller \
  -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  -e KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:${HOST_PORT},CONTROLLER://0.0.0.0:9093 \
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