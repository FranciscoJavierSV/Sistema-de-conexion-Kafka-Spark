#!/bin/bash

BOOTSTRAP=100.100.10.100:9094

TOPICOS=(
  ventas_json
  inventario_csv
  clientes_sql
  logs
  metricas
)

for TOPICO in "${TOPICOS[@]}"
do
  echo "Creando tópico: $TOPICO"

  docker exec kafka1 \
  /opt/kafka/bin/kafka-topics.sh \
    --create \
    --if-not-exists \
    --topic "$TOPICO" \
    --bootstrap-server "$BOOTSTRAP" \
    --partitions 3 \
    --replication-factor 3
done

echo "Tópicos creados"