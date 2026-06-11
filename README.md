# Proyecto Final ISC - Kafka + Spark

## Estructura actual

- `primeraParte/`
  - `Maquina1/run.sh`, `Maquina2/run.sh`, `Maquina3/run.sh`: arrancan tres nodos Kafka en modo KRaft.
  - `producer.js`: productor Kafka que envía mensajes JSON.
  - `consumer.js`: consumidor Kafka que lee mensajes desde el tópico `ventas`.
  - `create_topics.sh`: script para crear 5 tópicos con particiones y replicación.

- `segundaParte/`
  - `master.sh`: arranca el nodo maestro de Spark.
  - `worker1.sh`, `worker2.sh`: arranca dos trabajadores Spark.
  - `spark_example.py`: ejemplo de job Spark con JSON, CSV y SQL.
  - `run_spark_job.sh`: ejecuta el job Spark contra el clúster.
# Proyecto Final ISC — Kafka + Spark

Este manual explica cómo desplegar y verificar el clúster Kafka (KRaft) y el
clúster Spark en una red LAN (o Tailscale). Incluye comandos listos para usar
y pasos de comprobación. Los scripts del repo ya han sido adaptados para usar
`--network host` y para leer variables desde la raíz `.env` organizada por
máquina (`M1_`, `M2_`, `M3_`).

## Resumen rápido
- Archivo de configuración principal: `.env` (contiene `M1_`, `M2_`, `M3_`).
- Directorios principales: `primeraParte/` (Kafka), `segundaParte/` (Spark).
- Scripts clave: `primeraParte/Maquina1/run.sh` (y Maquina2/Maquina3),
  `segundaParte/master.sh`, `worker1.sh`, `worker2.sh`, `run_spark_job.sh`.

## 1) Preparar `primeraParte/.env` para producer/consumer

Ejecuta desde la raíz del repo (crea `primeraParte/.env` con los brokers y el
quorum de controllers):

```bash
cd /ruta/al/repo
set -a; source .env; set +a
cat > primeraParte/.env <<EOF
BROKERS=${M1_ADVERTISED_IP}:${M1_HOST_PORT},${M2_ADVERTISED_IP}:${M2_HOST_PORT},${M3_ADVERTISED_IP}:${M3_HOST_PORT}
CONTROLLER_VOTERS=1@${M1_ADVERTISED_IP}:9093,2@${M2_ADVERTISED_IP}:9093,3@${M3_ADVERTISED_IP}:9093
CLUSTER_ID=${CLUSTER_ID}
EOF
```

## 2) Añadir `/etc/hosts` (evita UnknownHostException de Java)

En cada máquina ejecuta:

```bash
sudo bash -c 'grep -q "100.100.10.100 kafka1" /etc/hosts || echo "100.100.10.100 kafka1" >> /etc/hosts'
sudo bash -c 'grep -q "100.100.10.101 kafka2" /etc/hosts || echo "100.100.10.101 kafka2" >> /etc/hosts'
sudo bash -c 'grep -q "100.100.10.102 kafka3" /etc/hosts || echo "100.100.10.102 kafka3" >> /etc/hosts'
```

Si usas Tailscale reemplaza las IPs por las IPs de Tailscale correspondientes.

## 3) Arranque de los nodos Kafka (orden recomendado)

- En Maquina1 (100.100.10.100):

```bash
cd /ruta/al/repo/primeraParte
docker rm -f kafka1 || true
bash Maquina1/run.sh
```

- En Maquina2 y Maquina3 repite con `Maquina2/run.sh` y `Maquina3/run.sh`.

## 4) Comprobaciones básicas tras arrancar

- Ver contenedores activos:

```bash
docker ps --filter name=kafka
```

- Seguir logs (esperar que se conecten los peers y formen quorum):

```bash
docker logs -f kafka1
```

- Probar conectividad TCP (controller 9093 y listener cliente según M*_HOST_PORT):

```bash
nc -vz 100.100.10.100 9093
nc -vz 100.100.10.100 9094   # M1_HOST_PORT=9094
nc -vz 100.100.10.101 9096   # M2_HOST_PORT=9096
```

- Probar broker Kafka (si tienes utilidades Kafka instaladas):

```bash
/opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server 100.100.10.100:9094
/opt/kafka/bin/kafka-topics.sh --bootstrap-server 100.100.10.100:9094 --list
```

## 5) Ejecutar producer y consumer

Desde `primeraParte/` (asegúrate de haber creado `primeraParte/.env`):

```bash
cd primeraParte
npm install
node consumer.js &   # ejecuta en segundo plano
node producer.js     # cuidado con el volumen de mensajes
```

El consumidor escribirá archivos JSONL en `primeraParte/consumer_output/`.

## 6) Spark — iniciar master y workers

- En la máquina configurada como Spark master (por defecto `SPARK_MASTER_HOST`):

```bash
cd /ruta/al/repo/segundaParte
bash master.sh
```

- En los hosts de los workers:

```bash
bash worker1.sh
bash worker2.sh
```

- Ejecutar job Spark:

```bash
bash run_spark_job.sh
```

## 7) Diagnóstico y troubleshooting rápido

- `UnknownHostException: kafka1` → añadir `/etc/hosts` o usar IP en
  `KAFKA_ADVERTISED_LISTENERS`.
- `Connection to node X could not be established` → comprobar que el puerto
  `9093` esté alcanzable en la IP indicada (nc, firewall/Tailscale ACL).
- `producer` no conecta → revisar `primeraParte/.env` y que `BROKERS` tenga
  las IP:PORT correctas.

Comandos útiles:

```bash
# puertos escuchando
ss -ltn | grep 9093

# probar TCP
nc -vz 100.100.10.101 9093

# mapping de puertos del contenedor (si no usas host network)
docker inspect kafka2 --format '{{json .NetworkSettings.Ports}}'

# logs
docker logs kafka2 | tail -n 200
```

## Notas finales

- Este manual asume que las IPs 100.100.10.100/101/102 son las rutas entre
  tus hosts (por ejemplo IPs de Tailscale o LAN). Si usas otras IPs modifica
  `.env` en consecuencia.
- Los `run.sh` han sido adaptados para seleccionar automáticamente las
  variables `M1_/M2_/M3_` desde `.env` y ejecutar en `--network host`.

---

Si quieres, puedo generar un script `deploy_hosts.sh` que:
- copie `primeraParte/.env` en cada host,
- añada las entradas en `/etc/hosts`, y
- arranque los `run.sh` via SSH en las tres máquinas.

Indica el usuario SSH a usar si lo deseas.
