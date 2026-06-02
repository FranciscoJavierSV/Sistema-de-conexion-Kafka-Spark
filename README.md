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

## Qué ya tienes

1. Un clúster Kafka básico en Docker con tres nodos `broker,controller`.
2. Un productor y consumidor en Node.js para enviar y leer mensajes.
3. Scripts para arrancar Spark master y workers.

## Qué falta o debe mejorarse

- Crear los tópicos con particiones y factor de replicación. Ahora hay un script para eso.
- Generar y procesar datos con 100,000 filas.
- Crear y procesar datos JSON/CSV/SQL en Spark.
- Documentar y ejecutar pruebas de caída de nodo para tolerancia a fallos.
- Ajustar direcciones IP en Kafka si usas tres máquinas físicas distintas.

## Uso recomendado

1. Edita `.env` en la raíz del proyecto con los valores de tu red.

2. Crea la red Docker si no existe:

docker network inspect kafka-redes-final >/dev/null 2>&1 || docker network create kafka-redes-final

3. Inicia los nodos Kafka:

bash primeraParte/Maquina1/run.sh
bash primeraParte/Maquina2/run.sh
bash primeraParte/Maquina3/run.sh

4. Crea los tópicos:

cd primeraParte
bash create_topics.sh

5. Instala dependencias y ejecuta el productor:

cd primeraParte
npm install
node producer.js

6. Ejecuta el consumidor:

cd primeraParte
node consumer.js

El consumidor creará archivos JSONL en `primeraParte/consumer_output/`.

### Verificar qué nodos Kafka están activos

Desde el host o desde un contenedor con Kafka:

```bash
docker ps | grep kafka
```

Dentro de un contenedor Kafka:

```bash
docker exec -it kafka1 bash
/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka1:9092 --describe --topic ventas
/opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server kafka1:9092
```

Para revisar el estado del grupo de consumidores:

```bash
/opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka1:9092 --describe --group grupo-examen-final
```

Si quieres ver los mensajes de un tópico con una herramienta de Kafka desde otro contenedor:

```bash
docker run --rm --network kafka-redes-final apache/kafka:latest \
  /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka2:9092 --topic credo --from-beginning --group demo-console
```

7. Inicia Spark master y workers:

cd segundaParte
bash master.sh
bash worker1.sh
bash worker2.sh

8. Ejecuta el job Spark:

bash run_spark_job.sh

### Comandos útiles de Spark

- Ver el estado del clúster Spark en la UI:

  * Master: `http://localhost:8080`
  * Workers: `http://localhost:8081`, `http://localhost:8082`

- Ver procesos Spark en los contenedores:

```bash
docker ps | grep spark
```

- Usar `spark-submit` directo si necesitas otro script:

```bash
/opt/spark/bin/spark-submit --master spark://spark-master:7077 /app/spark_example.py
```

- En el driver Spark puedes ejecutar un shell para pruebas:

```bash
/opt/spark/bin/spark-shell --master spark://spark-master:7077
```

Notas:

- Si usas la misma máquina para todo, `.env` puede quedarse igual y los scripts usarán los brokers definidos allí.
- Si usas máquinas físicas diferentes, copia `.env` a cada host y cambia `ADVERTISED_IP` y `NODE_ID` en cada uno.
- Si necesitas usar la red real en lugar de la red Docker, ajusta las IPs y puertos en `.env` y usa los scripts sin `--network` o con `--network host` según tu configuración.
- Si un nodo Kafka cae, el consumidor seguirá leyendo desde los demás brokers cuando recupere conexión.
