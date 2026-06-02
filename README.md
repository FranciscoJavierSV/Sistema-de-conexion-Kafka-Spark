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

### Kafka

1. Crear la red Docker:

```bash
docker network inspect kafka-redes-final >/dev/null 2>&1 || docker network create kafka-redes-final
```

2. En cada máquina Kafka, ejecutar el script correspondiente:

- `primeraParte/Maquina1/run.sh`
- `primeraParte/Maquina2/run.sh`
- `primeraParte/Maquina3/run.sh`

3. Crear los tópicos:

```bash
cd /home/javi/redes/primeraParte
bash create_topics.sh
```

4. Ejecutar el productor:

```bash
cd /home/javi/redes/primeraParte
npm install
node producer.js
```

5. Ejecutar el consumidor:

```bash
cd /home/javi/redes/primeraParte
node consumer.js
```

### Spark

1. Iniciar Spark master:

```bash
cd /home/javi/redes/segundaParte
bash master.sh
```

2. Iniciar workers:

```bash
bash worker1.sh
bash worker2.sh
```

3. Ejecutar el job Spark:

```bash
bash run_spark_job.sh
```

### Pruebas de tolerancia a fallos

- Para Kafka: detener un nodo (`docker stop kafka2`) y verificar que el productor/conversor siguen funcionando con los demás nodos.
- Para Spark: detener un worker (`docker stop spark-worker-1`) y observar que el job se sigue ejecutando con el worker restante.

## Nota importante

Si usas tres máquinas físicas reales, reemplaza las IPs en `primeraParte/Maquina1/run.sh`, `.../Maquina2/run.sh` y `.../Maquina3/run.sh` por la IP fija de cada equipo.
