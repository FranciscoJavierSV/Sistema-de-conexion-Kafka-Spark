# Proyecto: Kafka + Spark (Equipo1)

Este documento explica, de forma clara y natural, qué se configuró en la carpeta `Equipo1`, por qué se hizo así y cómo ejecutar cada parte paso a paso. Está pensado para que cualquiera en el equipo pueda levantar el clúster y reproducir las pruebas de procesamiento y tolerancia a fallos.

## Objetivo
Construir un entorno distribuido que integre Apache Kafka (modo KRaft) con Apache Spark para procesar datos en streaming. El objetivo final es demostrar particionamiento, replicación, procesamiento distribuido y tolerancia a fallos usando tres nodos.

## Resumen de qué hay en `Equipo1`
- Kafka (3 nodos): `Equipo1/kafka/Maquina1/Dockerfile`, `Equipo1/kafka/Maquina2/Dockerfile`, `Equipo1/kafka/Maquina3/Dockerfile`.
- Scripts: `Equipo1/kafka/topicos.sh` (crea 5 tópicos configurados con particiones y RF=3).
- Productor/Consumidor: `Equipo1/kafka/producer.js`, `Equipo1/kafka/consumer.js`.
- Spark: Master y dos workers (`Equipo1/spark/Master`, `Equipo1/spark/Maquina2`, `Equipo1/spark/Maquina3`) y el job `Equipo1/spark/Master/spark_job.py`.

## Qué se configuró y por qué (explicación de alto nivel)

- Kafka en modo KRaft con roles `broker,controller` en cada nodo: permite eliminar Zookeeper y que cada nodo pueda participar en el quorum de control (más simple para laboratorios y pruebas). Se usaron tres nodos para poder demostrar replicación (RF=3) y fallos de nodo.

- Tópicos: se definieron 5 tópicos distintos (ventas_json, inventario_csv, clientes_sql, logs, metricas) para cubrir los distintos formatos de datos exigidos (JSON, CSV, SQL, logs, métricas). Cada tópico se crea con 3 particiones y factor de replicación 3 para distribuir la carga y garantizar disponibilidad.

- Producer y Consumer: el `producer.js` genera datos en varios formatos (JSON, CSV, SQL, logs, métricas) para alimentar los tópicos. El `consumer.js` consume y guarda los mensajes (por defecto a MongoDB en desarrollo); sirve para validar que los mensajes llegan y se almacenan.

- Spark: el job `spark_job.py` consume desde Kafka tópicos seleccionados y escribe resultados en `/tmp/output/` en el master. Esto simula un pipeline de procesamiento en streaming y genera métricas por batch.

## IPs utilizadas (despliegue físico)
En tu despliegue físico se usarán estas IPs para los tres nodos:

- Nodo 1: 100.100.10.100
- Nodo 2: 100.100.10.101
- Nodo 3: 100.100.10.102

Los puertos internos para Kafka se mantienen tal como están en los Dockerfiles (por ejemplo 9094/9096/9098 para listeners PLAINTEXT y 9093/9095/9097 para controller).

## Estructura rápida de archivos

- Kafka:
  - [Equipo1/kafka/Maquina1/Dockerfile](kafka/Maquina1/Dockerfile)
  - [Equipo1/kafka/Maquina2/Dockerfile](kafka/Maquina2/Dockerfile)
  - [Equipo1/kafka/Maquina3/Dockerfile](kafka/Maquina3/Dockerfile)
  - [Equipo1/kafka/topicos.sh](kafka/topicos.sh)
  - [Equipo1/kafka/producer.js](kafka/producer.js)
  - [Equipo1/kafka/consumer.js](kafka/consumer.js)

- Spark:
  - [Equipo1/spark/Master/Dockerfile](spark/Master/Dockerfile)
  - [Equipo1/spark/Master/spark_job.py](spark/Master/spark_job.py)
  - [Equipo1/spark/Maquina2/Dockerfile](spark/Maquina2/Dockerfile)
  - [Equipo1/spark/Maquina3/Dockerfile](spark/Maquina3/Dockerfile)

## Paso a paso: desde la máquina apagada hasta la prueba

1) Preparación en cada host (Maquina1, Maquina2, Maquina3)

  - Sitúate en la carpeta del proyecto correspondiente al componente (ej. en la máquina 1: `Equipo1/kafka`).
  - Asegúrate de que cada máquina tiene su IP fija configurada en la red: 100.100.10.100, 100.100.10.101, 100.100.10.102.

2) Construir imágenes de Kafka (en cada host, desde la carpeta `kafka` correspondiente)

```bash
# En cada máquina, ajustar la ruta si es necesario
docker build -t kafka-node1 ./Maquina1   # en el host con IP 100.100.10.100
docker build -t kafka-node2 ./Maquina2   # en el host con IP 100.100.10.101
docker build -t kafka-node3 ./Maquina3   # en el host con IP 100.100.10.102
```

3) Ejecutar contenedores Kafka (cada imagen en su host, modo host-network)

```bash
docker run -d --network host --name kafka1 kafka-node1
docker run -d --network host --name kafka2 kafka-node2
docker run -d --network host --name kafka3 kafka-node3
```

Por qué usar `--network host`: facilita que Kafka escuche directamente en la IP del host, evitando mapeos complejos de puertos y problemas de listeners en experimentos distribuidos.

4) Verificar quorum del controller (desde cualquiera de los hosts)

```bash
/opt/kafka/bin/kafka-metadata-quorum.sh --bootstrap-server 100.100.10.100:9094 describe --status
```

Explicación corta: este comando te dice si el quorum de controllers está formado, quién es el líder, y quiénes son los votantes actuales.

5) Crear los tópicos (ejecutar `topicos.sh` desde la máquina que vea al broker1)

```bash
bash topicos.sh
```

El script crea 5 tópicos con 3 particiones y RF=3. Se eligió RF=3 para asegurar que cada partición tenga réplicas en los tres nodos.

6) Generar carga de prueba (producción de mensajes)

```bash
# En la máquina donde ejecutarás el producer
# Para producir 100k mensajes ajusta la variable antes de ejecutar
TOTAL_REGISTROS=100000 node producer.js
```

Por qué: las pruebas del proyecto exigen volúmenes grandes para evaluar comportamiento distribuido y rendimiento. El producer incluye generadores para JSON, CSV y otros formatos.

7) Consumir y persistir mensajes

```bash
# Opcional: configurar MONGO_URI si guardas en MongoDB
MONGO_URI="mongodb://<host_mongo>:27017" node consumer.js

# O si prefieres guardar en archivo (si el consumer soporta una opción de archivo):
SAVE_TO_FILE=1 node consumer.js
```

Comentario: el `consumer.js` del repo actualmente inserta documentos en MongoDB; si vas a ejecutar los containers, asegúrate de que el URI sea accesible desde los contenedores o que tengas un servicio Mongo corriendo en la red.

8) Ejecutar Spark job (en el Master de Spark)

```bash
docker exec -it spark-master bash
/opt/spark/bin/spark-submit /opt/spark-apps/spark_job.py
```

El job consume tópicos (p. ej. `ventas_json`) y escribe resultados en `/tmp/output/` en el contenedor master.

9) Verificaciones útiles (comandos rápidos)

```bash
# Listar tópicos
/opt/kafka/bin/kafka-topics.sh --list --bootstrap-server 100.100.10.100:9094

# Describir tópico
/opt/kafka/bin/kafka-topics.sh --describe --topic ventas_json --bootstrap-server 100.100.10.100:9094

# Ver estado de contenedores
docker ps

# Acceder a Spark UI (desde navegador)
http://100.100.10.100:8080
```

10) Prueba de tolerancia simple (qué observar y cómo hacerlo)

- Detener un nodo (por ejemplo el nodo 2):

```bash
docker stop kafka2
```

- Qué mirar: el comando `kafka-metadata-quorum.sh` debe seguir mostrando un cluster operativo (los líderes de las particiones deben reubicarse si es necesario) y los producers/consumers no deben bloquearse permanentemente. Luego reinicia el nodo y observa la re-replicación.

11) Notas y recomendaciones (lectura rápida)

- El proyecto ya trae las piezas necesarias: imágenes, scripts y jobs. El README te guía para levantar el entorno en tres máquinas con IPs fijas. Si por practicidad trabajas en un solo host con contenedores, el flujo es similar pero hay que documentar que es una emulación.
- Siempre verifica que las IPs configuradas en los Dockerfiles correspondan a las IPs reales de cada host antes de construir/ejectuar las imágenes.
- Si necesitas registrar resultados o compartir evidencias, guarda los `/tmp/output/` generados por Spark y las colecciones de la base del consumer.

## Checklist rápido

- [ ] Nodos con IPs 100.100.10.100/101/102 configurados en cada host
- [ ] Imágenes construidas en cada host
- [ ] Contenedores corriendo y quorum formado
- [ ] Tópicos creados (5 tópicos, 3 particiones, RF=3)
- [ ] Producción de 100k mensajes completada
- [ ] Spark job corriendo y archivos en `/tmp/output/`

---

Si quieres, puedo generar este README también en formato `REPORT.md` o añadir un guion de shell con los comandos (sin tocar el código). Dime si lo guardo en el repositorio o prefieres solo este contenido aquí.
