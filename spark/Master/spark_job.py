from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

import time
from datetime import datetime

# SPARK SESSION

spark = SparkSession.builder \
    .appName("KafkaMetrics") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# LEER KAFKA

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "100.100.10.100:9094") \
    .option("subscribe", "ventas_json,logs,metricas") \
    .option("startingOffsets", "earliest") \
    .load()

df = df.selectExpr(
    "topic",
    "CAST(value AS STRING) as value"
)

# VENTAS JSON
ventas_schema = StructType([
    StructField("id_transaccion", StringType()),
    StructField("monto", DoubleType()),
    StructField("pais", StringType()),
    StructField("metodo_pago", StringType())
])

ventas = df.filter(col("topic") == "ventas_json")

ventas_parsed = ventas.select(
    from_json(col("value"), ventas_schema).alias("data")
).select("data.*")


# METRICAS DEL SISTEMA

def guardar_metricas_sistema(batch_df, batch_id):

    inicio = time.time()

    total = batch_df.count()

    fin = time.time()

    tiempo = fin - inicio

    velocidad = 0

    if tiempo > 0:
        velocidad = total / tiempo

    datos = [
        (
            int(batch_id),
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            int(total),
            float(tiempo),
            float(velocidad)
        )
    ]

    schema = StructType([
        StructField("batch_id", IntegerType()),
        StructField("fecha", StringType()),
        StructField("registros_procesados", IntegerType()),
        StructField("tiempo_segundos", DoubleType()),
        StructField("registros_por_segundo", DoubleType())
    ])

    metrica_df = spark.createDataFrame(datos, schema)

    metrica_df.write \
        .mode("append") \
        .json("/tmp/output/metricas_sistema")

    print("\n" + "=" * 60)
    print(f"BATCH: {batch_id}")
    print(f"REGISTROS: {total}")
    print(f"TIEMPO: {tiempo:.4f} segundos")
    print(f"VELOCIDAD: {velocidad:.2f} registros/seg")
    print("=" * 60 + "\n")

# GUARDAR VENTAS

query_ventas = ventas_parsed.writeStream \
    .format("json") \
    .outputMode("append") \
    .option("path", "/tmp/output/ventas") \
    .option("checkpointLocation", "/tmp/checkpoints/ventas") \
    .start()

# GUARDAR METRICAS DEL JOB

query_metricas = ventas_parsed.writeStream \
    .foreachBatch(guardar_metricas_sistema) \
    .outputMode("append") \
    .option(
        "checkpointLocation",
        "/tmp/checkpoints/metricas_sistema"
    ) \
    .start()

# ESPERAR

spark.streams.awaitAnyTermination()