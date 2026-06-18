from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("KafkaToFiles") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# =========================
# LEER KAFKA
# =========================
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "100.100.10.103:9094") \
    .option("subscribe", "ventas_json,logs,metricas") \
    .option("startingOffsets", "latest") \
    .load()

df = df.selectExpr(
    "topic",
    "CAST(value AS STRING) as value"
)

# =========================
# VENTAS JSON
# =========================
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

# =========================
# LOGS
# =========================
logs = df.filter(col("topic") == "logs")

# =========================
# MÉTRICAS
# =========================
metricas = df.filter(col("topic") == "metricas")

# =========================
# GUARDAR A JSON
# =========================

query1 = ventas_parsed.writeStream \
    .format("json") \
    .outputMode("append") \
    .option("path", "/tmp/output/ventas") \
    .option("checkpointLocation", "/tmp/checkpoints/ventas") \
    .start()

query2 = logs.writeStream \
    .format("json") \
    .outputMode("append") \
    .option("path", "/tmp/output/logs") \
    .option("checkpointLocation", "/tmp/checkpoints/logs") \
    .start()

query3 = metricas.writeStream \
    .format("json") \
    .outputMode("append") \
    .option("path", "/tmp/output/metricas") \
    .option("checkpointLocation", "/tmp/checkpoints/metricas") \
    .start()

spark.streams.awaitAnyTermination()