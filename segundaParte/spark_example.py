import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("ProyectoFinalSpark").getOrCreate()

script_dir = os.path.dirname(os.path.realpath(__file__))
consumer_output_dir = os.path.normpath(os.path.join(script_dir, '..', 'primeraParte', 'consumer_output'))

if os.path.isdir(consumer_output_dir):
    json_files = [f for f in os.listdir(consumer_output_dir) if f.endswith('.jsonl') or f.endswith('.json')]
    if json_files:
        print(f"Leyendo datos del consumidor Kafka desde: {consumer_output_dir}")
        registros = spark.read.option("mode", "PERMISSIVE").json(os.path.join(consumer_output_dir, '*.jsonl'))
        registros.createOrReplaceTempView("registros")

        print("Resumen de mensajes por máquina:")
        spark.sql(
            "SELECT maquina, COUNT(*) AS total_mensajes, COUNT(DISTINCT topic) AS topicos FROM registros GROUP BY maquina ORDER BY total_mensajes DESC"
        ).show(truncate=False)

        print("Conteo total por máquina y partición:")
        spark.sql(
            "SELECT maquina, partition, COUNT(*) AS total FROM registros GROUP BY maquina, partition ORDER BY total DESC"
        ).show(truncate=False)

        registros.write.mode("overwrite").json("data/output_json")
        registros.write.mode("overwrite").option("header", True).csv("data/output_csv")
    else:
        print("No se encontraron archivos de salida del consumidor. Generando datos sintéticos en su lugar.")
        registros = None
else:
    print("No existe el directorio del consumidor. Generando datos sintéticos en su lugar.")
    registros = None

if registros is None:
    registros = spark.range(100000).selectExpr(
        "id as id_registro",
        "concat('user_', id % 1000) as usuario",
        "date_add('2026-01-01', cast(id % 365 as int)) as fecha",
        "cast((id % 50) * 1.5 as double) as valor",
        "cast((id % 5) as int) as categoria",
        "case when id % 2 = 0 then 'activo' else 'inactivo' end as estado",
        "concat('ciudad_', id % 20) as ciudad",
        "concat('producto_', id % 30) as producto",
        "cast(id * 0.01 as double) as descuento",
        "concat('texto_', id) as comentario",
    )
    registros.createOrReplaceTempView("registros")

    spark.sql(
        "SELECT categoria, COUNT(*) AS total, AVG(valor) AS promedio, SUM(descuento) AS suma_descuento FROM registros GROUP BY categoria ORDER BY categoria"
    ).show(truncate=False)

    spark.sql(
        "SELECT ciudad, COUNT(*) AS total FROM registros GROUP BY ciudad ORDER BY total DESC LIMIT 10"
    ).show(truncate=False)

    registros.write.mode("overwrite").json("data/output_json")
    registros.write.mode("overwrite").option("header", True).csv("data/output_csv")

spark.stop()
