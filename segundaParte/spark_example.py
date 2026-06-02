from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ProyectoFinalSpark").getOrCreate()

# Generar 100,000 filas con 10 columnas
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
    "concat('texto_', id) as comentario"
)

# Guardar datos en JSON y CSV
registros.write.mode("overwrite").json("data/output_json")
registros.write.mode("overwrite").option("header", True).csv("data/output_csv")

# Usar SQL para análisis
registros.createOrReplaceTempView("registros")

spark.sql(
    """
    SELECT categoria, COUNT(*) AS total, AVG(valor) AS promedio, SUM(descuento) AS suma_descuento
    FROM registros
    GROUP BY categoria
    ORDER BY categoria
    """
).show(truncate=False)

spark.sql(
    "SELECT ciudad, COUNT(*) AS total FROM registros GROUP BY ciudad ORDER BY total DESC LIMIT 10"
).show(truncate=False)

spark.stop()
