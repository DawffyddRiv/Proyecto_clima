from pyspark.sql import SparkSession

# Iniciamos la sesión de Spark
spark = SparkSession.builder \
    .appName("PruebaAirflowSpark") \
    .getOrCreate()

print("*" * 50)
print(f"¡LOGRADO! Spark está corriendo. Versión: {spark.version}")
print("*" * 50)

spark.stop()
