from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_date, from_unixtime, countDistinct
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType
import os

# Configuración
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
TOPIC_NAME = 'events-raw'

def get_spark_session():
    return SparkSession.builder \
        .appName("MiniclipBatchAggregator") \
        .master("spark://spark-master:7077") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .getOrCreate()

def main():
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print("--- Iniciando Batch Processing ---")

    # 1. Leer datos desde Kafka (Modo Batch: read, no readStream)
    # Cargamos 'earliest' para procesar todo lo histórico disponible en el topic
    df_raw = spark.read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", TOPIC_NAME) \
        .option("startingOffsets", "earliest") \
        .load()

    # Conversión de key/value a String
    df_str = df_raw.selectExpr("CAST(value AS STRING) as json_value")

    # 2. Definir Esquema para 'Init' events (que tienen la info necesaria: country, platform)
    # Solo nos interesan los eventos que nos permiten segmentar usuarios
    schema = StructType([
        StructField("event-type", StringType(), True),
        StructField("time", LongType(), True),
        StructField("user-id", StringType(), True),
        StructField("country", StringType(), True),
        StructField("platform", StringType(), True)
    ])

    # 3. Parsear JSON y extraer columnas
    df_parsed = df_str.withColumn("data", from_json(col("json_value"), schema)) \
        .select("data.*")

    # 4. Filtrar solo eventos 'init'
    # Asumimos que para contar usuarios activos por país/plataforma, el evento 'init' es la fuente de verdad
    df_init = df_parsed.filter(col("event-type") == "init")

    # 5. Transformar Timestamp a Fecha (Daily)
    df_with_date = df_init.withColumn("date", to_date(from_unixtime(col("time"))))

    # 6. Agregación: Usuarios distintos por Día, País y Plataforma
    result = df_with_date.groupBy("date", "country", "platform") \
        .agg(countDistinct("user-id").alias("distinct_users")) \
        .orderBy("date", "country", "platform")

    print("--- Resultados de Agregación Diaria ---")
    result.show(50, truncate=False)

    print("--- Procesamiento Completado ---")
    spark.stop()

if __name__ == "__main__":
    main()

