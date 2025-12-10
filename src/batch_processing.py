from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_date, from_unixtime, countDistinct
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType
import os

# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
SPARK_MASTER = os.getenv('SPARK_MASTER', 'spark://spark-master:7077')
TOPIC_NAME = 'events-raw'

def get_spark_session():
    return SparkSession.builder \
        .appName("MiniclipBatchAggregator") \
        .master(SPARK_MASTER) \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .getOrCreate()

def main():
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print("--- Starting Batch Processing ---")

    # 1. Read data from Kafka (Batch Mode: read, not readStream)
    # Load 'earliest' to process all historical data available in the topic
    df_raw = spark.read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", TOPIC_NAME) \
        .option("startingOffsets", "earliest") \
        .load()

    # Convert key/value to String
    df_str = df_raw.selectExpr("CAST(value AS STRING) as json_value")

    # 2. Define Schema for 'Init' events (containing necessary info: country, platform)
    # We are only interested in events that allow user segmentation
    schema = StructType([
        StructField("event-type", StringType(), True),
        StructField("time", LongType(), True),
        StructField("user-id", StringType(), True),
        StructField("country", StringType(), True),
        StructField("platform", StringType(), True)
    ])

    # 3. Parse JSON and extract columns
    df_parsed = df_str.withColumn("data", from_json(col("json_value"), schema)) \
        .select("data.*")

    # 4. Filter only 'init' events
    # We assume that for counting active users by country/platform, the 'init' event is the source of truth
    df_init = df_parsed.filter(col("event-type") == "init")

    # 5. Transform Timestamp to Date (Daily)
    df_with_date = df_init.withColumn("date", to_date(from_unixtime(col("time"))))

    # 6. Aggregation: Distinct users by Day, Country, and Platform
    result = df_with_date.groupBy("date", "country", "platform") \
        .agg(countDistinct("user-id").alias("distinct_users")) \
        .orderBy("date", "country", "platform")

    print("--- Daily Aggregation Results ---")
    result.show(50, truncate=False)

    print("--- Processing Completed ---")
    spark.stop()

if __name__ == "__main__":
    main()
