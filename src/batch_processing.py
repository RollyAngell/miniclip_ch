import os
import sys
import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import from_json, col, to_date, from_unixtime, countDistinct
from pyspark.sql.types import StructType, StructField, StringType, LongType

# --- Logging Configuration ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# --- General Configuration ---
class Config:
    KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
    SPARK_MASTER = os.getenv('SPARK_MASTER', 'spark://spark-master:7077')
    TOPIC_NAME = 'events-raw'
    APP_NAME = "MiniclipBatchAggregator"
    KAFKA_PKG = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"

def get_spark_session() -> SparkSession:
    """Initializes and returns Spark Session with Kafka dependencies."""
    try:
        session = SparkSession.builder \
            .appName(Config.APP_NAME) \
            .master(Config.SPARK_MASTER) \
            .config("spark.jars.packages", Config.KAFKA_PKG) \
            .getOrCreate()
        session.sparkContext.setLogLevel("WARN")
        return session
    except Exception as e:
        logger.error(f"Error initializing Spark Session: {e}")
        sys.exit(1)

def get_init_schema() -> StructType:
    """Defines schema for 'init' events."""
    return StructType([
        StructField("event-type", StringType(), True),
        StructField("time", LongType(), True),
        StructField("user-id", StringType(), True),
        StructField("country", StringType(), True),
        StructField("platform", StringType(), True)
    ])

def read_kafka_batch(spark: SparkSession) -> DataFrame:
    """Reads all historical data from Kafka in batch mode."""
    logger.info(f"Reading batch data from topic: {Config.TOPIC_NAME}")
    return spark.read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", Config.KAFKA_BOOTSTRAP) \
        .option("subscribe", Config.TOPIC_NAME) \
        .option("startingOffsets", "earliest") \
        .load()

def process_data(df_raw: DataFrame) -> DataFrame:
    """Parses, filters, and aggregates data to get daily active users by country and platform."""
    schema = get_init_schema()

    # Cast to String and parse JSON
    df_parsed = df_raw.selectExpr("CAST(value AS STRING) as json_value") \
        .withColumn("data", from_json(col("json_value"), schema)) \
        .select("data.*")

    # Filter 'init' events and extract date
    df_daily = df_parsed.filter(col("event-type") == "init") \
        .withColumn("date", to_date(from_unixtime(col("time"))))

    # Aggregation
    return df_daily.groupBy("date", "country", "platform") \
        .agg(countDistinct("user-id").alias("distinct_users")) \
        .orderBy("date", "country", "platform")

def main():
    spark = get_spark_session()
    logger.info("--- Starting Batch Processing ---")

    try:
        df_raw = read_kafka_batch(spark)
        result = process_data(df_raw)

        logger.info("--- Daily Aggregation Results ---")
        result.show(50, truncate=False)
        
        logger.info("--- Processing Completed ---")

    except Exception as e:
        logger.error(f"Critical error in batch processing: {e}", exc_info=True)
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
