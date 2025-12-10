import os
import sys
import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import from_json, col, window, count, sum as spark_sum, approx_count_distinct
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType

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
    SOURCE_TOPIC = 'events-enriched'
    APP_NAME = "MiniclipRealTimeAggregator"
    SHUFFLE_PARTITIONS = "2"
    WINDOW_DURATION = "1 minute"
    TRIGGER_INTERVAL = "1 minute"
    KAFKA_PKG = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"

def get_spark_session() -> SparkSession:
    """Initializes and returns Spark Session with Kafka dependencies."""
    try:
        session = SparkSession.builder \
            .appName(Config.APP_NAME) \
            .master(Config.SPARK_MASTER) \
            .config("spark.jars.packages", Config.KAFKA_PKG) \
            .config("spark.sql.shuffle.partitions", Config.SHUFFLE_PARTITIONS) \
            .getOrCreate()
        session.sparkContext.setLogLevel("WARN")
        return session
    except Exception as e:
        logger.error(f"Error initializing Spark Session: {e}")
        sys.exit(1)

def get_super_schema() -> StructType:
    """Defines unified schema for parsing events."""
    return StructType([
        StructField("event-type", StringType(), True),
        StructField("time", LongType(), True),  # Unix timestamp
        StructField("purchase_value", DoubleType(), True),
        StructField("user-id", StringType(), True),
        StructField("country", StringType(), True),
        StructField("platform", StringType(), True),
        StructField("product-id", StringType(), True)
    ])

def read_kafka_stream(spark: SparkSession) -> DataFrame:
    """Reads stream from Kafka."""
    return spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", Config.KAFKA_BOOTSTRAP) \
        .option("subscribe", Config.SOURCE_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()

def parse_and_watermark(df_raw: DataFrame) -> DataFrame:
    """Parses JSON and applies watermark based on event time."""
    schema = get_super_schema()
    
    # Cast to String and parse JSON
    df_parsed = df_raw.selectExpr("CAST(value AS STRING) as json_value") \
        .withColumn("data", from_json(col("json_value"), schema)) \
        .select("data.*")

    # Create event_time timestamp column and apply watermark
    return df_parsed.withColumn("event_time", (col("time").cast("timestamp"))) \
        .withWatermark("event_time", Config.WINDOW_DURATION)

def start_console_query(df: DataFrame, query_name: str):
    """Starts a streaming query with console output."""
    logger.info(f"Starting query: {query_name}")
    return df.writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", "false") \
        .trigger(processingTime=Config.TRIGGER_INTERVAL) \
        .queryName(query_name) \
        .start()

def main():
    spark = get_spark_session()
    logger.info(f"--- Starting Real-Time Aggregator reading from {Config.SOURCE_TOPIC} ---")

    try:
        # 1. Initial Reading and Processing
        df_raw = read_kafka_stream(spark)
        df_processed = parse_and_watermark(df_raw)

        # 2. Definition of filtered DataFrames
        df_purchases = df_processed.filter(col("event-type") == "in-app-purchase")
        df_matches = df_processed.filter(col("event-type") == "match")

        # 3. Aggregation Logic
        
        # KPI A: Financial Totals (Purchases & Revenue) per minute
        agg_financials = df_purchases.groupBy(window(col("event_time"), Config.WINDOW_DURATION)) \
            .agg(
                count("*").alias("total_purchases"),
                spark_sum("purchase_value").alias("total_revenue")
            )

        # KPI B: Distinct Users per minute (considering any event)
        agg_users = df_processed.groupBy(window(col("event_time"), Config.WINDOW_DURATION)) \
            .agg(approx_count_distinct("user-id").alias("distinct_users"))

        # KPI C: Revenue by Country per minute
        agg_country_revenue = df_purchases.groupBy(window(col("event_time"), Config.WINDOW_DURATION), "country") \
            .agg(spark_sum("purchase_value").alias("country_revenue"))

        # KPI D: Matches by Country per minute
        agg_matches_country = df_matches.groupBy(window(col("event_time"), Config.WINDOW_DURATION), "country") \
            .agg(count("*").alias("total_matches"))

        # 4. Start Streams
        queries = [
            start_console_query(agg_financials, "Financials"),
            start_console_query(agg_users, "Distinct Users"),
            start_console_query(agg_country_revenue, "Revenue by Country"),
            start_console_query(agg_matches_country, "Matches by Country")
        ]

        spark.streams.awaitAnyTermination()

    except KeyboardInterrupt:
        logger.info("Stopping streams by user request...")
    except Exception as e:
        logger.error(f"Critical error in stream execution: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()
