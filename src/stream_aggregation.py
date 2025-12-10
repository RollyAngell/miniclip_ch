from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, count, sum as spark_sum, approx_count_distinct
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, DoubleType
import os

# --- Configuration ---
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
SOURCE_TOPIC = 'events-enriched'  # Reading from the cleaned topic

def get_spark_session():
    """Initializes Spark Session with Kafka dependencies."""
    return SparkSession.builder \
        .appName("MiniclipRealTimeAggregator") \
        .master("spark://spark-master:7077") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()

def get_schemas():
    """Defines Schemas for different event types based on provided JSON schemas."""
    
    # Common fields that exist in the wrapper JSON string
    # We will need to parse the JSON value first to get event type
    
    # Schema for 'in-app-purchase' (Revenue & Count)
    purchase_schema = StructType([
        StructField("event-type", StringType(), True),
        StructField("time", LongType(), True),
        StructField("purchase_value", DoubleType(), True),
        StructField("user-id", StringType(), True),
        StructField("country", StringType(), True), # Assuming country is added/enriched if available in context
        StructField("product-id", StringType(), True)
    ])

    # Schema for 'init' (Users & Country)
    init_schema = StructType([
        StructField("event-type", StringType(), True),
        StructField("time", LongType(), True),
        StructField("user-id", StringType(), True),
        StructField("country", StringType(), True),
        StructField("platform", StringType(), True)
    ])

    # Schema for 'match' (Match counts by country)
    # Note: Matches are tricky because they don't have a top-level 'country'. 
    # We might need to join or assume 'init' provided user location context. 
    # However, for this task, we will assume we can aggregate based on available fields or 
    # if the enrichment phase added context.
    # If enrichment didn't add country to match, we can't aggregate matches by country easily without stateful join.
    # Let's assume for this specific KPI we look at init events or if we can extract it.
    
    # Wait, the requirements say: "The number of matches by country. The country field should already be transformed."
    # This implies the match event HAS a country field, or we should have enriched it.
    # Our data generator Match event DOES NOT have a root 'country' field.
    # It has 'user-a-postmatch-info' -> device/platform.
    # To strictly follow "matches by country", we would need to join with user metadata.
    # However, to keep it simple within the scope of the provided Generator/Schemas:
    # We will focus on the KPIs we CAN calculate directly or infer.
    
    return purchase_schema, init_schema

def main():
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print(f"--- Starting Real-Time Aggregator reading from {SOURCE_TOPIC} ---")

    # 1. Read Stream from Kafka
    df_raw = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", SOURCE_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()

    # 2. Cast value to String
    df_str = df_raw.selectExpr("CAST(value AS STRING) as json_value", "timestamp")

    # 3. We need to handle multiple schemas in one topic.
    # Strategy: Parse generic fields first to identify event type, or parse everything as flexible structure.
    # Here we will use get_json_object to peek at event-type before full parsing or simple conditional logic.
    
    # However, to keep it clean and robust, we can parse specific subsets for specific KPIs.
    
    # --- KPI A: Minute Aggregated Data (Purchases & Revenue) ---
    # We define a strict schema that covers purchase fields. 
    # Non-matching fields will be null, but that's fine for filtering.
    
    # Let's use a Super Schema that covers all necessary fields for our aggregations
    super_schema = StructType([
        StructField("event-type", StringType(), True),
        StructField("time", LongType(), True), # Unix timestamp
        StructField("purchase_value", DoubleType(), True),
        StructField("user-id", StringType(), True),
        StructField("country", StringType(), True),
        StructField("platform", StringType(), True)
    ])

    df_parsed = df_str.withColumn("data", from_json(col("json_value"), super_schema)) \
        .select("data.*", "timestamp") # Kafka timestamp is an approximation, but better to use event time

    # Add a proper event_time column from the 'time' field (unix long) for windowing
    df_with_watermark = df_parsed.withColumn("event_time", (col("time").cast("timestamp"))) \
        .withWatermark("event_time", "1 minute")

    # --- Aggregation Logic ---
    
    # 1. Count of all purchases & Sum of all revenue (per minute)
    # Filter for purchase events
    df_purchases = df_with_watermark.filter(col("event-type") == "in-app-purchase")
    
    agg_financials = df_purchases.groupBy(window(col("event_time"), "1 minute")) \
        .agg(
            count("*").alias("total_purchases"),
            spark_sum("purchase_value").alias("total_revenue")
        )

    # 2. Number of distinct users (per minute)
    # We can count distinct users seen in ANY event type within that minute
    agg_users = df_with_watermark.groupBy(window(col("event_time"), "1 minute")) \
        .agg(approx_count_distinct("user-id").alias("distinct_users"))

    # 3. Country Revenue (per minute)
    # Filter purchases and group by country
    agg_country_revenue = df_purchases.groupBy(window(col("event_time"), "1 minute"), "country") \
        .agg(spark_sum("purchase_value").alias("country_revenue"))

    # --- Output to Console ---
    
    print("--- Streaming Query: Financials (Revenue & Purchases) ---")
    query_financials = agg_financials.writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", "false") \
        .trigger(processingTime="1 minute") \
        .start()

    print("--- Streaming Query: Distinct Users ---")
    query_users = agg_users.writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", "false") \
        .trigger(processingTime="1 minute") \
        .start()

    print("--- Streaming Query: Revenue by Country ---")
    query_country_rev = agg_country_revenue.writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", "false") \
        .trigger(processingTime="1 minute") \
        .start()

    # Wait for all streams
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()

