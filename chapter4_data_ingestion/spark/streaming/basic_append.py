"""
Basic Spark Structured Streaming with Apache Iceberg - Append Mode
This example demonstrates simple append operations to an Iceberg table using native functionality. 
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time

# Initialize Spark Session with Iceberg configurations
spark = SparkSession.builder \
    .appName("IcebergBasicAppend") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.iceberg_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.iceberg_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
    .config("spark.sql.catalog.iceberg_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .config("spark.sql.catalog.iceberg_catalog.warehouse", "s3://your-bucket/warehouse/") \
    .getOrCreate()

# Create database
spark.sql("CREATE DATABASE IF NOT EXISTS iceberg_catalog.streaming_db")

# Create Iceberg table for basic streaming, note that we are using the fanout writer
spark.sql("""
CREATE TABLE IF NOT EXISTS iceberg_catalog.streaming_db.sales_events (
    order_id STRING,
    product_id STRING,
    quantity INT,
    price DECIMAL(10,2),
    customer_id STRING,
    event_time TIMESTAMP
) USING iceberg
TBLPROPERTIES (
    'format-version'='2',
    'write.spark.fanout.enabled'='true'
)
PARTITIONED BY (days(event_time))
""")

# Define schema for incoming data
schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("price", DecimalType(10,2), True),
    StructField("customer_id", StringType(), True)
])

# Read from Kafka (or any streaming source)
streaming_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sales-events") \
    .option("startingOffsets", "latest") \
    .load()

# Parse JSON data and add timestamp
parsed_df = streaming_df \
    .selectExpr("CAST(value AS STRING) as json_data") \
    .select(from_json("json_data", schema).alias("data")) \
    .select("data.*") \
    .withColumn("event_time", current_timestamp())

# Write to Iceberg table in append mode
query = parsed_df \
    .writeStream \
    .format("iceberg") \
    .outputMode("append") \
    .trigger(processingTime='10 seconds') \ 
    .option("checkpointLocation", "s3://your-bucket/checkpoints/basic-append/") \
    .toTable("iceberg_catalog.streaming_db.sales_events")

# Wait for streaming to process some data
query.awaitTermination(timeout=60)

print("Basic append streaming completed successfully!")
