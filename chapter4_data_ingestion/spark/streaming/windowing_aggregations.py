"""
Advanced Spark Structured Streaming with Apache Iceberg - Windowing and Aggregations
This example demonstrates complex windowing operations and stateful computations.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import time

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("IcebergWindowingAggregations") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.iceberg_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.iceberg_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
    .config("spark.sql.catalog.iceberg_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .config("spark.sql.catalog.iceberg_catalog.warehouse", "s3://your-bucket/warehouse/") \
    .getOrCreate()

# Schema for Kafka value payload
record_schema = StructType([
    StructField("id", StringType(), True),
    StructField("val", IntegerType(), True),
    StructField("event_time", TimestampType(), True)
])

# Kafka source
options = {
    "kafka.bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "subscribe": TOPIC_NAME,
    "startingOffsets": "latest",
    "kafka.sasl.mechanism": "AWS_MSK_IAM",
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.jaas.config": "software.amazon.msk.auth.iam.IAMLoginModule required;",
    "kafka.sasl.client.callback.handler.class": "software.amazon.msk.auth.iam.IAMClientCallbackHandler"
}

raw_stream = spark.readStream.format("kafka").options(**options).load()

# Parse the Kafka value JSON into DataFrame
events = raw_stream.selectExpr("CAST(value AS STRING) jsonData") \
    .select(from_json(col("jsonData"), record_schema).alias("events")) \
    .select("events.*")

# Add watermark of 5 minutes
events_with_watermark = events.withWatermark("event_time", "5 minutes")

# Stateful aggregation: count events per 1-minute window
events_per_minute = events_with_watermark.groupBy(
    window(col("event_time"), "1 minute")
).agg(
    count("*").alias("events_per_minute")
).select(
    # Truncate window.start to minute level to get timestamps like 2025-09-10 08:01:00
    date_trunc("minute", col("window.start")).alias("second_key"),
    col("events_per_minute")
)

ICEBERG_TABLE_NAME = "events_metrics.events_per_minute"

# Function to merge batch data into Iceberg table
def merge_batch_to_iceberg(df, epoch_id):
    print(f"Processing batch {epoch_id}")
    
    # Check if the DataFrame is empty
    if df.isEmpty():
        print(f"Batch {epoch_id} is empty, skipping merge operation")
        return

    # Calculate current timestamp once
    current_time = datetime.now()
    
    # Add last_update_time column with the same timestamp for all rows
    df_with_update_time = df.withColumn("update_time", lit(current_time))

    print("Create global temp view")
    # Create a temporary view of the batch data
    df_with_update_time.createOrReplaceGlobalTempView("updates")

    try:        
        # Execute the MERGE operation
        spark.sql(f"""
            MERGE INTO {ICEBERG_TABLE_NAME} t
            USING global_temp.updates s
            ON t.second_key = s.second_key
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
        
        print(f"Batch {epoch_id} successfully merged into {ICEBERG_TABLE_NAME}")
    except Exception as e:
        print(f"Error during merge operation: {str(e)}")
        raise
    finally:
        print("Dropping global temporary view 'updates'")
        # Clean up the temporary view
        spark.catalog.dropGlobalTempView("updates")
        print("Dropped global temporary view 'updates'")
        #df.unpersist()
        #print("Unpersisted'")


# Write stream to Iceberg table with merge
query = events_per_minute.writeStream \
    .outputMode("update") \
    .foreachBatch(merge_batch_to_iceberg) \
    .option("checkpointLocation", CHECKPOINTING_PATH) \
    .trigger(processingTime="30 seconds") \
    .start() \
    .awaitTermination()
