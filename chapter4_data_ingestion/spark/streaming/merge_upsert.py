"""
Spark Structured Streaming with Apache Iceberg - Merge/Upsert Operations
This example demonstrates merge operations for handling updates and inserts.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("IcebergMergeUpsert") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.iceberg_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.iceberg_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
    .config("spark.sql.catalog.iceberg_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .config("spark.sql.catalog.iceberg_catalog.warehouse", "s3://your-bucket/warehouse/") \
    .getOrCreate()

# Create silver table with Copy-on-Write mode
spark.sql("""
CREATE TABLE IF NOT EXISTS iceberg_catalog.streaming_db.customer_silver (
    customer_id STRING,
    name STRING,
    email STRING,
    phone STRING,
    address STRING,
    last_updated TIMESTAMP
) USING iceberg
TBLPROPERTIES (
    'format-version'='2',
    'write.spark.fanout.enabled'='true',
    'write.delete.mode'='copy-on-write',
    'write.update.mode'='copy-on-write',
    'write.merge.mode'='copy-on-write'
)
PARTITIONED BY (days(last_updated))
""")

# Define merge function for upsert operations
def merge_into_iceberg_table(df, batch_id):
    """
    Merge function to handle upserts in Iceberg table
    """
    spark_session = df.sparkSession
    # Check if the DataFrame is empty
    if df.isEmpty():
        print(f"Batch {batch_id} is empty, skipping merge operation")
        return
    # Deduplicate within batch - keep latest record per customer
    df_deduped = df.withColumn("row_num",
        row_number().over(
            Window.partitionBy("customer_id")
                  .orderBy(col("last_updated").desc())
        )
    ).filter(col("row_num") == 1).drop("row_num")

    # Create temporary view for merge operation
    df_deduped.createOrReplaceTempView("customer_updates")

    # Execute merge statement
    merge_sql = """
    MERGE INTO iceberg_catalog.streaming_db.customer_silver AS target
    USING customer_updates AS source
    ON target.customer_id = source.customer_id
    WHEN MATCHED THEN UPDATE SET
        target.name = source.name,
        target.email = source.email,
        target.phone = source.phone,
        target.address = source.address,
        target.last_updated = source.last_updated
    WHEN NOT MATCHED THEN INSERT (
        customer_id, name, email, phone, address, last_updated
    ) VALUES (
        source.customer_id, source.name, source.email, 
        source.phone, source.address, source.last_updated
    )
    """

    spark_session.sql(merge_sql)

# Read streaming data from bronze layer
bronze_stream = spark \
    .readStream \
    .format("iceberg") \
    .option("stream-from-timestamp", str(int(time.time()))) \
    .option("streaming-skip-overwrite-snapshots", "true") \
    .load("iceberg_catalog.streaming_db.customer_bronze")

# Add processing timestamp
processed_stream = bronze_stream.withColumn("last_updated", current_timestamp())

# Start streaming with merge operations
query = processed_stream \
    .writeStream \
    .queryName("customer_upsert") \
    .format("iceberg") \
    .outputMode("append") \
    .trigger(processingTime='30 seconds') \
    .foreachBatch(merge_into_iceberg_table) \
    .option("checkpointLocation", "s3://your-bucket/checkpoints/merge-upsert/") \
    .start()

print("Merge/Upsert streaming started. Processing updates...")
query.awaitTermination(timeout=120)

# Verify no duplicates exist
duplicate_count = spark.sql("""
    SELECT COUNT(*) as duplicates 
    FROM (
        SELECT customer_id, COUNT(*) as cnt
        FROM iceberg_catalog.streaming_db.customer_silver
        GROUP BY customer_id
        HAVING COUNT(*) > 1
    )
""").collect()[0][0]

print(f"Number of duplicate customers: {duplicate_count}")
print("Merge/Upsert operations completed successfully!")