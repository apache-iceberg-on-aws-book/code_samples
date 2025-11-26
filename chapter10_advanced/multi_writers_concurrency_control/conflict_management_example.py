import time 
import random 
from pyspark.sql import SparkSession 
from pyspark.sql.functions import col, lit, day 
from py4j.protocol import Py4JJavaError 
 
ICEBERG_DATABASE = 'iceberg_database' 
ICEBERG_TABLE = 'conflict_demo' 
S3_WAREHOUSE = 's3://amzn-s3-demo-bucket/iceberg-warehouse/' 
CHECKPOINT_LOCATION = 's3://amzn-s3-demo-bucket/checkpoints/conflict-demo/' 
AWS_REGION = 'us-east-1' 
CATALOG = 'glue_catalog' 
 
spark = SparkSession.builder \
    .config("spark.sql.extensions","org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog") \
    .config(f"spark.sql.catalog.{CATALOG}.type", "glue") \
    .config(f"spark.sql.catalog.{CATALOG}.warehouse", S3_WAREHOUSE) \
    .config(f"spark.sql.catalog.{CATALOG}.cache-enabled", "false") \
    .config("spark.sql.defaultCatalog", CATALOG) \
    .getOrCreate() 
 
# Create Glue database if it doesn't exist 
try: 
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {ICEBERG_DATABASE}") 
    print(f"Database {ICEBERG_DATABASE} ready") 
except Exception as e: 
    print(f"Note: Database creation: {str(e)}") 
 
# Create Iceberg table if it doesn't exist 
create_table_sql = f""" 
CREATE TABLE IF NOT EXISTS {ICEBERG_DATABASE}.{ICEBERG_TABLE} ( 
    event_id STRING, 
    user_id BIGINT, 
    event_type STRING, 
    timestamp TIMESTAMP 
) 
USING ICEBERG 
PARTITIONED BY (day(timestamp)) 
TBLPROPERTIES ( 
    'write.merge.mode' = 'merge-on-read', 
    'write.update.mode' = 'merge-on-read', 
    'write.delete.mode' = 'merge-on-read', 
    'commit.retry.num-retries' = '6', 
    'commit.retry.min-wait-ms' = '1500', 
    'commit.retry.max-wait-ms' = '45000' 
) 
""" 
 
try: 
    spark.sql(create_table_sql) 
    print(f"Successfully created/verified Iceberg table: {ICEBERG_DATABASE}.{ICEBERG_TABLE}") 
except Exception as e: 
    print(f"Error creating table: {str(e)}") 
    raise 
 
class IcebergConflictRetryHandler: 
    """Conflict-aware retry handler for Spark Structured Streaming with rate source""" 
     
    def __init__(self, max_retries=4, base_delay_ms=1500, max_delay_ms=45000): 
        self.max_retries = max_retries 
        self.base_delay_ms = base_delay_ms 
        self.max_delay_ms = max_delay_ms 
 
    def calculate_adaptive_delay(self, attempt, batch_id): 
        """Calculate delay optimized for Spark Structured Streaming with rate source""" 
        # Exponential backoff 
        base_delay = min(self.base_delay_ms * (1.8 ** (attempt - 1)), self.max_delay_ms) 
         
        # Jitter for distribution 
        jitter_range = base_delay * 0.15 
        jitter = random.uniform(-jitter_range, jitter_range) 
         
        # Batch-based offset 
        batch_offset = (batch_id % 1000) 
         
        total_delay = max(500, base_delay + jitter + batch_offset) 
        return total_delay / 1000.0 
     
    def is_iceberg_validation_conflict(self, exception): 
        """Detect Iceberg ValidationException for data update conflicts""" 
        current_ex = exception 
        max_depth = 5 
        depth = 0 
         
        while current_ex is not None and depth < max_depth: 
            exception_class = str(current_ex.getClass().getName()) 
            exception_msg = str(current_ex.getMessage()).lower() 
             
            if "ValidationException" in exception_class: 
                return True 
                 
            conflict_indicators = [ 
                "Found conflicting files that can contain records matching", 
                "Found new conflicting delete files that can apply to records matching", 
                "Found conflicting deleted files that can contain records matching", 
                "Found conflicting deleted files that can apply to records matching" 
            ] 
            if any(indicator in exception_msg for indicator in conflict_indicators): 
                return True 
                 
            current_ex = current_ex.getCause() 
            depth += 1 
             
        return False 
 
 
def process_rate_source_batch(data_frame, batch_id): 
    """ 
    Process rate source batch with Iceberg conflict handling 
    Rate source generates: timestamp, value columns 
    """ 
    if data_frame.count() == 0: 
        print(f"Batch {batch_id}: No data to process") 
        return 
     
    retry_handler = IcebergConflictRetryHandler() 
     
    try: 
        # Transform rate source data to create conflicts 
        processed_df = data_frame.select( 
            col("value").cast("string").alias("event_id"), 
            (col("value") % 10000).alias("user_id"), 
            lit("page_view").alias("event_type"), 
            col("timestamp") 
        ) 
 
        # Create temporary view for merge operation 
        temp_view_name = f"rate_batch_{batch_id}" 
        processed_df.createOrReplaceGlobalTempView(temp_view_name) 
         
        # Retry loop with exponential backoff 
        attempt = 0 
        while attempt < retry_handler.max_retries: 
            attempt += 1 
            start_time = time.time() 
             
            try: 
                # Run merge operation that will create conflicts 
                merge_sql = f""" 
                MERGE INTO {ICEBERG_DATABASE}.{ICEBERG_TABLE} AS t 
                USING global_temp.{temp_view_name} AS i 
                ON t.event_id = i.event_id 
                WHEN MATCHED THEN 
                  UPDATE SET 
                    t.user_id = i.user_id, 
                    t.event_type = i.event_type, 
                    t.timestamp = i.timestamp 
                WHEN NOT MATCHED THEN 
                  INSERT (event_id, user_id, event_type, timestamp) 
                    VALUES (i.event_id, i.user_id, i.event_type, i.timestamp) 
                """ 
                 
                spark.sql(merge_sql) 
                 
                # Success 
                execution_time = time.time() - start_time 
                print(f"[SUCCESS] Batch {batch_id} completed on attempt {attempt} " 
                      f"in {execution_time:.2f}s") 
                return 
                 
            except Py4JJavaError as e: 
                if retry_handler.is_iceberg_validation_conflict(e.java_exception): 
                    if attempt < retry_handler.max_retries: 
                        delay_seconds = retry_handler.calculate_adaptive_delay( 
                            attempt, batch_id 
                        ) 
                        print(f"[CONFLICT] Batch {batch_id} attempt {attempt} failed. " 
                              f"Retrying in {delay_seconds:.2f}s") 
                        time.sleep(delay_seconds) 
                    else: 
                        print(f"[FAILED] Batch {batch_id} exhausted all {retry_handler.max_retries} attempts") 
                        raise 
                else: 
                    print(f"[ERROR] Batch {batch_id} failed with non-conflict exception: {str(e)}") 
                    raise 
                     
    except Exception as e: 
        print(f"[ERROR] Batch {batch_id} processing failed: {str(e)}") 
        raise 
    finally: 
        # Clean up temporary view 
        try: 
            spark.catalog.dropGlobalTempView(temp_view_name) 
        except: 
            pass 
 
# Create rate source streaming DataFrame 
# Rate source generates rows with timestamp and value columns 
rate_df = spark.readStream \
    .format("rate") \
    .option("rowsPerSecond", 20) \
    .load()
 
print("Starting Spark Structured Streaming app with rate source") 
print(f"Target table: {ICEBERG_DATABASE}.{ICEBERG_TABLE}") 
print(f"Checkpoint location: {CHECKPOINT_LOCATION}") 
print("Rate source will generate 20 rows/second") 
 
try: 
    # Start streaming query with conflict handling 
    query = rate_df.writeStream \
        .trigger(processingTime='30 seconds') \
        .option("checkpointLocation", CHECKPOINT_LOCATION) \
        .foreachBatch(process_rate_source_batch) \
        .start() 
     
    print("Spark Structured Streaming app started to demonstrate conflict handling") 
    print("Check CloudWatch logs for [CONFLICT], [SUCCESS], and [FAILED] messages") 
     
    # Keep the app running 
    query.awaitTermination() 
     
except Exception as e: 
    print(f"Error in streaming app: {str(e)}") 
    raise 