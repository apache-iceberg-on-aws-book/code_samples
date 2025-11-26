# Apache Spark Data Ingestion Examples

Batch and streaming data ingestion patterns using Apache Spark with Iceberg on AWS.

## Streaming Examples

### [basic_append.py](streaming/basic_append.py)
Basic Kafka-to-Iceberg streaming with append-only writes. Demonstrates fanout writer configuration and daily partitioning for sales events.

### [merge_upsert.py](streaming/merge_upsert.py)
Streaming upsert operations using MERGE INTO. Handles deduplication within batches and maintains customer records without duplicates using Copy-on-Write mode.

### [windowing_aggregations.py](streaming/windowing_aggregations.py)
Stateful streaming with windowing and aggregations. Processes events per minute with watermarking and merges metrics into Iceberg tables.


## Batch Examples

### [incremental_batch_dynamodb.py](batch/incremental_batch_dynamodb.py)
Incremental batch processing using DynamoDB for watermark tracking. Demonstrates snapshot-based incremental reads, MERGE operations, and multi-stage pipeline orchestration with sales transactions and daily aggregations.

### [wap_pattern.py](batch/wap_pattern.py)
Write-Audit-Publish pattern implementation. Writes to staging branches, validates data quality, and publishes only after successful audits. Demonstrates branch management and fast-forward operations.

### [data-quality-deequ/](batch/data-quality-deequ/)
Comprehensive data quality validation using Amazon Deequ on open source Apache Spark that can run on EMR Serverless. 
### [iceberg_incremental/](batch/iceberg_incremental/)
Java-based incremental processing examples using Iceberg's native incremental read capabilities.

Remember that you have a full example on schema change via the MERGE approach on the AWS Big Data Blog: Unlock real-time data insights with schema evolution using Amazon MSK Serverless, Iceberg, and AWS Glue streaming. Although the example focus on Structured Streaming you can get a good guidance.

Note that after Spark 4.1 launches integrates MERGE INTO with SCHEMA EVOLUTION [link](https://github.com/apache/spark/pull/51698) this may get simplified on further Iceberg versions.
 