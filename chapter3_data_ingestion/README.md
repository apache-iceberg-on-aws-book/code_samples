# Chapter 3 - Data Ingestion Patterns / Reference Architectures

This chapter covers various data ingestion patterns and reference architectures for Apache Iceberg. It demonstrates best practices for streaming and batch data ingestion into Iceberg tables using AWS services and different Open Source technologies.

## [Apache Spark](spark/README.md)

Demonstrates batch and streaming data ingestion patterns using Apache Spark with Iceberg. Includes examples for basic append operations, merge/upsert patterns, windowing aggregations, incremental batch processing from DynamoDB, Write-Audit-Publish (WAP) pattern, data quality validation with Deequ, and schema evolution with CDC.

## [Apache Flink](flink/README.md)

Provides SQL-based streaming ingestion examples using Apache Flink. Covers basic streaming writes, windowed aggregations, CDC streaming patterns, and exactly-once semantics with checkpointing for reliable data ingestion into Iceberg tables.

## [Amazon Firehose](firehose/README.md)


Shows how to configure Amazon Data Firehose for serverless streaming ingestion directly into Iceberg tables. Demonstrates native upsert capabilities, inline parsing configuration, and integration with AWS Glue Data Catalog for managed streaming data delivery.

## [Apache Kafka](kafka/README.md)

Contains Kafka-based ingestion patterns including Kafka Connect with Debezium for CDC, Iceberg sink connectors, and AWS Glue Schema Registry integration for schema management and evolution in streaming pipelines.

