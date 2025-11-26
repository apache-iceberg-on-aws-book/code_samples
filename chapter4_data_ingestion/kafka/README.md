# Apache Kafka Data Ingestion Examples

Kafka-based ingestion patterns for streaming data into Apache Iceberg tables on AWS.

## [connect-debezium-cdc/](connect-debezium-cdc/)
Production-ready CDC pipeline using Debezium PostgreSQL connector with MSK Connect. Captures changes from Aurora PostgreSQL and streams to Iceberg via MSK. Includes connector configurations for both Debezium source and Iceberg sink connectors.

## [glue-registry-examples/](glue-registry-examples/)
Java-based examples for producing and consuming messages with Glue Schema Registry. Supports multiple serialization formats (Avro, JSON, Protocol Buffers) with schema validation. Includes Docker Compose setup for local Kafka testing and Maven project structure.
