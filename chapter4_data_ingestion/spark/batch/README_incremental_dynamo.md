# Iceberg Incremental Processing with DynamoDB Watermarks

This notebook demonstrates incremental processing in a medallion architecture (Bronze → Silver → Gold) using DynamoDB to store watermarks instead of Iceberg table properties.

## Overview

The notebook implements a CDC-based data pipeline with:
- **Bronze Layer**: Raw data ingestion with DMS-style operations (I/U/D)
- **Silver Layer**: Validated and deduplicated data
- **Gold Layer**: Aggregated analytics using Iceberg changelog views

Watermarks track the last processed snapshot ID for each pipeline stage, stored in DynamoDB for persistence across runs.

## DynamoDB Setup

Create the watermarks table:

```bash
aws dynamodb create-table \
  --table-name iceberg_watermarks \
  --attribute-definitions AttributeName=pipeline_id,AttributeType=S \
  --key-schema AttributeName=pipeline_id,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST
```

## Watermark Storage

Each pipeline stage stores its watermark with a unique key:
- Bronze → Silver: `bronze_db.orders->silver_db.orders`
- Silver → Gold: `silver_db.orders->gold_db.orders_by_year_state_category`

## Key Functions

- `get_watermark(pipeline_id)`: Retrieves last processed snapshot ID from DynamoDB
- `update_watermark(pipeline_id, snapshot_id)`: Updates watermark after successful processing
- `propagate_bronze_to_silver()`: Incremental CDC processing with validation
- `propagate_silver_to_gold()`: Incremental aggregation using changelog views

## Benefits

- **Persistent Watermarks**: Survives table compaction and maintenance operations
- **Centralized Tracking**: Single source of truth for all pipeline watermarks
- **Cross-Pipeline Visibility**: Easy monitoring of processing state across stages
- **Failure Recovery**: Watermarks only updated after successful processing

## Configuration

Update in the notebook:
- `S3_BASE_LOCATION`: Your S3 bucket for Iceberg tables
- `DYNAMODB_TABLE`: DynamoDB table name (default: `iceberg_watermarks`)
