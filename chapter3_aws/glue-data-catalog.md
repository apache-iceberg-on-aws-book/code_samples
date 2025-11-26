# AWS Glue Data Catalog with Apache Iceberg - Complete Setup Guide

This guide provides step-by-step instructions for setting up and working with Apache Iceberg tables using AWS Glue Data Catalog.

## Prerequisites

Complete the [Prerequisites and Setup](prerequisites.md) before starting this guide.

## Step 1: Create Your First Iceberg Table

### Create an Empty Iceberg Table

```bash
# Create an empty Iceberg table via Glue API
aws glue create-table \
    --database-name iceberg_examples \
    --table-input '{
        "Name": "sales_data",
        "TableType": "EXTERNAL_TABLE",
        "StorageDescriptor": {
            "Columns": [
                {"Name": "transaction_id", "Type": "bigint"},
                {"Name": "customer_id", "Type": "string"},
                {"Name": "amount", "Type": "decimal(10,2)"},
                {"Name": "transaction_date", "Type": "date"}
            ],
            "Location": "s3://${ICEBERG_BUCKET_NAME}/iceberg-tables/sales_data/"
        }
    }' \
    --open-table-format-input '{
        "IcebergInput": {
            "MetadataOperation": "CREATE",
            "Version": "2"
        }
    }'
```

### Verify Table Creation

```bash
# Verify table creation in Glue catalog
aws glue get-table --database-name iceberg_examples --name sales_data

# Check the Iceberg folder structure
aws glue get-table --database-name iceberg_examples --name sales_data \
    --query 'Table.Parameters.metadata_location' --output text

# List the metadata folder
aws s3 ls s3://${ICEBERG_BUCKET_NAME}/iceberg-tables/sales_data/metadata/
```

## Step 2: Set Up Automated Discovery with Glue Crawlers

### Create Iceberg Crawler

```bash
# Configure a crawler for Iceberg tables
aws glue create-crawler \
    --name iceberg-crawler \
    --role ${ICEBERG_GLUE_ROLE_ARN} \
    --database-name iceberg_examples \
    --targets '{
        "IcebergTargets": [
            {
                "Paths": ["s3://'${ICEBERG_BUCKET_NAME}'/iceberg-tables/"],
                "Exclusions": ["**/_temporary/**"]
            }
        ]
    }' \
    --configuration '{
        "Version": 1.0,
        "CrawlerOutput": {
            "Partitions": {"AddOrUpdateBehavior": "InheritFromTable"},
            "Tables": {"AddOrUpdateBehavior": "MergeNewColumns"}
        }
    }'

# Check crawler configuration
aws glue get-crawler --name iceberg-crawler
```

## Step 3: Configure Table Optimizer

### Basic Optimizer Setup
Configuring the Glue Table Optimizer for compaction involves setting up the optimizer and defining target parameters: 

```bash
# Enable basic compaction optimizer
aws glue create-table-optimizer \
    --database-name iceberg_examples \
    --table-name sales_data \
    --type compaction \
    --table-optimizer-configuration '{
        "roleArn": "'${ICEBERG_GLUE_ROLE_ARN}'",
        "enabled": true
    }'
```

### Configuration Examples for Snapshot Expiration 
Snapshot expiration configuration controls how the optimizer manages historical snapshots: 

```bash
# Retention configuration balances storage costs with time travel capabilities 

aws glue create-table-optimizer \
    --database-name iceberg_examples \
    --table-name sales_data \
    --type retention \
    --table-optimizer-configuration '{
        "roleArn": "arn:aws:iam::account:role/GlueRole",
        "enabled": true,
        "retentionConfiguration": {
            "icebergConfiguration": {
                "snapshotRetentionPeriodInDays": 5,
                "numberOfSnapshotsToRetain": 1,
                "cleanExpiredFiles": true
            }
        }
    }'
```

### Configuration Examples for Orphan File Deletion
Orphan file cleanup configuration ensures safe removal of unreferenced files: 

```bash
aws glue create-table-optimizer \
    --database-name iceberg_examples \
    --table-name sales_data \
    --type orphan_file_deletion \
    --table-optimizer-configuration '{
        "roleArn": "arn:aws:iam::account:role/GlueRole",
        "enabled": true,
        "orphanFileDeletionConfiguration": {
            "icebergConfiguration": {
                "orphanFileRetentionPeriodInDays": 3,
                "location": "s3://your-iceberg-bucket/iceberg-tables/sales_data/"
            }
        }
    }' 
```


## Step 4: Basic Column Statistics Setup

```bash
# Start basic column statistics task
aws glue start-column-statistics-task-run \
    --database-name iceberg_examples \
    --table-name sales_data \
    --role ${ICEBERG_GLUE_ROLE_ARN} \
    --sample-size 100.0
```

## Step 5: Monitor and Verify Setup

### Check Table Optimizer Status

```bash
# Check optimizer status
aws glue get-table-optimizer \
    --database-name iceberg_examples \
    --table-name sales_data \
    --type compaction
```

### Monitor Column Statistics Tasks

```bash
# List all column statistics task runs
aws glue list-column-statistics-task-runs

# Get details of a specific task run
aws glue get-column-statistics-task-run \
    --column-statistics-task-run-id <task-run-id>
```

## Next Steps

Your AWS Glue Data Catalog is now configured with Apache Iceberg support. You can:

1. Use this catalog with other AWS services (Athena, EMR, etc.)
2. Create additional Iceberg tables
3. Monitor table optimization through CloudWatch
4. Leverage column statistics for query optimization

## Troubleshooting

### Common Issues

1. **Permission Errors**: Ensure your IAM roles have the necessary permissions as defined in the prerequisites
2. **S3 Location Issues**: Verify that S3 bucket paths are correct and accessible
3. **Table Optimizer Failures**: Check CloudWatch logs for detailed error messages

### Verification Commands

```bash
# Verify table exists and is properly configured
aws glue get-table --database-name iceberg_examples --name sales_data \
    --query 'Table.Parameters' --output json

# Check if optimizers are running
aws glue get-table-optimizer \
    --database-name iceberg_examples \
    --table-name sales_data \
    --type compaction
```
