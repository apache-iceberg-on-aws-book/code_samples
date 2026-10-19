# Amazon Data Firehose to Iceberg Table with Upsert

This example demonstrates how to configure Amazon Data Firehose to deliver streaming data into an Apache Iceberg table with upsert functionality using Amazon Athena.

If you want more detail please go to this [AWS Blog](https://aws.amazon.com/blogs/big-data/stream-real-time-data-into-apache-iceberg-tables-in-amazon-s3-using-amazon-data-firehose/) or [AWS official docs](https://docs.aws.amazon.com/firehose/latest/dev/apache-iceberg-destination.html) for Iceberg as destination.

## Prerequisites

- AWS CLI configured with appropriate permissions ( or CloudShell).
- An S3 bucket for temporary storage.
- An S3 bucket for the Iceberg table storage.
- Athena workgroup with Iceberg support enabled.

## Setup Instructions

### 1. Create the Iceberg Table in Athena

First, create a database and table in Athena that supports Iceberg format with upsert capabilities:

```sql
CREATE DATABASE IF NOT EXISTS demo_db;

CREATE TABLE demo_db.user_events_iceberg (
    user_id STRING,
    event_type STRING,
    event_data STRING,
    event_time TIMESTAMP,
    updated_at TIMESTAMP
)
LOCATION 's3://your-iceberg-bucket/warehouse/demo_db.db/user_events_iceberg/'
TBLPROPERTIES (
    'table_type' = 'ICEBERG',
    'format' = 'parquet',
    'write_compression' = 'snappy'
);
```

### 2. Configure IAM Role for Firehose

Create an IAM role that Firehose can assume with permissions to write to S3 and interact with Athena:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "Service": "firehose.amazonaws.com"
            },
            "Action": "sts:AssumeRole"
        }
    ]
}
```

Attach policies for S3, Glue, and Athena access.

### 3. Create the Firehose Delivery Stream

Use the AWS CLI to create a Firehose delivery stream that delivers to the Iceberg table:

```bash
aws firehose create-delivery-stream \
    --delivery-stream-name user-events-iceberg-stream \
    --delivery-stream-type DirectPut \
    --iceberg-destination-configuration '{
        "RoleARN": "arn:aws:iam::YOUR_ACCOUNT:role/firehose-iceberg-role",
        "CatalogConfiguration": {
            "CatalogARN": "arn:aws:glue:REGION:ACCOUNT:catalog"
        },
        "DestinationTableConfigurationList": [
            {
                "DestinationDatabaseName": "demo_db",
                "DestinationTableName": "user_events_iceberg"
            }
        ],
        "BufferingHints": {
            "SizeInMBs": 128,
            "IntervalInSeconds": 300
        },
        "S3Configuration": {
            "RoleARN": "arn:aws:iam::YOUR_ACCOUNT:role/firehose-iceberg-role",
            "BucketARN": "arn:aws:s3:::your-temp-bucket",
            "Prefix": "firehose-failed/",
            "ErrorOutputPrefix": "firehose-errors/",
            "BufferingHints": {
                "SizeInMBs": 5,
                "IntervalInSeconds": 300
            },
            "CompressionFormat": "UNCOMPRESSED"
        },
        "ProcessingConfiguration": {
            "Enabled": false
        },
        "CloudWatchLoggingOptions": {
            "Enabled": true,
            "LogGroupName": "/aws/kinesisfirehose/user-events-iceberg-stream",
            "LogStreamName": "IcebergDelivery"
        },
        "RetryOptions": {
            "DurationInSeconds": 300
        }
    }'
```

**Note:** Replace `YOUR_ACCOUNT` and `REGION` with your actual AWS account ID and region.

### 4. Configure Iceberg Table for Upsert

**Important:** For Firehose to perform upserts, you need to specify the upsert configuration when creating the delivery stream. Update the delivery stream configuration to include:

```bash
aws firehose update-destination \
    --delivery-stream-name user-events-iceberg-stream \
    --current-delivery-stream-version-id 1 \
    --destination-id destinationId-000000000001 \
    --iceberg-destination-update '{
        "DestinationTableConfigurationList": [
            {
                "DestinationDatabaseName": "demo_db",
                "DestinationTableName": "user_events_iceberg",
                "UniqueKeys": ["user_id"]
            }
        ]
    }'
```

The `UniqueKeys` parameter tells Firehose which columns to use for identifying duplicate records to perform upserts.

Go to Firehose Stream on the AWS console, click on the stream ```user-events-iceberg-stream```, then open the `Configuration` tab an click on 'edit' on Destination: 

<img width="1703" height="671" alt="image" src="https://github.com/user-attachments/assets/99789500-72bf-45ab-9cf4-706cdc37fd12" />

Now you need to enable `inline parsing` like on the screenshot and provide the required values as static values for Database expression and Table expression. Because you want to be able to update records, you also need to specify the Operation expression.
Database expression: “demo_db”
Table expression: “user_events_iceberg”
Operation expression: “update”

Include double quotation marks to use the literal value for the database and table name. If you do not use double quotations marks, Firehose assumes that this is a JSON Query expression and will attempt to parse the expression when processing your stream and fail.



### 5. Send Sample Data to Firehose

Use the AWS CLI to send sample data to your Firehose delivery stream. The data must be in JSON format:

```bash
# Send initial records
aws firehose put-record \
    --delivery-stream-name user-events-iceberg-stream \
    --record '{"Data":"eyJ1c2VyX2lkIjoiMTIzIiwiZXZlbnRfdHlwZSI6ImxvZ2luIiwiZXZlbnRfZGF0YSI6InVzZXIgbG9naW4iLCJldmVudF90aW1lIjoiMjAyMy0wNi0wMVQxMDowMDowMFoiLCJ1cGRhdGVkX2F0IjoiMjAyMy0wNi0wMVQxMDowMDowMFoifQ=="}'

aws firehose put-record \
    --delivery-stream-name user-events-iceberg-stream \
    --record '{"Data":"eyJ1c2VyX2lkIjoiNDU2IiwiZXZlbnRfdHlwZSI6InB1cmNoYXNlIiwiZXZlbnRfZGF0YSI6Iml0ZW0gcHVyY2hhc2VkIiwiZXZlbnRfdGltZSI6IjIwMjMtMDYtMDFUMTA6MDU6MDBaIiwidXBkYXRlZF9hdCI6IjIwMjMtMDYtMDFUMTA6MDU6MDBaIn0="}'

# Wait for the data to be processed (buffering interval)
sleep 360

# Send an update for user 123 (upsert) - this will replace the previous record
aws firehose put-record \
    --delivery-stream-name user-events-iceberg-stream \
    --record '{"Data":"eyJ1c2VyX2lkIjoiMTIzIiwiZXZlbnRfdHlwZSI6ImxvZ291dCIsImV2ZW50X2RhdGEiOiJ1c2VyIGxvZ291dCIsImV2ZW50X3RpbWUiOiIyMDIzLTA2LTAxVDExOjAwOjAwWiIsInVwZGF0ZWRfYXQiOiIyMDIzLTA2LTAxVDExOjAwOjAwWiJ9"}'
```

### 6. Query the Iceberg Table

After the data has been processed by Firehose, query the Iceberg table in Athena:

```sql
SELECT * FROM demo_db.user_events_iceberg ORDER BY user_id, updated_at;
```

You should see that user 123 has been updated with the logout event, demonstrating the upsert functionality.

## How It Works

1. Firehose receives streaming JSON data and buffers it based on size (128 MB) or time (300 seconds)
2. When the buffer is full, Firehose writes data directly to the Iceberg table using the Glue Data Catalog
3. The `UniqueKeys` configuration tells Firehose to perform upserts based on the `user_id` column
4. Records with the same `user_id` are merged - newer records replace older ones
5. Failed records are written to the S3 backup location for troubleshooting
6. The Iceberg table maintains ACID compliance and provides a consistent view of the data

**Key Differences from S3 Destination:**
- Uses `IcebergDestinationConfiguration` instead of `ExtendedS3DestinationConfiguration`
- Requires Glue Data Catalog integration
- Supports native upsert operations via `UniqueKeys`
- No need for separate Lambda processors or manual merge operations

## Cleanup

To remove the resources created:

```bash
aws firehose delete-delivery-stream --delivery-stream-name user-events-iceberg-stream
DROP TABLE demo_db.user_events_iceberg;
DROP DATABASE demo_db;
```
