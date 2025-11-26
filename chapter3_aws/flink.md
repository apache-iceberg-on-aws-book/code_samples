# Amazon Managed Service for Apache Flink with Iceberg - Complete Setup Guide

This guide demonstrates deploying the IcebergSQLSink example from the AWS samples repository to Amazon Managed Service for Apache Flink.

## Prerequisites

Complete the [Prerequisites and Setup](prerequisites.md) before starting this guide.

## Overview

This example demonstrates:
- Flink SQL with Apache Iceberg integration
- Synthetic stock price data generation using DataGeneratorSource
- Writing streaming data to Iceberg tables in AWS Glue Data Catalog
- Partitioning by stock symbol for efficient queries

## Step 1: Clone the AWS Samples Repository

```bash
# Clone the repository
git clone https://github.com/aws-samples/amazon-managed-service-for-apache-flink-examples.git
cd amazon-managed-service-for-apache-flink-examples/java/Iceberg/IcebergSQLSink
```

## Step 2: Review the Application Code

The application consists of three main components:

### StockPrice Domain Model

```java
package com.amazonaws.services.msf.domain;

public class StockPrice {
    private String timestamp;
    private String symbol;
    private Float price;
    private Integer volumes;
    
    // Constructor, getters, and setters
}
```

### Data Generator

```java
package com.amazonaws.services.msf.source;

public class StockPriceGeneratorFunction implements GeneratorFunction<Long, StockPrice> {
    private static final String[] SYMBOLS = {"AAPL", "AMZN", "MSFT", "INTC", "TBV"};

    @Override
    public StockPrice map(Long sequence) throws Exception {
        String symbol = SYMBOLS[RandomUtils.nextInt(0, SYMBOLS.length)];
        float price = RandomUtils.nextFloat(0, 10);
        int volumes = RandomUtils.nextInt(0, 1000000);
        String timestamp = Instant.now().toString();
        return new StockPrice(timestamp, symbol, price, volumes);
    }
}
```

### Main Application

The main application creates an Iceberg catalog, table, and streams data:

```java
// Create Iceberg catalog
String createCatalogSQL = "CREATE CATALOG glue WITH (" +
    "'type' = 'iceberg', " +
    "'catalog-impl' = 'org.apache.iceberg.aws.glue.GlueCatalog'," +
    "'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO'," +
    "'warehouse' = '" + s3BucketPrefix + "')";

// Create Iceberg table partitioned by symbol
String createTableSQL = "CREATE TABLE IF NOT EXISTS " + sinkTableName + " (" +
    "`timestamp` STRING, " +
    "symbol STRING," +
    "price FLOAT," +
    "volumes INT" +
    ") PARTITIONED BY (symbol)";

// Insert streaming data
String insertSQL = "INSERT INTO " + sinkTableName + " " +
    "SELECT `timestamp`, symbol, price, volumes " +
    "FROM default_catalog.default_database.stockPriceTable";
```

## Step 3: Build the Application

```bash
# Build with Maven
mvn clean package

# Verify the JAR was created
ls target/iceberg-sql-sink-1.0.jar
```

## Step 4: Set Up AWS Resources

> **Important**: Run all commands in this section in the same shell session to preserve environment variables. Alternatively, save the variables to a file and source it before each command.

### Set Environment Variables

```bash
export AWS_REGION="us-east-1"
export AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
export APP_NAME="flink-iceberg-demo"
export TIMESTAMP=$(date +%s)
export S3_BUCKET="flink-iceberg-app-${TIMESTAMP}"

# Verify variables are set
echo "AWS_REGION: ${AWS_REGION}"
echo "AWS_ACCOUNT_ID: ${AWS_ACCOUNT_ID}"
echo "APP_NAME: ${APP_NAME}"
echo "S3_BUCKET: ${S3_BUCKET}"
```

### Create S3 Bucket

```bash
# Create S3 bucket for application and data
aws s3 mb s3://${S3_BUCKET} --region ${AWS_REGION}

# Upload application JAR
aws s3 cp target/iceberg-sql-sink-1.0.jar \
    s3://${S3_BUCKET}/app/iceberg-sql-sink-1.0.jar
```

### Create IAM Role

```bash
# Create trust policy
cat > trust-policy.json << EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "kinesisanalytics.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
EOF

# Create IAM role
aws iam create-role \
    --role-name FlinkIcebergAppRole \
    --assume-role-policy-document file://trust-policy.json

# Create and attach policy with bucket variable
cat > flink-policy.json << EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::${S3_BUCKET}",
        "arn:aws:s3:::${S3_BUCKET}/*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "glue:GetDatabase",
        "glue:GetTable",
        "glue:CreateTable",
        "glue:UpdateTable",
        "glue:GetDatabases",
        "glue:GetTables",
        "glue:CreateDatabase"
      ],
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents",
        "logs:DescribeLogStreams"
      ],
      "Resource": "arn:aws:logs:*:*:log-group:/aws/kinesis-analytics/*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "cloudwatch:PutMetricData"
      ],
      "Resource": "*"
    }
  ]
}
EOF

aws iam put-role-policy \
    --role-name FlinkIcebergAppRole \
    --policy-name FlinkAppPolicy \
    --policy-document file://flink-policy.json

# Wait for IAM policy to propagate
echo "Waiting 10 seconds for IAM policy to propagate..."
sleep 10
```

### Create Glue Database

```bash
# Create Glue database for Iceberg tables
# Note: Avoid using "default" as it's a reserved SQL keyword
aws glue create-database \
    --database-input '{
        "Name": "iceberg_db",
        "Description": "Database for Iceberg tables from Flink"
    }' \
    --region ${AWS_REGION}
```

## Step 5: Create Managed Flink Application

```bash
# Create the Flink application with Flink 1.20 runtime
aws kinesisanalyticsv2 create-application \
    --application-name ${APP_NAME} \
    --runtime-environment FLINK-1_20 \
    --service-execution-role arn:aws:iam::${AWS_ACCOUNT_ID}:role/FlinkIcebergAppRole \
    --application-configuration '{
        "ApplicationCodeConfiguration": {
            "CodeContent": {
                "S3ContentLocation": {
                    "BucketARN": "arn:aws:s3:::'${S3_BUCKET}'",
                    "FileKey": "app/iceberg-sql-sink-1.0.jar"
                }
            },
            "CodeContentType": "ZIPFILE"
        },
        "EnvironmentProperties": {
            "PropertyGroups": [
                {
                    "PropertyGroupId": "Iceberg",
                    "PropertyMap": {
                        "bucket.prefix": "s3://'${S3_BUCKET}'/warehouse",
                        "catalog.db": "iceberg_db",
                        "catalog.table": "prices_iceberg"
                    }
                },
                {
                    "PropertyGroupId": "DataGen",
                    "PropertyMap": {
                        "records.per.sec": "10.0"
                    }
                }
            ]
        },
        "FlinkApplicationConfiguration": {
            "ParallelismConfiguration": {
                "ConfigurationType": "CUSTOM",
                "Parallelism": 1,
                "ParallelismPerKPU": 1
            }
        }
    }' \
    --region ${AWS_REGION}

# Add CloudWatch logging
aws kinesisanalyticsv2 add-application-cloud-watch-logging-option \
    --application-name ${APP_NAME} \
    --current-application-version-id 1 \
    --cloud-watch-logging-option '{
        "LogStreamARN": "arn:aws:logs:'${AWS_REGION}':'${AWS_ACCOUNT_ID}':log-group:/aws/kinesis-analytics/flink-iceberg-sample:log-stream:kinesis-analytics-log-stream"
    }' \
    --region ${AWS_REGION}
```

## Step 6: Start the Application

```bash
# Start the application
aws kinesisanalyticsv2 start-application \
    --application-name ${APP_NAME} \
    --run-configuration '{}' \
    --region ${AWS_REGION}

# Monitor application status (takes ~2-3 minutes to start)
for i in {1..18}; do 
    echo "Check $i at $(date +%H:%M:%S):"
    STATUS=$(aws kinesisanalyticsv2 describe-application \
        --application-name ${APP_NAME} \
        --region ${AWS_REGION} \
        --query 'ApplicationDetail.ApplicationStatus' \
        --output text)
    echo "  Status: $STATUS"
    
    if [ "$STATUS" = "RUNNING" ]; then
        echo ""
        echo "════════════════════════════════════════"
        echo "✓✓✓ SUCCESS! Application is RUNNING ✓✓✓"
        echo "════════════════════════════════════════"
        break
    fi
    
    if [ "$STATUS" = "READY" ]; then
        echo "✗ Failed - check CloudWatch logs"
        break
    fi
    
    sleep 18
done
```

## Step 7: Verify Data Ingestion

### Check Glue Table

```bash
# Verify the Iceberg table was created
aws glue get-table \
    --database-name iceberg_db \
    --name prices_iceberg \
    --region ${AWS_REGION} \
    --query 'Table.{Name:Name,Location:StorageDescriptor.Location}'
```

### Check S3 Data Files

```bash
# List data files in S3
aws s3 ls s3://${S3_BUCKET}/warehouse/iceberg_db.db/prices_iceberg/data/ \
    --recursive --region ${AWS_REGION}

# You should see partitioned data files like:
# symbol=AAPL/00000-0-*.parquet
# symbol=AMZN/00000-0-*.parquet
# symbol=MSFT/00000-0-*.parquet
```

### Query with Athena

```sql
-- Query the data in Athena
SELECT * FROM iceberg_db.prices_iceberg LIMIT 10;

-- Count records by symbol
SELECT symbol, COUNT(*) as count 
FROM iceberg_db.prices_iceberg 
GROUP BY symbol;

-- Check latest prices
SELECT 
    symbol, 
    MAX(timestamp) as latest_time,
    AVG(price) as avg_price,
    SUM(volumes) as total_volume
FROM iceberg_db.prices_iceberg 
GROUP BY symbol;
```

## Step 8: Monitor the Application

### View CloudWatch Logs

```bash
# List log streams
aws logs describe-log-streams \
    --log-group-name /aws/kinesis-analytics/${APP_NAME} \
    --region ${AWS_REGION}

# Get recent log events
aws logs tail /aws/kinesis-analytics/${APP_NAME} \
    --follow \
    --region ${AWS_REGION}
```

### Check Application Metrics

```bash
# Get application details
aws kinesisanalyticsv2 describe-application \
    --application-name ${APP_NAME} \
    --region ${AWS_REGION} \
    --query 'ApplicationDetail.{
        Status:ApplicationStatus,
        Version:ApplicationVersionId,
        LastUpdate:LastUpdateTimestamp
    }'
```

## Step 9: Configuration Tuning

### Adjust Data Generation Rate

```bash
# Update to generate more records per second
aws kinesisanalyticsv2 update-application \
    --application-name ${APP_NAME} \
    --current-application-version-id 1 \
    --application-configuration-update '{
        "EnvironmentPropertyUpdates": {
            "PropertyGroups": [
                {
                    "PropertyGroupId": "DataGen",
                    "PropertyMap": {
                        "records.per.sec": "100.0"
                    }
                }
            ]
        }
    }' \
    --region ${AWS_REGION}
```

### Adjust Checkpoint Interval

```bash
# Update checkpoint configuration
aws kinesisanalyticsv2 update-application \
    --application-name ${APP_NAME} \
    --current-application-version-id 2 \
    --application-configuration-update '{
        "FlinkApplicationConfigurationUpdate": {
            "CheckpointConfigurationUpdate": {
                "CheckpointIntervalUpdate": 120000
            }
        }
    }' \
    --region ${AWS_REGION}
```

## Step 10: Cleanup

### Stop and Delete Application

```bash
# Stop the application
aws kinesisanalyticsv2 stop-application \
    --application-name ${APP_NAME} \
    --region ${AWS_REGION}

# Wait for it to stop
aws kinesisanalyticsv2 describe-application \
    --application-name ${APP_NAME} \
    --region ${AWS_REGION} \
    --query 'ApplicationDetail.ApplicationStatus'

# Delete the application
aws kinesisanalyticsv2 delete-application \
    --application-name ${APP_NAME} \
    --create-timestamp $(aws kinesisanalyticsv2 describe-application \
        --application-name ${APP_NAME} \
        --region ${AWS_REGION} \
        --query 'ApplicationDetail.CreateTimestamp' \
        --output text) \
    --region ${AWS_REGION}
```

### Clean Up Resources

```bash
# Delete Glue table and database
aws glue delete-table \
    --database-name iceberg_db \
    --name prices_iceberg \
    --region ${AWS_REGION}

aws glue delete-database \
    --name iceberg_db \
    --region ${AWS_REGION}

# Delete S3 bucket contents and bucket
aws s3 rm s3://${S3_BUCKET} --recursive --region ${AWS_REGION}
aws s3 rb s3://${S3_BUCKET} --region ${AWS_REGION}

# Delete IAM role and policy
aws iam delete-role-policy \
    --role-name FlinkIcebergAppRole \
    --policy-name FlinkAppPolicy

aws iam delete-role --role-name FlinkIcebergAppRole

# Clean up local files
rm trust-policy.json flink-policy.json
```

## Key Concepts

### Checkpointing and Data Visibility

Iceberg commits data on Flink checkpoints. The checkpoint interval determines:
- **Data visibility latency**: How quickly data appears in the table
- **Throughput**: Longer intervals = better throughput
- **Recovery time**: Shorter intervals = faster recovery from failures

Recommended settings:
- Development: 30 seconds (faster feedback)
- Production: 60-300 seconds (balance throughput and latency)

### Partitioning Strategy

The example partitions by `symbol`, which:
- Enables efficient queries filtering by stock symbol
- Organizes data files for better query performance
- Allows partition-level operations

### Application Properties

The application uses two property groups:
- **Iceberg**: S3 warehouse location, database, and table names
- **DataGen**: Data generation rate configuration

## Troubleshooting

### Common Issues Encountered

**1. ClassNotFoundException for DataGen Connector**

Error: `ClassNotFoundException: org.apache.flink.connector.datagen.source.GeneratorFunction`

Solution:
- The AWS sample requires Flink 1.20 runtime
- Ensure you create the application with `--runtime-environment FLINK-1_20`
- Earlier Flink versions (1.15, 1.18) have different DataGen APIs

**2. SQL Parse Error with "default" Database**

Error: `SqlParseException: Encountered "default" at line 1, column 31`

Solution:
- "default" is a reserved SQL keyword in Flink SQL
- Use a different database name like "iceberg_db"
- Update the Iceberg property group with the correct database name

**3. NullPointerException on Application Start**

Error: `NullPointerException` in application logs

Solution:
- Ensure both property groups are configured: "Iceberg" and "DataGen"
- When updating application configuration, include ALL property groups
- Property updates replace existing groups, they don't merge

**4. Application Returns to READY State**

If application goes from STARTING → READY instead of RUNNING:

Check CloudWatch logs:
```bash
aws logs get-log-events \
    --log-group-name /aws/kinesis-analytics/flink-iceberg-sample \
    --log-stream-name kinesis-analytics-log-stream \
    --region ${AWS_REGION} \
    --start-time $(($(date +%s) * 1000 - 300000)) \
    --limit 500 \
    --query 'events[-100:].message' \
    --output json | jq -r '.[]' | jq -r '.message' | grep -i "error\|exception"
```

Common causes:
- Missing or incorrect property groups
- IAM permission issues
- Invalid S3 paths
- Glue database doesn't exist

### Diagnostic Commands

```bash
# Check application status and configuration
aws kinesisanalyticsv2 describe-application \
    --application-name ${APP_NAME} \
    --region ${AWS_REGION} \
    --query 'ApplicationDetail.{
        Name:ApplicationName,
        Status:ApplicationStatus,
        Runtime:RuntimeEnvironment,
        Version:ApplicationVersionId
    }'

# View environment properties
aws kinesisanalyticsv2 describe-application \
    --application-name ${APP_NAME} \
    --region ${AWS_REGION} \
    --query 'ApplicationDetail.ApplicationConfigurationDescription.EnvironmentPropertyDescriptions'

# Check if Glue table was created
aws glue get-table \
    --database-name iceberg_db \
    --name prices_iceberg \
    --region ${AWS_REGION} \
    --query 'Table.{Name:Name,Location:StorageDescriptor.Location}'

# Verify data files in S3
aws s3 ls s3://${S3_BUCKET}/warehouse/iceberg_db.db/prices_iceberg/data/ \
    --recursive --region ${AWS_REGION}
```

## Best Practices

1. **Start with low data rates** during testing (10 records/sec)
2. **Monitor checkpoint duration** - should be < checkpoint interval
3. **Use appropriate partition strategies** for your query patterns
4. **Enable CloudWatch logging** for troubleshooting
5. **Test with snapshots disabled** initially, enable for production

## Next Steps

- Modify the data generator to use real data sources (Kinesis, Kafka)
- Implement windowed aggregations
- Add data quality checks
- Set up monitoring and alerting
- Explore CDC patterns with upsert mode

This example provides a foundation for building production streaming pipelines with Flink and Iceberg on AWS.
