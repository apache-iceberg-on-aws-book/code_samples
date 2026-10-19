# Apache Spark with Iceberg

This guide provides complete step-by-step instructions for setting up EMR Serverless with Apache Iceberg, from infrastructure setup to interactive development.

## Prerequisites

Complete the [Prerequisites and Setup](prerequisites.md) before starting this guide.

## Step 1: Initial EMR Serverless Setup

### Create EMR Serverless Application

```bash
# Set environment variables
export AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
export AWS_REGION=us-east-1
export EMR_SERVERLESS_APP_NAME=iceberg-spark-app

# Create EMR Serverless application with interactive configuration
aws emr-serverless create-application \
    --name ${EMR_SERVERLESS_APP_NAME} \
    --type SPARK \
    --release-label emr-7.10.0 \
    --initial-capacity '{
        "DRIVER": {
            "workerCount": 1,
            "workerConfiguration": {
                "cpu": "4 vCPU",
                "memory": "16 GB"
            }
        },
        "EXECUTOR": {
            "workerCount": 2,
            "workerConfiguration": {
                "cpu": "4 vCPU",
                "memory": "16 GB"
            }
        }
    }' \
    --maximum-capacity '{
        "cpu": "400 vCPU",
        "memory": "3000 GB"
    }' \
    --auto-start-configuration enabled=true \
    --auto-stop-configuration enabled=true,idleTimeoutMinutes=30 \
    --interactive-configuration '{
        "studioEnabled": true,
        "livyEndpointEnabled": false
    }' \
    --runtime-configuration '[
        {
            "classification": "spark-defaults",
            "properties": {
                "spark.hadoop.hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
            }
        }
    ]'

# Get application ID
export EMR_APP_ID=$(aws emr-serverless list-applications \
    --query "applications[?name=='${EMR_SERVERLESS_APP_NAME}'].id" \
    --output text)

echo "EMR Serverless Application ID: ${EMR_APP_ID}"
```

### Create IAM Roles

```bash
# Create EMR Serverless execution role
cat > emr-serverless-trust-policy.json << EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "Service": "emr-serverless.amazonaws.com"
            },
            "Action": "sts:AssumeRole"
        }
    ]
}
EOF

aws iam create-role \
    --role-name EMRServerlessIcebergRole \
    --assume-role-policy-document file://emr-serverless-trust-policy.json

# Attach necessary policies
aws iam attach-role-policy \
    --role-name EMRServerlessIcebergRole \
    --policy-arn arn:aws:iam::aws:policy/AmazonS3FullAccess

aws iam attach-role-policy \
    --role-name EMRServerlessIcebergRole \
    --policy-arn arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole

# Create custom policy for Glue Data Catalog access
cat > glue-catalog-policy.json << EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "glue:GetDatabase",
                "glue:GetDatabases",
                "glue:GetTable",
                "glue:GetTables",
                "glue:GetPartition",
                "glue:GetPartitions",
                "glue:CreateTable",
                "glue:UpdateTable",
                "glue:DeleteTable",
                "glue:BatchCreatePartition",
                "glue:BatchDeletePartition",
                "glue:BatchUpdatePartition"
            ],
            "Resource": "*"
        }
    ]
}
EOF

aws iam create-policy \
    --policy-name GlueCatalogAccessPolicy \
    --policy-document file://glue-catalog-policy.json

aws iam attach-role-policy \
    --role-name EMRServerlessIcebergRole \
    --policy-arn arn:aws:iam::${AWS_ACCOUNT_ID}:policy/GlueCatalogAccessPolicy
```

### Create S3 Infrastructure

```bash
# Create S3 bucket for EMR Serverless
aws s3 mb s3://emr-serverless-iceberg-${AWS_ACCOUNT_ID}-${AWS_REGION}

# Create directory structure
aws s3api put-object \
    --bucket emr-serverless-iceberg-${AWS_ACCOUNT_ID}-${AWS_REGION} \
    --key scripts/
aws s3api put-object \
    --bucket emr-serverless-iceberg-${AWS_ACCOUNT_ID}-${AWS_REGION} \
    --key warehouse/
aws s3api put-object \
    --bucket emr-serverless-iceberg-${AWS_ACCOUNT_ID}-${AWS_REGION} \
    --key logs/
```

## Step 2: Create and Submit Spark Jobs

### Create Sample Spark Script

```python
# Create iceberg_operations.py
cat > iceberg_operations.py << 'EOF'
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, month
import sys

def main():
    # Initialize Spark session with Iceberg configuration
    spark = SparkSession.builder \
        .appName("IcebergOperationsEMRServerless") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.glue_catalog.type", "glue") \
        .config("spark.sql.catalog.glue_catalog.warehouse", f"s3://emr-serverless-iceberg-{sys.argv[1]}-{sys.argv[2]}/warehouse/") \
        .config("spark.sql.defaultCatalog", "glue_catalog") \
        .getOrCreate()
    
    # Create database if not exists
    spark.sql("CREATE DATABASE IF NOT EXISTS iceberg_examples")
    
    # Create Iceberg table
    spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg_examples.customer_transactions (
            transaction_id bigint,
            customer_id string,
            amount decimal(10,2),
            transaction_date date
        ) USING ICEBERG
        PARTITIONED BY (month(transaction_date))
    """)
    
    # Insert sample data
    spark.sql("""
        INSERT INTO iceberg_examples.customer_transactions VALUES
            (1001, 'CUST_001', 250.00, DATE '2025-01-20'),
            (1002, 'CUST_002', 175.50, DATE '2025-01-20'),
            (1003, 'CUST_003', 89.99, DATE '2025-01-21'),
            (1004, 'CUST_001', 320.75, DATE '2025-01-21'),
            (1005, 'CUST_004', 156.25, DATE '2025-01-22')
    """)
    
    # Query data
    result = spark.sql("""
        SELECT 
            customer_id,
            COUNT(*) as transaction_count,
            SUM(amount) as total_amount,
            AVG(amount) as avg_amount
        FROM iceberg_examples.customer_transactions
        GROUP BY customer_id
        ORDER BY total_amount DESC
    """)
    
    print("Customer Transaction Summary:")
    result.show()
    
    # Demonstrate time travel
    snapshots = spark.sql("""
        SELECT snapshot_id, committed_at, operation
        FROM iceberg_examples.customer_transactions.snapshots
        ORDER BY committed_at DESC
    """)
    
    print("Table Snapshots:")
    snapshots.show()
    
    # Update data
    spark.sql("""
        UPDATE iceberg_examples.customer_transactions
        SET amount = amount * 1.1
        WHERE customer_id = 'CUST_001'
    """)
    
    print("Updated data for CUST_001:")
    updated_result = spark.sql("""
        SELECT * FROM iceberg_examples.customer_transactions
        WHERE customer_id = 'CUST_001'
        ORDER BY transaction_date
    """)
    updated_result.show()
    
    spark.stop()

if __name__ == "__main__":
    main()
EOF

# Upload script to S3
aws s3 cp iceberg_operations.py s3://emr-serverless-iceberg-${AWS_ACCOUNT_ID}-${AWS_REGION}/scripts/
```

### Submit Spark Job

```bash
# Submit Spark job
aws emr-serverless start-job-run \
    --application-id ${EMR_APP_ID} \
    --execution-role-arn arn:aws:iam::${AWS_ACCOUNT_ID}:role/EMRServerlessIcebergRole \
    --name "iceberg-operations-job" \
    --job-driver '{
        "sparkSubmit": {
            "entryPoint": "s3://emr-serverless-iceberg-'${AWS_ACCOUNT_ID}'-'${AWS_REGION}'/scripts/iceberg_operations.py",
            "entryPointArguments": ["'${AWS_ACCOUNT_ID}'", "'${AWS_REGION}'"],
            "sparkSubmitParameters": "--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.glue_catalog.type=glue"
        }
    }' \
    --configuration-overrides '{
        "monitoringConfiguration": {
            "s3MonitoringConfiguration": {
                "logUri": "s3://emr-serverless-iceberg-'${AWS_ACCOUNT_ID}'-'${AWS_REGION}'/logs/"
            }
        }
    }'
```

### Monitor Job Execution

```bash
# Get job run ID
export JOB_RUN_ID=$(aws emr-serverless list-job-runs \
    --application-id ${EMR_APP_ID} \
    --query "jobRuns[0].id" \
    --output text)

echo "Job Run ID: ${JOB_RUN_ID}"

# Monitor job status
for i in {1..10}; do
    STATUS=$(aws emr-serverless get-job-run \
        --application-id ${EMR_APP_ID} \
        --job-run-id ${JOB_RUN_ID} \
        --query "jobRun.state" \
        --output text)
    echo "Job Status: $STATUS"
    
    if [ "$STATUS" != "PENDING" ] && [ "$STATUS" != "SCHEDULED" ] && [ "$STATUS" != "RUNNING" ]; then
        echo "Job completed with status: $STATUS"
        break
    fi
    
    sleep 30
done

# Download and view logs if successful
if [ "$STATUS" = "SUCCESS" ]; then
    aws s3 cp s3://emr-serverless-iceberg-${AWS_ACCOUNT_ID}-${AWS_REGION}/logs/applications/${EMR_APP_ID}/jobs/${JOB_RUN_ID}/SPARK_DRIVER/stdout.gz ./emr-stdout.gz
    gunzip -f emr-stdout.gz
    cat emr-stdout
fi
```

## Step 3: Set Up EMR Studio for Interactive Development

### Create EMR Studio Runtime Role

```bash
# Create runtime role for EMR Studio
cat > emr-studio-runtime-trust-policy.json << EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "ServerlessTrustPolicy",
            "Effect": "Allow",
            "Principal": {
                "Service": "emr-serverless.amazonaws.com"
            },
            "Action": "sts:AssumeRole",
            "Condition": {
                "StringLike": {
                    "aws:SourceArn": "arn:aws:emr-serverless:us-east-1:${AWS_ACCOUNT_ID}:/applications/${EMR_APP_ID}",
                    "aws:SourceAccount": "${AWS_ACCOUNT_ID}"
                }
            }
        }
    ]
}
EOF

aws iam create-role \
    --role-name EMRStudioRuntimeRole-IcebergApp \
    --assume-role-policy-document file://emr-studio-runtime-trust-policy.json \
    --path /service-role/

# Attach policies to runtime role
aws iam attach-role-policy \
    --role-name EMRStudioRuntimeRole-IcebergApp \
    --policy-arn arn:aws:iam::aws:policy/AmazonS3FullAccess

aws iam attach-role-policy \
    --role-name EMRStudioRuntimeRole-IcebergApp \
    --policy-arn arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole

aws iam attach-role-policy \
    --role-name EMRStudioRuntimeRole-IcebergApp \
    --policy-arn arn:aws:iam::aws:policy/CloudWatchLogsFullAccess
```

### Create EMR Studio Service Role

```bash
# Create EMR Studio Service Role
cat > emr-studio-service-trust-policy.json << EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "Service": "elasticmapreduce.amazonaws.com"
            },
            "Action": "sts:AssumeRole"
        }
    ]
}
EOF

aws iam create-role \
    --role-name EMRStudioServiceRole \
    --assume-role-policy-document file://emr-studio-service-trust-policy.json

aws iam attach-role-policy \
    --role-name EMRStudioServiceRole \
    --policy-arn arn:aws:iam::aws:policy/AmazonElasticMapReduceRole
```

### Create EMR Studio (Console Steps)

1. Navigate to **EMR console** → **Studios**
2. Click **Create Studio**
3. **Setup options**: `Custom`
4. **Studio settings**:
   - **Studio name**: `IcebergDemo`
   - **S3 location for Workspace storage**: `s3://emr-serverless-iceberg-<YOUR-ACCOUNT-ID>-<YOUR-REGION>/notebooks/`
   - **Service role**: `EMRStudioServiceRole`
5. **Workspace settings**:
   - **Workspace name**: `IcebergDemo`
6. **Authentication**: `AWS Identity and Access Management (IAM)`
7. Click **Create Studio and launch Workspace**

### Configure Notebook for Iceberg

In your EMR Studio notebook, use this configuration:

```python
%%configure -f
{   
    "conf":{
         "spark.sql.extensions":"org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
         "spark.sql.catalog.glue_catalog":"org.apache.iceberg.spark.SparkCatalog",
         "spark.sql.catalog.glue_catalog.type":"glue",
         "spark.sql.catalog.glue_catalog.warehouse":"s3://emr-serverless-iceberg-<YOUR-ACCOUNT-ID>-<YOUR-REGION>/warehouse/",
         "spark.sql.defaultCatalog":"glue_catalog"
    }
}
```

## Step 4: Interactive Iceberg Operations

### Basic Table Operations

```sql
%%sql
-- Show available databases
SHOW DATABASES

-- Show tables in iceberg_examples
SHOW TABLES IN iceberg_examples

-- Query customer transaction data
SELECT 
    customer_id,
    COUNT(*) as transactions,
    SUM(amount) as total_spent,
    MAX(transaction_date) as last_transaction
FROM iceberg_examples.customer_transactions
GROUP BY customer_id
ORDER BY total_spent DESC
```

### ACID Operations

```sql
%%sql
-- Insert new records
INSERT INTO iceberg_examples.customer_transactions VALUES 
    (1006, 'CUST_005', 199.99, DATE '2025-01-23')

-- Update existing records
UPDATE iceberg_examples.customer_transactions
SET amount = amount * 1.1
WHERE transaction_date = DATE '2025-01-20'

-- Delete records
DELETE FROM iceberg_examples.customer_transactions
WHERE amount < 100
```

### Time Travel and Snapshots

```sql
%%sql
-- View table snapshots
SELECT snapshot_id, committed_at, operation, summary
FROM iceberg_examples.customer_transactions.snapshots
ORDER BY committed_at DESC

-- Query historical data (replace with actual snapshot ID)
SELECT * FROM iceberg_examples.customer_transactions
VERSION AS OF 12345678901234567890
```

### Advanced ACID Operations
Spark's ACID implementation provides more sophisticated transaction handling compared to other engines, particularly for complex data transformations.

#### Complex MERGE Operations
```sql
-- Multi-condition MERGE with complex business logic
MERGE INTO iceberg_examples.customer_transactions AS target
USING ( 
    SELECT
        transaction_id,
        customer_id,
        amount,
        transaction_date,
        source_system,
        data_quality_score
    FROM iceberg_examples.staging_transactions
    WHERE data_quality_score >= 0.8
) AS source
ON target.transaction_id = source.transaction_id
WHEN MATCHED AND source.amount != target.amount THEN
    UPDATE SET
        amount = source.amount,
WHEN MATCHED AND source.amount > 0 THEN
    DELETE
WHEN NOT MATCHED AND source.amount > 0 THEN
    INSERT (transaction_id, customer_id, amount, transaction_date)
    VALUES (source.transaction_id, source.customer_id, source.amount, source.transaction_date); 
```

#### Streaming with MERGE Operations
Even in Spark Structured Streaming, you can use MERGE statements in the same way as working with batch.

```python
# Read from Kafka 
raw_stream = spark.readStream.format("kafka").options(**options).load()

# Parse the Kafka value JSON into DataFrame 
events = raw_stream.selectExpr("CAST(value AS STRING) jsonData") \
  .select(from_json(col("jsonData"), record_schema).alias("events")) \
  .select("events.*")

ICEBERG_TABLE_NAME = "events_metrics.events_per_minute"  

# Function to merge batch data into Iceberg table
def merge_batch_to_iceberg(df, epoch_id):
  ... prepare df_prepared  

  # Create a global temporary view of the batch data - note global view is needed
  df_prepared.createOrReplaceGlobalTempView("updates") 

  # Execute the MERGE operation 
  spark.sql(f"""
    MERGE INTO {ICEBERG_TABLE_NAME} t
    USING global_temp.updates s
    ON t.id = s.id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
  """)
 
# Write stream to Iceberg table with merge 
query = events.writeStream \
  .foreachBatch(merge_batch_to_iceberg) \
  .option("checkpointLocation", CHECKPOINTING_PATH) \
  .trigger(processingTime="30 seconds") \
  .start() \
  .awaitTermination() 
```

#### Batch UPDATE Operations
Spark optimizes batch updates by analyzing update patterns and choosing appropriate execution strategies: 

```sql
-- Batch update with complex conditions
UPDATE iceberg_examples.customer_transactions
SET amount = amount * 1.10
WHERE transaction_date >= current_date() - INTERVAL 30 DAYS; 
```

### Partition Evolution

#### Dynamic Partition Evolution
Spark can automatically evolve partition schemes based on data patterns:

```sql
-- Start with monthly partitioning
CREATE TABLE iceberg_examples.time_series_data (
    event_id bigint,
    event_timestamp timestamp,
    event_data string
) USING ICEBERG
PARTITIONED BY (month(event_timestamp)); 

-- Replace monthly with daily partitioning
ALTER TABLE iceberg_examples.time_series_data
REPLACE PARTITION FIELD month(event_timestamp) WITH day(event_timestamp); 
```

### Branch and Tag Operations
Spark provides the most comprehensive support for Iceberg's branching and tagging features, enabling sophisticated data versioning workflows.

#### Branch Management
Create and manage branches for parallel development or experimentation: 

```sql
-- Create branch for data experimentation
ALTER TABLE iceberg_examples.customer_transactions
CREATE BRANCH experimental_features; 

-- Create branch from specific snapshot (need to replace snapshot id) 
ALTER TABLE iceberg_examples.customer_transactions
CREATE BRANCH audit_branch AS OF VERSION 8745692581473829164;

-- Work with branch data
INSERT INTO iceberg_examples.customer_transactions.branch_experimental_features
VALUES (9999, 'TEST_CUSTOMER', 500.00, current_date()); 

-- Query branch-specific data
SELECT COUNT(*) FROM iceberg_examples.customer_transactions.branch_experimental_features; 
```

#### Tag Management
Create immutable references to specific table states:

```sql
-- Create tag for release version 
ALTER TABLE iceberg_examples.customer_transactions 
CREATE TAG v1_0_release; 

-- Query tagged version
SELECT
    COUNT(*) as record_count,
    SUM(amount) as total_amount
FROM iceberg_examples.customer_transactions.tag_v1_0_release; 
```


## Step 5: Resource Cleanup

```bash
# Stop EMR Serverless application
aws emr-serverless stop-application --application-id ${EMR_APP_ID}

# Delete EMR Serverless application (optional)
aws emr-serverless delete-application --application-id ${EMR_APP_ID}

# Delete S3 bucket contents
aws s3 rm s3://emr-serverless-iceberg-${AWS_ACCOUNT_ID}-${AWS_REGION} --recursive

# Delete S3 bucket
aws s3 rb s3://emr-serverless-iceberg-${AWS_ACCOUNT_ID}-${AWS_REGION}

# Delete IAM roles and policies
aws iam detach-role-policy --role-name EMRServerlessIcebergRole --policy-arn arn:aws:iam::aws:policy/AmazonS3FullAccess
aws iam detach-role-policy --role-name EMRServerlessIcebergRole --policy-arn arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole
aws iam detach-role-policy --role-name EMRServerlessIcebergRole --policy-arn arn:aws:iam::${AWS_ACCOUNT_ID}:policy/GlueCatalogAccessPolicy
aws iam delete-role --role-name EMRServerlessIcebergRole
aws iam delete-policy --policy-arn arn:aws:iam::${AWS_ACCOUNT_ID}:policy/GlueCatalogAccessPolicy

# Clean up local files
rm -f emr-serverless-trust-policy.json glue-catalog-policy.json iceberg_operations.py emr-stdout
```

## Troubleshooting

### Common Issues

1. **Application Creation Fails**: Check IAM permissions and region availability
2. **Job Submission Errors**: Verify S3 bucket permissions and script upload
3. **EMR Studio Connection Issues**: Ensure runtime role has correct permissions

### Verification Commands

```bash
# Check application status
aws emr-serverless get-application --application-id ${EMR_APP_ID}

# List job runs
aws emr-serverless list-job-runs --application-id ${EMR_APP_ID}

# Check table in Glue catalog
aws glue get-table --database-name iceberg_examples --name customer_transactions
```

## Appendix: Use Iceberg V3 spec
Coming soon

### Create a new Iceberg V3 format table

TBD

### Migrate from an existing Iceberg V2 format table to Iceberg V3 format 

TBD

### Understand how Deletion Vectors work on Iceberg V3

TBD
