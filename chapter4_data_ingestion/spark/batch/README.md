# Spark Batch Processing Examples

Batch data ingestion patterns for Apache Iceberg on AWS EMR Serverless.

## Prerequisites

- AWS CLI configured with appropriate credentials
- S3 bucket for scripts and data storage
- Permissions to create EMR Serverless applications and DynamoDB tables
- Python 3.9+ (for local testing)

## Scripts Overview

### [incremental_dynamodb.ipynb](incremental_dynamodb.ipynb)
Incremental batch processing using DynamoDB for watermark tracking. Demonstrates snapshot-based incremental reads, MERGE operations, and multi-stage pipeline orchestration.

### [wap_pattern.py](wap_pattern.py)
Write-Audit-Publish pattern with Iceberg branches for data validation before publishing.

### [data-quality-deequ/](data-quality-deequ/)
Data quality validation using Amazon Deequ library. See subfolder README for details.

## Running on EMR Serverless

### 1. Create EMR Serverless Application

```bash
aws emr-serverless create-application \
  --name incremental-batch-app \
  --release-label emr-7.0.0 \
  --type SPARK \
  --architecture ARM64 \
  --initial-capacity '{
    "DRIVER": {
      "workerCount": 1,
      "workerConfiguration": {
        "cpu": "4vCPU",
        "memory": "8GB"
      }
    },
    "EXECUTOR": {
      "workerCount": 2,
      "workerConfiguration": {
        "cpu": "4vCPU",
        "memory": "8GB"
      }
    }
  }' \
  --auto-start-configuration enabled=true \
  --auto-stop-configuration enabled=true,idleTimeoutMinutes=15 \
  --region us-east-1
```

### 2. Create IAM Execution Role

```bash
# Trust policy
cat > emr-trust-policy.json << 'EOF'
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": {"Service": "emr-serverless.amazonaws.com"},
    "Action": "sts:AssumeRole"
  }]
}
EOF

aws iam create-role \
  --role-name EMRServerlessBatchRole \
  --assume-role-policy-document file://emr-trust-policy.json

# Attach policies
cat > emr-permissions.json << 'EOF'
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject", "s3:ListBucket", "s3:DeleteObject"],
      "Resource": ["arn:aws:s3:::your-bucket/*", "arn:aws:s3:::your-bucket"]
    },
    {
      "Effect": "Allow",
      "Action": ["glue:*"],
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": ["dynamodb:*"],
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": ["logs:*"],
      "Resource": "*"
    }
  ]
}
EOF

aws iam put-role-policy \
  --role-name EMRServerlessBatchRole \
  --policy-name EMRBatchPermissions \
  --policy-document file://emr-permissions.json
```

### 3. Package Dependencies

Create `requirements.txt`:
```
boto3==1.34.0
```

Package for EMR Serverless:
```bash
pip install -t ./package boto3
cd package
zip -r ../dependencies.zip .
cd ..
aws s3 cp dependencies.zip s3://your-bucket/dependencies/
```

### 4. Upload Script to S3

```bash
aws s3 cp incremental_batch_dynamodb.py s3://your-bucket/scripts/
```

### 5. Submit Job

```bash
APPLICATION_ID="<your-application-id>"
ROLE_ARN="arn:aws:iam::<account-id>:role/EMRServerlessBatchRole"

aws emr-serverless start-job-run \
  --application-id $APPLICATION_ID \
  --execution-role-arn $ROLE_ARN \
  --name "incremental-batch-$(date +%Y%m%d-%H%M%S)" \
  --job-driver '{
    "sparkSubmit": {
      "entryPoint": "s3://your-bucket/scripts/incremental_batch_dynamodb.py",
      "sparkSubmitParameters": "--conf spark.jars.packages=org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.0,software.amazon.awssdk:bundle:2.20.18 --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.sql.catalog.iceberg_catalog=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.iceberg_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog --conf spark.sql.catalog.iceberg_catalog.warehouse=s3://your-bucket/warehouse/ --conf spark.archives=s3://your-bucket/dependencies/dependencies.zip#environment --conf spark.emr-serverless.driverEnv.PYSPARK_DRIVER_PYTHON=./environment/bin/python --conf spark.emr-serverless.driverEnv.PYSPARK_PYTHON=./environment/bin/python --conf spark.executorEnv.PYSPARK_PYTHON=./environment/bin/python"
    }
  }' \
  --configuration-overrides '{
    "monitoringConfiguration": {
      "s3MonitoringConfiguration": {
        "logUri": "s3://your-bucket/logs/"
      },
      "cloudWatchLoggingConfiguration": {
        "enabled": true,
        "logGroupName": "/aws/emr-serverless/incremental-batch"
      }
    }
  }' \
  --region us-east-1
```

### 6. Monitor Job

```bash
JOB_RUN_ID="<job-run-id>"

# Check status
aws emr-serverless get-job-run \
  --application-id $APPLICATION_ID \
  --job-run-id $JOB_RUN_ID

# View logs
aws logs tail /aws/emr-serverless/incremental-batch --follow
```

## Configuration

Update the script before running:

```python
# Line 169: Update S3 warehouse location
.config("spark.sql.catalog.iceberg_catalog.warehouse", "s3://YOUR-BUCKET/warehouse/")

# Line 15: Update DynamoDB region if needed
def __init__(self, table_name="data_pipeline_watermarks", region="us-east-1"):
```

## DynamoDB Table

The script automatically creates a DynamoDB table named `data_pipeline_watermarks` with:
- Partition key: `pipeline_id` (String)
- Sort key: `table_name` (String)
- Billing mode: Pay-per-request

## Testing Locally

```bash
# Install dependencies
pip install pyspark boto3

# Set AWS credentials
export AWS_ACCESS_KEY_ID=your-key
export AWS_SECRET_ACCESS_KEY=your-secret
export AWS_DEFAULT_REGION=us-east-1

# Run script
python incremental_batch_dynamodb.py
```

## Cleanup

```bash
# Stop application
aws emr-serverless stop-application --application-id $APPLICATION_ID

# Delete application
aws emr-serverless delete-application --application-id $APPLICATION_ID

# Delete DynamoDB table
aws dynamodb delete-table --table-name data_pipeline_watermarks
```
