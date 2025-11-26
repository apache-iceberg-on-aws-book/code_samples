# AWS Lambda with PyIceberg - Complete Setup Guide

This guide provides step-by-step instructions for using AWS Lambda with PyIceberg for serverless Iceberg analytics and data processing.

## Prerequisites

Complete the [Prerequisites and Setup](prerequisites.md) before starting this guide.

## Step 1: Environment Setup

### Set Environment Variables

```bash
# Set environment variables
export AWS_REGION="us-east-1"
export LAMBDA_FUNCTION_NAME="iceberg-analytics"
export S3_BUCKET_NAME="${ICEBERG_BUCKET_NAME}"
export GLUE_DATABASE_NAME="iceberg_examples"
export AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
```

### Create IAM Role for Lambda

```bash
# Create Lambda execution role
aws iam create-role \
    --role-name LambdaIcebergRole \
    --assume-role-policy-document '{
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {
                    "Service": "lambda.amazonaws.com"
                },
                "Action": "sts:AssumeRole"
            }
        ]
    }'

# Attach basic Lambda execution policy
aws iam attach-role-policy \
    --role-name LambdaIcebergRole \
    --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole

# Create custom policy for Iceberg operations
aws iam put-role-policy \
    --role-name LambdaIcebergRole \
    --policy-name IcebergAccessPolicy \
    --policy-document '{
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "glue:GetDatabase",
                    "glue:GetTable",
                    "glue:GetTables",
                    "glue:UpdateTable",
                    "glue:CreateTable"
                ],
                "Resource": "*"
            },
            {
                "Effect": "Allow",
                "Action": [
                    "s3:GetObject",
                    "s3:PutObject",
                    "s3:DeleteObject",
                    "s3:ListBucket"
                ],
                "Resource": [
                    "arn:aws:s3:::'${S3_BUCKET_NAME}'",
                    "arn:aws:s3:::'${S3_BUCKET_NAME}'/*"
                ]
            }
        ]
    }'
```

## Step 2: Create Lambda Container Image

### Open AWS CloudShell

1. Open AWS CloudShell in the AWS Console
2. CloudShell provides Docker and all necessary tools pre-installed

### Create Project Files in CloudShell

```bash
# Create project directory in CloudShell
mkdir pyiceberg-lambda
cd pyiceberg-lambda

# Create requirements.txt
cat > requirements.txt << 'EOF'
pyiceberg>=0.5.0
boto3>=1.28.0
botocore>=1.31.0
pandas>=2.0.0
pyarrow>=12.0.0
fsspec>=2023.5.0
s3fs>=2023.5.0
mypy-boto3-glue
EOF

# Create Dockerfile
cat > Dockerfile << 'EOF'
FROM public.ecr.aws/lambda/python:3.13

# Set working directory as copy destination
WORKDIR ${LAMBDA_TASK_ROOT}

# Copy requirements.txt
COPY requirements.txt .

# Install dependencies
RUN pip install -r requirements.txt

# Copy Lambda function code
COPY lambda_function.py .

# Set Lambda function handler
CMD [ "lambda_function.handler" ]
EOF
```

### Create Lambda Function Code

```python
# Create lambda_function.py
cat > lambda_function.py << 'EOF'
import json
import os
from pyiceberg.catalog import load_catalog

def handler(event, context):
    try:
        # Load catalog configuration
        catalog = load_catalog(
            "glue_catalog",
            **{
                "type": "glue",
                "s3.endpoint": f"https://s3.{os.environ.get('AWS_REGION', 'us-east-1')}.amazonaws.com",
            }
        )
        
        operation = event.get('operation')
        table_name = event.get('table_name', 'sales_data')
        
        # Handle database.table format or default to iceberg_examples
        if '.' in table_name:
            full_table_name = table_name
        else:
            full_table_name = f"iceberg_examples.{table_name}"
        
        if operation == 'read':
            # Read operation
            table = catalog.load_table(full_table_name)
            df = table.scan().to_pandas()
            
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': f'Successfully read {len(df)} rows from {full_table_name}',
                    'row_count': len(df),
                    'columns': list(df.columns)
                })
            }
            
        elif operation == 'write':
            # Write operation example
            import pandas as pd
            
            # Create sample data
            sample_data = pd.DataFrame({
                'transaction_id': [1001, 1002, 1003],
                'customer_id': ['CUST001', 'CUST002', 'CUST003'],
                'amount': [150.75, 89.50, 234.25],
                'transaction_date': ['2024-01-15', '2024-01-16', '2024-01-17']
            })
            
            table = catalog.load_table(full_table_name)
            table.append(sample_data)
            
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': f'Successfully wrote {len(sample_data)} rows to {full_table_name}'
                })
            }
            
        elif operation == 'analyze':
            # Analyze table metadata
            table = catalog.load_table(full_table_name)
            
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': f'Table analysis for {full_table_name}',
                    'schema': str(table.schema()),
                    'location': table.location(),
                    'metadata_location': table.metadata_location
                })
            }
            
        else:
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'error': 'Invalid operation. Use: read, write, or analyze'
                })
            }
            
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e)
            })
        }
EOF
```

## Step 3: Build and Deploy Container

### Build Container Image in CloudShell

```bash
# Set up variables in CloudShell
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
REGION="us-east-1"
REPO_NAME="pyiceberg-lambda"

# Create ECR repository
aws ecr create-repository \
    --repository-name ${REPO_NAME} \
    --region ${REGION} || echo "Repository may already exist"

# Login to ECR
aws ecr get-login-password --region ${REGION} | docker login --username AWS --password-stdin ${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com

# Build Docker image in CloudShell
docker build -t ${REPO_NAME} .

# Tag image for ECR
docker tag ${REPO_NAME}:latest ${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com/${REPO_NAME}:latest

# Push image to ECR
docker push ${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com/${REPO_NAME}:latest

echo "Container image deployed to ECR from CloudShell"
```

## Step 4: Create Lambda Function

### Create Lambda Function with Container Image

```bash
# Create Lambda function with container image
aws lambda create-function \
    --function-name ${LAMBDA_FUNCTION_NAME} \
    --role arn:aws:iam::${ACCOUNT_ID}:role/LambdaIcebergRole \
    --code ImageUri=${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com/pyiceberg-lambda:latest \
    --package-type Image \
    --environment Variables="{
        CATALOG_URI=https://glue.${AWS_REGION}.amazonaws.com,
        WAREHOUSE_PATH=s3://${S3_BUCKET_NAME}/warehouse/,
        GLUE_DATABASE=${GLUE_DATABASE_NAME}
    }" \
    --timeout 300 \
    --memory-size 1024

echo "Lambda function deployment completed successfully!"
```

## Step 5: Test Lambda Function

### Test Basic Operations

```bash
# Test analyze operation
echo "Testing analyze operation..."
aws lambda invoke \
    --function-name ${LAMBDA_FUNCTION_NAME} \
    --payload '{"operation": "analyze", "table_name": "sales_data"}' \
    response.json \
    --region ${AWS_REGION}

echo "Analyze response:"
cat response.json
echo ""

# Test read operation  
echo "Testing read operation..."
aws lambda invoke \
    --function-name ${LAMBDA_FUNCTION_NAME} \
    --payload '{"operation": "read", "table_name": "sales_data"}' \
    response.json \
    --region ${AWS_REGION}

echo "Read response:"
cat response.json
echo ""

# Test write operation
echo "Testing write operation..."
aws lambda invoke \
    --function-name ${LAMBDA_FUNCTION_NAME} \
    --payload '{"operation": "write", "table_name": "sales_data"}' \
    response.json \
    --region ${AWS_REGION}

echo "Write response:"
cat response.json
```

## Step 6: Advanced Lambda Functions

### Create Event-Driven Processing Function

```python
# Create advanced_lambda_function.py
cat > advanced_lambda_function.py << 'EOF'
import json
import boto3
import pandas as pd
from pyiceberg.catalog import load_catalog
from datetime import datetime

def s3_event_handler(event, context):
    """Process S3 events and update Iceberg tables"""
    s3_client = boto3.client('s3')
    catalog = load_catalog("glue_catalog", **{"type": "glue"})
    
    results = []
    
    for record in event['Records']:
        # Parse S3 event
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']
        
        if key.endswith('.csv'):
            # Read CSV from S3
            response = s3_client.get_object(Bucket=bucket, Key=key)
            df = pd.read_csv(response['Body'])
            
            # Load Iceberg table
            table = catalog.load_table("iceberg_examples.sales_data")
            
            # Append data to Iceberg table
            table.append(df)
            
            results.append({
                "file": f"s3://{bucket}/{key}",
                "records_processed": len(df),
                "status": "success"
            })
    
    return {
        "statusCode": 200,
        "body": {
            "processed_files": len(results),
            "results": results
        }
    }

def table_management_handler(event, context):
    """Lambda function for Iceberg table management"""
    from pyiceberg.catalog import load_catalog
    from pyiceberg.schema import Schema
    from pyiceberg.types import NestedField, StringType, DecimalType, DateType, LongType
    
    catalog = load_catalog("glue_catalog", **{"type": "glue"})
    
    # Create new table if it doesn't exist
    schema = Schema(
        NestedField(1, "transaction_id", LongType(), required=True),
        NestedField(2, "customer_id", StringType(), required=True),
        NestedField(3, "amount", DecimalType(10, 2), required=True),
        NestedField(4, "transaction_date", DateType(), required=True)
    )
    
    table_name = "iceberg_examples.new_transactions"
    
    try:
        # Try to load existing table
        table = catalog.load_table(table_name)
        operation = "loaded_existing"
    except:
        # Create new table if it doesn't exist
        table = catalog.create_table(
            identifier=table_name,
            schema=schema,
            location=f"s3://{os.environ['S3_BUCKET_NAME']}/warehouse/new_transactions/"
        )
        operation = "created_new"
    
    # Get table metadata
    metadata = {
        "table_name": table_name,
        "operation": operation,
        "schema_fields": len(table.schema().fields),
        "location": str(table.location()),
        "current_snapshot_id": table.current_snapshot().snapshot_id if table.current_snapshot() else None
    }
    
    return {
        "statusCode": 200,
        "body": metadata
    }

def handler(event, context):
    """Main Lambda handler routing to different functions"""
    handler_type = event.get('handler_type', 'basic')
    
    if handler_type == 's3_event':
        return s3_event_handler(event, context)
    elif handler_type == 'table_management':
        return table_management_handler(event, context)
    else:
        # Default to basic operations from previous function
        return basic_handler(event, context)
EOF
```

### Deploy Advanced Function

```bash
# Update Lambda function with advanced code
cp advanced_lambda_function.py lambda_function.py

# Rebuild and deploy
docker build -t ${REPO_NAME} .
docker tag ${REPO_NAME}:latest ${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com/${REPO_NAME}:latest
docker push ${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com/${REPO_NAME}:latest

# Update Lambda function
aws lambda update-function-code \
    --function-name ${LAMBDA_FUNCTION_NAME} \
    --image-uri ${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com/${REPO_NAME}:latest
```

## Step 7: Set Up Event-Driven Processing

### Configure S3 Event Trigger

```bash
# Create S3 bucket notification configuration
aws s3api put-bucket-notification-configuration \
    --bucket ${S3_BUCKET_NAME} \
    --notification-configuration '{
        "LambdaConfigurations": [
            {
                "Id": "IcebergProcessing",
                "LambdaFunctionArn": "arn:aws:lambda:'${AWS_REGION}':'${AWS_ACCOUNT_ID}':function:'${LAMBDA_FUNCTION_NAME}'",
                "Events": ["s3:ObjectCreated:*"],
                "Filter": {
                    "Key": {
                        "FilterRules": [
                            {
                                "Name": "prefix",
                                "Value": "incoming-data/"
                            },
                            {
                                "Name": "suffix",
                                "Value": ".csv"
                            }
                        ]
                    }
                }
            }
        ]
    }'

# Add permission for S3 to invoke Lambda
aws lambda add-permission \
    --function-name ${LAMBDA_FUNCTION_NAME} \
    --principal s3.amazonaws.com \
    --action lambda:InvokeFunction \
    --source-arn arn:aws:s3:::${S3_BUCKET_NAME} \
    --statement-id s3-trigger
```

### Test Event-Driven Processing

```bash
# Create test CSV file
cat > test_data.csv << 'EOF'
transaction_id,customer_id,amount,transaction_date
2001,CUST_008,125.50,2025-01-25
2002,CUST_009,275.00,2025-01-25
2003,CUST_010,89.99,2025-01-25
EOF

# Upload test file to trigger Lambda
aws s3 cp test_data.csv s3://${S3_BUCKET_NAME}/incoming-data/test_data.csv

# Check Lambda logs
aws logs filter-log-events \
    --log-group-name "/aws/lambda/${LAMBDA_FUNCTION_NAME}" \
    --start-time $(date -d '5 minutes ago' +%s)000
```

## Step 8: Performance Optimization

### Memory Management for Large Datasets

```python
# Create optimized_lambda_function.py
cat > optimized_lambda_function.py << 'EOF'
import os
import json
import pandas as pd
from pyiceberg.catalog import load_catalog

def optimized_read_handler(event, context):
    """Optimized reading for large datasets"""
    catalog = load_catalog("glue_catalog", **{"type": "glue"})
    table = catalog.load_table("iceberg_examples.sales_data")
    
    # Use lazy evaluation and column pruning
    scan = table.scan(
        row_filter="transaction_date >= '2025-01-01'",
        selected_fields=["customer_id", "amount"]
    )
    
    # Process data in chunks to manage memory
    chunk_size = 10000
    total_amount = 0
    record_count = 0
    
    for batch in scan.to_arrow().to_batches(max_chunksize=chunk_size):
        df_chunk = batch.to_pandas()
        total_amount += float(df_chunk["amount"].sum())
        record_count += len(df_chunk)
    
    return {
        "statusCode": 200,
        "body": {
            "total_records": record_count,
            "total_amount": total_amount,
            "processing_method": "chunked"
        }
    }

def handler(event, context):
    operation = event.get('operation', 'optimized_read')
    
    if operation == 'optimized_read':
        return optimized_read_handler(event, context)
    else:
        return {"statusCode": 400, "body": {"error": "Unknown operation"}}
EOF
```

## Step 9: Monitoring and Logging

### Set Up CloudWatch Monitoring

```bash
# Create CloudWatch alarm for Lambda errors
aws cloudwatch put-metric-alarm \
    --alarm-name "${LAMBDA_FUNCTION_NAME}-Errors" \
    --alarm-description "Alert when Lambda function has errors" \
    --metric-name "Errors" \
    --namespace "AWS/Lambda" \
    --statistic "Sum" \
    --period 300 \
    --threshold 1 \
    --comparison-operator "GreaterThanOrEqualToThreshold" \
    --dimensions Name=FunctionName,Value=${LAMBDA_FUNCTION_NAME} \
    --evaluation-periods 1

# Create alarm for duration
aws cloudwatch put-metric-alarm \
    --alarm-name "${LAMBDA_FUNCTION_NAME}-Duration" \
    --alarm-description "Alert when Lambda function duration is high" \
    --metric-name "Duration" \
    --namespace "AWS/Lambda" \
    --statistic "Average" \
    --period 300 \
    --threshold 240000 \
    --comparison-operator "GreaterThanThreshold" \
    --dimensions Name=FunctionName,Value=${LAMBDA_FUNCTION_NAME} \
    --evaluation-periods 2
```

### View Function Metrics

```bash
# Get function metrics
aws cloudwatch get-metric-statistics \
    --namespace AWS/Lambda \
    --metric-name Invocations \
    --dimensions Name=FunctionName,Value=${LAMBDA_FUNCTION_NAME} \
    --statistics Sum \
    --start-time $(date -d '1 hour ago' -u +%Y-%m-%dT%H:%M:%S) \
    --end-time $(date -u +%Y-%m-%dT%H:%M:%S) \
    --period 300
```

## Step 10: Cleanup

### Delete Lambda Function and Resources

```bash
# Delete Lambda function
aws lambda delete-function --function-name ${LAMBDA_FUNCTION_NAME}

# Delete ECR repository
aws ecr delete-repository --repository-name pyiceberg-lambda --force

# Remove S3 bucket notification
aws s3api put-bucket-notification-configuration \
    --bucket ${S3_BUCKET_NAME} \
    --notification-configuration '{}'

# Delete IAM role and policies
aws iam detach-role-policy \
    --role-name LambdaIcebergRole \
    --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole

aws iam delete-role-policy \
    --role-name LambdaIcebergRole \
    --policy-name IcebergAccessPolicy

aws iam delete-role --role-name LambdaIcebergRole

# Clean up CloudShell files
cd ..
rm -rf pyiceberg-lambda
rm -f test_data.csv response.json

echo "Cleanup completed"
```

## Troubleshooting

### Common Issues

1. **Container Build Failures**: Ensure Docker is available in CloudShell
2. **Permission Errors**: Check IAM role permissions for Glue and S3
3. **Memory Issues**: Increase Lambda memory or use chunked processing
4. **Timeout Issues**: Increase Lambda timeout for large operations

### Diagnostic Commands

```bash
# Check function configuration
aws lambda get-function --function-name ${LAMBDA_FUNCTION_NAME}

# View recent logs
aws logs filter-log-events \
    --log-group-name "/aws/lambda/${LAMBDA_FUNCTION_NAME}" \
    --start-time $(date -d '1 hour ago' +%s)000

# Test function directly
aws lambda invoke \
    --function-name ${LAMBDA_FUNCTION_NAME} \
    --payload '{"operation": "analyze", "table_name": "sales_data"}' \
    test_response.json && cat test_response.json
```

## Best Practices

1. **Container Images**: Use container images for PyIceberg due to dependency size
2. **Memory Sizing**: Size Lambda functions appropriately (1024MB+ recommended)
3. **Error Handling**: Implement comprehensive error handling and logging
4. **Cost Optimization**: Use appropriate timeout settings and memory allocation
5. **Security**: Follow least-privilege principles for IAM roles

AWS Lambda with PyIceberg provides a powerful serverless solution for lightweight Iceberg analytics and event-driven data processing.
