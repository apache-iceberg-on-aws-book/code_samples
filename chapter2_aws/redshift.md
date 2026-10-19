# Amazon Redshift with Apache Iceberg - Complete Setup Guide

This guide provides step-by-step instructions for integrating Amazon Redshift with Apache Iceberg tables through Redshift Spectrum for federated analytics.

## Prerequisites

Complete the [Prerequisites and Setup](prerequisites.md) before starting this guide.

## Step 1: Environment Setup

### Set Environment Variables

```bash
# Set environment variables
export AWS_REGION="us-east-1"
export REDSHIFT_WORKGROUP="iceberg-workgroup"
export REDSHIFT_DATABASE="icebergdb"
export REDSHIFT_USER="iceberguser"
export S3_BUCKET_NAME="${ICEBERG_BUCKET_NAME}"
export GLUE_DATABASE_NAME="iceberg_examples"
export AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
```

### Create IAM Role for Redshift

```bash
# Create Redshift service role
aws iam create-role \
    --role-name RedshiftIcebergRole \
    --assume-role-policy-document '{
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {
                    "Service": "redshift.amazonaws.com"
                },
                "Action": "sts:AssumeRole"
            }
        ]
    }'

# Attach S3 access policy
aws iam attach-role-policy \
    --role-name RedshiftIcebergRole \
    --policy-arn arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess

# Attach Glue Data Catalog access policy
aws iam attach-role-policy \
    --role-name RedshiftIcebergRole \
    --policy-arn arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole

# Create custom policy for Iceberg operations
aws iam put-role-policy \
    --role-name RedshiftIcebergRole \
    --policy-name IcebergS3Policy \
    --policy-document '{
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "s3:GetObject",
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

## Step 2: Set Up Redshift Serverless

### Create Redshift Serverless Namespace

```bash
# Create Redshift Serverless namespace
aws redshift-serverless create-namespace \
    --namespace-name ${REDSHIFT_DATABASE} \
    --admin-username ${REDSHIFT_USER} \
    --admin-user-password 'SecurePassword123!' \
    --iam-roles arn:aws:iam::${AWS_ACCOUNT_ID}:role/RedshiftIcebergRole

echo "Redshift namespace created: ${REDSHIFT_DATABASE}"
```

### Create Redshift Serverless Workgroup

```bash
# Create Redshift Serverless workgroup
aws redshift-serverless create-workgroup \
    --workgroup-name ${REDSHIFT_WORKGROUP} \
    --namespace-name ${REDSHIFT_DATABASE} \
    --base-capacity 8 \
    --enhanced-vpc-routing false \
    --publicly-accessible true

# Wait for workgroup to be available
echo "Waiting for workgroup to be available..."
while true; do
    STATUS=$(aws redshift-serverless get-workgroup \
        --workgroup-name ${REDSHIFT_WORKGROUP} \
        --query 'workgroup.status' \
        --output text)
    echo "Workgroup status: $STATUS"
    if [ "$STATUS" = "AVAILABLE" ]; then
        break
    fi
    sleep 30
done

echo "Redshift Serverless workgroup ${REDSHIFT_WORKGROUP} is ready"
```

## Step 3: Create External Schema for Iceberg

### Using Redshift Data API

```bash
# Create external schema using Data API
aws redshift-data execute-statement \
    --workgroup-name ${REDSHIFT_WORKGROUP} \
    --database dev \
    --sql "
CREATE EXTERNAL SCHEMA IF NOT EXISTS iceberg_spectrum
FROM DATA CATALOG
DATABASE '${GLUE_DATABASE_NAME}'
IAM_ROLE 'arn:aws:iam::${AWS_ACCOUNT_ID}:role/RedshiftIcebergRole';"

# Get statement ID for monitoring
export STATEMENT_ID=$(aws redshift-data execute-statement \
    --workgroup-name ${REDSHIFT_WORKGROUP} \
    --database dev \
    --sql "SELECT 1;" \
    --query 'Id' \
    --output text)

echo "External schema creation initiated"
```

### Verify External Schema

```bash
# Verify external schema creation
aws redshift-data execute-statement \
    --workgroup-name ${REDSHIFT_WORKGROUP} \
    --database dev \
    --sql "
SELECT schemaname, tablename, location 
FROM SVV_EXTERNAL_TABLES 
WHERE schemaname = 'iceberg_spectrum';"
```

## Step 4: Query Iceberg Tables

### Basic Queries

```bash
# Query Iceberg table through Spectrum
aws redshift-data execute-statement \
    --workgroup-name ${REDSHIFT_WORKGROUP} \
    --database dev \
    --sql "
SELECT 
    customer_id,
    SUM(amount) as total_spent,
    COUNT(*) as transaction_count,
    AVG(amount) as avg_transaction
FROM iceberg_spectrum.sales_data
WHERE transaction_date >= '2025-01-01'
GROUP BY customer_id
ORDER BY total_spent DESC
LIMIT 10;"

# Get query results
export QUERY_ID=$(aws redshift-data list-statements \
    --workgroup-name ${REDSHIFT_WORKGROUP} \
    --query 'Statements[0].Id' \
    --output text)

# Wait for query completion and get results
aws redshift-data get-statement-result --id ${QUERY_ID}
```

### Advanced Analytics Queries

```bash
# Complex analytical query
aws redshift-data execute-statement \
    --workgroup-name ${REDSHIFT_WORKGROUP} \
    --database dev \
    --sql "
WITH monthly_sales AS (
    SELECT 
        DATE_TRUNC('month', transaction_date) as month,
        customer_id,
        SUM(amount) as monthly_total,
        COUNT(*) as monthly_transactions
    FROM iceberg_spectrum.sales_data
    WHERE transaction_date >= '2025-01-01'
    GROUP BY DATE_TRUNC('month', transaction_date), customer_id
),
customer_metrics AS (
    SELECT 
        customer_id,
        COUNT(DISTINCT month) as active_months,
        SUM(monthly_total) as total_spent,
        AVG(monthly_total) as avg_monthly_spend,
        MAX(monthly_total) as max_monthly_spend
    FROM monthly_sales
    GROUP BY customer_id
)
SELECT 
    customer_id,
    active_months,
    total_spent,
    avg_monthly_spend,
    max_monthly_spend,
    CASE 
        WHEN total_spent > 5000 THEN 'HIGH_VALUE'
        WHEN total_spent > 1000 THEN 'MEDIUM_VALUE'
        ELSE 'LOW_VALUE'
    END as customer_segment
FROM customer_metrics
WHERE active_months >= 2
ORDER BY total_spent DESC;"
```

## Step 5: Table Structure Analysis

### Analyze Iceberg Metadata

```bash
# Get table columns information
aws redshift-data execute-statement \
    --workgroup-name ${REDSHIFT_WORKGROUP} \
    --database dev \
    --sql "
SELECT 
    columnname,
    external_type,
    is_nullable,
    columnnum
FROM SVV_EXTERNAL_COLUMNS
WHERE schemaname = 'iceberg_spectrum'
AND tablename = 'sales_data'
ORDER BY columnnum;"

# Analyze table statistics
aws redshift-data execute-statement \
    --workgroup-name ${REDSHIFT_WORKGROUP} \
    --database dev \
    --sql "
SELECT 
    COUNT(*) as total_records,
    MIN(transaction_date) as earliest_date,
    MAX(transaction_date) as latest_date,
    COUNT(DISTINCT customer_id) as unique_customers,
    SUM(amount) as total_amount,
    AVG(amount) as avg_amount
FROM iceberg_spectrum.sales_data;"
```

### Partition Analysis

```bash
# Analyze data distribution across partitions
aws redshift-data execute-statement \
    --workgroup-name ${REDSHIFT_WORKGROUP} \
    --database dev \
    --sql "
SELECT 
    DATE_TRUNC('day', transaction_date) as partition_date,
    COUNT(*) as records_per_partition,
    SUM(amount) as amount_per_partition,
    MIN(amount) as min_amount,
    MAX(amount) as max_amount,
    AVG(amount) as avg_amount
FROM iceberg_spectrum.sales_data
GROUP BY DATE_TRUNC('day', transaction_date)
ORDER BY partition_date;"
```

## Step 6: Performance Optimization

### Query Optimization Techniques

```bash
# Use column projection for better performance
aws redshift-data execute-statement \
    --workgroup-name ${REDSHIFT_WORKGROUP} \
    --database dev \
    --sql "
SELECT customer_id, amount 
FROM iceberg_spectrum.sales_data
WHERE transaction_date = '2025-01-20';"

# Predicate pushdown with partition elimination
aws redshift-data execute-statement \
    --workgroup-name ${REDSHIFT_WORKGROUP} \
    --database dev \
    --sql "
SELECT 
    COUNT(*) as transaction_count,
    AVG(amount) as avg_amount
FROM iceberg_spectrum.sales_data
WHERE transaction_date BETWEEN '2025-01-20' AND '2025-01-21'
AND amount > 200;"
```

### Join Optimization

```bash
# Create a Redshift table for joining with Iceberg data
aws redshift-data execute-statement \
    --workgroup-name ${REDSHIFT_WORKGROUP} \
    --database dev \
    --sql "
CREATE TABLE customer_segments (
    customer_id VARCHAR(50),
    segment VARCHAR(20),
    join_date DATE
);

INSERT INTO customer_segments VALUES 
    ('CUST_001', 'PREMIUM', '2024-01-01'),
    ('CUST_002', 'STANDARD', '2024-02-15'),
    ('CUST_003', 'PREMIUM', '2024-03-10'),
    ('CUST_004', 'STANDARD', '2024-04-20');"

# Perform federated join between Redshift and Iceberg data
aws redshift-data execute-statement \
    --workgroup-name ${REDSHIFT_WORKGROUP} \
    --database dev \
    --sql "
SELECT 
    cs.customer_id,
    cs.segment,
    COUNT(sd.transaction_id) as transaction_count,
    SUM(sd.amount) as total_spent,
    AVG(sd.amount) as avg_transaction
FROM customer_segments cs
LEFT JOIN iceberg_spectrum.sales_data sd 
    ON cs.customer_id = sd.customer_id
WHERE sd.transaction_date >= '2025-01-01'
GROUP BY cs.customer_id, cs.segment
ORDER BY total_spent DESC;"
```

## Step 7: Schema Evolution Handling

### Query Tables with Evolved Schemas

```bash
# Query with schema evolution - handles missing columns gracefully
aws redshift-data execute-statement \
    --workgroup-name ${REDSHIFT_WORKGROUP} \
    --database dev \
    --sql "
SELECT 
    customer_id,
    amount,
    transaction_date,
    -- Handle potentially missing columns from schema evolution
    CASE 
        WHEN EXISTS (
            SELECT 1 FROM SVV_EXTERNAL_COLUMNS 
            WHERE schemaname = 'iceberg_spectrum' 
            AND tablename = 'sales_data'
            AND columnname = 'payment_method'
        ) THEN 'Available'
        ELSE 'Not Available'
    END as payment_method_status
FROM iceberg_spectrum.sales_data
LIMIT 5;"
```

### Monitor Schema Changes

```bash
# Monitor table schema over time
aws redshift-data execute-statement \
    --workgroup-name ${REDSHIFT_WORKGROUP} \
    --database dev \
    --sql "
SELECT 
    schemaname,
    tablename,
    columnname,
    external_type,
    is_nullable
FROM SVV_EXTERNAL_COLUMNS
WHERE schemaname = 'iceberg_spectrum'
AND tablename = 'sales_data'
ORDER BY columnnum;"
```

## Step 8: Data Quality and Monitoring

### Data Quality Checks

```bash
# Comprehensive data quality analysis
aws redshift-data execute-statement \
    --workgroup-name ${REDSHIFT_WORKGROUP} \
    --database dev \
    --sql "
SELECT 
    'Total Records' as metric,
    COUNT(*)::VARCHAR as value
FROM iceberg_spectrum.sales_data
UNION ALL
SELECT 
    'Null Customer IDs',
    COUNT(CASE WHEN customer_id IS NULL THEN 1 END)::VARCHAR
FROM iceberg_spectrum.sales_data
UNION ALL
SELECT 
    'Negative Amounts',
    COUNT(CASE WHEN amount < 0 THEN 1 END)::VARCHAR
FROM iceberg_spectrum.sales_data
UNION ALL
SELECT 
    'Future Dates',
    COUNT(CASE WHEN transaction_date > CURRENT_DATE THEN 1 END)::VARCHAR
FROM iceberg_spectrum.sales_data
UNION ALL
SELECT 
    'Duplicate Transaction IDs',
    (COUNT(*) - COUNT(DISTINCT transaction_id))::VARCHAR
FROM iceberg_spectrum.sales_data;"
```

### Performance Monitoring

```bash
# Monitor query performance
aws redshift-data execute-statement \
    --workgroup-name ${REDSHIFT_WORKGROUP} \
    --database dev \
    --sql "
SELECT 
    query,
    starttime,
    endtime,
    DATEDIFF(seconds, starttime, endtime) as duration_seconds,
    aborted
FROM STL_QUERY
WHERE query LIKE '%iceberg_spectrum%'
AND starttime >= DATEADD(hour, -1, GETDATE())
ORDER BY starttime DESC
LIMIT 10;"
```

## Step 9: Integration with BI Tools

### Configure JDBC Connection

```bash
# Get Redshift endpoint for BI tool configuration
aws redshift-serverless get-workgroup \
    --workgroup-name ${REDSHIFT_WORKGROUP} \
    --query 'workgroup.endpoint' \
    --output table

echo "Use this endpoint for BI tool connections:"
echo "JDBC URL: jdbc:redshift://endpoint:5439/dev"
echo "Username: ${REDSHIFT_USER}"
echo "Database: dev"
```

### Create Views for BI Tools

```bash
# Create materialized views for better BI performance
aws redshift-data execute-statement \
    --workgroup-name ${REDSHIFT_WORKGROUP} \
    --database dev \
    --sql "
CREATE MATERIALIZED VIEW customer_summary AS
SELECT 
    customer_id,
    COUNT(*) as total_transactions,
    SUM(amount) as total_spent,
    AVG(amount) as avg_transaction,
    MIN(transaction_date) as first_transaction,
    MAX(transaction_date) as last_transaction,
    DATEDIFF(day, MIN(transaction_date), MAX(transaction_date)) as customer_lifetime_days
FROM iceberg_spectrum.sales_data
GROUP BY customer_id;"

# Refresh materialized view
aws redshift-data execute-statement \
    --workgroup-name ${REDSHIFT_WORKGROUP} \
    --database dev \
    --sql "REFRESH MATERIALIZED VIEW customer_summary;"
```

## Step 10: Cost Optimization

### Monitor Spectrum Usage

```bash
# Monitor Spectrum query costs
aws redshift-data execute-statement \
    --workgroup-name ${REDSHIFT_WORKGROUP} \
    --database dev \
    --sql "
SELECT 
    DATE_TRUNC('day', starttime) as query_date,
    COUNT(*) as query_count,
    SUM(spectrum_scan_size_mb) as total_data_scanned_mb,
    AVG(spectrum_scan_size_mb) as avg_data_scanned_mb
FROM SVL_S3QUERY
WHERE starttime >= DATEADD(day, -7, GETDATE())
GROUP BY DATE_TRUNC('day', starttime)
ORDER BY query_date DESC;"
```

### Optimize Query Costs

```bash
# Use partition pruning to minimize data scanned
aws redshift-data execute-statement \
    --workgroup-name ${REDSHIFT_WORKGROUP} \
    --database dev \
    --sql "
-- Good: Uses partition pruning
SELECT customer_id, SUM(amount)
FROM iceberg_spectrum.sales_data
WHERE transaction_date = '2025-01-20'
GROUP BY customer_id;

-- Better: Also uses column selection
SELECT customer_id, amount
FROM iceberg_spectrum.sales_data
WHERE transaction_date = '2025-01-20'
AND customer_id IN ('CUST_001', 'CUST_002');"
```

## Step 11: Backup and Recovery

### Create Snapshots

```bash
# Create manual snapshot of Redshift Serverless
aws redshift-serverless create-snapshot \
    --namespace-name ${REDSHIFT_DATABASE} \
    --snapshot-name "iceberg-integration-$(date +%Y%m%d)"

# List snapshots
aws redshift-serverless list-snapshots \
    --namespace-name ${REDSHIFT_DATABASE}
```

### Export Query Results

```bash
# Export query results to S3
aws redshift-data execute-statement \
    --workgroup-name ${REDSHIFT_WORKGROUP} \
    --database dev \
    --sql "
UNLOAD ('
    SELECT 
        customer_id,
        SUM(amount) as total_spent,
        COUNT(*) as transaction_count
    FROM iceberg_spectrum.sales_data
    GROUP BY customer_id
')
TO 's3://${S3_BUCKET_NAME}/redshift-exports/customer-summary-'
IAM_ROLE 'arn:aws:iam::${AWS_ACCOUNT_ID}:role/RedshiftIcebergRole'
FORMAT AS PARQUET;"
```

## Step 12: Cleanup

### Delete Redshift Resources

```bash
# Delete workgroup
aws redshift-serverless delete-workgroup \
    --workgroup-name ${REDSHIFT_WORKGROUP}

# Delete namespace
aws redshift-serverless delete-namespace \
    --namespace-name ${REDSHIFT_DATABASE}

echo "Redshift Serverless resources deleted"
```

### Clean Up IAM Resources

```bash
# Detach policies and delete role
aws iam detach-role-policy \
    --role-name RedshiftIcebergRole \
    --policy-arn arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess

aws iam detach-role-policy \
    --role-name RedshiftIcebergRole \
    --policy-arn arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole

aws iam delete-role-policy \
    --role-name RedshiftIcebergRole \
    --policy-name IcebergS3Policy

aws iam delete-role --role-name RedshiftIcebergRole

echo "IAM resources cleaned up"
```

## Troubleshooting

### Common Issues

1. **External Schema Creation Fails**: Check IAM role permissions for Glue and S3
2. **Query Performance Issues**: Verify partition pruning and column selection
3. **Connection Issues**: Ensure workgroup is publicly accessible or configure VPC
4. **Data Not Visible**: Confirm Glue crawler has discovered Iceberg tables

### Diagnostic Queries

```bash
# Check external tables
aws redshift-data execute-statement \
    --workgroup-name ${REDSHIFT_WORKGROUP} \
    --database dev \
    --sql "
SELECT schemaname, tablename, location, input_format
FROM SVV_EXTERNAL_TABLES
WHERE schemaname = 'iceberg_spectrum';"

# Check recent queries
aws redshift-data execute-statement \
    --workgroup-name ${REDSHIFT_WORKGROUP} \
    --database dev \
    --sql "
SELECT 
    query,
    starttime,
    endtime,
    aborted,
    substring(querytxt, 1, 100) as query_text
FROM STL_QUERY
WHERE starttime >= DATEADD(hour, -1, GETDATE())
ORDER BY starttime DESC
LIMIT 5;"
```

### Performance Diagnostics

```bash
# Analyze Spectrum scan performance
aws redshift-data execute-statement \
    --workgroup-name ${REDSHIFT_WORKGROUP} \
    --database dev \
    --sql "
SELECT 
    query,
    segment,
    node,
    spectrum_scan_size_mb,
    spectrum_scan_row_count,
    spectrum_scan_duration_ms
FROM SVL_S3QUERY
WHERE starttime >= DATEADD(hour, -1, GETDATE())
ORDER BY spectrum_scan_duration_ms DESC
LIMIT 10;"
```

## Best Practices

1. **Query Optimization**: Use partition pruning and column selection to minimize data scanned
2. **Schema Design**: Design Iceberg partitions to align with common query patterns
3. **Cost Management**: Monitor Spectrum usage and optimize queries for cost efficiency
4. **Security**: Use least-privilege IAM roles and secure network configurations
5. **Performance**: Create materialized views for frequently accessed aggregations

Amazon Redshift's integration with Iceberg through Spectrum enables powerful federated analytics, allowing you to combine the performance of Redshift with the flexibility of your data lake.
