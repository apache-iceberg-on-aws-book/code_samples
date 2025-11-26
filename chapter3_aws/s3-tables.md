# Amazon S3 Tables with Apache Iceberg - Complete Setup Guide

This guide provides step-by-step instructions for setting up and working with Amazon S3 Tables, the first storage class specifically optimized for tabular data with built-in Apache Iceberg support.

## Prerequisites

Complete the [Prerequisites and Setup](prerequisites.md) before starting this guide.

## Step 1: Create S3 Tables Infrastructure

### Create Table Bucket

```bash
# Set environment variables
export AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
export AWS_REGION=us-east-1
export TABLE_BUCKET_NAME="analytics-tables-bucket-${AWS_ACCOUNT_ID}"

# Create S3 Tables bucket
aws s3tables create-table-bucket \
    --name ${TABLE_BUCKET_NAME} \
    --region ${AWS_REGION}

# Verify table bucket creation
aws s3tables list-table-buckets --region ${AWS_REGION}
```

### Create Namespace

```bash
# Create a namespace within the table bucket
aws s3tables create-namespace \
    --table-bucket-arn arn:aws:s3tables:${AWS_REGION}:${AWS_ACCOUNT_ID}:bucket/${TABLE_BUCKET_NAME} \
    --namespace tutorial

# Verify namespace creation
aws s3tables list-namespaces \
    --table-bucket-arn arn:aws:s3tables:${AWS_REGION}:${AWS_ACCOUNT_ID}:bucket/${TABLE_BUCKET_NAME}
```

## Step 2: Create Your First S3 Table

### Create Table

```bash
# Create an S3 table with Iceberg format
aws s3tables create-table \
    --table-bucket-arn arn:aws:s3tables:${AWS_REGION}:${AWS_ACCOUNT_ID}:bucket/${TABLE_BUCKET_NAME} \
    --namespace tutorial \
    --name customer_transactions \
    --format ICEBERG

# Verify table creation
aws s3tables get-table \
    --table-bucket-arn arn:aws:s3tables:${AWS_REGION}:${AWS_ACCOUNT_ID}:bucket/${TABLE_BUCKET_NAME} \
    --namespace tutorial \
    --name customer_transactions
```

### Basic Maintenance Configuration

```bash
# Basic maintenance configuration (optional)
aws s3tables put-table-maintenance-configuration \
    --table-bucket-arn arn:aws:s3tables:${AWS_REGION}:${AWS_ACCOUNT_ID}:bucket/${TABLE_BUCKET_NAME} \
    --namespace tutorial \
    --name customer_transactions \
    --configuration '{
        "compaction": {
            "targetFileSizeMB": 256
        }
    }'
```

> **📖 Production Operations**: For comprehensive S3 Tables optimization configuration, cost analysis, and advanced maintenance settings, see [Chapter 06 - S3 Tables Optimization Configuration](../../chapter06/instructions/s3-tables-optimization-configuration.md).

## Step 3: Set Up Athena Integration

### Create Federated Catalog

```bash
# Create S3 Tables federated catalog in Glue
aws glue create-catalog \
    --name s3tablescatalog \
    --catalog-input '{
        "FederatedCatalog": {
            "Identifier": "arn:aws:s3tables:'${AWS_REGION}':'${AWS_ACCOUNT_ID}':bucket/*",
            "ConnectionName": "aws:s3tables"
        }
    }'
```

### Configure Lake Formation Permissions

```bash
# Grant database permissions
aws lakeformation grant-permissions \
    --principal DataLakePrincipalIdentifier=arn:aws:iam::${AWS_ACCOUNT_ID}:user/$(aws sts get-caller-identity --query 'Arn' --output text | cut -d'/' -f2) \
    --resource '{
        "Database": {
            "CatalogId": "'${AWS_ACCOUNT_ID}':s3tablescatalog/'${TABLE_BUCKET_NAME}'",
            "Name": "tutorial"
        }
    }' \
    --permissions DESCRIBE CREATE_TABLE

# Grant table permissions
aws lakeformation grant-permissions \
    --principal DataLakePrincipalIdentifier=arn:aws:iam::${AWS_ACCOUNT_ID}:user/$(aws sts get-caller-identity --query 'Arn' --output text | cut -d'/' -f2) \
    --resource '{
        "Table": {
            "CatalogId": "'${AWS_ACCOUNT_ID}':s3tablescatalog/'${TABLE_BUCKET_NAME}'",
            "DatabaseName": "tutorial",
            "Name": "customer_transactions"
        }
    }' \
    --permissions SELECT DESCRIBE INSERT ALTER
```

## Step 4: Create Table Schema and Add Data

### Using Athena Console (Recommended)

1. Open Athena Console
2. Select catalog: `awsdatacatalog/s3tablescatalog/${TABLE_BUCKET_NAME}`
3. Select database: `tutorial`
4. Run the following SQL:

```sql
-- Create table with schema
CREATE TABLE `tutorial`.customer_transactions (
    transaction_id bigint,
    customer_id string,
    amount decimal(10,2),
    transaction_date date
)
PARTITIONED BY (month(transaction_date))
TBLPROPERTIES ('table_type' = 'iceberg');

-- Insert sample data
INSERT INTO customer_transactions VALUES
    (1001, 'CUST_001', 250.00, DATE '2025-01-20'),
    (1002, 'CUST_002', 175.50, DATE '2025-01-20'),
    (1003, 'CUST_003', 89.99, DATE '2025-01-21'),
    (1004, 'CUST_001', 320.75, DATE '2025-01-21'),
    (1005, 'CUST_004', 156.25, DATE '2025-01-22');
```

### Using AWS CLI

```bash
# Set up Athena result location
export ATHENA_RESULT_LOCATION="s3://${ICEBERG_BUCKET_NAME}/athena-results/"

# Create table schema
aws athena start-query-execution \
    --query-string "CREATE TABLE \`tutorial\`.customer_transactions (
        transaction_id bigint,
        customer_id string,
        amount decimal(10,2),
        transaction_date date
    ) PARTITIONED BY (month(transaction_date))
    TBLPROPERTIES ('table_type' = 'iceberg');" \
    --query-execution-context Database=tutorial,Catalog=awsdatacatalog/s3tablescatalog/${TABLE_BUCKET_NAME} \
    --result-configuration OutputLocation=${ATHENA_RESULT_LOCATION} \
    --work-group primary

# Insert sample data
aws athena start-query-execution \
    --query-string "INSERT INTO customer_transactions VALUES
        (1001, 'CUST_001', 250.00, DATE '2025-01-20'),
        (1002, 'CUST_002', 175.50, DATE '2025-01-20'),
        (1003, 'CUST_003', 89.99, DATE '2025-01-21'),
        (1004, 'CUST_001', 320.75, DATE '2025-01-21'),
        (1005, 'CUST_004', 156.25, DATE '2025-01-22');" \
    --query-execution-context Database=tutorial,Catalog=awsdatacatalog/s3tablescatalog/${TABLE_BUCKET_NAME} \
    --result-configuration OutputLocation=${ATHENA_RESULT_LOCATION} \
    --work-group primary
```

## Step 5: Query and Analyze Data

### Basic Queries

```sql
-- Count total transactions
SELECT COUNT(*) as total_transactions FROM customer_transactions;

-- Analyze customer spending patterns
SELECT 
    customer_id,
    COUNT(*) as transaction_count,
    SUM(amount) as total_spent,
    AVG(amount) as avg_transaction_amount
FROM customer_transactions
GROUP BY customer_id
ORDER BY total_spent DESC;

-- Daily transaction summary
SELECT 
    transaction_date,
    COUNT(*) as daily_transactions,
    SUM(amount) as daily_revenue,
    AVG(amount) as avg_transaction_amount
FROM customer_transactions
GROUP BY transaction_date
ORDER BY transaction_date;
```

### Advanced Iceberg Features

```sql
-- View table snapshots
SELECT snapshot_id, committed_at, operation
FROM "tutorial"."customer_transactions$snapshots"
ORDER BY committed_at DESC;

-- Check table files and optimization status
SELECT 
    COUNT(*) as file_count,
    SUM(file_size_in_bytes) as total_size_bytes,
    AVG(file_size_in_bytes) as avg_file_size_bytes
FROM "tutorial"."customer_transactions$files";

-- Time travel query (use actual timestamp from snapshots)
SELECT * FROM "tutorial"."customer_transactions"
FOR TIMESTAMP AS OF TIMESTAMP '2025-01-21 10:00:00'
WHERE customer_id = 'CUST_001';
```

## Step 6: Monitor Table Optimization

### Check Optimization Status

```bash
# Monitor table optimization status
aws s3tables get-table \
    --table-bucket-arn arn:aws:s3tables:${AWS_REGION}:${AWS_ACCOUNT_ID}:bucket/${TABLE_BUCKET_NAME} \
    --namespace tutorial \
    --name customer_transactions \
    --query 'maintenanceConfiguration'
```

### View Optimization Metrics

S3 Tables automatically optimizes your table. You can monitor the optimization through:

1. **CloudWatch Metrics**: S3 Tables publishes metrics about optimization operations
2. **Table Metadata**: Query Iceberg metadata tables to see file counts and sizes
3. **Query Performance**: Monitor query execution times to see optimization benefits

## Step 7: Set Up Access Control

### Table-Level Policies

```bash
# Create table-level access policy
aws s3tables put-table-policy \
    --table-bucket-arn arn:aws:s3tables:${AWS_REGION}:${AWS_ACCOUNT_ID}:bucket/${TABLE_BUCKET_NAME} \
    --namespace tutorial \
    --name customer_transactions \
    --resource-policy '{
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"AWS": "arn:aws:iam::'${AWS_ACCOUNT_ID}':role/AnalyticsRole"},
                "Action": ["s3tables:GetTableData", "s3tables:GetTableMetadata"],
                "Resource": "*"
            }
        ]
    }'
```

## Step 8: Performance Testing

### Load Test Data

```sql
-- Create larger dataset for performance testing
INSERT INTO customer_transactions
SELECT 
    transaction_id + 10000,
    CONCAT('CUST_', LPAD(CAST((transaction_id % 1000) AS STRING), 3, '0')),
    amount * (1 + (transaction_id % 10) * 0.1),
    transaction_date + INTERVAL (transaction_id % 30) DAY
FROM customer_transactions
CROSS JOIN (SELECT 1 as dummy UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5);
```

### Performance Queries

```sql
-- Test query performance with larger dataset
SELECT 
    customer_id,
    COUNT(*) as transaction_count,
    SUM(amount) as total_amount,
    AVG(amount) as avg_amount,
    MIN(transaction_date) as first_transaction,
    MAX(transaction_date) as last_transaction
FROM customer_transactions
WHERE transaction_date >= DATE '2025-01-01'
GROUP BY customer_id
HAVING COUNT(*) > 5
ORDER BY total_amount DESC
LIMIT 100;
```

## Step 9: Integration with Other Services

### EMR Serverless Integration

```python
# Configure Spark to use S3 Tables catalog
spark = SparkSession.builder \
    .config("spark.sql.catalog.s3tables", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.s3tables.type", "rest") \
    .config("spark.sql.catalog.s3tables.uri", f"https://s3tables.{AWS_REGION}.amazonaws.com") \
    .getOrCreate()

# Query S3 Tables from Spark
df = spark.sql("SELECT * FROM s3tables.tutorial.customer_transactions")
df.show()
```

### Redshift Integration

```sql
-- Create external schema in Redshift pointing to S3 Tables
CREATE EXTERNAL SCHEMA s3tables_schema
FROM DATA CATALOG
DATABASE 'tutorial'
IAM_ROLE 'arn:aws:iam::account:role/RedshiftRole'
CATALOG_ID 'account:s3tablescatalog/table-bucket-name';

-- Query from Redshift
SELECT customer_id, SUM(amount) as total_spent
FROM s3tables_schema.customer_transactions
GROUP BY customer_id;
```

## Step 10: Cleanup

### Delete Table and Resources

```bash
# Delete table
aws s3tables delete-table \
    --table-bucket-arn arn:aws:s3tables:${AWS_REGION}:${AWS_ACCOUNT_ID}:bucket/${TABLE_BUCKET_NAME} \
    --namespace tutorial \
    --name customer_transactions

# Delete namespace
aws s3tables delete-namespace \
    --table-bucket-arn arn:aws:s3tables:${AWS_REGION}:${AWS_ACCOUNT_ID}:bucket/${TABLE_BUCKET_NAME} \
    --namespace tutorial

# Delete table bucket
aws s3tables delete-table-bucket \
    --name ${TABLE_BUCKET_NAME} \
    --region ${AWS_REGION}

# Delete federated catalog
aws glue delete-catalog --name s3tablescatalog
```

## Troubleshooting

### Common Issues

1. **Permission Errors**: Ensure Lake Formation permissions are correctly configured
2. **Catalog Access Issues**: Verify the federated catalog is properly created
3. **Query Failures**: Check that you're using the correct catalog format in Athena

### Verification Commands

```bash
# Verify table bucket exists
aws s3tables get-table-bucket --name ${TABLE_BUCKET_NAME}

# Check table status
aws s3tables get-table \
    --table-bucket-arn arn:aws:s3tables:${AWS_REGION}:${AWS_ACCOUNT_ID}:bucket/${TABLE_BUCKET_NAME} \
    --namespace tutorial \
    --name customer_transactions

# Test Athena query
aws athena start-query-execution \
    --query-string "SELECT COUNT(*) FROM customer_transactions;" \
    --query-execution-context Database=tutorial,Catalog=awsdatacatalog/s3tablescatalog/${TABLE_BUCKET_NAME} \
    --result-configuration OutputLocation=${ATHENA_RESULT_LOCATION}
```

## Cost Optimization Tips

1. **Monitor Usage**: Use CloudWatch to track request patterns and optimize accordingly
2. **Right-size Tables**: Use appropriate partitioning strategies for your query patterns
3. **Leverage Automatic Optimization**: Let S3 Tables handle compaction automatically
4. **Query Optimization**: Use partition pruning and column selection to minimize costs

S3 Tables provides a fully managed Iceberg experience with automatic optimization, making it ideal for organizations that want the benefits of Iceberg without operational overhead.
