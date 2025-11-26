# Trino on Amazon EMR with Apache Iceberg - Complete Setup Guide

This guide provides step-by-step instructions for setting up Trino on Amazon EMR with Apache Iceberg support, from cluster creation to advanced analytics.

## Prerequisites

Complete the [Prerequisites and Setup](prerequisites.md) before starting this guide.

## Step 1: Set Up Environment Variables

```bash
# Set environment variables
export AWS_REGION="us-east-1"
export EMR_CLUSTER_NAME="trino-iceberg-cluster"
export EMR_LOG_URI="s3://${ICEBERG_BUCKET_NAME}/emr-logs/"
export EMR_SERVICE_ROLE="EMR_DefaultRole"
export EMR_EC2_INSTANCE_PROFILE="EMR_EC2_DefaultRole"
export EMR_KEY_NAME="your-ec2-key-pair"  # Replace with your key pair name
export EMR_SUBNET_ID="subnet-xxxxxxxxx"  # Replace with your subnet ID
```

## Step 2: Create IAM Roles

### Create EMR Service Role

```bash
# Create EMR service role (if not exists)
aws iam create-role \
    --role-name EMR_DefaultRole \
    --assume-role-policy-document '{
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
    }' || echo "Role may already exist"

# Attach EMR service policy
aws iam attach-role-policy \
    --role-name EMR_DefaultRole \
    --policy-arn arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceRole
```

### Create EMR EC2 Instance Profile

```bash
# Create EMR EC2 instance role
aws iam create-role \
    --role-name EMR_EC2_DefaultRole \
    --assume-role-policy-document '{
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {
                    "Service": "ec2.amazonaws.com"
                },
                "Action": "sts:AssumeRole"
            }
        ]
    }' || echo "Role may already exist"

# Attach EC2 instance profile policy
aws iam attach-role-policy \
    --role-name EMR_EC2_DefaultRole \
    --policy-arn arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceforEC2Role

# Create instance profile
aws iam create-instance-profile --instance-profile-name EMR_EC2_DefaultRole || echo "Profile may already exist"
aws iam add-role-to-instance-profile \
    --instance-profile-name EMR_EC2_DefaultRole \
    --role-name EMR_EC2_DefaultRole || echo "Role may already be added"
```

## Step 3: Create EMR Cluster with Trino

### Get Default VPC and Subnet

```bash
# Get default VPC
export VPC_ID=$(aws ec2 describe-vpcs --filters "Name=is-default,Values=true" --query "Vpcs[0].VpcId" --output text)

# Get a public subnet from the default VPC
export EMR_SUBNET_ID=$(aws ec2 describe-subnets \
    --filters "Name=vpc-id,Values=${VPC_ID}" "Name=default-for-az,Values=true" \
    --query "Subnets[0].SubnetId" \
    --output text)

echo "Using VPC: ${VPC_ID}"
echo "Using Subnet: ${EMR_SUBNET_ID}"
```

### Create EMR Cluster

```bash
# Create EMR cluster with Trino and Iceberg configuration
aws emr create-cluster \
    --name "${EMR_CLUSTER_NAME}" \
    --release-label emr-7.10.0 \
    --applications Name=Trino \
    --instance-type m5.xlarge \
    --instance-count 3 \
    --service-role ${EMR_SERVICE_ROLE} \
    --ec2-attributes InstanceProfile=${EMR_EC2_INSTANCE_PROFILE},KeyName=${EMR_KEY_NAME},SubnetId=${EMR_SUBNET_ID} \
    --log-uri ${EMR_LOG_URI} \
    --configurations '[
        {
            "Classification": "trino-connector-iceberg",
            "Properties": {
                "connector.name": "iceberg",
                "iceberg.catalog.type": "glue"
            }
        }
    ]'

# Get cluster ID
export CLUSTER_ID=$(aws emr list-clusters \
    --cluster-states STARTING BOOTSTRAPPING RUNNING \
    --query 'Clusters[?Name==`'${EMR_CLUSTER_NAME}'`].Id' \
    --output text)

echo "Cluster ID: ${CLUSTER_ID}"
```

### Wait for Cluster to be Ready

```bash
# Wait for cluster to be ready
echo "Waiting for cluster to be ready..."
aws emr wait cluster-running --cluster-id ${CLUSTER_ID}
echo "Cluster is ready!"

# Get master node DNS
export MASTER_DNS=$(aws emr describe-cluster \
    --cluster-id ${CLUSTER_ID} \
    --query 'Cluster.MasterPublicDnsName' \
    --output text)

echo "Master node: ${MASTER_DNS}"
```

## Step 4: Connect to Trino

### SSH to Master Node

```bash
# Connect to master node
ssh -i ~/.ssh/${EMR_KEY_NAME}.pem hadoop@${MASTER_DNS}
```

### Connect to Trino CLI

```bash
# On the master node, connect to Trino
trino-cli --server localhost:8889 --catalog iceberg --schema iceberg_examples
```

### Verify Connection

```sql
-- Check available catalogs
SHOW CATALOGS;

-- Check available schemas in Iceberg catalog
SHOW SCHEMAS;

-- Check tables in iceberg_examples schema
SHOW TABLES;
```

## Step 5: Create and Populate Tables

### Create Iceberg Table

```sql
-- Create Iceberg table with partitioning
CREATE TABLE iceberg_examples.trino_orders (
    order_id bigint,
    customer_id varchar,
    order_amount decimal(12,2),
    order_date date,
    region varchar
) WITH (
    format = 'PARQUET',
    partitioning = ARRAY['region', 'bucket(order_id, 16)'],
    location = 's3://your-bucket-name/iceberg-tables/trino_orders/'
);
```

### Insert Sample Data

```sql
-- Insert sample data
INSERT INTO iceberg_examples.trino_orders VALUES 
    (1001, 'CUST_A001', 1250.00, DATE '2025-01-20', 'US-EAST'),
    (1002, 'CUST_B002', 875.50, DATE '2025-01-20', 'US-WEST'),
    (1003, 'CUST_C003', 2100.75, DATE '2025-01-21', 'US-EAST'),
    (1004, 'CUST_D004', 450.25, DATE '2025-01-21', 'US-WEST'),
    (1005, 'CUST_E005', 1875.00, DATE '2025-01-22', 'EU-WEST');

-- Verify data insertion
SELECT COUNT(*) as total_orders FROM iceberg_examples.trino_orders;

-- View sample data
SELECT * FROM iceberg_examples.trino_orders ORDER BY order_id;
```

### Create Table from Existing Data

```sql
-- Create table using CREATE TABLE AS SELECT (CTAS)
CREATE TABLE iceberg_examples.high_value_orders
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['order_date'],
    location = 's3://your-bucket-name/iceberg-tables/high_value_orders/'
) AS
SELECT 
    order_id,
    customer_id,
    order_amount,
    order_date,
    region,
    'HIGH_VALUE' as order_category
FROM iceberg_examples.trino_orders
WHERE order_amount > 1000;

-- Verify CTAS result
SELECT * FROM iceberg_examples.high_value_orders;
```

## Step 6: ACID Operations

### Update Operations

```sql
-- Simple update
UPDATE iceberg_examples.trino_orders
SET order_amount = order_amount * 1.05
WHERE region = 'US-EAST' AND order_date >= DATE '2025-01-20';

-- Complex update with subquery
UPDATE iceberg_examples.trino_orders
SET order_amount = (
    SELECT AVG(order_amount) * 1.1 
    FROM iceberg_examples.trino_orders o2 
    WHERE o2.region = trino_orders.region
)
WHERE order_date = DATE '2025-01-20';

-- Verify updates
SELECT region, AVG(order_amount) as avg_amount
FROM iceberg_examples.trino_orders
GROUP BY region;
```

### Delete Operations

```sql
-- Delete with simple condition
DELETE FROM iceberg_examples.trino_orders 
WHERE order_amount < 500;

-- Delete with complex condition
DELETE FROM iceberg_examples.trino_orders 
WHERE customer_id IN (
    SELECT customer_id 
    FROM iceberg_examples.customer_blacklist 
    WHERE status = 'INACTIVE'
);

-- Verify deletions
SELECT COUNT(*) as remaining_orders 
FROM iceberg_examples.trino_orders;
```

### MERGE Operations

```sql
-- Create staging table for MERGE
CREATE TABLE iceberg_examples.staging_orders (
    order_id bigint,
    customer_id varchar,
    order_amount decimal(12,2),
    order_date date,
    region varchar
) WITH (
    format = 'PARQUET',
    location = 's3://your-bucket-name/iceberg-tables/staging_orders/'
);

-- Insert staging data
INSERT INTO iceberg_examples.staging_orders VALUES 
    (1001, 'CUST_A001', 1300.00, DATE '2025-01-20', 'US-EAST'),  -- Update existing
    (1006, 'CUST_F006', 950.00, DATE '2025-01-23', 'EU-WEST'),   -- Insert new
    (1007, 'CUST_G007', 1150.00, DATE '2025-01-23', 'US-CENTRAL'); -- Insert new

-- Perform MERGE operation
MERGE INTO iceberg_examples.trino_orders AS target
USING iceberg_examples.staging_orders AS source
ON target.order_id = source.order_id
WHEN MATCHED THEN
    UPDATE SET 
        order_amount = source.order_amount,
        region = source.region
WHEN NOT MATCHED THEN
    INSERT (order_id, customer_id, order_amount, order_date, region)
    VALUES (source.order_id, source.customer_id, source.order_amount, source.order_date, source.region);

-- Verify MERGE results
SELECT * FROM iceberg_examples.trino_orders 
WHERE order_id IN (1001, 1006, 1007) 
ORDER BY order_id;
```

## Step 7: Time Travel Queries

### View Table Snapshots

```sql
-- View table snapshots
SELECT 
    snapshot_id, 
    committed_at, 
    operation, 
    summary
FROM iceberg_examples."trino_orders$snapshots"
ORDER BY committed_at DESC
LIMIT 5;
```

### Time Travel Queries

```sql
-- Query data at specific timestamp (replace with actual timestamp)
SELECT 
    region,
    COUNT(*) as order_count,
    SUM(order_amount) as total_amount
FROM iceberg_examples.trino_orders 
FOR TIMESTAMP AS OF TIMESTAMP '2025-01-21 15:30:00'
GROUP BY region
ORDER BY total_amount DESC;

-- Query data at specific snapshot (replace with actual snapshot ID)
SELECT COUNT(*) as order_count
FROM iceberg_examples.trino_orders 
FOR VERSION AS OF 8745692581473829164;

-- Compare current vs historical data
WITH current_totals AS (
    SELECT region, SUM(order_amount) as current_total
    FROM iceberg_examples.trino_orders
    GROUP BY region
),
historical_totals AS (
    SELECT region, SUM(order_amount) as historical_total
    FROM iceberg_examples.trino_orders 
    FOR TIMESTAMP AS OF TIMESTAMP '2025-01-20 00:00:00'
    GROUP BY region
)
SELECT 
    c.region,
    c.current_total,
    COALESCE(h.historical_total, 0) as historical_total,
    c.current_total - COALESCE(h.historical_total, 0) as growth
FROM current_totals c
LEFT JOIN historical_totals h ON c.region = h.region
ORDER BY growth DESC;
```

## Step 8: Schema Evolution

### Add Columns

```sql
-- Add new columns
ALTER TABLE iceberg_examples.trino_orders
ADD COLUMN payment_method varchar;

ALTER TABLE iceberg_examples.trino_orders
ADD COLUMN discount_percent decimal(5,2);

-- Verify schema changes
SHOW CREATE TABLE iceberg_examples.trino_orders;
```

### Column Operations

```sql
-- Rename column
ALTER TABLE iceberg_examples.trino_orders
RENAME COLUMN customer_id TO client_id;

-- Change column type (compatible promotions only)
ALTER TABLE iceberg_examples.trino_orders
ALTER COLUMN order_amount SET DATA TYPE decimal(15,2);

-- Drop column
ALTER TABLE iceberg_examples.trino_orders
DROP COLUMN discount_percent;

-- Insert data with new schema
INSERT INTO iceberg_examples.trino_orders VALUES 
    (1008, 'CUST_H008', 750.00, DATE '2025-01-24', 'ASIA-PACIFIC', 'CREDIT_CARD');

-- Query with schema evolution
SELECT 
    order_id,
    client_id,
    order_amount,
    COALESCE(payment_method, 'UNKNOWN') as payment_method
FROM iceberg_examples.trino_orders
ORDER BY order_id DESC
LIMIT 5;
```

## Step 9: Partition Management

### View Current Partitioning

```sql
-- Show table structure including partitioning
SHOW CREATE TABLE iceberg_examples.trino_orders;

-- View partition information
SELECT 
    partition, 
    record_count, 
    file_count, 
    total_size
FROM iceberg_examples."trino_orders$partitions"
ORDER BY record_count DESC;
```

### Partition Evolution

```sql
-- Add new partition field
ALTER TABLE iceberg_examples.trino_orders
ADD PARTITION FIELD month(order_date);

-- Replace existing partition specification
ALTER TABLE iceberg_examples.trino_orders
REPLACE PARTITION FIELD bucket(order_id, 16) WITH bucket(order_id, 32);

-- Drop partition field
ALTER TABLE iceberg_examples.trino_orders
DROP PARTITION FIELD month(order_date);

-- Verify partition changes
SHOW CREATE TABLE iceberg_examples.trino_orders;
```

## Step 10: Table Maintenance

### Optimize Table Files

```sql
-- Basic optimization
ALTER TABLE iceberg_examples.trino_orders EXECUTE optimize;

-- Optimize with custom file size threshold
ALTER TABLE iceberg_examples.trino_orders 
EXECUTE optimize(file_size_threshold => '100MB');

-- Optimize specific partitions
ALTER TABLE iceberg_examples.trino_orders 
EXECUTE optimize
WHERE region = 'US-EAST';
```

### Snapshot and File Management

```sql
-- Remove orphaned files
ALTER TABLE iceberg_examples.trino_orders 
EXECUTE remove_orphan_files(retention_threshold => '7d');

-- Expire old snapshots
ALTER TABLE iceberg_examples.trino_orders 
EXECUTE expire_snapshots(retention_threshold => '5d');

-- Expire snapshots with minimum retention count
ALTER TABLE iceberg_examples.trino_orders 
EXECUTE expire_snapshots(retention_threshold => '7d', min_snapshots_to_keep => 3);
```

## Step 11: Metadata Analysis

### Table Metadata Queries

```sql
-- View table files
SELECT 
    file_path, 
    file_format, 
    record_count, 
    file_size_in_bytes / 1024 / 1024 as size_mb
FROM iceberg_examples."trino_orders$files"
ORDER BY file_size_in_bytes DESC
LIMIT 10;

-- View table history
SELECT 
    made_current_at, 
    snapshot_id, 
    parent_id, 
    is_current_ancestor
FROM iceberg_examples."trino_orders$history"
ORDER BY made_current_at DESC
LIMIT 10;

-- View manifest files
SELECT 
    path, 
    length, 
    partition_spec_id, 
    added_snapshot_id
FROM iceberg_examples."trino_orders$manifests"
ORDER BY added_snapshot_id DESC
LIMIT 5;
```

### Performance Analysis

```sql
-- Analyze file size distribution
SELECT 
    CASE 
        WHEN file_size_in_bytes < 10*1024*1024 THEN 'Small (<10MB)'
        WHEN file_size_in_bytes < 100*1024*1024 THEN 'Medium (10-100MB)'
        WHEN file_size_in_bytes < 500*1024*1024 THEN 'Large (100-500MB)'
        ELSE 'Very Large (>500MB)'
    END as file_size_category,
    COUNT(*) as file_count,
    SUM(record_count) as total_records
FROM iceberg_examples."trino_orders$files"
GROUP BY 1
ORDER BY 1;
```

## Step 12: Advanced Analytics

### Complex Analytical Queries

```sql
-- Window functions with partitioning
SELECT 
    order_id,
    client_id,
    order_amount,
    region,
    order_date,
    ROW_NUMBER() OVER (PARTITION BY region ORDER BY order_amount DESC) as region_rank,
    SUM(order_amount) OVER (PARTITION BY region) as region_total,
    AVG(order_amount) OVER (PARTITION BY DATE_TRUNC('month', order_date)) as monthly_avg
FROM iceberg_examples.trino_orders
ORDER BY region, region_rank;

-- Advanced aggregations
SELECT 
    region,
    DATE_TRUNC('month', order_date) as month,
    COUNT(*) as order_count,
    SUM(order_amount) as total_revenue,
    AVG(order_amount) as avg_order_value,
    STDDEV(order_amount) as amount_stddev,
    MIN(order_amount) as min_order,
    MAX(order_amount) as max_order,
    COUNT(DISTINCT client_id) as unique_customers
FROM iceberg_examples.trino_orders
GROUP BY region, DATE_TRUNC('month', order_date)
ORDER BY region, month;
```

### Cross-Table Analytics

```sql
-- Join with other tables for comprehensive analysis
SELECT 
    o.region,
    o.client_id,
    COUNT(o.order_id) as total_orders,
    SUM(o.order_amount) as total_spent,
    AVG(o.order_amount) as avg_order_value,
    MAX(o.order_date) as last_order_date
FROM iceberg_examples.trino_orders o
GROUP BY o.region, o.client_id
HAVING COUNT(o.order_id) > 1
ORDER BY total_spent DESC;
```

## Step 13: Performance Optimization

### Query Optimization

```sql
-- Use partition pruning for better performance
SELECT 
    client_id,
    SUM(order_amount) as total_spent
FROM iceberg_examples.trino_orders
WHERE region IN ('US-EAST', 'US-WEST')
  AND order_date BETWEEN DATE '2025-01-20' AND DATE '2025-01-25'
GROUP BY client_id
ORDER BY total_spent DESC;

-- Leverage bucket partitioning for joins
SELECT 
    o1.client_id,
    o1.order_amount as first_order,
    o2.order_amount as second_order
FROM iceberg_examples.trino_orders o1
JOIN iceberg_examples.trino_orders o2 
  ON o1.client_id = o2.client_id 
  AND o1.order_id < o2.order_id
WHERE o1.region = 'US-EAST';
```

## Step 14: Monitoring and Troubleshooting

### Check Cluster Status

```bash
# Check cluster status
aws emr describe-cluster --cluster-id ${CLUSTER_ID} --query 'Cluster.Status'

# View cluster logs
aws emr list-steps --cluster-id ${CLUSTER_ID}
```

### Trino Query Monitoring

```sql
-- Check running queries (from Trino CLI)
SELECT 
    query_id,
    state,
    user,
    source,
    query
FROM system.runtime.queries
WHERE state = 'RUNNING';

-- View query statistics
SELECT 
    query_id,
    state,
    total_cpu_time,
    total_scheduled_time,
    total_blocked_time,
    raw_input_rows,
    raw_input_data_size
FROM system.runtime.queries
WHERE state = 'FINISHED'
ORDER BY total_cpu_time DESC
LIMIT 10;
```

## Step 15: Cleanup

### Terminate EMR Cluster

```bash
# Terminate the EMR cluster
aws emr terminate-clusters --cluster-ids ${CLUSTER_ID}

# Wait for termination
aws emr wait cluster-terminated --cluster-id ${CLUSTER_ID}

echo "Cluster terminated successfully"
```

### Clean Up IAM Resources (Optional)

```bash
# Remove IAM role policies and roles (only if not used elsewhere)
aws iam remove-role-from-instance-profile \
    --instance-profile-name EMR_EC2_DefaultRole \
    --role-name EMR_EC2_DefaultRole

aws iam delete-instance-profile --instance-profile-name EMR_EC2_DefaultRole

aws iam detach-role-policy \
    --role-name EMR_EC2_DefaultRole \
    --policy-arn arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceforEC2Role

aws iam detach-role-policy \
    --role-name EMR_DefaultRole \
    --policy-arn arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceRole

aws iam delete-role --role-name EMR_EC2_DefaultRole
aws iam delete-role --role-name EMR_DefaultRole
```

## Troubleshooting

### Common Issues

1. **SSH Connection Issues**: Check security groups allow SSH (port 22) from your IP
2. **Trino Connection Failures**: Verify cluster is in WAITING state
3. **Permission Errors**: Ensure IAM roles have proper S3 and Glue permissions
4. **Query Performance**: Check partition pruning and file sizes

### Diagnostic Commands

```bash
# Check cluster applications
aws emr describe-cluster --cluster-id ${CLUSTER_ID} --query 'Cluster.Applications'

# View cluster configuration
aws emr describe-cluster --cluster-id ${CLUSTER_ID} --query 'Cluster.Configurations'

# Check step status
aws emr list-steps --cluster-id ${CLUSTER_ID}
```

## Best Practices

1. **Cluster Sizing**: Choose appropriate instance types based on workload
2. **Partition Strategy**: Design partitions based on query patterns
3. **File Management**: Regular optimization to maintain performance
4. **Cost Control**: Use spot instances for non-critical workloads
5. **Security**: Implement proper IAM roles and security groups

Trino on EMR provides excellent performance for analytical queries on Iceberg tables, making it ideal for complex analytics and cross-source data federation.
