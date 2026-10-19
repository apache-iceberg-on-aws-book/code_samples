# Amazon Athena with Apache Iceberg

This guide provides step-by-step instructions for using Amazon Athena with Apache Iceberg tables, from basic setup to advanced operations.

## Prerequisites

Complete the [Prerequisites and Setup](prerequisites.md) before starting this guide.

## Step 1: Configure Athena Environment

### Set Up Query Result Location

```bash
# Set environment variables
export ATHENA_RESULT_LOCATION="s3://${ICEBERG_BUCKET_NAME}/athena-results/"

# Create results directory
aws s3api put-object \
    --bucket ${ICEBERG_BUCKET_NAME} \
    --key athena-results/
```

### Configure Athena Console (First Time Setup)

1. Open Athena Console: https://console.aws.amazon.com/athena/
2. Click "Settings" tab
3. Set "Query result location" to `s3://${ICEBERG_BUCKET_NAME}/athena-results/`
4. Click "Save"
5. Select Database: `iceberg_examples`

## Step 2: Create Your First Iceberg Table

### Using Athena Console (Recommended)

```sql
-- Create Iceberg table with partitioning
CREATE TABLE iceberg_examples.sales_data (
    transaction_id bigint,
    customer_id string,
    amount decimal(10,2),
    transaction_date date
)
USING ICEBERG
LOCATION 's3://your-bucket-name/iceberg-tables/sales_data/'
TBLPROPERTIES (
    'table_type' = 'ICEBERG',
    'format' = 'PARQUET'
);
```

### Using AWS CLI

```bash
# Create table using CLI
aws athena start-query-execution \
    --query-string "CREATE TABLE iceberg_examples.sales_data (
        transaction_id bigint,
        customer_id string,
        amount decimal(10,2),
        transaction_date date
    ) USING ICEBERG
    LOCATION 's3://${ICEBERG_BUCKET_NAME}/iceberg-tables/sales_data/'
    TBLPROPERTIES ('table_type' = 'ICEBERG');" \
    --result-configuration "OutputLocation=${ATHENA_RESULT_LOCATION}" \
    --work-group "primary"
```

## Step 3: Basic Data Operations

### Insert Sample Data

```sql
-- Insert initial data
INSERT INTO iceberg_examples.sales_data VALUES 
    (1001, 'CUST_001', 250.00, DATE '2025-01-20'),
    (1002, 'CUST_002', 175.50, DATE '2025-01-20'),
    (1003, 'CUST_003', 320.75, DATE '2025-01-21'),
    (1004, 'CUST_001', 89.99, DATE '2025-01-21'),
    (1005, 'CUST_004', 156.25, DATE '2025-01-22');

-- Verify data insertion
SELECT COUNT(*) as total_records FROM iceberg_examples.sales_data;

-- View sample data
SELECT * FROM iceberg_examples.sales_data ORDER BY transaction_id;
```

### Update Operations

```sql
-- Update existing records
UPDATE iceberg_examples.sales_data 
SET amount = amount * 1.1 
WHERE customer_id = 'CUST_001';

-- Verify updates
SELECT * FROM iceberg_examples.sales_data 
WHERE customer_id = 'CUST_001' 
ORDER BY transaction_date;

-- Complex update with conditions
UPDATE iceberg_examples.sales_data 
SET amount = CASE 
    WHEN amount > 200 THEN amount * 1.05
    ELSE amount * 1.10
END
WHERE transaction_date >= DATE '2025-01-21';
```

### Delete Operations

```sql
-- Delete records with conditions
DELETE FROM iceberg_examples.sales_data 
WHERE amount < 100;

-- Verify deletion
SELECT COUNT(*) as remaining_records 
FROM iceberg_examples.sales_data;
```

## Step 4: Advanced ACID Operations

### Create Staging Table for MERGE

```sql
-- Create staging table
CREATE TABLE iceberg_examples.staging_sales (
    transaction_id bigint,
    customer_id string,
    amount decimal(10,2),
    transaction_date date
)
USING ICEBERG
LOCATION 's3://your-bucket-name/iceberg-tables/staging_sales/'
TBLPROPERTIES ('table_type' = 'ICEBERG');

-- Insert staging data
INSERT INTO iceberg_examples.staging_sales VALUES 
    (1001, 'CUST_001', 300.00, DATE '2025-01-20'),  -- Update existing
    (1006, 'CUST_005', 125.50, DATE '2025-01-24'),  -- Insert new
    (1007, 'CUST_006', 275.00, DATE '2025-01-24');  -- Insert new
```

### MERGE Operations

```sql
-- Perform MERGE operation
MERGE INTO iceberg_examples.sales_data AS target
USING iceberg_examples.staging_sales AS source
ON target.transaction_id = source.transaction_id
WHEN MATCHED THEN 
    UPDATE SET 
        amount = source.amount,
        customer_id = source.customer_id,
        transaction_date = source.transaction_date
WHEN NOT MATCHED THEN 
    INSERT (transaction_id, customer_id, amount, transaction_date)
    VALUES (source.transaction_id, source.customer_id, source.amount, source.transaction_date);

-- Verify merge results
SELECT * FROM iceberg_examples.sales_data 
WHERE transaction_id IN (1001, 1006, 1007) 
ORDER BY transaction_id;
```

### Complex MERGE with Conditions

```sql
-- Advanced MERGE with multiple conditions
MERGE INTO iceberg_examples.sales_data AS target
USING (
    SELECT 
        transaction_id,
        customer_id,
        amount,
        transaction_date,
        'STAGING' as source_system
    FROM iceberg_examples.staging_sales
    WHERE amount > 0
) AS source
ON target.transaction_id = source.transaction_id
WHEN MATCHED AND source.amount > target.amount THEN 
    UPDATE SET 
        amount = source.amount,
        last_updated = CURRENT_TIMESTAMP
WHEN MATCHED AND source.amount <= target.amount THEN 
    DELETE
WHEN NOT MATCHED AND source.amount >= 100 THEN 
    INSERT (transaction_id, customer_id, amount, transaction_date)
    VALUES (source.transaction_id, source.customer_id, source.amount, source.transaction_date);
```

## Step 5: Time Travel Queries

### View Table Snapshots

```sql
-- View all snapshots
SELECT 
    snapshot_id, 
    committed_at, 
    operation, 
    summary
FROM iceberg_examples."sales_data$snapshots"
ORDER BY committed_at DESC
LIMIT 10;
```

### Timestamp-Based Time Travel

```sql
-- Query data as of specific timestamp (replace with actual timestamp)
SELECT 
    customer_id,
    COUNT(*) as transaction_count,
    SUM(amount) as total_amount
FROM iceberg_examples.sales_data 
FOR TIMESTAMP AS OF TIMESTAMP '2025-01-21 12:00:00'
GROUP BY customer_id
ORDER BY total_amount DESC;

```

### Snapshot-Based Time Travel

```sql
-- Query specific snapshot (replace with actual snapshot ID)
SELECT COUNT(*) as record_count
FROM iceberg_examples.sales_data 
FOR VERSION AS OF 1234567890123456789;
```

## Step 6: Schema Evolution

### Add New Columns

```sql
-- Add single column
ALTER TABLE iceberg_examples.sales_data 
ADD COLUMNS (payment_method string);

-- Add multiple columns
ALTER TABLE iceberg_examples.sales_data 
ADD COLUMNS (discount_applied decimal(5,2));

-- Query with new columns shows 'UNKNOWN' and 0.0 respectively for historical records 
SELECT
    transaction_id, 
    customer_id, 
    amount, 
    COALESCE(payment_method, 'UNKNOWN') as payment_method, 
    COALESCE(discount_applied, 0.00) as discount_applied 
FROM iceberg_examples.sales_data 
ORDER BY transaction_date DESC; 

-- Verify schema changes
DESCRIBE iceberg_examples.sales_data;
```


### Column Operations

```sql
-- Rename column
ALTER TABLE iceberg_examples.sales_data 
CHANGE COLUMN customer_id TO client_id; 

-- Verify column rename
DESCRIBE iceberg_examples.sales_data;

-- Query with new column name
SELECT transaction_id, client_id, amount 
FROM iceberg_examples.sales_data 
WHERE transaction_id = 1008;

-- Drop column (if needed)
ALTER TABLE iceberg_examples.sales_data 
DROP COLUMN region;
```


## Step 7: Metadata Analysis

### Snapshot analysis

```sql
-- Analyze table snapshots and operations 
SELECT
    snapshot_id,
    committed_at,
    operation,
    summary['added-data-files'] as files_added,
    summary['deleted-data-files'] as files_deleted,
    summary['added-records'] as records_added
FROM iceberg_examples."sales_data$snapshots"
ORDER BY committed_at DESC
LIMIT 10;
```

### File-level analysis

```sql
-- Analyze file sizes and distribution
SELECT
    file_format,
    COUNT(*) as file_count,
    AVG(file_size_in_bytes) as avg_file_size,
    MIN(file_size_in_bytes) as min_file_size,
    MAX(file_size_in_bytes) as max_file_size,
    SUM(record_count) as total_records
FROM iceberg_examples."sales_data$files"
GROUP BY file_format; 

-- Identify small files that need optimization 
SELECT
    file_path,
    CAST(file_size_in_bytes AS DOUBLE) / 1024 / 1024 as size_mb,
    record_count
FROM iceberg_examples."sales_data$files"
WHERE file_size_in_bytes < 50 * 1024 * 1024  -- Files smaller than 50MB
ORDER BY file_size_in_bytes; 
```

> **📖 Advanced Analysis**: For comprehensive metadata analysis, file-level optimization queries, and performance monitoring, see [Chapter 4 - Athena Maintenance Commands](../chapter4_best_practices_operations/athena-maintenance-commands.md).

## Step 8: Branch and Tag operations
Athena supports querying specific Iceberg branches and tags using the $refs metadata table to identify snapshot IDs. While Athena doesn't directly support branch creation (which requires Spark), it can query data from existing branches and tags created by other engines.

### Querying Branch and Tag information
Use the $refs metadata table to view available branches and tags with their corresponding snapshot IDs:

```sql
-- View all branches and tags 
SELECT * FROM iceberg_examples."sales_data$refs";

-- Filter for specific branch types
SELECT name, type, snapshot_id
FROM iceberg_examples."sales_data$refs"
WHERE type = 'BRANCH';

-- Filter for tags only
SELECT name, type, snapshot_id
FROM iceberg_examples."sales_data$refs"
WHERE type = 'TAG';
```

### Querying Specific Branches
Query data from specific branches using the snapshot ID obtained from $refs: 

```sql
-- Get snapshot ID for a specific branch 
SELECT snapshot_id
FROM iceberg_examples."sales_data$refs"
WHERE name = 'main' AND type = 'BRANCH';

-- Query data from the 'main' branch using the snapshot ID
SELECT customer_id, amount, transaction_date
FROM iceberg_examples.sales_data FOR VERSION AS OF 3214003454918213205
WHERE amount > 200; 
```

### Querying Specific Tags
Tags work similarly to branches, allowing you to query data at tagged points in time: 

```sql
-- Get snapshot ID for a specific tag 
SELECT snapshot_id  
FROM iceberg_examples."sales_data$refs" 
WHERE name = 'v1_0_release' AND type = 'TAG'; 

-- Query data from tagged version 
SELECT
    COUNT(*) as record_count,
    SUM(amount) as total_amount,
    AVG(amount) as avg_amount
FROM iceberg_examples.sales_data FOR VERSION AS OF 8745692581473829164; 
```

## Step 9: Performance Monitoring

### Query Performance Analysis

```sql
-- Create performance test queries
SELECT 
    client_id,
    COUNT(*) as transaction_count,
    SUM(amount) as total_spent,
    AVG(amount) as avg_transaction,
    MIN(transaction_date) as first_transaction,
    MAX(transaction_date) as last_transaction
FROM iceberg_examples.sales_data
WHERE transaction_date >= DATE '2025-01-01'
GROUP BY client_id
HAVING COUNT(*) > 1
ORDER BY total_spent DESC;

-- Test predicate pushdown efficiency
SELECT COUNT(*) as filtered_count
FROM iceberg_examples.sales_data
WHERE amount BETWEEN 100 AND 500
  AND transaction_date = DATE '2025-01-21'
  AND client_id LIKE 'CUST_%';
```

### Monitor Query Execution

Use Athena Console to monitor:
1. Query execution time
2. Data scanned
3. Query cost
4. Execution details

## Step 10: Integration Testing

### Test with Other AWS Services

```bash
# Test Glue Crawler discovery
aws glue start-crawler --name iceberg-crawler

# Check if crawler discovered the table
aws glue get-table --database-name iceberg_examples --name sales_data
```

### Verify Cross-Service Compatibility

```sql
-- Query that can be run from multiple services
SELECT 
    DATE_TRUNC('month', transaction_date) as month,
    COUNT(*) as transactions,
    SUM(amount) as revenue,
    COUNT(DISTINCT client_id) as unique_customers
FROM iceberg_examples.sales_data
GROUP BY DATE_TRUNC('month', transaction_date)
ORDER BY month;
```

## Troubleshooting

### Common Issues

1. **Query Timeout**: Increase timeout in workgroup settings
2. **Permission Errors**: Check IAM roles and S3 bucket policies
3. **Schema Evolution Issues**: Verify Iceberg table format version
4. **Performance Issues**: Check file sizes and consider optimization

### Diagnostic Queries

```sql
-- Check table properties
SHOW TBLPROPERTIES iceberg_examples.sales_data;

-- Verify table format
SELECT 
    'Table Type' as property,
    table_type as value
FROM information_schema.tables 
WHERE table_name = 'sales_data' 
  AND table_schema = 'iceberg_examples';

-- Check recent operations
SELECT 
    operation,
    committed_at,
    summary
FROM iceberg_examples."sales_data$snapshots"
WHERE committed_at >= CURRENT_TIMESTAMP - INTERVAL '1' DAY
ORDER BY committed_at DESC;
```

## Best Practices

1. **Query Optimization**: Use partition pruning and column selection
2. **File Management**: Regular OPTIMIZE operations for better performance
3. **Snapshot Management**: Regular VACUUM to control storage costs
4. **Schema Design**: Plan schema evolution to minimize breaking changes
5. **Cost Control**: Monitor data scanned and optimize queries accordingly

Amazon Athena provides an excellent serverless SQL interface for Iceberg tables, making it ideal for ad-hoc analytics and interactive data exploration.
