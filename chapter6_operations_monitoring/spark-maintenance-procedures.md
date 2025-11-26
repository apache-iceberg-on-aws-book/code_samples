# Apache Spark Maintenance Procedures

This document provides detailed examples and code for implementing Iceberg table maintenance operations using Apache Spark procedures.

## Basic Spark Procedures

### rewrite_data_files Procedure for Compaction

#### Basic Data File Compaction

Basic compaction for entire table:

```sql
-- Basic compaction for entire table
CALL system.rewrite_data_files( 
   table => 'iceberg_examples.customer_transactions' 
); 
```

#### Compaction with Target File Size

Compaction with specific target file size (512MB):

```sql
-- Compaction with specific target file size (512MB)
CALL system.rewrite_data_files( 
    table => 'iceberg_examples.customer_transactions', 
    options => map( 
        'target-file-size-bytes', '134217728',  -- 128MB 
        'min-input-files', '5', 
        'rewrite-all', 'false' 
    ) 
); 
```

#### Partition-Specific Compaction

Compact specific partitions:

```sql
-- Compact specific partitions
CALL system.rewrite_data_files( 
    table => 'iceberg_examples.customer_transactions', 
    where => 'transaction_date >= date("2025-01-01")', 
    options => map('target-file-size-bytes', '268435456')  -- 256MB 
); 
```


#### Sort-Based Compaction

Compaction with data sorting:

```sql
-- Compaction with data sorting
CALL system.rewrite_data_files(
    table => 'iceberg_examples.customer_transactions',
    strategy => 'sort',
    sort_order => 'transaction_date, customer_id',
    options => map(
        'target-file-size-bytes', '268435456'
    )
);
```

Z-order optimization for better query performance 

```sql
-- Z-order compaction
CALL system.rewrite_data_files( 
    table => 'iceberg_examples.customer_transactions', 
    strategy => 'sort', 
    sort_order => 'zorder(customer_id, transaction_date)'
); 
```

### Advanced rewrite_data_files Options

#### File Size Thresholds

Compact only small files below threshold:

```sql
-- Compact only small files below threshold
CALL system.rewrite_data_files(
    table => 'iceberg_examples.customer_transactions',
    options => map(
        'max-file-size-bytes', '104857600',  -- Only files smaller than 100MB
        'target-file-size-bytes', '536870912'
    )
);
```

#### Parallelism Control

Control compaction parallelism:

```sql
-- Control compaction parallelism
CALL system.rewrite_data_files(
    table => 'iceberg_examples.customer_transactions',
    options => map(
        'max-concurrent-file-group-rewrites', '4',
        'max-file-group-size-bytes', '2147483648',  -- 2GB per group
        'target-file-size-bytes', '536870912'
    )
);
```

#### Conditional Compaction

Compact only if minimum file count threshold is met:

```sql
-- Compact only if minimum file count threshold is met
CALL system.rewrite_data_files(
    table => 'iceberg_examples.customer_transactions',
    where => 'transaction_date >= date("2025-01-01")', 
    options => map(
        'min-input-files', '10',  -- Only compact if 10+ files exist
        'target-file-size-bytes', '536870912'
    )
);
```

## Snapshot Management Procedures

### expire_snapshots Procedure

#### Time-Based Snapshot Expiration

Expire snapshots older than 7 days:

```sql
-- Expire snapshots older than 7 days
CALL system.expire_snapshots(
    table => 'iceberg_examples.customer_transactions',
    older_than => TIMESTAMP '2025-01-08 00:00:00'
);
```

#### Count-Based Snapshot Retention

Keep only the most recent 50 snapshots:

```sql
-- Keep only the most recent 50 snapshots
CALL system.expire_snapshots(
    table => 'iceberg_examples.customer_transactions',
    retain_last => 50
);
```

#### Hybrid Snapshot Retention

Combine time and count-based retention:

```sql
-- Combine time and count-based retention
CALL system.expire_snapshots(
    table => 'iceberg_examples.customer_transactions',
    older_than => TIMESTAMP '2025-01-01 00:00:00',
    retain_last => 25
);
```

#### Snapshot Expiration with Stream Retention

Expire snapshots while preserving streaming metadata:

```sql
-- Expire snapshots while preserving streaming metadata
CALL system.expire_snapshots(
    table => 'iceberg_examples.customer_transactions',
    older_than => TIMESTAMP '2025-01-08 00:00:00',
    stream_results => true,
    max_concurrent_deletes => 4
);
```

## Orphan File Cleanup Procedures

### remove_orphan_files Procedure

#### Basic Orphan File Removal

Remove orphan files older than 24 hours:

```sql
-- Remove orphan files older than 24 hours
CALL system.remove_orphan_files(
    table => 'iceberg_examples.customer_transactions',
    older_than => TIMESTAMP '2025-01-14 00:00:00'
);
```

#### Orphan File Removal with Safety Buffer

Remove orphan files with 48-hour safety buffer:

```sql
-- Remove orphan files with 48-hour safety buffer
CALL system.remove_orphan_files(
    table => 'iceberg_examples.customer_transactions',
    older_than => TIMESTAMP '2025-01-13 00:00:00',
    max_concurrent_deletes => 2
);
```

#### Dry Run for Orphan Detection

Identify orphan files without removing them:

```sql
-- Identify orphan files without removing them
CALL system.remove_orphan_files(
    table => 'iceberg_examples.customer_transactions',
    older_than => TIMESTAMP '2025-01-14 00:00:00',
    dry_run => true
);
```

#### Location-Specific Orphan Cleanup

Remove orphan files from specific location:

```sql
-- Remove orphan files from specific location
CALL system.remove_orphan_files(
    table => 'iceberg_examples.customer_transactions',
    older_than => TIMESTAMP '2025-01-14 00:00:00',
    location => 's3://your-bucket/warehouse/iceberg_examples.customer_transactions/data/partition_date=2025-01-15/'
);
```


## EMR Step Configuration

### EMR Step for Spark Maintenance

```json
{
    "Name": "Iceberg Table Maintenance",
    "ActionOnFailure": "CONTINUE",
    "HadoopJarStep": {
        "Jar": "command-runner.jar",
        "Args": [
            "spark-submit",
            "--class", "org.apache.spark.sql.SparkSession",
            "--conf", "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            "--conf", "spark.sql.catalog.org.apache.iceberg.spark.SparkCatalog",
            "--conf", "spark.sql.catalog.type=glue",
            "--conf", "spark.sql.catalog.warehouse=s3://your-bucket/warehouse/",
            "--packages", "org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.4.2",
            "s3://your-scripts-bucket/maintenance/iceberg_maintenance.py"
        ]
    }
}
```

### Glue Job Configuration

```python
# Glue Job script for Iceberg maintenance
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'table_name', 'operation'])

# Create Glue context with Iceberg support
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Configure Iceberg
spark.conf.set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
spark.conf.set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.type", "glue")

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Execute maintenance based on operation parameter
table_name = args['table_name']
operation = args['operation']

if operation == 'compact':
    spark.sql(f"""
        CALL glue_catalog.system.rewrite_data_files(
            table => '{table_name}',
            options => map('target-file-size-bytes', '536870912')
        )
    """)
elif operation == 'expire_snapshots':
    spark.sql(f"""
        CALL glue_catalog.system.expire_snapshots(
            table => '{table_name}',
            older_than => CURRENT_TIMESTAMP - INTERVAL '7' DAY,
            retain_last => 50
        )
    """)
elif operation == 'cleanup_orphans':
    spark.sql(f"""
        CALL glue_catalog.system.remove_orphan_files(
            table => '{table_name}',
            older_than => CURRENT_TIMESTAMP - INTERVAL '1' DAY
        )
    """)

job.commit()
```
