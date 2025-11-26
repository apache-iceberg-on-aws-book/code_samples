# Amazon Athena Maintenance Commands

This document provides detailed SQL commands and examples for performing Iceberg table maintenance operations using Amazon Athena.

## OPTIMIZE Command for Compaction

### OPTIMIZE Syntax

Basic compaction using BIN_PACK strategy:

```sql
OPTIMIZE iceberg_examples.sales_data 
REWRITE DATA USING BIN_PACK;
```

### Single Partition Optimization

Optimize specific partition:

```sql
-- Optimize specific partition
OPTIMIZE iceberg_db.transactions 
REWRITE DATA USING BIN_PACK
WHERE partition_date = DATE '2025-01-15';
```

## VACUUM Command for Snapshot Expiration

### Basic Snapshot Expiration

Remove snapshots older than 5 days:

```sql
-- Snapshot cleanup and Orphan file cleanup 
ALTER TABLE iceberg_table SET TBLPROPERTIES ( 
  'vacuum_max_snapshot_age_seconds'='432000' 
) 

VACUUM iceberg_examples.sales_data;
```

## Metadata Queries and Analysis 
### Snapshot Management Examples

```sql
-- View current snapshots with detailed information
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
