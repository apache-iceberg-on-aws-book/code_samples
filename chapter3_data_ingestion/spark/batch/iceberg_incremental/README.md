# Iceberg Incremental Processing with Watermarking

This example demonstrates incremental processing on Apache Iceberg tables using watermarking stored in commit properties.

## Overview

The pattern reads only new data from a source table since the last processed snapshot, merges it into a target table, and atomically updates the watermark in the same commit. This eliminates the need for separate control tables and ensures exactly-once processing semantics.

## How It Works

1. **Read Watermark**: Retrieves the last processed snapshot ID from the target table's snapshot summary (`watermark:raw.log_source`)
2. **Incremental Read**: Reads only new records between `start-snapshot-id` and `end-snapshot-id` from the source table
3. **Merge Data**: Executes a MERGE statement to deduplicate and insert new records
4. **Update Watermark**: Atomically updates the watermark in the commit properties using `CommitMetadata.withCommitProperties`

## Key Features

- **Stateless Processing**: Watermark stored in Iceberg metadata, no external state management required
- **Atomic Updates**: Watermark and data changes committed together, preventing inconsistencies

## Dependencies

- Apache Spark 3.5
- Apache Iceberg 1.10.0
- Java 17
