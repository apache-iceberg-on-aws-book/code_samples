# Chapter 7 – Advanced Topics

This chapter explores advanced Apache Iceberg features and use cases for experienced practitioners. It covers complex scenarios, advanced configurations, and cutting-edge techniques for maximizing Iceberg's capabilities.

## 10.1 Backup and Recovery

- [Strategy 1: Periodic Full Table Backup](backup_and_recovery/solution1_full_periodic_copy/write_iceberg_multi_region.ipynb) - Notebooks sample. This approach represents the most straightforward backup implementation for Apache Iceberg tables. It involves periodically running a backup job that leverages CREATE TABLE AS SELECT (CTAS) statements to read the latest snapshot of your Iceberg table and create a duplicate table in a designated backup location. This backup location—including both the AWS Glue Data Catalog and S3 storage—can reside in the same region or, for enhanced fault tolerance, in a different AWS region.
- [Strategy 3: Iceberg rewrite_table_path procedure](backup_and_recovery/solution3_rewrite_table_path/rewrite_table_path.ipynb) - Notebook sample demonstrating full + incremental sync. Rewrite metadata files for backup buckets leveraging the `rewrite_table_path` Iceberg Spark procedure, copy Iceberg files between buckets and replicate Glue Catalog table metadata.

## 10.3 Iceberg Multi-Writer Concurrency Control
- [Conflict Management Example](multi_writers_concurrency_control/conflict_management_example.py) - PySpark application example. Production-ready conflict resolution using Spark Structured Streaming.

## 10.4 Medallion Architecture with Iceberg
- [Medallion Architecture](medallion_architecture/bronze_silver_gold_iceberg.ipynb) - Notebook sample. End-to-end example of Medallion Architecture (Bronze-Silver-Gold) implementation using Apache Iceberg and Spark. This demonstration shows how to incrementally propagate changes across bronze, silver, and gold layers while maintaining data integrity.