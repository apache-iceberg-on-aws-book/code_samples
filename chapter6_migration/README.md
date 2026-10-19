# Chapter 6 – Migrate to Apache Iceberg

This chapter provides comprehensive guidance on migrating existing tables to Apache Iceberg. It covers migration strategies, tools, and best practices for transitioning from legacy formats to Iceberg tables.

## 9.2 Table Data Migration Options
### Full migration

You can find the following examples for full migration to Iceberg in S3 general buckets:
- [Full migration - Hive to Iceberg in General S3](full_migration/full_table_migration_hive_iceberg_general_s3.ipynb) - Notebooks showing how to perform full migration for Hive tables to Iceberg in general S3
- [Full migration - Delta Lake to Iceberg in General S3](full_migration/full_table_migration_delta_iceberg_general_s3.ipynb) - Notebooks showing how to perform full migration for Delta Lake tables to Iceberg in general S3
- [Full migration - Hudi to Iceberg in General S3](full_migration/full_table_migration_hudi_iceberg_general_s3.ipynb) - Notebooks showing how to perform full migration for Hudi tables to Iceberg in general S3

You can find the following examples for full migration to Iceberg in S3 Tables:
- [Full migration - Hive to Iceberg in S3 Tables](full_migration/full_table_migration_hive_iceberg_s3_tables.ipynb) - Notebooks showing how to perform full migration for Hive tables to Iceberg in S3 Tables
- [Full migration - Delta Lake to Iceberg in S3 Tables](full_migration/full_table_migration_delta_iceberg_s3_tables.ipynb) - Notebooks showing how to perform full migration for Delta Lake tables to Iceberg in S3 Tables
- [Full migration - Hudi to Iceberg in S3 Tables](full_migration/full_table_migration_hudi_iceberg_s3_tables.ipynb) - Notebooks showing how to perform full migration for Hudi tables to Iceberg in S3 Tables

### In-Place migration

#### In-place migration for Hive-style tables

You can find the following examples for in-place migration of Hive-style tables:

- [Snapshot Migration Procedure](in_place_migration/hive/snapshot_procedure.ipynb) - Notebooks showing how to use Iceberg `snapshot` and `add_files` procedures to migrate Hive-style tables to Iceberg in general S3 and keep Iceberg tables in sync
- [Replicate Migrate Procedure](in_place_migration/hive/replicate_migrate_procedure_in_gdc.ipynb) - Notebooks showing how to replicate the Iceberg `migrate` procedure behavior in Glue Data Catalog to migrate Hive-style tables to Iceberg in general S3
- [Selective In-Place Migration for Large Historical Tables using the add_files procedure](in_place_migration/hive/migrate_with_add_files_procedure.ipynb) - Notebook showing how to use the `add_files` procedure to in-place migrate portions of Hive tables to Iceberg in general S3

#### In-place Migration for Modern Table Formats: Delta Lake and Apache Hudi

You can find the following examples for in-place migration of Hudi and Delta Lake tables:

- [In-place Migration for Delta Lake and Apache Hudi with Apache XTable](in_place_migration/delta_hudi/apache_xtable) - Guide showing how to in-place migrate Delta Lake and Hudi tables to Iceberg in general S3 using Apache XTable
- [In-place Migration for Apache Hudi using add_files procedure](in_place_migration/delta_hudi/in_place_migration_add_files_hudi.ipynb) - Notebook showing how to in-place migrate Hudi tables to Iceberg in general S3 using Iceberg add_files procedure
- [In-place Migration for Delta Lake using add_files procedure](in_place_migration/delta_hudi/in_place_migration_add_files_delta.ipynb) - Notebook showing how to in-place migrate Delta Lake tables to Iceberg in general S3 using Iceberg add_files procedure

## 9.3 How to approach the migration

There are multiple ways to approach the migration that should be carefully evaluated.

### Phased Transition

Maintain both source table and Iceberg table active for a transition period till all readers and writers are ready to fully migrate

#### Scenario 1: With Writer Modifications and Maintenance Window

In this situation, we assume that writer applications can be modified and a maintenance window is available for the initial migration.

You can find examples for the following use cases:
- [Dual Writes - Full migration Hive to Iceberg in General S3](how_to_approach_the_migration/phased_transition/scenario_1_with_writer_modifications_and_maintenance_window/full_table_migration_dual_writes_hive_iceberg_general_s3.ipynb) - Notebooks showing how to approach initial migration and dual writes for Hive tables to Iceberg in general S3

#### Scenario 2: Without Writer Modifications or Maintenance Window
In this situation, we assume that writer applications cannot be modified and no maintenance window is available for the initial migration. Synchronization is achieved through automated processes that run independently of the writer applications

You can find examples for the following use cases:

- [Use case: Apache Hudi Source Table and Full Migration](how_to_approach_the_migration/phased_transition/scenario_2_without_writer_modifications_or_maintenance_window/Phased_transition_scenario2_Hudi_to_Iceberg.ipynb) - Notebook showing how to approach initial full migration and periodic sync for Hudi tables to Iceberg in general S3 (replicate Hudi CDC to Iceberg)
- [Use case: Delta Lake Source Table with Full Migration](how_to_approach_the_migration/phased_transition/scenario_2_without_writer_modifications_or_maintenance_window/Phased_transition_scenario2_Delta_to_Iceberg.ipynb) - Notebook showing how to approach initial full migration and periodic sync for Delta tables to Iceberg in general S3 (replicate Delta Lake CDC to Iceberg)
- [Use case: Apache Hudi Table with In-Place Migration](how_to_approach_the_migration/phased_transition/scenario_2_without_writer_modifications_or_maintenance_window/apache_x_table) - Automated Hudi to Iceberg in-place migration sync with Apache XTable, EventBridge and ECS