# Do in-place migration with Apache XTable Sync tool

This section provides instructions to perform in-place migration from Hudi/Delta Lake to Iceberg leveraging the Apache xTable sync tool.

The sync utility performs the following:
1. First run: In-place migrate a Hudi/DeltaLake table to Iceberg
2. Subsequent run: Keep Iceberg table in sync with the Hudi/DeltaLake table over time

We see how you can quickly test the tool locally or inside an EC2 instance

## Prerequisites

- Java11 installed
- proper IAM permissions configured in the machine
- A Hudi/DeltaLake table pre-created that you want to migrate to Iceberg (initial migration) and continuous sync

## 1. Prepare Apache XTable Utilities
Compile `xtable-utilities` following the instructions in the Apache XTable GitHub repository (https://github.com/apache/incubator-xtable) and  copy `xtable-utilities_2.12-0.3.0-incubating-bundled.jar` in the current working directory.

## 2. Configure Apache XTables Configuration files

Update the `datasetConfig.yaml` file with your specific settings:
- `tableBasePath`: the source Hudi/DeltaLake table location
- `tableName`: the target Iceberg table name
- `namespace`: the target Database where to create the Iceberg table
- `partitionSpec`: the current partitioning of the source table

Update the  `glueDataCatalog.yaml` file with your specific settings:
- `warehouse`: the Iceberg warehouse location under which databases and tables are created

## 3. Test the utility in full sync mode

Run the utility with this command:

```
java -jar xtable-utilities_2.12-0.3.0-incubating-bundled.jar \
    --datasetConfig datasetConfig.yaml \
    --icebergCatalogConfig glueDataCatalog.yaml
```

Verify the Iceberg table is created and the content is matching the source table.

## 4. Test the utility in incremental sync mode

After the first sync, perform write operations (e.g, insert/delete/merge) in the Hudi/Delta Lake table.
Run the utility again:

```
java -jar xtable-utilities_2.12-0.3.0-incubating-bundled.jar \
    --datasetConfig datasetConfig.yaml \
    --icebergCatalogConfig glueDataCatalog.yaml
```

Verify the Iceberg table content is matching the source table.

## Notes

Note this will work for a Hudi COW table. If using a MOR table the resulting Iceberg table will match the Hudi Read Optimized view table (eg., `<hudi_table_name>_ro`).