package org.example;

import java.util.Map;
import java.util.Collections;



import org.apache.iceberg.Table;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.CommitMetadata;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Example program demonstrating incremental processing with Apache Iceberg and Spark.
 *
 * <p>This class follows the pattern described in the Tabular Apache Iceberg Cookbook
 * recipe for incremental processing. It retrieves a watermark from the target table's
 * snapshot summary, reads only new records from the source table between the last
 * processed and current snapshot IDs, merges those records into the target table, and
 * updates the watermark atomically in the same commit. The implementation uses
 * Iceberg's snapshot metadata to keep track of processing state rather than a
 * separate control table, improving correctness and operational simplicity.
 */
public class IncrementalProcessing {
    public static void main(String[] args) throws Exception {
        // Initialize a Spark session. Additional Iceberg catalog configuration
        // would normally be provided here.
        SparkSession spark = SparkSession.builder()
            .appName("IncrementalProcessingExample")
            .config("spark.sql.catalog.iceberg_catalog", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.iceberg_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
            .config("spark.sql.catalog.iceberg_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
            .config("spark.sql.catalog.iceberg_catalog.warehouse", "s3://your-bucket/warehouse/")
            .getOrCreate();

        // Load the deduplicated logs table (final.logs). This table stores
        // incremental processing state in its snapshot summary under the key
        // "watermark:raw.log_source".
        Table logsTable = Spark3Util.loadIcebergTable(spark, "final.logs");

        // Look up the last processed snapshot ID from the logs table's current
        // snapshot summary. If the current snapshot does not contain a
        // watermark property (for example, after compaction), walk the
        // snapshot lineage to find the last snapshot with a watermark.
        String lastProcessedId = null;
        if (logsTable != null && logsTable.currentSnapshot() != null) {
            Map<String, String> summary = logsTable.currentSnapshot().summary();
            if (summary != null) {
                lastProcessedId = summary.get("watermark:raw.log_source");
            }
        }
        if (lastProcessedId == null) {
            Iterable<Snapshot> ancestors = SnapshotUtil.currentAncestors(logsTable);
            if (ancestors != null) {
                for (Snapshot snapshot : ancestors) {
                    Map<String, String> summary = snapshot.summary();
                    if (summary != null) {
                        lastProcessedId = summary.get("watermark:raw.log_source");
                        if (lastProcessedId != null) {
                            break;
                        }
                    }
                }
            }
        }

        // Load the source table (raw.log_source) and determine the latest snapshot ID.
        Table logSourceTable = Spark3Util.loadIcebergTable(spark, "raw.log_source");
        String endSnapshotId = null;
        if (logSourceTable != null && logSourceTable.currentSnapshot() != null) {
            endSnapshotId = String.valueOf(logSourceTable.currentSnapshot().snapshotId());
        }

        // Read only new rows between the last processed and current snapshots.
        Dataset<Row> newLogs = spark.read()
            .format("iceberg")
            .option("start-snapshot-id", lastProcessedId)
            .option("end-snapshot-id", endSnapshotId)
            .table("raw.log_source");

        // Register the incremental data as a temporary view for use in SQL.
        newLogs.createOrReplaceTempView("log_source");

        // Construct the MERGE statement to insert new records into the logs table.
        String mergeSql =
            "MERGE INTO final.logs AS t " +
            "USING log_source AS s " +
            "ON t.log_id = s.log_id " +
            "WHEN NOT MATCHED THEN INSERT *";

        // Execute the MERGE and update the watermark atomically using
        // CommitMetadata.withCommitProperties
        CommitMetadata.withCommitProperties(
            Collections.singletonMap("watermark:raw.log_source", endSnapshotId),
            () -> {
                spark.sql(mergeSql);
                return null;
            },
            RuntimeException.class);

        // Shut down the Spark session.
        spark.stop();
    }
}
