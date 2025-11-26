# Iceberg MetricReporter Implementation Guide

This guide provides step-by-step instructions for implementing and configuring the Apache Iceberg MetricReporter framework to capture real-time operational metrics during Iceberg operations.

## Understanding MetricReporter Framework

The MetricReporter framework is an Apache Iceberg interface that captures metrics during table operations in real-time. Unlike querying metadata tables periodically, MetricReporter provides immediate visibility into operation performance as it happens.

> **Note**: This guide is written for Apache Iceberg 1.10.0. The MetricReporter interface has been stable across recent versions, but always verify compatibility with your specific Iceberg version.

### What MetricReporter Captures

**Commit Operations**
- Files added, removed, or rewritten
- Records inserted, updated, or deleted
- Operation duration and performance
- Data volume and size changes

**Scan Operations**
- Files scanned and bytes processed
- Query planning metrics
- Scan efficiency indicators
- Filter effectiveness

**Maintenance Operations**
- Compaction effectiveness
- Snapshot expiration results
- Orphan file cleanup metrics

## Prerequisites

### Development Environment Setup

```bash
# Create project directory
mkdir iceberg-metric-reporter
cd iceberg-metric-reporter

# Verify Java development environment
java -version
mvn -version

# Create Maven project structure
mvn archetype:generate \
    -DgroupId=com.example.iceberg \
    -DartifactId=metric-reporter \
    -DarchetypeArtifactId=maven-archetype-quickstart \
    -DinteractiveMode=false

cd metric-reporter
```

### Maven Dependencies

Add required dependencies to your `pom.xml`:

```xml
<properties>
    <maven.compiler.source>11</maven.compiler.source>
    <maven.compiler.target>11</maven.compiler.target>
    <iceberg.version>1.10.0</iceberg.version>
    <aws.sdk.version>2.20.0</aws.sdk.version>
</properties>

<dependencies>
    <!-- Iceberg Core -->
    <dependency>
        <groupId>org.apache.iceberg</groupId>
        <artifactId>iceberg-core</artifactId>
        <version>${iceberg.version}</version>
    </dependency>
    
    <!-- AWS SDK for CloudWatch -->
    <dependency>
        <groupId>software.amazon.awssdk</groupId>
        <artifactId>cloudwatch</artifactId>
        <version>${aws.sdk.version}</version>
    </dependency>
    
    <!-- Logging -->
    <dependency>
        <groupId>org.slf4j</groupId>
        <artifactId>slf4j-api</artifactId>
        <version>1.7.36</version>
    </dependency>
    
    <!-- Testing -->
    <dependency>
        <groupId>junit</groupId>
        <artifactId>junit</artifactId>
        <version>4.13.2</version>
        <scope>test</scope>
    </dependency>
</dependencies>
```

## Implementation Approaches

### Approach 1: Custom CloudWatch MetricReporter

Create a custom implementation that publishes metrics to Amazon CloudWatch:

```java
package com.example.iceberg;

import org.apache.iceberg.metrics.CommitReport;
import org.apache.iceberg.metrics.MetricsReport;
import org.apache.iceberg.metrics.MetricsReporter;
import org.apache.iceberg.metrics.ScanReport;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.cloudwatch.model.Dimension;
import software.amazon.awssdk.services.cloudwatch.model.MetricDatum;
import software.amazon.awssdk.services.cloudwatch.model.PutMetricDataRequest;
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CloudWatchMetricsReporter implements MetricsReporter {
    
    private final CloudWatchClient cloudWatchClient;
    private final String namespace;
    private final Map<String, String> additionalDimensions;
    
    public CloudWatchMetricsReporter(String namespace) {
        this(namespace, Map.of());
    }
    
    public CloudWatchMetricsReporter(String namespace, Map<String, String> additionalDimensions) {
        this.cloudWatchClient = CloudWatchClient.create();
        this.namespace = namespace;
        this.additionalDimensions = additionalDimensions;
    }
    
    @Override
    public void report(MetricsReport report) {
        List<MetricDatum> metrics = new ArrayList<>();
        
        // Check if this is a CommitReport
        if (report instanceof CommitReport) {
            metrics.addAll(processCommitReport((CommitReport) report));
        }
        
        // Check if this is a ScanReport
        if (report instanceof ScanReport) {
            metrics.addAll(processScanReport((ScanReport) report));
        }
        
        // Publish metrics to CloudWatch
        if (!metrics.isEmpty()) {
            publishMetrics(metrics);
        }
    }
    
    private List<MetricDatum> processCommitReport(CommitReport commitReport) {
        List<MetricDatum> metrics = new ArrayList<>();
        Instant timestamp = Instant.now();
        String tableName = commitReport.tableName();
        
        // Get commit metrics if available
        if (commitReport.commitMetrics() != null) {
            var commitMetrics = commitReport.commitMetrics();
            
            // Files added metrics
            if (commitMetrics.addedDataFiles() != null) {
                metrics.add(createMetric("AddedDataFiles", tableName,
                    (double) commitMetrics.addedDataFiles().value(),
                    StandardUnit.COUNT, timestamp));
            }
            
            // Files removed metrics
            if (commitMetrics.removedDataFiles() != null) {
                metrics.add(createMetric("RemovedDataFiles", tableName,
                    (double) commitMetrics.removedDataFiles().value(),
                    StandardUnit.COUNT, timestamp));
            }
            
            // Records added metrics
            if (commitMetrics.addedRecords() != null) {
                metrics.add(createMetric("AddedRecords", tableName,
                    (double) commitMetrics.addedRecords().value(),
                    StandardUnit.COUNT, timestamp));
            }
            
            // Records removed metrics
            if (commitMetrics.removedRecords() != null) {
                metrics.add(createMetric("RemovedRecords", tableName,
                    (double) commitMetrics.removedRecords().value(),
                    StandardUnit.COUNT, timestamp));
            }
            
            // File size metrics
            if (commitMetrics.addedFilesSizeInBytes() != null) {
                metrics.add(createMetric("AddedFilesSizeInBytes", tableName,
                    (double) commitMetrics.addedFilesSizeInBytes().value(),
                    StandardUnit.BYTES, timestamp));
            }
            
            // Commit time
            if (commitMetrics.totalDuration() != null) {
                metrics.add(createMetric("CommitTimeMs", tableName,
                    (double) commitMetrics.totalDuration().totalDuration().toMillis(),
                    StandardUnit.MILLISECONDS, timestamp));
            }
        }
        
        return metrics;
    }
    
    private List<MetricDatum> processScanReport(ScanReport scanReport) {
        List<MetricDatum> metrics = new ArrayList<>();
        Instant timestamp = Instant.now();
        String tableName = scanReport.tableName();
        
        // Get scan metrics if available
        if (scanReport.scanMetrics() != null) {
            var scanMetrics = scanReport.scanMetrics();
            
            // Result data files
            if (scanMetrics.resultDataFiles() != null) {
                metrics.add(createMetric("ResultDataFiles", tableName,
                    (double) scanMetrics.resultDataFiles().value(),
                    StandardUnit.COUNT, timestamp));
            }
            
            // Result delete files
            if (scanMetrics.resultDeleteFiles() != null) {
                metrics.add(createMetric("ResultDeleteFiles", tableName,
                    (double) scanMetrics.resultDeleteFiles().value(),
                    StandardUnit.COUNT, timestamp));
            }
            
            // Scanned data manifests
            if (scanMetrics.scannedDataManifests() != null) {
                metrics.add(createMetric("ScannedDataManifests", tableName,
                    (double) scanMetrics.scannedDataManifests().value(),
                    StandardUnit.COUNT, timestamp));
            }
            
            // Skipped data files
            if (scanMetrics.skippedDataFiles() != null) {
                metrics.add(createMetric("SkippedDataFiles", tableName,
                    (double) scanMetrics.skippedDataFiles().value(),
                    StandardUnit.COUNT, timestamp));
            }
            
            // Planning time
            if (scanMetrics.totalPlanningDuration() != null) {
                metrics.add(createMetric("PlanningTimeMs", tableName,
                    (double) scanMetrics.totalPlanningDuration().totalDuration().toMillis(),
                    StandardUnit.MILLISECONDS, timestamp));
            }
        }
        
        return metrics;
    }
    
    private MetricDatum createMetric(String metricName, String tableName, double value, 
                                   StandardUnit unit, Instant timestamp) {
        List<Dimension> dimensions = new ArrayList<>();
        
        // Add table name dimension
        dimensions.add(Dimension.builder()
            .name("TableName")
            .value(tableName)
            .build());
            
        // Add additional dimensions
        additionalDimensions.forEach((key, val) -> 
            dimensions.add(Dimension.builder()
                .name(key)
                .value(val)
                .build()));
        
        return MetricDatum.builder()
            .metricName(metricName)
            .value(value)
            .unit(unit)
            .timestamp(timestamp)
            .dimensions(dimensions)
            .build();
    }
    
    private void publishMetrics(List<MetricDatum> metrics) {
        try {
            // CloudWatch allows max 20 metrics per request
            int batchSize = 20;
            for (int i = 0; i < metrics.size(); i += batchSize) {
                List<MetricDatum> batch = metrics.subList(i, 
                    Math.min(i + batchSize, metrics.size()));
                
                PutMetricDataRequest request = PutMetricDataRequest.builder()
                    .namespace(namespace)
                    .metricData(batch)
                    .build();
                
                cloudWatchClient.putMetricData(request);
            }
        } catch (Exception e) {
            // Log error but don't fail the operation
            System.err.println("Failed to publish metrics to CloudWatch: " + e.getMessage());
        }
    }
    
    @Override
    public void close() {
        if (cloudWatchClient != null) {
            cloudWatchClient.close();
        }
    }
}
```

### Approach 2: Logging MetricReporter

For environments where CloudWatch integration isn't available, create a logging-based reporter:

```java
package com.example.iceberg;

import org.apache.iceberg.metrics.CommitReport;
import org.apache.iceberg.metrics.MetricsReport;
import org.apache.iceberg.metrics.MetricsReporter;
import org.apache.iceberg.metrics.ScanReport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class LoggingMetricsReporter implements MetricsReporter {
    
    private static final Logger logger = LoggerFactory.getLogger(LoggingMetricsReporter.class);
    
    private final Map<String, String> additionalTags;
    
    public LoggingMetricsReporter() {
        this(Map.of());
    }
    
    public LoggingMetricsReporter(Map<String, String> additionalTags) {
        this.additionalTags = additionalTags;
    }
    
    @Override
    public void report(MetricsReport report) {
        // Check if this is a CommitReport
        if (report instanceof CommitReport) {
            logCommitMetrics((CommitReport) report);
        }
        
        // Check if this is a ScanReport
        if (report instanceof ScanReport) {
            logScanMetrics((ScanReport) report);
        }
    }
    
    private void logCommitMetrics(CommitReport commitReport) {
        StringBuilder logMessage = new StringBuilder();
        logMessage.append("ICEBERG_COMMIT_METRICS table=").append(commitReport.tableName());
        logMessage.append(" operation=").append(commitReport.operation());
        logMessage.append(" snapshotId=").append(commitReport.snapshotId());
        
        // Add additional tags
        additionalTags.forEach((key, value) -> 
            logMessage.append(" ").append(key).append("=").append(value));
        
        // Add commit metrics if available
        if (commitReport.commitMetrics() != null) {
            var commitMetrics = commitReport.commitMetrics();
            
            if (commitMetrics.addedDataFiles() != null) {
                logMessage.append(" files_added=").append(commitMetrics.addedDataFiles().value());
            }
            
            if (commitMetrics.removedDataFiles() != null) {
                logMessage.append(" files_removed=").append(commitMetrics.removedDataFiles().value());
            }
            
            if (commitMetrics.addedRecords() != null) {
                logMessage.append(" records_added=").append(commitMetrics.addedRecords().value());
            }
            
            if (commitMetrics.removedRecords() != null) {
                logMessage.append(" records_removed=").append(commitMetrics.removedRecords().value());
            }
            
            if (commitMetrics.addedFilesSizeInBytes() != null) {
                logMessage.append(" added_files_size_bytes=").append(commitMetrics.addedFilesSizeInBytes().value());
            }
            
            if (commitMetrics.totalDuration() != null) {
                logMessage.append(" commit_time_ms=").append(commitMetrics.totalDuration().totalDuration().toMillis());
            }
        }
        
        logger.info(logMessage.toString());
    }
    
    private void logScanMetrics(ScanReport scanReport) {
        StringBuilder logMessage = new StringBuilder();
        logMessage.append("ICEBERG_SCAN_METRICS table=").append(scanReport.tableName());
        logMessage.append(" snapshotId=").append(scanReport.snapshotId());
        logMessage.append(" schemaId=").append(scanReport.schemaId());
        
        // Add additional tags
        additionalTags.forEach((key, value) -> 
            logMessage.append(" ").append(key).append("=").append(value));
        
        // Add scan metrics if available
        if (scanReport.scanMetrics() != null) {
            var scanMetrics = scanReport.scanMetrics();
            
            if (scanMetrics.resultDataFiles() != null) {
                logMessage.append(" result_data_files=").append(scanMetrics.resultDataFiles().value());
            }
            
            if (scanMetrics.resultDeleteFiles() != null) {
                logMessage.append(" result_delete_files=").append(scanMetrics.resultDeleteFiles().value());
            }
            
            if (scanMetrics.scannedDataManifests() != null) {
                logMessage.append(" scanned_data_manifests=").append(scanMetrics.scannedDataManifests().value());
            }
            
            if (scanMetrics.skippedDataFiles() != null) {
                logMessage.append(" skipped_data_files=").append(scanMetrics.skippedDataFiles().value());
            }
            
            if (scanMetrics.totalPlanningDuration() != null) {
                logMessage.append(" planning_time_ms=").append(scanMetrics.totalPlanningDuration().totalDuration().toMillis());
            }
        }
        
        logger.info(logMessage.toString());
    }
}
```

## Configuration in Different Environments

### Apache Spark Configuration

Configure MetricReporter in Spark applications:

```python
# PySpark configuration
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("IcebergWithMetrics") \
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.iceberg.type", "glue") \
    .config("spark.sql.catalog.iceberg.warehouse", "s3://your-bucket/warehouse/") \
    .config("spark.sql.catalog.iceberg.metrics-reporter-impl", 
            "com.example.iceberg.metrics.CloudWatchMetricsReporter") \
    .config("spark.sql.catalog.iceberg.metrics.namespace", "Iceberg/Operations") \
    .config("spark.sql.catalog.iceberg.metrics.table-name", "transactions") \
    .getOrCreate()

# Now all Iceberg operations will automatically report metrics
spark.sql("""
    INSERT INTO iceberg.db.transactions 
    VALUES (1, 'user1', 100.0, current_timestamp())
""")
```

### AWS Glue Configuration

Configure MetricReporter in AWS Glue jobs using SparkSession:

```python
from pyspark.sql import SparkSession

# Initialize Spark session with Iceberg and MetricReporter configuration
spark = SparkSession.builder \
    .appName("IcebergJobWithMetrics") \
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.iceberg.type", "glue") \
    .config("spark.sql.catalog.iceberg.warehouse", "s3://your-bucket/warehouse/") \
    .config("spark.sql.catalog.iceberg.metrics-reporter-impl", 
            "com.example.iceberg.metrics.CloudWatchMetricsReporter") \
    .config("spark.sql.catalog.iceberg.metrics.namespace", "Iceberg/Glue") \
    .config("spark.sql.catalog.iceberg.metrics.table-name", "transactions") \
    .enableHiveSupport() \
    .getOrCreate()

# Your Iceberg operations using standard Spark SQL
# Read from source table
source_df = spark.sql("SELECT * FROM iceberg.db.source_table")

# Write to target table with metrics automatically collected
source_df.createOrReplaceTempView("temp_data")
spark.sql("""
    INSERT INTO iceberg.db.target_table
    SELECT * FROM temp_data
""")

# Alternative: Using DataFrame API
df = spark.read.format("iceberg").load("iceberg.db.source_table")
df.write.format("iceberg").mode("append").save("iceberg.db.target_table")

spark.stop()
```

### EMR Configuration

Configure MetricReporter in EMR applications:

```bash
# EMR step configuration with MetricReporter
aws emr add-steps \
    --cluster-id j-1234567890123 \
    --steps '[{
        "Name": "Iceberg Job with Metrics",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--conf", "spark.sql.catalog.iceberg.metrics-reporter-impl=com.example.iceberg.metrics.CloudWatchMetricsReporter",
                "--conf", "spark.sql.catalog.iceberg.metrics.namespace=Iceberg/EMR",
                "--conf", "spark.sql.catalog.iceberg.metrics.table-name=transactions",
                "--jars", "s3://your-bucket/jars/metric-reporter-1.0.jar",
                "s3://your-bucket/scripts/iceberg-job.py"
            ]
        }
    }]'
```

## Testing and Validation

### Unit Testing

Create unit tests for your MetricReporter implementation:

```java
package com.example.iceberg.metrics;

import org.apache.iceberg.metrics.CommitReport;
import org.apache.iceberg.metrics.MetricsReport;
import org.junit.Test;
import org.junit.Before;
import static org.mockito.Mockito.*;

public class CloudWatchMetricsReporterTest {
    
    private CloudWatchMetricsReporter reporter;
    
    @Before
    public void setUp() {
        reporter = new CloudWatchMetricsReporter("TestNamespace", "test_table");
    }
    
    @Test
    public void testCommitReportProcessing() {
        // Create mock commit report
        CommitReport commitReport = mock(CommitReport.class);
        MetricsReport metricsReport = mock(MetricsReport.class);
        
        when(metricsReport.commitReport()).thenReturn(commitReport);
        
        // Test that report method doesn't throw exceptions
        reporter.report(metricsReport);
    }
    
    @Test
    public void testReporterClosesCleanly() {
        reporter.close();
        // Verify no exceptions thrown
    }
}
```

### Integration Testing

Test MetricReporter with actual Iceberg operations:

```java
package com.example.iceberg.metrics;

import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;

public class MetricReporterIntegrationTest {
    
    @Test
    public void testMetricsReportingWithRealOperations() {
        SparkSession spark = SparkSession.builder()
            .appName("MetricReporter Integration Test")
            .master("local[*]")
            .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.iceberg.type", "hadoop")
            .config("spark.sql.catalog.iceberg.warehouse", "/tmp/iceberg-warehouse")
            .config("spark.sql.catalog.iceberg.metrics-reporter-impl", 
                    "com.example.iceberg.metrics.LoggingMetricsReporter")
            .getOrCreate();
        
        // Create test table
        spark.sql("CREATE TABLE iceberg.test.metrics_test (id INT, name STRING) USING iceberg");
        
        // Insert data - should trigger metrics reporting
        spark.sql("INSERT INTO iceberg.test.metrics_test VALUES (1, 'test')");
        
        // Verify metrics were logged (check logs)
        spark.stop();
    }
}
```

## Deployment and Packaging

### Building the JAR

```bash
# Build the project
mvn clean compile

# Run tests
mvn test

# Package the JAR
mvn package

# The JAR will be created at target/metric-reporter-1.0.jar
```

### Deployment to S3

```bash
# Upload JAR to S3 for use in EMR/Glue
aws s3 cp target/metric-reporter-1.0.jar s3://your-bucket/jars/

# Verify upload
aws s3 ls s3://your-bucket/jars/metric-reporter-1.0.jar
```

## Monitoring MetricReporter Performance

### CloudWatch Metrics for MetricReporter

Monitor the MetricReporter itself to ensure it's working correctly:

```bash
# Check if metrics are being published
aws cloudwatch list-metrics --namespace "Iceberg/Operations"

# Get metric statistics
aws cloudwatch get-metric-statistics \
    --namespace "Iceberg/Operations" \
    --metric-name "FilesAdded" \
    --dimensions Name=TableName,Value=transactions \
    --start-time 2024-01-01T00:00:00Z \
    --end-time 2024-01-02T00:00:00Z \
    --period 3600 \
    --statistics Sum,Average
```

### Troubleshooting Common Issues

**Metrics Not Appearing in CloudWatch**
1. Check IAM permissions for CloudWatch PutMetricData
2. Verify MetricReporter implementation is in classpath
3. Check application logs for errors
4. Ensure proper configuration parameters

**High CloudWatch Costs**
1. Implement metric batching (already included in examples)
2. Use metric filters to reduce volume
3. Consider sampling for high-frequency operations
4. Monitor CloudWatch usage and costs

**Performance Impact**
1. Make metrics publishing asynchronous if needed
2. Implement circuit breakers for CloudWatch failures
3. Monitor MetricReporter execution time
4. Consider using logging reporter for high-volume operations

### Debug Configuration

```python
# Enable debug logging for metrics in Spark
spark.conf.set("spark.sql.catalog.iceberg.metrics.debug", "true")

# Test MetricReporter configuration
spark.sql("SHOW TBLPROPERTIES iceberg.db.test_table")
```

## Best Practices

### Performance Considerations
- Keep metric publishing lightweight and non-blocking
- Batch CloudWatch API calls to reduce costs
- Handle failures gracefully without impacting Iceberg operations
- Consider async publishing for high-volume operations

### Security
- Use IAM roles with minimal required permissions
- Encrypt metrics data in transit and at rest
- Avoid logging sensitive data in metrics
- Regularly rotate AWS credentials

### Cost Optimization
- Monitor CloudWatch costs and usage
- Use appropriate metric retention periods
- Consider sampling for very high-frequency operations
- Batch metric publishing to reduce API calls

### Operational Excellence
- Include comprehensive error handling
- Log metric publishing failures for debugging
- Monitor MetricReporter performance impact
- Test thoroughly before production deployment

## Next Steps

- Review [monitoring-queries.md](monitoring-queries.md) for comprehensive monitoring queries
- See [dashboard-configuration.md](dashboard-configuration.md) for visualization setup
- Check [alerting-configuration.md](alerting-configuration.md) for alerting configuration
- Explore [cloudwatch-integration.md](cloudwatch-integration.md) for advanced CloudWatch integration