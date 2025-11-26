# 8.5 Connecting to S3 Tables with Glue Catalog IRC

## OSS Spark

Here instructions for Spark 3.5.0.

### Prerequisites

- pre-created S3 table bucket
- proper IAM/Lake Formation permissions setup for the role

### 1. Start PySpark with Required Dependencies

Replace `<AWS_REGION>`, `<ACCOUNT_ID>` and `<S3_TABLE_BUCKET_NAME>` with your values:

```
pyspark \
--packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.1,software.amazon.awssdk:bundle:2.20.160,software.amazon.awssdk:url-connection-client:2.20.160 \
  --conf "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions" \
  --conf "spark.sql.defaultCatalog=spark_catalog" \
   --conf "spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkCatalog" \
  --conf "spark.sql.catalog.spark_catalog.type=rest" \
  --conf "spark.sql.catalog.spark_catalog.uri=https://s3tables.<AWS_REGION>.amazonaws.com/iceberg" \
  --conf "spark.sql.catalog.spark_catalog.warehouse=arn:aws:s3tables:<AWS_REGION>:<ACCOUNT_ID>:bucket/<S3_TABLE_BUCKET_NAME>" \
  --conf "spark.sql.catalog.spark_catalog.rest.sigv4-enabled=true" \
  --conf "spark.sql.catalog.spark_catalog.rest.signing-name=s3tables" \
  --conf "spark.sql.catalog.spark_catalog.rest.signing-region=<AWS_REGION>" \
  --conf "spark.sql.catalog.spark_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO" \
  --conf "spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialProvider" \
  --conf "spark.sql.catalog.spark_catalog.rest-metrics-reporting-enabled=false"               

```

Inside the CLI;
- create a namespace
- create an Iceberg table
- insert some data inside the Iceberg table
- Query the Iceberg table

```
#Customize these variables
namespace_name="mynamespace"
table_name="iceberg_s3tables_rest"

# Create a namespace
spark.sql(f"""
CREATE NAMESPACE IF NOT EXISTS {namespace_name}
""")

#Create table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {namespace_name}.{table_name}(
    id INT,
    col1 INT,
    col2 INT,
    col3 INT,
    col4 INT,
    country string
)
USING ICEBERG
PARTITIONED BY (country)
""")

#Insert data
spark.sql(f"""
    INSERT INTO {namespace_name}.{table_name}
    VALUES 
    (1, 100, 200, 300, 400, 'italy'),
    (2, 101, 201, 301, 401, 'france')
""")

# Query table
spark.sql(f"""
SELECT * FROM {namespace_name}.{table_name}
""").show(truncate=False)
```