# 8.4 Connecting to Iceberg Tables in General S3 Buckets with Glue Catalog IRC

## OSS Spark

Here instructions for Spark 3.5.0.

### 1. Start PySpark with Required Dependencies

Replace `<AWS_REGION>` and `<ACCOUNT_ID>` with your values:

```
pyspark \
--packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.1,software.amazon.awssdk:bundle:2.20.160,software.amazon.awssdk:url-connection-client:2.20.160 \
--conf "spark.sql.defaultCatalog=mydatacatalog" \
--conf "spark.sql.catalog.mydatacatalog=org.apache.iceberg.spark.SparkCatalog" \
--conf "spark.sql.catalog.mydatacatalog.type=rest" \
--conf "spark.sql.catalog.mydatacatalog.uri=https://glue.<AWS_REGION>.amazonaws.com/iceberg" \
--conf "spark.sql.catalog.mydatacatalog.warehouse=<ACCOUNT_ID>" \
--conf "spark.sql.catalog.mydatacatalog.rest.sigv4-enabled=true" \
--conf "spark.sql.catalog.mydatacatalog.rest.signing-name=glue" \
--conf "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
```

Inside the CLI;
- create a DB
- create an Iceberg table
- insert some data inside the Iceberg table
- Query the Iceberg table

```
#Customize these variables
db_name="mydb"
table_name="iceberg_rest_table"
bucket_name="mybucket1"

#Create DB
spark.sql(f"""
CREATE DATABASE IF NOT EXISTS {db_name}
LOCATION 's3://{bucket_name}/{db_name}/'
"""
)

#Create table
spark.sql(f"""
CREATE EXTERNAL TABLE IF NOT EXISTS {db_name}.{table_name}(
    id INT,
    col1 INT,
    col2 INT,
    col3 INT,
    col4 INT,
    country string
)
USING ICEBERG
PARTITIONED BY (country)
LOCATION 's3://{bucket_name}/{db_name}/{table_name}/'
""")

#Insert data
spark.sql(f"""
    INSERT INTO {db_name}.{table_name}
    VALUES 
    (1, 100, 200, 300, 400, 'italy'),
    (2, 101, 201, 301, 401, 'france')
""")

# Query table
spark.sql(f"""
SELECT * FROM {db_name}.{table_name}
""").show(truncate=False)
```