# DuckDB with S3 tables

A hands-on guide to using DuckDB with Apache Iceberg tables stored in Amazon S3 Tables — covering setup, AWS credential configuration, catalog attachment, and workflow examples to read (and optionally write) Iceberg-formatted tables in S3 using DuckDB.

## Requirements

- An S3 Tables bucket in your AWS account.
- A namespace called duckdb ont that bucket(created via the AWS console).
- The ARN of your S3 Tables bucket.

Let’s get started.


Open the DuckDB CLI by running:

```bash
duckdb
```

### Step 1: Install Required Extensions

We’ll use the built-in AWS extension to authenticate via the standard credential chain (env vars, IAM roles, or config files):

```sql
INSTALL aws;
LOAD aws;
INSTALL httpfs;
LOAD httpfs;
INSTALL iceberg;
LOAD iceberg;
INSTALL parquet;
LOAD parquet;
```

### Step 2: Configure AWS Authentication

We’ll use the built-in AWS extension to authenticate via the standard credential chain (env vars, IAM roles, or config files):


```sql
CALL load_aws_credentials();

CREATE SECRET (
    TYPE s3,
    PROVIDER credential_chain
);
```

This automatically picks up credentials from your AWS CLI configuration, EC2 instance roles, or environment variables — no need for hardcoded credentials in your scripts.

### Step 3: Connect to Your S3 Tables Catalog

Next, attach your S3 Tables backed Iceberg catalog. Replace <<CUSTOM_ARN>> with the ARN of your S3 table bucket:


```sql
ATTACH '<<CUSTOM_ARN>>' AS s3_tables (
   TYPE iceberg,
   ENDPOINT_TYPE s3_tables
);
```

This single command gives you access to all tables within your S3 Tables bucket, organized in a familiar `database.schema.table` hierarchy.

### Step 4: Connect to Your S3 Tables Catalog

Check that your catalog is live:

```sql
SHOW ALL TABLES;
```
If your namespace ```duckdb``` is configured correctly, you’ll see it listed.



### Step 5: Create Synthetic Data

Let’s create a table by pulling in some real CSV data from the DuckDB public datasets:

```
-- Create a new managed table from external data
CREATE TABLE s3_tables_db.s3tablescatalog.services AS
FROM 'https://blobs.duckdb.org/nl-railway/services-2025-03.csv.gz';
```

This writes an Iceberg table into our S3 Table bucket directly from DuckDB.

### Step 6: Explore with the DuckDB UI

DuckDB also ships with a simple web UI for interactive exploration:

```sql
CALL start_ui();
```

Open the provided URL in your browser and start querying your freshly created Iceberg table.

You can create a notebook a issue this query to see the inserted data. Note that you need to change the database to memory on each cells as shown in below image ( use this for all the cells you run).

<img width="1568" height="119" alt="image" src="https://github.com/user-attachments/assets/1685619f-8330-4b8a-99a6-25a840570fb9" />

And Example query to run:

```
from s3_tables3.duckdb.services
select
 "Service:RDT-ID",
 "Service:Date",
 "Service:Type",
 "Service:Company",
 "Service:Train number",
 "Service:Completely cancelled",
 "Service:Partly cancelled",
 "Service:Maximum delay",
 "Stop:RDT-ID",
 "Stop:Station code",
 "Stop:Station name",
 "Stop:Arrival time",
 "Stop:Arrival delay",
 "Stop:Arrival cancelled",
 "Stop:Departure time",
 "Stop:Departure delay",
 "Stop:Departure cancelled"
limit 100
```

This will show the data that we have previously created. 

Let’s create now a table from our existing data.

```
CREATE TABLE s3_tables3.duckdb.services_per_month AS
     SELECT
         month("Service:Date") AS month,
         "Stop:Station name" AS station,
         count(*) AS num_services
     FROM s3_tables3.duckdb.services
     GROUP BY ALL;
```

Then we can visualize our newly created data, that would be available also for any compliant Iceberg engine. For example: Amazon Athena, AWS Glue, Amazon EMR, Spark, Flink,…etc.

```
SELECT * FROM s3_tables3.duckdb.services_per_month;
```

You can explore the column statistics on the Duckdb ui along with the capability to download query results.

<img width="720" height="372" alt="image" src="https://github.com/user-attachments/assets/be8d68d7-1221-4445-ad21-f27f21762837" />

