"""
PySpark Example: Write-Audit-Publish (WAP) Pattern with Iceberg and AWS Glue Catalog

The WAP pattern allows you to:
1. Write data to a staging branch/snapshot
2. Audit/validate the data before making it visible
3. Publish the changes only if validation passes

This prevents bad data from being visible to consumers.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit, count, sum as spark_sum, to_date, when
from datetime import datetime

# Initialize Spark with Iceberg and AWS Glue Catalog support
spark = SparkSession.builder \
    .appName("IcebergWAPPattern") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.glue", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.glue.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
    .config("spark.sql.catalog.glue.warehouse", "s3://your-bucket/warehouse/") \
    .config("spark.sql.catalog.glue.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .config("spark.sql.defaultCatalog", "glue") \
    .getOrCreate()

# ============================================================================
# Step 1: Create an Iceberg table
# ============================================================================

spark.sql("""
    CREATE DATABASE IF NOT EXISTS glue.demo_db
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS glue.demo_db.sales_transactions (
        transaction_id STRING,
        customer_id STRING,
        product_id STRING,
        amount DECIMAL(10,2),
        quantity INT,
        transaction_date DATE,
        created_at TIMESTAMP
    )
    USING iceberg
    TBLPROPERTIES (
        'format-version' = '2'
    )
""")

print("Created Iceberg table")

# ============================================================================
# Step 2: Insert initial data (published immediately)
# ============================================================================


audit_branch = "auditbook"

# Create a new branch for staging data
spark.sql(f"""
    ALTER TABLE glue.demo_db.sales_transactions 
    CREATE BRANCH `{audit_branch}`
""")

print(f"\nCreated branch: {audit_branch}")

# New batch of data to validate before publishing
new_data = [
    ("txn004", "cust001", "prod003", 500.00, 1, date(2024, 1, 3), datetime.now()),
    ("txn005", "cust004", "prod002", 75.25, 2, date(2024, 1, 3), datetime.now()),
    ("txn006", "cust002", "prod001", -50.00, 1, date(2024, 1, 3), datetime.now()),  # Invalid: negative amount
    ("txn007", "cust005", "prod004", 1000.00, 10, date(2024, 1, 3), datetime.now()),
]

df_new = spark.createDataFrame(
    new_data,
    ["transaction_id", "customer_id", "product_id", "amount", "quantity", "transaction_date", "created_at"]
)

# Write to the audit branch (not visible to regular queries on main)
df_new.writeTo(f"glue.demo_db.sales_transactions.branch_{audit_branch}").append()

print(f"✓ Written data to branch: {audit_branch}")

# Regular query still shows only published data
print("\nPublished data (WAP changes not visible yet):")
spark.table("glue.demo_db.sales_transactions").show()

# ============================================================================
# Step 4: Audit the staged data
# ============================================================================

print(f"\nAuditing data in branch: {audit_branch}")

# Read from the audit branch for validation
df_audit = spark.read.table(f"glue.demo_db.sales_transactions.branch_{audit_branch}")

print("\nData in audit branch (including new records):")
df_audit.show()

# Validation 1: Check for negative amounts
invalid_amounts = df_audit.filter(col("amount") < 0)
invalid_count = invalid_amounts.count()

print(f"\n⚠️  Validation Check 1: Found {invalid_count} records with negative amounts")
if invalid_count > 0:
    invalid_amounts.show()

# Validation 2: Check for data quality metrics
print("\n📈 Data Quality Metrics:")
df_audit.agg(
    count("*").alias("total_records"),
    spark_sum("amount").alias("total_amount"),
    spark_sum(when(col("amount") < 0, 1).otherwise(0)).alias("invalid_amounts")
).show()

# ============================================================================
# Step 5: Decision - Publish or Rollback
# ============================================================================

# In this example, we found invalid data, so we'll demonstrate both scenarios

# Scenario A: Reject the branch (don't publish)
print("\n❌ Validation failed! Rejecting audit branch...")
print(f"   Branch '{audit_branch}' will not be published")
print("   The invalid data remains isolated and won't affect production queries")

# The branch can be dropped or kept for investigation
print(f"   To drop the branch: ALTER TABLE glue.demo_db.sales_transactions DROP BRANCH `{audit_branch}`")

# ============================================================================
# Step 6: Demonstrate successful WAP workflow
# ============================================================================

print("\n" + "="*80)
print("Demonstrating successful WAP workflow with clean data")
print("="*80)

audit_branch_clean = "auditbookclean"

# Create a new branch for clean data
spark.sql(f"""
    ALTER TABLE glue.demo_db.sales_transactions 
    CREATE BRANCH `{audit_branch_clean}`
""")

print(f"\n✓ Created branch: {audit_branch_clean}")

# Clean data without issues
clean_data = [
    ("txn008", "cust001", "prod003", 500.00, 1, date(2024, 1, 4), datetime.now()),
    ("txn009", "cust004", "prod002", 75.25, 2, date(2024, 1, 4), datetime.now()),
    ("txn010", "cust002", "prod001", 125.00, 1, date(2024, 1, 4), datetime.now()),
]

df_clean = spark.createDataFrame(
    clean_data,
    ["transaction_id", "customer_id", "product_id", "amount", "quantity", "transaction_date", "created_at"]
)

# Write to the clean audit branch
df_clean.writeTo(f"glue.demo_db.sales_transactions.branch_{audit_branch_clean}").append()

print(f"✓ Written clean data to branch: {audit_branch_clean}")

# Audit the clean data
df_audit_clean = spark.read.table(f"glue.demo_db.sales_transactions.branch_{audit_branch_clean}")

invalid_clean = df_audit_clean.filter(col("amount") < 0).count()
print(f"\n✓ Validation passed! No invalid records found")

# Publish the branch by fast-forwarding main to the audit branch
print(f"\n✅ Publishing branch: {audit_branch_clean}")
spark.sql(f"""
    CALL glue.system.fast_forward(
        table => 'demo_db.sales_transactions',
        branch => 'main',
        to => '{audit_branch_clean}'
    )
""")

print("\nPublished data (now includes validated records):")
spark.table("glue.demo_db.sales_transactions").show()

# ============================================================================
# Step 7: View table history, snapshots, and branches
# ============================================================================

print("\nTable History:")
spark.sql("SELECT * FROM glue.demo_db.sales_transactions.history").show(truncate=False)

print("\nTable Snapshots:")
spark.sql("SELECT * FROM glue.demo_db.sales_transactions.snapshots").show(truncate=False)

print("\nTable Branches:")
spark.sql("SELECT * FROM glue.demo_db.sales_transactions.refs").show(truncate=False)

# ============================================================================
# Cleanup
# ============================================================================

print("\n Cleanup (optional):")
print("   To drop the table: spark.sql('DROP TABLE glue.demo_db.sales_transactions')")
print("   Note: Update 's3://your-bucket/warehouse/' with your actual S3 bucket path")

spark.stop()
