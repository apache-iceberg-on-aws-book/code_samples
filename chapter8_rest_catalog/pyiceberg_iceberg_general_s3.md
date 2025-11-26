# 8.4 Connecting to Iceberg Tables in General S3 Buckets with Glue Catalog IRC

## PyIceberg

Create Python Virtual environment with PyIceberg

```
python3 -m venv my_env
cd my_env/bin/
source activate
pip install "pyiceberg[pyarrow,pandas]"
pip install boto3
python
```

Inside the CLI;
- create a DB
- create an Iceberg table
- insert some data inside the Iceberg table
- Query the Iceberg table

```
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType, DoubleType
import pyarrow as pa

# Customize with your values
region = "us-east-2"
db_name="mydb"
table_name="pyiceberg_rest_table"
bucket_name="mybucket1"

s3_db_path=f"s3://{bucket_name}/{db_name}/"
s3_table_path=f"s3://{bucket_name}/{db_name}/{table_name}"

 
# Setup REST catalog connection

rest_catalog = load_catalog(
     'default',
     **{
         "type": "rest",
         "uri": f"https://glue.{region}.amazonaws.com/iceberg",      
              "rest.sigv4-enabled": "true",
                "rest.signing-name": "glue",
                "rest.signing-region": region
     }
 )

# Create database

rest_catalog.create_namespace(db_name, properties={"location": s3_db_path})

# Create table

schema = Schema(
    NestedField(1, "city", StringType(), required=False),
    NestedField(2, "lat", DoubleType(), required=False),
    NestedField(3, "long", DoubleType(), required=False),
)

rest_catalog.create_table(f"{db_name}.{table_name}", schema=schema, location=s3_table_path)

# Insert data

df = pa.Table.from_pylist(
    [
        {"city": "Amsterdam", "lat": 52.371807, "long": 4.896029},
        {"city": "San Francisco", "lat": 37.773972, "long": -122.431297},
        {"city": "Drachten", "lat": 53.11254, "long": 6.0989},
        {"city": "Paris", "lat": 48.864716, "long": 2.349014},
    ],
)

table = rest_catalog.load_table(f"{db_name}.{table_name}")
table.append(df)

# Query table

table= rest_catalog.load_table(f"{db_name}.{table_name}")
scan_df = table.scan(
    row_filter=(
        f"city = 'Amsterdam'"
    ),
    selected_fields=("city", "lat"),
    limit=100,
).to_pandas()

print(scan_df)
```