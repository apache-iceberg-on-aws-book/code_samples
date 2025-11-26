# Daft + Apache Iceberg + AWS

Let´s see how you can use [Daft](https://github.com/Eventual-Inc/Daft) to interact with Iceberg tables on top of AWS S3 Tables. 

Remember that Daft also have support for the [Glue](https://docs.daft.ai/en/stable/connectors/glue/) Catalog if you need Lake Formation permissions.                                  
                                 
            
## Prerequisites

- `uv` installed, create a project for the guide. 
- Python 3.11+ available on your system

## Steps

### Steps 1: Install daft

Let´s install the required libraries. 
                                 
```bash
uv add "daft[aws]" "pyiceberg"
```

### Step 2: Configure your credentials

Use IAM roles or get temporary credentials to authenticate. 

```python

import os
os.environ["AWS_ACCESS_KEY_ID"] = "<access-id>"
os.environ["AWS_SECRET_ACCESS_KEY"] = "<access-key>"
os.environ["AWS_DEFAULT_REGION"] = "<region>"
```

### Step 3: Example, S3 tables 

```python                                
import daft
from daft import Catalog

catalog = Catalog.from_s3tables("arn:aws:s3tables:<region>:<account>:bucket/<bucket>")

# verify we are connected
catalog.list_tables("<namespace>")

# read table table contentes
catalog.read_table("<my_namespace.my_tableZ").show()
```

### Step 4: Example, Glue Catalog


```python                                
import daft
from daft import Catalog
from daft.catalog.__glue import load_glue

# Load a glue catalog instance
catalog = load_glue(
    name="my_glue_catalog",
    region_name="<region>"
)

# Load a glue table
tbl = catalog.get_table("<database.table>")

tbl.show()
```