import boto3
import os
import re
from botocore.exceptions import ClientError

def get_glue_client(region_name):
    return boto3.client('glue', region_name=region_name)

def copy_table_metadata(source_database, source_table, source_region, target_region, source_bucket, target_bucket):
    glue_client = get_glue_client(source_region)
    target_glue_client = get_glue_client(target_region)
    try:
        # Get source table metadata
        source = glue_client.get_table(DatabaseName=source_database, Name=source_table)
        table_input = source['Table']

        # Prepare new table input with required fields
        new_table_input = {
            'Name': table_input['Name'],
            'StorageDescriptor': table_input['StorageDescriptor'],
            'TableType': table_input['TableType'],
            'Parameters': table_input['Parameters']
        }

        # Handle optional fields
        optional_fields = ['Owner', 'Description', 'Retention', 'ViewOriginalText',
                           'ViewExpandedText', 'TargetTable']

        for field in optional_fields:
            if field in table_input:
                new_table_input[field] = table_input[field]

        print("new_table_input BEFORE replace")
        print(new_table_input)

        # Replace bucket name in all relevant fields
        # 1. Replace in StorageDescriptor.Location
        if 'Location' in new_table_input['StorageDescriptor']:
            new_table_input['StorageDescriptor']['Location'] = new_table_input['StorageDescriptor']['Location'].replace(
                f's3://{source_bucket}/', f's3://{target_bucket}/')
        # 2. Replace in AdditionalLocations if present
        if 'AdditionalLocations' in new_table_input['StorageDescriptor']:
            new_locations = []
            for location in new_table_input['StorageDescriptor']['AdditionalLocations']:
                new_locations.append(location.replace(f's3://{source_bucket}/', f's3://{target_bucket}/'))

            new_table_input['StorageDescriptor']['AdditionalLocations'] = new_locations

        # 3. Replace in Parameters (especially important for Iceberg tables)
        if 'Parameters' in new_table_input:
            for key, value in new_table_input['Parameters'].items():
                if isinstance(value, str) and source_bucket in value:
                    new_table_input['Parameters'][key] = value.replace(
                        f's3://{source_bucket}/', f's3://{target_bucket}/')

        print("new_table_input AFTER bucket replacement:")
        print(new_table_input)

        # Check if the table already exists in the target region
        table_exists = False

        try:
            target_glue_client.get_table(DatabaseName=source_database, Name=source_table)
            table_exists = True
        except ClientError as e:
            # If the error is EntityNotFoundException, the table doesn't exist
            if e.response['Error']['Code'] == 'EntityNotFoundException':
                table_exists = False
            else:
                # For any other error, raise it
                raise e

        # Create or update the table based on whether it exists
        if table_exists:
            print(f"Table {source_database}.{source_table} already exists in target region. Updating...")
            target_glue_client.update_table(
                DatabaseName=source_database,
                TableInput=new_table_input
            )
            print(f"Table metadata successfully updated for {source_database}.{source_table}")
        else:
            print(f"Creating new table {source_database}.{source_table} in target region...")
            target_glue_client.create_table(
                DatabaseName=source_database,
                TableInput=new_table_input
            )
            print(f"Table metadata successfully created for {source_database}.{source_table}")
        return True

    except Exception as e:
        print(f"Error copying table metadata: {str(e)}")
        return False



def replicate_table_metadata(source_database, source_table, source_region, target_region, source_bucket, target_bucket):
    print(f"Starting replication of metadata of {source_database}.{source_table} from {source_region} to {target_region}")
    print(f"Replacing bucket {source_bucket} with {target_bucket}")

    if copy_table_metadata(source_database, source_table, source_region, target_region, source_bucket, target_bucket):
        print(f"Copy of {source_database}.{source_table} from {source_region} to {target_region} completed successfully")
        return True
    else:
        print(f"Copy failed: Unable to copy table metadata")
        return False


#To replicate the Iceberg table metadata from EU West to US East you can invoke the function in this way

db_name="mydb"
table_name="test_rewrite_table"
bucket_name="mybucket1"
target_bucket_name="mybucket2"
region1="eu-west-1"
region2="us-east-1"


replicate_table_metadata(
    source_database=db_name,
    source_table=table_name,
    source_region=region1,
    target_region=region2,
    source_bucket=bucket_name,
    target_bucket=target_bucket_name
)