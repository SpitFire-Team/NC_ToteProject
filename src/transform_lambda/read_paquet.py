import boto3
import pandas as pd
from io import BytesIO

def load_parquet_from_s3(bucket_name, table_names):
    """
    Loads latest Parquet file for each table from S3 and returns a dictionary of DataFrames.
    """
    s3= boto3.client("s3")
    result = {}

    for table in table_names:  # List all objects in the table folder
        objects = s3.list_objects_v2(Bucket=bucket_name, Prefix=f"{table}/")
        files = objects.get("Contents", [])

        if not files:
            print(f"No files found for {table}")
            continue

        # Get the latest file by LastModified timestamp
        latest_file = max(files, key=lambda x: x["LastModified"])
        key = latest_file["Key"]

        # Read the file into a DataFrame
        obj = s3.get_object(Bucket=bucket_name, Key=key)
        df = pd.read_parquet(BytesIO(obj["Body"].read()))

        result[table] = df
        print(f" Loaded {table}: {key} ({len(df)} rows)")

    return result
