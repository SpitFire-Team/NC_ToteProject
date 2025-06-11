import pandas as pd
from io import BytesIO


def load_parquet_from_s3(s3_client, bucket_name, latest_ingestion):
    """
    Loads latest Parquet file for each table from S3 and returns a list of dictionaries.
    Each dictionary contains: table_name, and data (as list of dicts).
    """
    results = []

    try:
        response = s3_client.list_objects_v2(
            Bucket=bucket_name
        )  # List all objects in the s3 bucket
    except Exception:
        print(f"Failed to list objects in bucket '{bucket_name}")
        return results  # return empty results to prevent subsequent exceptions

    if "Contents" not in response:
        return results  # No files found in s3 bucket

    for obj in response["Contents"]:
        key = obj["Key"]  # key = file name e.g. 'currency/06-06-2025_19:26.json'
        print(key)
        if latest_ingestion in key:
            table_name = key.split("/")[0]  # e.g. returns 'currency'
            s3_object = s3_client.get_object(Bucket=bucket_name, Key=key)

            df = pd.read_parquet(BytesIO(s3_object["Body"].read()))

            results.append({f"{table_name}": df})

    return results
