import pandas as pd
from io import BytesIO
from pprint import pprint

#- uncomment for testing

# from src.utils.aws_utils import make_s3_client, get_bucket_name

#- uncomment for testing

def load_parquet_from_s3(s3_client, bucket_name, latest_ingestion):
    """
    Loads latest Parquet file for each table from S3 and returns a list of dictionaries.
    Each dictionary contains: table_name, and data (as list of dicts).
    """
    results = []
    print(bucket_name)

    try:
        response = s3_client.list_objects_v2(
            Bucket=bucket_name
        )  # List all objects in the s3 bucket
        # pprint(response)
    except Exception:
        print(f"Failed to list objects in bucket '{bucket_name}")  # return empty results to prevent subsequent exceptions

    if "Contents" not in response:
        print("No files found in s3 bucket")

    for obj in response["Contents"]:
        key = obj["Key"]  # key = file name e.g. 'currency/06-06-2025_19:26.json'
        print(key)
        if latest_ingestion in key:
            print("here")
            table_name = key.split("/")[0]  # e.g. returns 'currency'
            s3_object = s3_client.get_object(Bucket=bucket_name, Key=key)
            df = pd.read_parquet(BytesIO(s3_object["Body"].read()))
            results.append({table_name: df})
            # print({table_name: df})

    pprint(results)
    return results

# s3_client = make_s3_client()
# pprint(load_parquet_from_s3(s3_client, get_bucket_name(s3_client,"processed-data"), "11-06-2025_23:57"))