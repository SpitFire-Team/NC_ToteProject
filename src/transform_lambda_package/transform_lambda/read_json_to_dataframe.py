import pandas as pd
from io import StringIO


def read_json_to_dataframe(s3_client, ingested_data_bucket_name, latest_ingestion):
    """
    Inputs:
        - s3 client: boto3 s3 client from lambda handler
        - latest_ingestion: datetime string (from last-updated function)like "09-06-2025_07:46"
        - ingested_data_bucket_name: full S3 bucket name

    Reads ingested JSON files from s3 bucket.
    loops through every table folder in s3 bucket and checks for files which have been updated at last extraction.

    Returns:
        -  List of dicts mapping table names to Pandas dataframes. e.g. [{staff: <staff_dataframe>}, {address: <address_dataframe>}
    """

    results = []

    try:
        response = s3_client.list_objects(
            Bucket=ingested_data_bucket_name
        )  # List all objects in the s3 bucket
    except Exception:
        print(f"Failed to list objects in bucket '{ingested_data_bucket_name}")
        return results  # return empty results to prevent subsequent exceptions

    if "Contents" not in response:
        return results  # No files found in s3 bucket

    for obj in response["Contents"]:
        key = obj["Key"]  # key = file name e.g. 'currency/06-06-2025_19:26.json'

        if latest_ingestion in key:
            table_name = key.split("/")[0]  # e.g. returns 'currency'

            s3_object = s3_client.get_object(Bucket=ingested_data_bucket_name, Key=key)
            json_body = s3_object["Body"].read()

            json_body_str = json_body.decode("utf-8")
            dataframe = pd.read_json(StringIO(json_body_str))

            results.append({table_name: dataframe})

    return results
