# def lambda_handler(event, context):
#     return event



import boto3

# - uncomment for testing

from src.load_lambda_package.load_lambda.read_paquet import load_parquet_from_s3
from src.load_lambda_package.load_lambda.warehouse_load import (
    wh_connection_engine,
    load_to_warehouse_loop,
)
from src.utils.aws_utils import get_bucket_name, make_s3_client

# - uncomment for testing

# - uncomment for deployment

# from load_lambda.read_paquet import load_parquet_from_s3
# from load_lambda.warehouse_load import wh_connection_engine, load_to_warehouse_loop
# from utils.aws_utils import get_bucket_name

# - uncomment for deployment


def lambda_handler(event, context):

    date_time_str_last_ingestion = event[0]["last_ingested_str"]

    if date_time_str_last_ingestion == "no data ingested":
        return [{"last_ingested_str": "no data ingested"}]

    # s3_client = boto3.client("s3")
    s3_client = make_s3_client()
    bucket_prefix = "processed-data"
    bucket_name = get_bucket_name(s3_client,bucket_prefix)

    try:
        df_dict_list= load_parquet_from_s3(
            s3_client, bucket_name, date_time_str_last_ingestion
        )
        # print(df_dict_list)
    except Exception:
        return [{"error": "could not load parquet files from processed data bucket"}]

    if len(df_dict_list) == 0:
        return [{"error": "0 files in df list"}]

    try:
        conn = wh_connection_engine()
    except Exception as e:
        return [{"error": f"could not connect to warehouse database {str(e)}"}]

    # try:
    items_loaded = load_to_warehouse_loop(df_dict_list, conn)
    return [{"success": f"{items_loaded}"}]
    # except Exception:
    #     return [{"error": "could not append to warehouse table"}]


event = [
  {
    "last_ingested_str": "12-06-2025_01:17"
  }
]

print(lambda_handler(event, {}))