import boto3

# - uncomment for testing

from src.load_lambda_package.load_lambda.read_paquet import load_parquet_from_s3
from src.load_lambda_package.load_lambda.warehouse_load import (
    wh_connection_engine,
    load_to_warehouse_loop,
)
from src.utils.aws_utils import get_bucket_name

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

    s3_client = boto3.client("s3")
    bucket_prefix = "processed_data"
    bucket_name = get_bucket_name(bucket_prefix)

    try:
        table_df_dict = load_parquet_from_s3(
            s3_client, bucket_name, date_time_str_last_ingestion
        )
    except Exception:
        return [{"error": "could not load parquet files from processed data bucket"}]

    try:
        conn = wh_connection_engine()
    except Exception:
        return [{"error": "could not connect to warehouse database"}]

    try:
        load_to_warehouse_loop(table_df_dict, conn)
        return [{"success": "successful load"}]
    except Exception:
        return [{"error": "could not append to warehouse table"}]
