
from src.utils.aws_utils import get_bucket_name, read_file_in_bucket, make_s3_client
import boto3

def get_date_time_str_of_last_extraction(s3_client):

    bucket_name = get_bucket_name(s3_client, "ingested-data")
    last_extraction_date_time_str = read_file_in_bucket(
        s3_client, bucket_name, "last_ingestion.txt"
    )
    return last_extraction_date_time_str





def lambda_handler(event, context):
    s3_client = boto3.client("s3")
    date_time_str = get_date_time_str_of_last_extraction(make_s3_client())