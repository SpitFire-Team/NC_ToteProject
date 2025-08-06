import boto3
from dotenv import load_dotenv
import os


def get_bucket_name(s3_client, bucket_prefix: str):
    """
    Finds s3 bucket with starting with given prefix and returns full name as found on
    console
            Parameters:
                    s3_client (boto3.client('s3')): a boto3 client for an s3 bucket
                    bucket_prefix ({(<updated_table_name>, column_list) : all updated
                    date since last call})
            Returns:
                    full s3 bucket name from aws consol (string)
    """
    buckets_list = s3_client.list_buckets()["Buckets"]
    bucket_names = [bucket["Name"] for bucket in buckets_list]
    result = [name for name in bucket_names if bucket_prefix in name]

    if len(result) > 1:
        raise Exception(f"error there are buckets with prefix {bucket_prefix}")

    elif len(result) == 0:
        raise Exception(f"error there no buckets with prefix {bucket_prefix}")
    return result[0]


def add_data_to_s3_bucket(s3_client, bucket_name: str, data, file_path: str):
    """
    puts data in correct table folder in s3 bucket with the data
            Parameters:
                    s3_client (boto3.client('s3')): a boto3 client for an s3 bucket
                    bucket_name (string) : prefix of bucket type
                    data : updated values from databased
                    converted into a list of dictionaries
            Returns:
                    none
    """

    try:
        s3_client.put_object(
            Body=data,
            Bucket=bucket_name,
            Key=file_path,
        )
        return f"data addded to {file_path}"
    except Exception as e:
        raise Exception(f"File upload failure: {e}")


def make_s3_client():
    """
    Creates s3 client with functional aws credentials not required when run as lambda
    but required for testing s3 on aws console
            Parameters:
                    none

            Returns:
                    s3_client (boto3.client('s3')): a boto3 client for an s3 bucket
    """
    load_dotenv()
    
    session = boto3.Session(
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("AWS_SECRET_KEY"),
    )
    
    client = session.client("s3")
    return client



def read_file_in_bucket(s3_client, bucket_name: str, file_key: str):
    """
    reads object in s3 and return content
            Parameters:
                    s3_client (boto3.client('s3')): a boto3 client for an s3 bucket
                    bucket_name (string)
                    file_key (string))
            Returns:
                    full s3 bucket name from aws consol (string)
    """
    response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
    response_body = response["Body"].read().decode("utf-8")
    return response_body
