from src.transformation_lambda.transform_lambda_handler import get_date_time_str_of_last_extraction, lambda_handler
from moto import mock_aws
import boto3
import pytest

@pytest.fixture
def s3_client():
    with mock_aws():
        s3_client = boto3.client("s3", region_name="eu-west-2")
        yield s3_client

def add_S3_bucket(s3_client, bucket_name):
    return s3_client.create_bucket(
        Bucket=bucket_name,
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )

@pytest.fixture
def s3_client_one_bucket_one_file(s3_client):
    bucket_prefix = "ingested-data"
    add_S3_bucket(s3_client, f"{bucket_prefix}-bucket-1")
    return s3_client

class TestGetDateTimeStrLast_extraction:
    def test_get_date_time_str_returns_str(self, s3_client_one_bucket_one_file):
        body = "test-1"
        s3_client_one_bucket_one_file.put_object(
            Body=body,
            Bucket="ingested-data-bucket-1",
            Key="last_ingestion.txt",
        )
        output = get_date_time_str_of_last_extraction(s3_client_one_bucket_one_file)
        assert type(output) is str
        
    def test_body_saved_in_txt_is_read(self, s3_client_one_bucket_one_file):
        body = "test-1"
        s3_client_one_bucket_one_file.put_object(
            Body=body,
            Bucket="ingested-data-bucket-1",
            Key="last_ingestion.txt",
        )
        output = get_date_time_str_of_last_extraction(s3_client_one_bucket_one_file)
        assert body == output
        