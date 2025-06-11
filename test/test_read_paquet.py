import pytest
import boto3
import pandas as pd
from io import BytesIO
from moto import mock_aws
from pandas.testing import assert_frame_equal


from src.load_lambda_package.load_lambda.read_paquet import load_parquet_from_s3


@pytest.fixture
def parquet_test_data():
    return pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"], "value": [100, 200]})


@pytest.fixture
def latest_ingestion_date_time():
    return "06-06-2025_19"


@pytest.fixture
def mock_s3_bucket_with_parquet(parquet_test_data, latest_ingestion_date_time):
    with mock_aws():
        s3_client = boto3.client("s3", region_name="eu-west-2")
        bucket_name = "test-processed-bucket"
        table_name = "dim_sample"

        # Create bucket
        s3_client.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

        # Create Parquet file in memory
        buffer = BytesIO()
        parquet_test_data.to_parquet(buffer, index=False, engine="pyarrow")
        buffer.seek(0)

        # Upload parquet to S3
        key = f"{table_name}/{latest_ingestion_date_time}.parquet"
        s3_client.put_object(Bucket=bucket_name, Key=key, Body=buffer.getvalue())
        yield bucket_name, s3_client


class TestLoadParquetFromS3:

    def test_returns_list_of_dicts(
        self, mock_s3_bucket_with_parquet, latest_ingestion_date_time
    ):
        bucket_name, s3_client = mock_s3_bucket_with_parquet
        result = load_parquet_from_s3(
            s3_client, bucket_name, latest_ingestion_date_time
        )
        assert isinstance(result, list)
        assert all(isinstance(entry, dict) for entry in result)
        # assert all(type(str) in entry and "data" in entry for entry in result)

    def test_returns_correct_table_name(
        self, mock_s3_bucket_with_parquet, latest_ingestion_date_time
    ):
        bucket_name, s3_client = mock_s3_bucket_with_parquet
        results = load_parquet_from_s3(
            s3_client, bucket_name, latest_ingestion_date_time
        )
        table_names = [list(result.keys())[0] for result in results]
        assert "dim_sample" in table_names

    def test_data_has_expected_content(
        self, mock_s3_bucket_with_parquet, parquet_test_data, latest_ingestion_date_time
    ):
        bucket_name, s3_client = mock_s3_bucket_with_parquet
        result = load_parquet_from_s3(
            s3_client, bucket_name, latest_ingestion_date_time
        )
        expected_data = parquet_test_data
        loaded_data = next(
            list(item.values())[0]
            for item in result
            if list(item.keys())[0] == "dim_sample"
        )
        assert_frame_equal(loaded_data, expected_data)

    def test_returns_empty_list_if_no_files(self, latest_ingestion_date_time):
        with mock_aws():
            s3_client = boto3.client("s3", region_name="eu-west-2")
            bucket_name = "empty-bucket"
            s3_client.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
            )
            result = load_parquet_from_s3(
                s3_client, bucket_name, latest_ingestion_date_time
            )
            assert result == []
