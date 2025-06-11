import pytest
import boto3
import pandas as pd
from io import BytesIO
from moto import mock_aws
from datetime import datetime, timezone

from src.load_lambda_package.load_lambda.read_paquet import load_parquet_from_s3


@pytest.fixture
def parquet_test_data():
    return pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"], "value": [100, 200]})


@pytest.fixture
def mock_s3_bucket_with_parquet(parquet_test_data):
    with mock_aws():
        s3 = boto3.client("s3", region_name="eu-west-2")
        bucket_name = "test-processed-bucket"
        table_name = "dim_sample"

        # Create bucket
        s3.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

        # Create Parquet file in memory
        buffer = BytesIO()
        parquet_test_data.to_parquet(buffer, index=False, engine="pyarrow")
        buffer.seek(0)

        # Upload parquet to S3
        timestamp = datetime.now(timezone.utc).isoformat()
        key = f"{table_name}/{timestamp}.parquet"
        s3.put_object(Bucket=bucket_name, Key=key, Body=buffer.getvalue())

        yield bucket_name, [table_name]


class TestLoadParquetFromS3:

    def test_returns_list_of_dicts(self, mock_s3_bucket_with_parquet):
        bucket, tables = mock_s3_bucket_with_parquet
        result = load_parquet_from_s3(bucket, tables)
        assert isinstance(result, list)
        assert all(isinstance(entry, dict) for entry in result)
        assert all("table_name" in entry and "data" in entry for entry in result)

    def test_returns_correct_table_name(self, mock_s3_bucket_with_parquet):
        bucket, tables = mock_s3_bucket_with_parquet
        result = load_parquet_from_s3(bucket, tables)
        table_names = [entry["table_name"] for entry in result]
        assert "dim_sample" in table_names

    def test_data_has_expected_content(
        self, mock_s3_bucket_with_parquet, parquet_test_data
    ):
        bucket, tables = mock_s3_bucket_with_parquet
        result = load_parquet_from_s3(bucket, tables)
        expected_data = parquet_test_data.to_dict(orient="records")
        loaded_data = next(
            item for item in result if item["table_name"] == "dim_sample"
        )["data"]
        assert loaded_data == expected_data

    def test_returns_empty_list_if_no_files(self):
        with mock_aws():
            s3 = boto3.client("s3", region_name="eu-west-2")
            bucket_name = "empty-bucket"
            s3.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
            )
            result = load_parquet_from_s3(bucket_name, ["dim_empty"])
            assert result == []
