import pytest
import boto3
import pandas as pd
from io import BytesIO
from moto import mock_aws
from datetime import datetime, timezone

from src.transform_lambda.read_paquet import load_parquet_from_s3

@pytest.fixture
def parquet_test_data():
    return pd.DataFrame({
        "id": [1, 2],
        "name": ["Alice", "Bob"],
        "value": [100, 200]
    })

@pytest.fixture
def mock_s3_bucket_with_parquet(parquet_test_data):
    with mock_aws():
        s3 = boto3.client("s3", region_name="eu-west-2")
        bucket_name = "test-processed-bucket"
        table_name = "dim_sample"

        # Create bucket
        s3.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"}
        )

        # Create Parquet file in memory
        buffer = BytesIO()
        parquet_test_data.to_parquet(buffer, index=False, engine="pyarrow")
        buffer.seek(0)

        # Upload parquet to S3
        key = f"{table_name}/{datetime.now(timezone.utc).strftime('%Y-%m-%dT%H-%M-%SZ')}.parquet"
        s3.put_object(Bucket=bucket_name, Key=key, Body=buffer.getvalue())

        yield bucket_name, [table_name]

class TestLoadParquetFromS3:

    def test_returns_dict_of_dataframes(self, mock_s3_bucket_with_parquet):
        bucket, tables = mock_s3_bucket_with_parquet
        result = load_parquet_from_s3(bucket, tables)
        assert isinstance(result, dict)
        assert all(isinstance(df, pd.DataFrame) for df in result.values())

    def test_returns_corrected_table_name(self,mock_s3_bucket_with_parquet):
        bucket, tables = mock_s3_bucket_with_parquet
        result = load_parquet_from_s3(bucket, tables)
        assert "dim_sample" in result

    def test_dataframe_has_expected_content(self, mock_s3_bucket_with_parquet, parquet_test_data):    
        bucket, tables = mock_s3_bucket_with_parquet
        result = load_parquet_from_s3(bucket, tables)
        df_loaded = result.get(tables[0])

        assert df_loaded.equals(parquet_test_data)

    def test_returns_empty_dict_if_no_files(self):
        with mock_aws():
            s3 = boto3.client("s3", region_name="eu-west-2")
            bucket_name = "empty-bucket"
            s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={"LocationConstraint": "eu-west-2"})

            result = load_parquet_from_s3(bucket_name, ["dim_empty"])
            assert result == {}

    def test_skips_unknown_table(self):
        with mock_aws():
            s3 = boto3.client("s3", region_name="eu-west-2")
            bucket = "test-bucket"
            s3.create_bucket(Bucket=bucket, CreateBucketConfiguration={"LocationConstraint": "eu-west-2"})

            result = load_parquet_from_s3(bucket, ["non_existent_table"])
            assert result == {}

