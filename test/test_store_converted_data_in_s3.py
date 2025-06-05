import pytest
from src.extraction_lambda.store_converted_data_in_s3 import (
    transform_data_to_compatable_format,
    input_updated_data_into_s3,
)
from moto import mock_aws
import boto3


@pytest.fixture
def db_values():
    column_names = ("id", "example_col", "test_data")
    db_values = {
        ("address", column_names): [
            (2, "test_data2", "1030-01-01T00:00:00+00:00"),
            (3, "test_data3", "1940-01-01T00:00:00+00:00"),
            (4, "test_data4", "1001-01-01T00:00:00+00:00"),
            (5, "test_data5", "1070-01-01T00:00:00+00:00"),
            (6, "test_data6", "1005-01-01T00:00:00+00:00"),
            (7, "test_data7", "1080-01-01T00:00:00+00:00"),
            (8, "test_data8", "1031-01-01T00:00:00+00:00"),
        ],
        ("payment", column_names): [
            (1, "test_data1", "2000-01-01T00:00:00+00:00"),
            (2, "test_data2", "2030-01-01T00:00:00+00:00"),
            (3, "test_data3", "1940-01-01T00:00:00+00:00"),
            (4, "test_data4", "2001-01-01T00:00:00+00:00"),
            (5, "test_data5", "2070-01-01T00:00:00+00:00"),
            (6, "test_data6", "2005-01-01T00:00:00+00:00"),
            (7, "test_data7", "2080-01-01T00:00:00+00:00"),
            (8, "test_data8", "2031-01-01T00:00:00+00:00"),
        ],
    }
    return db_values


@pytest.fixture
def s3_client():
    with mock_aws():
        s3_client = boto3.client("s3", region_name="eu-west-2")
        yield s3_client


def add_S3_bucket(client, bucket_name):
    return client.create_bucket(
        Bucket=bucket_name,
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )


@pytest.fixture
def s3_client_two_buckets_one_prefix(s3_client):
    bucket_prefix = "ingested-data"
    add_S3_bucket(s3_client, f"{bucket_prefix}-bucket-1")
    add_S3_bucket(s3_client, f"random-name-bucket-2")
    return s3_client


class TestTransformDataToCompatableFormat:
    """
    Test suite for the imput_into_s3 function.
    """

    def test_returns_correct_format(self, db_values):
        value_json_compat = transform_data_to_compatable_format(db_values)
        assert type(value_json_compat) is type({})
        assert type(value_json_compat["address"]) is type([])
        assert type(value_json_compat["address"][0]) is type({})

    def test_does_not_mutate_data(self, db_values):
        db_values_before = db_values
        transform_data_to_compatable_format(db_values)
        db_values_after = db_values
        assert db_values_before == db_values_after
        assert db_values_before is db_values_after

    def test_col_names_are_keys_for_new_data(self, db_values):
        table_values_json_compat = transform_data_to_compatable_format(db_values)
        column_names = ("id", "example_col", "test_data")

        db_data_table_names = []

        for key in db_values.keys():
            (table_name, column_names) = key
            db_data_table_names.append(table_name)

        count = 0
        for table_key, table_value in table_values_json_compat.items():
            oldCount = count
            count += 1
            assert table_key == db_data_table_names[oldCount]


class TestInputUpdatedDataIntoS3:

    def test_s3_client_not_modified(self, s3_client_two_buckets_one_prefix, db_values):
        client_before = s3_client_two_buckets_one_prefix
        input_updated_data_into_s3(s3_client_two_buckets_one_prefix, db_values)
        client_after = s3_client_two_buckets_one_prefix

        assert client_before == client_after
        assert client_before is client_after

    def test_db_values_not_modified(self, s3_client_two_buckets_one_prefix, db_values):
        db_values_before = db_values
        input_updated_data_into_s3(s3_client_two_buckets_one_prefix, db_values)
        db_values_after = db_values

        assert db_values_before == db_values_after
        assert db_values_before is db_values_after
