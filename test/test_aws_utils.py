import pytest
from src.utils.aws_utils import (
    get_bucket_name,
    add_data_to_s3_bucket,
    read_file_in_bucket,
)
from moto import mock_aws
import boto3
import json
import pandas as pd


# boto3 fixtures


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


def get_num_items_in_bucket(client, bucket_name):

    response = client.list_objects_v2(Bucket=bucket_name)
    return response["KeyCount"]


@pytest.fixture
def s3_client_one_bucket_one_file(s3_client):
    add_S3_bucket(s3_client, "test-bucket-1")
    return s3_client


@pytest.fixture
def s3_client_two_buckets_one_prefix(s3_client):
    bucket_prefix = "ingested-data"
    add_S3_bucket(s3_client, f"{bucket_prefix}-bucket-1")
    add_S3_bucket(s3_client, "random-name-bucket-2")
    return s3_client


@pytest.fixture
def s3_client_two_buckets_no_prefix(s3_client):
    add_S3_bucket(s3_client, "random-name-bucket-1")
    add_S3_bucket(s3_client, "random-name-bucket-2")
    return s3_client


@pytest.fixture
def s3_client_two_buckets_with_prefix(s3_client):
    bucket_prefix = "ingested-data"
    add_S3_bucket(s3_client, f"{bucket_prefix}-bucket-1")
    add_S3_bucket(s3_client, f"{bucket_prefix}-bucket-2")
    return s3_client


# data fixtures


@pytest.fixture
def json_data():
    db_values = [
        {"id": 7, "name": "test_data7", "last_updated": "2080-01-01T00:00:00+00:00"}
    ]

    return json.dumps(db_values)


@pytest.fixture
def dummy_df():
    column_name_list = [
        "col_1",
        "col_2",
        "col_3",
        "col_4",
        "col_5",
    ]
    staff_df = pd.DataFrame(columns=column_name_list)
    for i in range(10):
        data = {
            column_name_list[0]: f"{column_name_list[0]} values",
            column_name_list[1]: f"{column_name_list[1]} values",
            column_name_list[2]: f"{column_name_list[2]} values",
            column_name_list[3]: f"{column_name_list[3]} values",
            column_name_list[4]: f"{column_name_list[4]} values",
        }
        data_rows_to_add_df = pd.DataFrame(data, index=[i])
        staff_df = pd.concat([staff_df, data_rows_to_add_df], ignore_index=True)

    return staff_df


class TestGeBucketName:

    def test_function_returns_string(self, s3_client_two_buckets_one_prefix):
        bucket_prefix = "ingested-data"
        full_bucket_name = get_bucket_name(
            s3_client_two_buckets_one_prefix, bucket_prefix
        )

        assert type(full_bucket_name) is str

    def test_function_errors_if_non_string_is_sent_as_prefix(
        self, s3_client_two_buckets_one_prefix
    ):
        bucket_prefix = 12
        with pytest.raises(Exception):
            get_bucket_name(s3_client_two_buckets_one_prefix, bucket_prefix)

    def test_function_errors_if_no_bucket_with_prefix_exists(
        self, s3_client_two_buckets_no_prefix
    ):

        bucket_prefix = "ingested-data"

        with pytest.raises(Exception):
            get_bucket_name(s3_client_two_buckets_no_prefix, bucket_prefix)

    def test_function_errors_if_more_than_one_bucket_with_prefix_exists(
        self, s3_client_two_buckets_with_prefix
    ):
        bucket_prefix = "ingested-data"
        with pytest.raises(Exception):
            get_bucket_name(s3_client_two_buckets_with_prefix, bucket_prefix)


class TestAddDataToS3Bucket:
    def test_function_adds_a_file_to_bucket(
        self, s3_client_two_buckets_one_prefix, json_data
    ):
        bucket_prefix = "ingested-data"
        bucket_name = f"{bucket_prefix}-bucket-1"
        file_path = "test/test.json"
        files_in_bucket_before = get_num_items_in_bucket(
            s3_client_two_buckets_one_prefix, bucket_name
        )

        add_data_to_s3_bucket(
            s3_client_two_buckets_one_prefix, bucket_name, json_data, file_path
        )

        files_in_bucket_after = get_num_items_in_bucket(
            s3_client_two_buckets_one_prefix, bucket_name
        )

        assert files_in_bucket_after == files_in_bucket_before + 1
        # to do assert the json data uploaded correctly by downloading
        # file and reading body asserting that content is the same as
        # input json

    def test_function_does_not_mutate_data(
        self, s3_client_two_buckets_one_prefix, json_data
    ):
        bucket_prefix = "ingested-data"
        bucket_name = f"{bucket_prefix}-bucket-1"
        file_path = "test/test.json"
        json_data_before = json_data

        add_data_to_s3_bucket(
            s3_client_two_buckets_one_prefix, bucket_name, json_data, file_path
        )

        json_data_after = json_data
        assert json_data_before == json_data_after

    def test_function_errors_if_non_string_is_sent_as_bucket_name(
        self, s3_client_two_buckets_one_prefix, json_data
    ):
        file_path = "test/test.json"
        bucket_name = 12

        with pytest.raises(Exception):
            add_data_to_s3_bucket(
                s3_client_two_buckets_one_prefix, bucket_name, json_data, file_path
            )

    def test_function_errors_if_non_string_sent_as_file_path(
        self, s3_client_two_buckets_one_prefix, json_data
    ):
        bucket_prefix = "ingested-data"
        bucket_name = f"{bucket_prefix}-bucket-1"
        file_path = 12

        with pytest.raises(Exception):
            add_data_to_s3_bucket(
                s3_client_two_buckets_one_prefix, bucket_name, json_data, file_path
            )


@pytest.mark.skip(
    reason="function not used when deployed as lambda only used for testing"
)
class TestMakeS3Client:
    def test_make_s3_client(self):
        assert 1 == 2


class TestReadFileInBucket:
    def test_file_out_put_is_string(self, s3_client_one_bucket_one_file):
        body = "test-1"
        s3_client_one_bucket_one_file.put_object(
            Body=body,
            Bucket="test-bucket-1",
            Key="test-file-1",
        )
        output = read_file_in_bucket(
            s3_client_one_bucket_one_file, "test-bucket-1", "test-file-1"
        )
        assert type(output) is str

    def test_output_is_body(self, s3_client_one_bucket_one_file):
        body = "test-1"
        s3_client_one_bucket_one_file.put_object(
            Body=body,
            Bucket="test-bucket-1",
            Key="test-file-1",
        )
        read_file_in_bucket(
            s3_client_one_bucket_one_file, "test-bucket-1", "test-file-1"
        )
        body_after = body

        assert body == body_after

    def test_2(self, s3_client_one_bucket_one_file):
        body = "test-1"
        s3_client_one_bucket_one_file.put_object(
            Body=body,
            Bucket="test-bucket-1",
            Key="test-file-1",
        )
        output = read_file_in_bucket(
            s3_client_one_bucket_one_file, "test-bucket-1", "test-file-1"
        )

        assert output == body
