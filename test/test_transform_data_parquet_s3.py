import pytest
import pandas as pd
from src.transform_lambda.transform_data_parquet_s3 import (
    transform_data_to_parquet_on_s3,
)
from src.utils.file_utils import get_path_date_time_string
from moto import mock_aws
import boto3
from io import BytesIO


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
def s3_client_two_buckets_one_prefix(s3_client):
    bucket_prefix = "processed-data"
    add_S3_bucket(s3_client, f"{bucket_prefix}-bucket-1")
    add_S3_bucket(s3_client, "random-name-bucket-2")
    return s3_client


@pytest.fixture
def date_time_last_ingestion():
    return get_path_date_time_string()


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


@pytest.fixture
def dummy_dic_list(dummy_df):
    df_dict = {}
    list_dict = []
    table_name_list = [
        "fact_sales_order",
        "dim_staff",
        "dim_location",
        "dim_design",
        "dim_date",
        "dim_currency",
        "dim_counterparty",
    ]
    for table_name in table_name_list:
        df_dict[table_name] = dummy_df
        list_dict.append(df_dict)
    return list_dict


class TestTransformDataParquetS3:

    def test_function_adds_a_file_to_bucket(
        self, s3_client_two_buckets_one_prefix, dummy_dic_list, date_time_last_ingestion
    ):
        bucket_prefix = "processed-data"
        bucket_name = f"{bucket_prefix}-bucket-1"

        files_in_bucket_before = get_num_items_in_bucket(
            s3_client_two_buckets_one_prefix, bucket_name
        )

        transform_data_to_parquet_on_s3(
            s3_client_two_buckets_one_prefix,
            dummy_dic_list,
            date_time_last_ingestion,
        )

        files_in_bucket_after = get_num_items_in_bucket(
            s3_client_two_buckets_one_prefix, bucket_name
        )

        assert files_in_bucket_after == files_in_bucket_before + len(dummy_dic_list)

    def test_function_adds_parquet_file_to_bucket(
        self, s3_client_two_buckets_one_prefix, dummy_dic_list, date_time_last_ingestion
    ):
        bucket_prefix = "processed-data"
        bucket_name = f"{bucket_prefix}-bucket-1"
        table_name = list(dummy_dic_list[0].keys())[0]
        file_path = f"{table_name}/{date_time_last_ingestion}.parquet"

        transform_data_to_parquet_on_s3(
            s3_client_two_buckets_one_prefix, dummy_dic_list, date_time_last_ingestion
        )

        response = s3_client_two_buckets_one_prefix.get_object(
            Bucket=bucket_name, Key=file_path
        )
        file = pd.read_parquet(BytesIO(response["Body"].read()))

        assert (file == dummy_dic_list[0][table_name]).all().all()
