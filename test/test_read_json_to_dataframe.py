import boto3
import pytest
import os
import pandas as pd
from dotenv import load_dotenv
from src.utils.aws_utils import get_bucket_name
from src.transform_lambda_pkg.transform_lambda.read_json_to_dataframe import (
    read_json_to_dataframe,
)

# load .env from project root (configured to find .env in project root, when file is run from test/dir)
dotenv_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".env"))
load_dotenv(dotenv_path, override=True)
# optional debug print("check DB_NAME. DB_NAME from .env is:", os.getenv("DB_NAME"))

# AWS credentials from .env file
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")


@pytest.fixture
def s3_client():
    return boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name="eu-west-2",
    )


@pytest.fixture
def ingested_data_bucket_name(s3_client):
    # use bucket prefix to return the full / actual bucket name
    return get_bucket_name(s3_client, "ingested-data-bucket-")


@pytest.fixture
def latest_ingestion():
    return "06-06-2025_23:55"


def test_read_json_to_dataframe_returns_a_list(
    s3_client, ingested_data_bucket_name, latest_ingestion
):
    results = read_json_to_dataframe(
        s3_client, ingested_data_bucket_name, latest_ingestion
    )

    assert isinstance(results, list)


@pytest.mark.skip
def test_read_json_to_dataframe_returns_a_list_with_single_dict(
    s3_client, ingested_data_bucket_name, latest_ingestion
):
    results = read_json_to_dataframe(
        s3_client, ingested_data_bucket_name, latest_ingestion
    )
    print(">>>>", results)
    assert isinstance(results[0], dict)


def test_read_json_to_dataframe_returns_a_list_with_multiple_dicts(
    s3_client, ingested_data_bucket_name, latest_ingestion
):
    results = read_json_to_dataframe(
        s3_client, ingested_data_bucket_name, latest_ingestion
    )

    for item in results:
        assert isinstance(item, dict)


def test_read_json_to_dataframe_returns_a_list_of_dicts_with_one_table(
    s3_client, ingested_data_bucket_name, latest_ingestion
):
    results = read_json_to_dataframe(
        s3_client, ingested_data_bucket_name, latest_ingestion
    )

    for item in results:
        assert len(item) == 1


def test_read_json_to_dataframe_returns_a_list_of_dicts_with_valid_table_names(
    s3_client, ingested_data_bucket_name, latest_ingestion
):
    results = read_json_to_dataframe(
        s3_client, ingested_data_bucket_name, latest_ingestion
    )

    for item in results:
        for table_name in item:
            assert table_name
            assert isinstance(table_name, str)


def test_read_json_to_dataframe_returns_a_list_of_dicts_with_valid_dataframe_values(
    s3_client, ingested_data_bucket_name, latest_ingestion
):
    results = read_json_to_dataframe(
        s3_client, ingested_data_bucket_name, latest_ingestion
    )

    for item in results:
        for table_name, dataframe in item.items():
            assert not dataframe.empty
            assert isinstance(dataframe, pd.DataFrame)
