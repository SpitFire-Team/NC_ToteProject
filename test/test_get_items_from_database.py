import boto3
import pytest
import time
import sqlite3
from moto import mock_aws
from unittest.mock import Mock
from datetime import datetime, timezone

# from src.extraction_lambda.extraction_lambda import db_connection
from src.extraction_lambda_package.extraction_lambda.get_items_from_database import (
    set_latest_updated_time,
    check_database_updates,
    query_all_tables,
)

# Nice to have - move fixtures to seperate file for clarity


@pytest.fixture
def client():
    with mock_aws():
        yield boto3.client("s3", region_name="eu-west-2")


@pytest.fixture
def s3_bucket(client):
    return client.create_bucket(
        Bucket="test_bucket",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )


@pytest.fixture
def mock_database():
    conn = sqlite3.connect(":memory:")
    cursor = conn.cursor()

    cursor.executescript(
        """
        CREATE TABLE payment (
            id INTEGER PRIMARY KEY,
            name TEXT,
            last_updated TIMESTAMP
        );
        CREATE TABLE counterparty (
            id INTEGER PRIMARY KEY,
            name TEXT,
            last_updated TIMESTAMP
        );
        CREATE TABLE currency (
            id INTEGER PRIMARY KEY,
            name TEXT,
            last_updated TIMESTAMP
        );
        CREATE TABLE department (
            id INTEGER PRIMARY KEY,
            name TEXT,
            last_updated TIMESTAMP
        );
        CREATE TABLE design (
            id INTEGER PRIMARY KEY,
            name TEXT,
            last_updated TIMESTAMP
        );
        CREATE TABLE staff (
            id INTEGER PRIMARY KEY,
            name TEXT,
            last_updated TIMESTAMP
        );
        CREATE TABLE sales_order (
            id INTEGER PRIMARY KEY,
            name TEXT,
            last_updated TIMESTAMP
        );
        CREATE TABLE address (
            id INTEGER PRIMARY KEY,
            name TEXT,
            last_updated TIMESTAMP
        );
        CREATE TABLE purchase_order (
            id INTEGER PRIMARY KEY,
            name TEXT,
            last_updated TIMESTAMP
        );
        CREATE TABLE payment_type (
            id INTEGER PRIMARY KEY,
            name TEXT,
            last_updated TIMESTAMP
        );
        CREATE TABLE "transaction" (
        id INTEGER PRIMARY KEY,
        name TEXT,
        last_updated TIMESTAMP
        );
        """
    )

    cursor.executemany(
        """
        INSERT INTO payment (name, last_updated)
        VALUES (?, ?)
    """,
        [
            ("test_data1", datetime(2000, 1, 1, tzinfo=timezone.utc).isoformat()),
            ("test_data2", datetime(2030, 1, 1, tzinfo=timezone.utc).isoformat()),
            ("test_data3", datetime(1940, 1, 1, tzinfo=timezone.utc).isoformat()),
            ("test_data4", datetime(2001, 1, 1, tzinfo=timezone.utc).isoformat()),
            ("test_data5", datetime(2070, 1, 1, tzinfo=timezone.utc).isoformat()),
            ("test_data6", datetime(2005, 1, 1, tzinfo=timezone.utc).isoformat()),
            ("test_data7", datetime(2080, 1, 1, tzinfo=timezone.utc).isoformat()),
            ("test_data8", datetime(2031, 1, 1, tzinfo=timezone.utc).isoformat()),
        ],
    )

    cursor.executemany(
        """
        INSERT INTO address (name, last_updated)
        VALUES (?, ?)
    """,
        [
            ("test_data1", datetime(1000, 1, 1, tzinfo=timezone.utc).isoformat()),
            ("test_data2", datetime(1030, 1, 1, tzinfo=timezone.utc).isoformat()),
            ("test_data3", datetime(1940, 1, 1, tzinfo=timezone.utc).isoformat()),
            ("test_data4", datetime(1001, 1, 1, tzinfo=timezone.utc).isoformat()),
            ("test_data5", datetime(1070, 1, 1, tzinfo=timezone.utc).isoformat()),
            ("test_data6", datetime(1005, 1, 1, tzinfo=timezone.utc).isoformat()),
            ("test_data7", datetime(1080, 1, 1, tzinfo=timezone.utc).isoformat()),
            ("test_data8", datetime(1031, 1, 1, tzinfo=timezone.utc).isoformat()),
        ],
    )
    conn.commit()

    return conn


class TestSetLatestUpdatedTimeFunction:
    def test_datetime_returned(self, client, s3_bucket):
        result1 = set_latest_updated_time("test_bucket", client)

        current_time = datetime.now(timezone.utc)

        client.put_object(
            Bucket="test_bucket", Key=f"{str(current_time)}.ndjson", Body="test body"
        )

        result2 = set_latest_updated_time("test_bucket", client)

        assert isinstance(result1, datetime)
        assert isinstance(result2, datetime)

    def test_returns_1970_from_empty_s3(self, client, s3_bucket):
        result = set_latest_updated_time("test_bucket", client)

        expected = datetime(1970, 1, 1, 0, 0, 0, 0, tzinfo=timezone.utc)

        assert result == expected

    def test_time_single_item_bucket(self, client, s3_bucket):
        current_time = datetime.now(timezone.utc)

        client.put_object(
            Bucket="test_bucket", Key=f"{str(current_time)}.ndjson", Body="test body"
        )

        result = set_latest_updated_time("test_bucket", client)

        assert abs((result - current_time).total_seconds()) < 1

    def test_multi_item_bucket(self, client, s3_bucket):
        lowest_time = datetime.now(timezone.utc)
        time.sleep(1)

        middle_time = datetime.now(timezone.utc)
        time.sleep(1)

        highest_time = datetime.now(timezone.utc)

        client.put_object(
            Bucket="test_bucket", Key=f"{str(lowest_time)}.ndjson", Body="test body"
        )
        client.put_object(
            Bucket="test_bucket", Key=f"{str(middle_time)}.ndjson", Body="test body"
        )
        client.put_object(
            Bucket="test_bucket", Key=f"{str(highest_time)}.ndjson", Body="test body"
        )

        result = set_latest_updated_time("test_bucket", client)

        assert abs((result - highest_time).total_seconds()) < 1


class TestCheckDatabaseUpdatesFunction:
    def test_returns_row(self, mock_database):

        expected_data = (
            ["id", "name", "last_updated"],
            [(7, "test_data7", "2080-01-01T00:00:00+00:00")],
        )

        last_updated_time = datetime(2079, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

        results = check_database_updates(mock_database, "payment", last_updated_time)

        assert results == expected_data

    def test_returns_empty_list_for_no_new_updates(self, mock_database):
        expected_data = (["id", "name", "last_updated"], [])

        last_updated_time = datetime(2090, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

        results = check_database_updates(mock_database, "payment", last_updated_time)

        assert results == expected_data

    def test_returns_multiple_rows(self, mock_database):
        expected_data = (
            ["id", "name", "last_updated"],
            [
                (2, "test_data2", "2030-01-01T00:00:00+00:00"),
                (5, "test_data5", "2070-01-01T00:00:00+00:00"),
                (7, "test_data7", "2080-01-01T00:00:00+00:00"),
                (8, "test_data8", "2031-01-01T00:00:00+00:00"),
            ],
        )

        last_updated_time = datetime(2029, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

        results = check_database_updates(mock_database, "payment", last_updated_time)

        assert results == expected_data


class TestLastUpdatedTimePassedToCheckDatabaseUpdates:
    def test_last_updated_time_passed_from_s3_bucket_into_check_database_query(
        self, client, s3_bucket, mock_database
    ):
        expected_data = (
            ["id", "name", "last_updated"],
            [(7, "test_data7", "2080-01-01T00:00:00+00:00")],
        )

        object_time = datetime(2079, 1, 1, tzinfo=timezone.utc).isoformat()

        client.put_object(
            Bucket="test_bucket", Key=f"{str(object_time)}.ndjson", Body="test body"
        )

        client.list_objects = Mock(
            return_value={
                "Contents": [
                    {
                        "Key": "2079-01-01T00:00:00+00:00.ndjson",
                        "LastModified": datetime(2079, 1, 1, tzinfo=timezone.utc),
                    }
                ]
            }
        )

        last_updated_time = set_latest_updated_time("test_bucket", client)

        results = check_database_updates(mock_database, "payment", last_updated_time)

        assert results == expected_data


class TestQueryAllTablesFunction:
    def test_query_all_tables_returns_empty_dict_when_no_updates_found(
        self, mock_database
    ):

        expected_result = {}

        last_updated_time = datetime(3000, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

        result = query_all_tables(mock_database, last_updated_time)
        assert result == expected_result

    def test_query_all_tables_returns_single_updated_row_from_one_table(
        self, mock_database
    ):

        expected_result = {
            ("payment", ("id", "name", "last_updated")): [
                (7, "test_data7", "2080-01-01T00:00:00+00:00")
            ]
        }

        last_updated_time = datetime(2079, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

        result = query_all_tables(mock_database, last_updated_time)
        assert result == expected_result

    def test_query_all_tables_returns_multiple_updated_rows_from_multiple_tables(
        self, mock_database
    ):
        last_updated_time = datetime(1000, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

        result = query_all_tables(mock_database, last_updated_time)
        assert len(result) == 2
        assert len(result[("address", ("id", "name", "last_updated"))]) == 7
        assert len(result[("payment", ("id", "name", "last_updated"))]) == 8
