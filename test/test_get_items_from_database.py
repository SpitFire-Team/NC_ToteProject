import boto3
import pytest
import time
import sqlite3
from moto import mock_aws
from unittest.mock import patch, Mock
from datetime import datetime, timezone
from src.extraction_lambda.extraction_lambda import db_connection
from src.extraction_lambda.get_items_from_database import (
    set_latest_updated_time,
    check_database_updates,
)


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

    cursor.execute(
        """
        CREATE TABLE payment (
            id INTEGER PRIMARY KEY, 
            name TEXT,
            last_updated TIMESTAMP
        )
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
    conn.commit()

    return conn


class TestSetLatestUpdatedTime:
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


class TestCheckDatabaseUpdates:
    def test_returns_query(self, mock_database):

        expected_data = [
            (
                7,
                "test_data7",
                datetime(2080, 1, 1, 0, 0, 0, tzinfo=timezone.utc).isoformat(),
            )
        ]

        last_checked_time = datetime(2079, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

        results = check_database_updates(mock_database, "payment", last_checked_time)

        assert results == expected_data

    def test_returns_empty_list_for_no_new_updates(self, mock_database):
        expected_data = []

        last_checked_time = datetime(2090, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

        results = check_database_updates(mock_database, "payment", last_checked_time)

        assert results == expected_data

    def test_returns_multiple_queries_with_new(self, mock_database):
        expected_data = [
            (
                2,
                "test_data2",
                datetime(2030, 1, 1, 0, 0, 0, tzinfo=timezone.utc).isoformat(),
            ),
            (
                5,
                "test_data5",
                datetime(2070, 1, 1, 0, 0, 0, tzinfo=timezone.utc).isoformat(),
            ),
            (
                7,
                "test_data7",
                datetime(2080, 1, 1, 0, 0, 0, tzinfo=timezone.utc).isoformat(),
            ),
            (
                8,
                "test_data8",
                datetime(2031, 1, 1, 0, 0, 0, tzinfo=timezone.utc).isoformat(),
            ),
        ]

        last_checked_time = datetime(2029, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

        results = check_database_updates(mock_database, "payment", last_checked_time)

        assert results == expected_data


class TestLatestTimeAndCheckDatabaseUpdates:
    def test_latest_time_passed_into_check_database_single_query(
        self, client, s3_bucket, mock_database
    ):
        expected_data = [
            (
                7,
                "test_data7",
                datetime(2080, 1, 1, 0, 0, 0, tzinfo=timezone.utc).isoformat(),
            )
        ]

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

        last_checked_time = set_latest_updated_time("test_bucket", client)

        results = check_database_updates(mock_database, "payment", last_checked_time)

        assert results == expected_data


# test multiple queries
# test multiple objects in the bucket
# test multiple queries with multiple buckets
