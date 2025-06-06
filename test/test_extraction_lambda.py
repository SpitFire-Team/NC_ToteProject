import boto3
import pytest
import time
from moto import mock_aws
from unittest.mock import patch, Mock
from src.extraction_lambda.extraction_lambda import (
    db_connection,
    filter_buckets,
    find_latest_ingestion_bucket,
)



@pytest.fixture
def client():
    with mock_aws():
        yield boto3.client("s3", region_name="eu-west-2")


class TestDatabaseConnection:
    """
    Test suite for the db_connection function.
    """

    @patch(
        "src.extraction_lambda.extraction_lambda.pg8000.connect",
        side_effect=Exception("Connection failed"),
    )
    def test_failed_connection(self, mock_connect, monkeypatch):
        """
        Tests that db_connection raises an error when the database fails to connect.
        """
        monkeypatch.setenv("USER", "test_user")
        monkeypatch.setenv("PASSWORD", "test_password")
        monkeypatch.setenv("HOST", "test_host")
        monkeypatch.setenv("PORT", "5342")
        monkeypatch.setenv("DATABASE", "test_database")

        with pytest.raises(Exception, match="Connection failed"):
            db_connection()

    @patch("src.extraction_lambda.extraction_lambda.pg8000.connect")
    def test_successful_connection(self, mock_connect, monkeypatch):
        """
        Tests that db_connection returns the connection object when the connection is successful.
        """
        monkeypatch.setenv("USER", "test_user")
        monkeypatch.setenv("PASSWORD", "test_password")
        monkeypatch.setenv("HOST", "test_host")
        monkeypatch.setenv("PORT", "5342")
        monkeypatch.setenv("DATABASE", "test_database")

        mock_conn = Mock()
        mock_connect.return_value = mock_conn

        result = db_connection()

        assert result == mock_conn

    def test_env_variables(self, monkeypatch):
        """
        Tests that db_connection correctly reads environment variables.
        """
        monkeypatch.setenv("USER", "test_user")
        monkeypatch.setenv("PASSWORD", "test_password")
        monkeypatch.setenv("HOST", "test_host")
        monkeypatch.setenv("PORT", "5342")
        monkeypatch.setenv("DATABASE", "test_database")

        with patch(
            "src.extraction_lambda.extraction_lambda.pg8000.connect"
        ) as mock_connect:
            mock_conn = Mock()
            mock_connect.return_value = mock_conn

            result = db_connection()

            mock_connect.assert_called_once_with(
                user="test_user",
                password="test_password",
                host="test_host",
                port=5342,
                database="test_database",
            )

        assert result == mock_conn


class TestFilterBuckets:
    """
    Test suite for filter_buckets function
    """

    def test_returns_empty_list_when_no_buckets(self):
        """
        Test that empty list is returned with no buckets passed
        """
        data = []

        expected = []

        result = filter_buckets(data)

        assert result == expected

    def test_returns_empty_list_when_no_matching_buckets(self):
        """
        Test returns empty list when theres no matching buckets
        """
        data = [
            {"Name": "test-bucket-1"},
            {"Name": "test-bucket-2"},
            {"Name": "test-bucket-3"},
        ]

        expected = []

        result = filter_buckets(data)

        assert result == expected

    def test_correct_bucket_single_bucket(self):
        """
        Test the correct bucket is returned when only a single bucket is passed
        """
        data = [{"Name": "ingested-data-bucket-45879345"}]

        expected = [{"Name": "ingested-data-bucket-45879345"}]

        result = filter_buckets(data)

        assert result == expected

    def test_single_correct_bucket_multi_items(self):
        """
        Test the correct bucket is returned when multiple wrong and one correct bucket is passed
        """
        data = [
            {"Name": "test-bucket-1"},
            {"Name": "ingested-data-bucket-45879345"},
            {"Name": "test-bucket-2"},
            {"Name": "test-bucket-3"},
        ]

        expected = [{"Name": "ingested-data-bucket-45879345"}]

        result = filter_buckets(data)

        assert result == expected

    def test_returns_multiple_correct_buckets(self):
        """
        Test correct buckets are returned when multiple correct buckets are passed
        """
        data = [
            {"Name": "test-bucket-1"},
            {"Name": "ingested-data-bucket-45879345"},
            {"Name": "test-bucket-2"},
            {"Name": "ingested-data-bucket-452349809345"},
            {"Name": "test-bucket-3"},
            {"Name": "ingested-data-bucket-45832904879345"},
        ]

        expected = [
            {"Name": "ingested-data-bucket-45879345"},
            {"Name": "ingested-data-bucket-452349809345"},
            {"Name": "ingested-data-bucket-45832904879345"},
        ]

        result = filter_buckets(data)

        assert result == expected


class TestFindLatestIngestionBucket:
    """
    Test suite for find_latest_ingestion_bucket
    """

    def test_returns_none_no_buckets(self, client):
        """
        Test an appropriate response for no available buckets
        """
        expected = None  # In the future will change to an error

        result = find_latest_ingestion_bucket(client)

        assert result == expected

    def test_returns_none_with_no_matching_buckets(self, client):
        """
        Test an appropriate response when theres no matching buckets
        """
        client.create_bucket(
            Bucket="test-bucket1",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

        client.create_bucket(
            Bucket="test-bucket2",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

        expected = None  # In the future will change to an error

        result = find_latest_ingestion_bucket(client)

        assert result == expected

    def test_returns_correct_bucket_one_matching_bucket(self, client):
        """
        Test the correct bucket returned when theres only one matching bucket
        """
        client.create_bucket(
            Bucket="test-bucket1",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

        client.create_bucket(
            Bucket="ingested-data-bucket-429373",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

        client.create_bucket(
            Bucket="test-bucket2",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

        expected = "ingested-data-bucket-429373"

        result = find_latest_ingestion_bucket(client)

        assert result == expected

    def test_returns_correct_bucket_multiple_matching_buckets(self, client):
        """
        Test the correct bucket is returned when theres multiple matching buckets
        """
        client.create_bucket(
            Bucket="test-bucket1",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

        client.create_bucket(
            Bucket="ingested-data-bucket-429373",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

        client.create_bucket(
            Bucket="test-bucket2",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

        client.create_bucket(
            Bucket="ingested-data-bucket-42937343",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

        time.sleep(3)

        client.create_bucket(
            Bucket="ingested-data-bucket-423",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

        expected = "ingested-data-bucket-423"

        result = find_latest_ingestion_bucket(client)

        assert result == expected

    def test_returns_correct_bucket_latest_created_not_matching(self, client):
        """
        Test the correct bucket is returned when the latest created bucket wasnt an ingestion bucket
        """
        client.create_bucket(
            Bucket="test-bucket1",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

        client.create_bucket(
            Bucket="ingested-data-bucket-429373",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

        client.create_bucket(
            Bucket="test-bucket2",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

        time.sleep(2)

        client.create_bucket(
            Bucket="ingested-data-bucket-42937343",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

        time.sleep(2)

        client.create_bucket(
            Bucket="test-bucket3",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

        expected = "ingested-data-bucket-42937343"

        result = find_latest_ingestion_bucket(client)

        assert result == expected
