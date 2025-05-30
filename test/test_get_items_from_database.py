import boto3
import pytest
import time
from moto import mock_aws
from datetime import datetime, timezone
from src.extraction_lambda.get_items_from_database import set_latest_updated_time

@pytest.fixture
def client():
    with mock_aws():
        yield boto3.client("s3", region_name="eu-west-2")

@pytest.fixture
def s3_bucket(client):
    return client.create_bucket(Bucket="test_bucket", CreateBucketConfiguration={"LocationConstraint": "eu-west-2"})

class TestSetLatestUpdatedTime:
    def test_datetime_returned(self, client, s3_bucket):
        result1 = set_latest_updated_time("test_bucket", client)

        current_time = datetime.now(timezone.utc)

        client.put_object(Bucket="test_bucket", Key=f"{str(current_time)}.ndjson", Body="test body")

        result2 = set_latest_updated_time("test_bucket", client)

        assert isinstance(result1, datetime)
        assert isinstance(result2, datetime)

    def test_returns_1970_from_empty_s3(self, client, s3_bucket):
        result = set_latest_updated_time("test_bucket", client)

        expected = datetime(1970, 1, 1, 0, 0, 0, 0, tzinfo=timezone.utc)

        assert result == expected

    def test_time_single_item_bucket(self, client, s3_bucket):
        current_time = datetime.now(timezone.utc)

        client.put_object(Bucket="test_bucket", Key=f"{str(current_time)}.ndjson", Body="test body")

        result = set_latest_updated_time("test_bucket", client)

        assert abs((result - current_time).total_seconds()) < 1

    def test_multi_item_bucket(self, client, s3_bucket):
        lowest_time = datetime.now(timezone.utc)
        time.sleep(1)

        middle_time = datetime.now(timezone.utc) 
        time.sleep(1)

        highest_time = datetime.now(timezone.utc)

        client.put_object(Bucket="test_bucket", Key=f"{str(lowest_time)}.ndjson", Body="test body")
        client.put_object(Bucket="test_bucket", Key=f"{str(middle_time)}.ndjson", Body="test body")
        client.put_object(Bucket="test_bucket", Key=f"{str(highest_time)}.ndjson", Body="test body")

        result = set_latest_updated_time("test_bucket", client)

        assert abs((result - highest_time).total_seconds()) < 1
