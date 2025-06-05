import pytest
from unittest.mock import patch, Mock
from src.utils.aws_utils import get_bucket_name, add_json_to_s3_bucket, make_s3_client
from moto import mock_aws
import boto3
from pprint import pprint
import json


#boto3 fixtures

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
    return response['KeyCount']

@pytest.fixture
def s3_client_two_buckets_one_prefix(s3_client): 
    bucket_prefix = 'ingested-data'  
    add_S3_bucket(s3_client, f'{bucket_prefix}-bucket-1')
    add_S3_bucket(s3_client, f'random-name-bucket-2')
    return s3_client


@pytest.fixture
def s3_client_two_buckets_no_prefix(s3_client): 
    add_S3_bucket(s3_client, f'random-name-bucket-1')
    add_S3_bucket(s3_client, f'random-name-bucket-2')
    return s3_client

@pytest.fixture
def s3_client_two_buckets_with_prefix(s3_client): 
    bucket_prefix = 'ingested-data'  
    add_S3_bucket(s3_client, f'{bucket_prefix}-bucket-1')
    add_S3_bucket(s3_client, f'{bucket_prefix}-bucket-2')
    return s3_client

## data fixtures

@pytest.fixture
def json_data():
    db_values = [{
                'id': 7,
                'name': 'test_data7',
                'last_updated': '2080-01-01T00:00:00+00:00'}]
                
    return json.dumps(db_values)
   

class TestGeBucketName:
    
    def test_function_returns_string(self,s3_client_two_buckets_one_prefix):
        bucket_prefix = "ingested-data"
        full_bucket_name = get_bucket_name(s3_client_two_buckets_one_prefix, bucket_prefix)
        
        assert type(full_bucket_name) is str
        
    def test_function_errors_if_non_string_is_sent_as_prefix(self,s3_client_two_buckets_one_prefix):
        bucket_prefix = 12
        with pytest.raises(Exception):
            full_bucket_name = get_bucket_name(s3_client_two_buckets_one_prefix, bucket_prefix)

    def test_function_errors_if_no_bucket_with_prefix_exists(self,s3_client_two_buckets_no_prefix):

        bucket_prefix = "ingested-data"
        
        with pytest.raises(Exception):
            full_bucket_name = get_bucket_name(s3_client_two_buckets_no_prefix, bucket_prefix)
            
    
    def test_function_errors_if_more_than_one_bucket_with_prefix_exists(self,s3_client_two_buckets_with_prefix):
        bucket_prefix = "ingested-data"
        with pytest.raises(Exception):
            full_bucket_name = get_bucket_name(s3_client_two_buckets_with_prefix, bucket_prefix)
        
  
class TestAddJsonToS3Bucket:
    def test_function_adds_a_file_to_bucket(self,s3_client_two_buckets_one_prefix, json_data):
        bucket_prefix = 'ingested-data'  
        bucket_name =  f'{bucket_prefix}-bucket-1'
        file_path = "test/test.json"
        files_in_bucket_before = get_num_items_in_bucket(s3_client_two_buckets_one_prefix, bucket_name)
        
        add_json_to_s3_bucket(s3_client_two_buckets_one_prefix, bucket_name, json_data, file_path)
        
        files_in_bucket_after = get_num_items_in_bucket(s3_client_two_buckets_one_prefix, bucket_name)

        assert files_in_bucket_after == files_in_bucket_before + 1
        # to do assert the json data uploaded correctly by downloading
        # file and reading body asserting that content is the same as
        #input json
        
    def test_function_does_not_mutate_data(self,s3_client_two_buckets_one_prefix, json_data):
            bucket_prefix = 'ingested-data'  
            bucket_name =  f'{bucket_prefix}-bucket-1'
            file_path = "test/test.json"
            json_data_before = json_data
            
            add_json_to_s3_bucket(s3_client_two_buckets_one_prefix, bucket_name, json_data, file_path)
            
            json_data_after = json_data
            assert json_data_before == json_data_after
        
        
    def test_function_errors_if_non_string_is_sent_as_bucket_name(self,s3_client_two_buckets_one_prefix, json_data):
        file_path = "test/test.json"
        bucket_name = 12
        with pytest.raises(Exception):
            add_json_to_s3_bucket(s3_client_two_buckets_one_prefix, bucket_name, json_data, file_path)
            
    def test_function_errors_if_non_string_sent_as_file_path(self,s3_client_two_buckets_one_prefix, json_data):
        bucket_prefix = 'ingested-data'  
        bucket_name =  f'{bucket_prefix}-bucket-1'
        file_path = 12
        with pytest.raises(Exception):
            add_json_to_s3_bucket(s3_client_two_buckets_one_prefix, bucket_name, json_data, file_path)


class TestMakeS3Client:
    @pytest.mark.skip(reason="function not used when deployed as lambda only used for testing")
    def test_make_s3_client():
        assert 1 == 2

