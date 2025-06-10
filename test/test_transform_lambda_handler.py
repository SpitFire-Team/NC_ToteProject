import pytest
from src.utils.file_utils import get_path_date_time_string
from src.transform_lambda.transform_lambda import lambda_handler


@pytest.fixture
def dummy_event_timenow():
    date_time_last_ingestion = get_path_date_time_string()
    return [{"last_ingested_str": date_time_last_ingestion}]

@pytest.fixture
def dummy_event_fixedtime():
    return [{"last_ingested_str": '06-06-2025_23:55'}] # this timestamp includes files with created_at and last_updated columns

class TestLambdaHandler:
    @pytest.mark.skip
    def test_datetime_string_passed_into_lambda_handler(self, dummy_event_timenow):
        result = lambda_handler(dummy_event_timenow, [{}])
        assert result == dummy_event_timenow
    
    @pytest.mark.skip
    def test_json_to_dataframe_does_not_error_when_created_at_and_last_updated_columns_not_present(self):
        last_ingestion_time = [{"last_ingested_str": '06-06-2025_23:54'}] # this timestamp includes files with no created_at and last_updated columns
        result = lambda_handler(last_ingestion_time, [{}])
        # test with mock up s3 bucket

    @pytest.mark.skip
    def test_json_to_dataframe_returns_list_of_dicts(self, dummy_event_fixedtime):
        result = lambda_handler(dummy_event_fixedtime, [{}])
        # test with mock up s3 bucket - query data structure

    @pytest.mark.skip
    def test_json_to_dataframe_returns_modified_columns(self, dummy_event_fixedtime):
        result = lambda_handler(dummy_event_fixedtime, [{}])
        # test with mock up s3 bucket - show column number reducing by 2
    
    @pytest.mark.skip
    def test_json_to_dataframe_returns_staff_and_department_dicts_only(self, dummy_event_fixedtime):
        result = lambda_handler(dummy_event_fixedtime, [{}])
        # test with mock up s3 bucket - show new list output containing list of dicts with staff and department only
    
    def test_json_to_dataframe_returns_list_of_dicts(self, dummy_event_fixedtime):
        result = lambda_handler(dummy_event_fixedtime, [{}])
        # test with mock up s3 bucket - show new list output containing list of dicts with staff and department only


    
