import pytest
from src.utils.file_utils import get_path_date_time_string
from src.transform_lambda_pkg.transform_lambda.transform_lambda_function import (
    lambda_handler,
)


@pytest.fixture
def dummy_event_timenow():
    date_time_last_ingestion = get_path_date_time_string()
    return [{"last_ingested_str": date_time_last_ingestion}]


@pytest.fixture
def dummy_event_fixedtime():
    return [
        {"last_ingested_str": "06-06-2025_23:55"}
    ]  # this timestamp includes files with created_at and last_updated columns


@pytest.mark.skip("Skipped testing directly to AWS due to time constraints")
class TestLambdaHandler:
    @pytest.mark.skip
    def test_datetime_string_passed_into_lambda_handler(self, dummy_event_timenow):
        result = lambda_handler(dummy_event_timenow, [{}])
        assert result == dummy_event_timenow

    @pytest.mark.skip
    def test_json_to_dataframe_does_not_error_when_created_at_and_last_updated_columns_not_present(
        self,
    ):
        last_ingestion_time = [
            {"last_ingested_str": "06-06-2025_23:54"}
        ]  # this timestamp includes files with no created_at and last_updated columns
        result = lambda_handler(last_ingestion_time, [{}])
        assert result is not None
        # test with mock up s3 bucket

    @pytest.mark.skip
    def test_json_to_dataframe_returns_list_of_dicts(self, dummy_event_fixedtime):
        result = lambda_handler(dummy_event_fixedtime, [{}])
        assert result is not None
        # test with mock up s3 bucket - query data structure

    @pytest.mark.skip
    def test_json_to_dataframe_returns_modified_columns(self, dummy_event_fixedtime):
        result = lambda_handler(dummy_event_fixedtime, [{}])
        assert result is not None
        # test with mock up s3 bucket - show column number reducing by 2

    @pytest.mark.skip
    def test_json_to_dataframe_returns_staff_and_department_dicts_only(
        self, dummy_event_fixedtime
    ):
        result = lambda_handler(dummy_event_fixedtime, [{}])
        assert result is not None
        # test with mock up s3 bucket - show new list output containing list of dicts with staff and department only

    @pytest.mark.skip
    def test_json_to_dataframe_extracts_staff_and_department_dataframes(
        self, dummy_event_fixedtime
    ):
        result = lambda_handler(dummy_event_fixedtime, [{}])
        assert result is not None
        # test with mock up s3 bucket - show staff and department dataframes created

    @pytest.mark.skip
    def test_json_to_dataframe_returns_combined_dim_staff_table(
        self, dummy_event_fixedtime
    ):
        result = lambda_handler(dummy_event_fixedtime, [{}])
        assert result is not None
        # test with mock up s3 bucket - show combined dim staff table output

    @pytest.mark.skip
    def test_json_to_dataframe_returns_adds_dim_staff_to_list_of_dicts(
        self, dummy_event_fixedtime
    ):
        result = lambda_handler(dummy_event_fixedtime, [{}])
        assert result is not None
        # test with mock up s3 bucket - show modified_data includes dim_staff dataframe

    @pytest.mark.skip
    def test_json_to_dataframe_returns_removes_staff_and_department_from_list_of_dicts(
        self, dummy_event_fixedtime
    ):
        result = lambda_handler(dummy_event_fixedtime, [{}])
        assert result is not None
        # test with mock up s3 bucket - show modified_data removes staff and department dfs along with empty dicts

    def test_json_to_dataframe_returns_transforms_list_of_dicts_to_parquet(
        self, dummy_event_fixedtime
    ):
        result = lambda_handler(dummy_event_fixedtime, [{}])
        assert result is not None
        # test with mock up s3 bucket - show list of dicts transformed as parquet
