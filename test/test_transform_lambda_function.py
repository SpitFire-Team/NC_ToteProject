import pytest
from src.utils.file_utils import get_path_date_time_string
from src.transform_lambda_pkg.transform_lambda.transform_lambda_function import (
    lambda_handler, combine_tables, check_against_star_schema
)
from datetime import datetime, timezone
import datetime
from src.transform_lambda_pkg.transform_lambda.transform_data import star_schema_ref, db_ref, transform_table_names
import pandas as pd
from copy import deepcopy

@pytest.fixture
def currency_df():
    column_name_list = db_ref["currency"]
    rows = []
    for i in range(10):
        row = {}
        for col_name in column_name_list:
            row[col_name] = f"{i}"
        rows.append(row) 
    df = pd.DataFrame(rows, columns=column_name_list)
    return df

@pytest.fixture
def design_df():
    column_name_list = db_ref["design"]
    rows = []
    for i in range(10):
        row = {}
        for col_name in column_name_list:
            row[col_name] = f"{i}"
        rows.append(row) 
    df = pd.DataFrame(rows, columns=column_name_list)
    return df

@pytest.fixture
def staff_df():
    column_name_list = db_ref["staff"]
    rows = []
    for i in range(10):
        row = {}
        for col_name in column_name_list:
            row[col_name] = f"{i}"
        rows.append(row) 
    df = pd.DataFrame(rows, columns=column_name_list)
    return df

@pytest.fixture
def payment_type_df():
    column_name_list = db_ref["payment_type"]
    rows = []
    for i in range(10):
        row = {}
        for col_name in column_name_list:
            row[col_name] = f"{i}"
        rows.append(row) 
    df = pd.DataFrame(rows, columns=column_name_list)
    return df

@pytest.fixture
def transaction_df():
    column_name_list = db_ref["transaction"]
    rows = []
    for i in range(10):
        row = {}
        for col_name in column_name_list:
            row[col_name] = f"{i}"
        rows.append(row) 
    df = pd.DataFrame(rows, columns=column_name_list)
    return df

@pytest.fixture
def purchase_order_df():
    column_name_list = db_ref["purchase_order"]
    rows = []
    for i in range(10):
        row = {}
        for col_name in column_name_list:
            if col_name == "last_updated" or col_name == "created_at":
                row[col_name] = datetime.datetime(2000, 1, 1, tzinfo=timezone.utc).isoformat()
            else:
                row[col_name] = f"{i}"
        rows.append(row) 
    df = pd.DataFrame(rows, columns=column_name_list)
    return df

@pytest.fixture
def payment_df():
    column_name_list = db_ref["payment"]
    rows = []
    for i in range(10):
        row = {}
        for col_name in column_name_list:
            if col_name == "last_updated" or col_name == "created_at":
                row[col_name] = datetime.datetime(2000, 1, 1, tzinfo=timezone.utc).isoformat()
            else:
                row[col_name] = f"{i}"
        rows.append(row) 
    df = pd.DataFrame(rows, columns=column_name_list)
    return df

@pytest.fixture
def staff_df():
    column_name_list = [
        "staff_id",
        "first_name",
        "last_name",
        "department_id",
        "email_address",
    ]
    staff_df = pd.DataFrame(columns=column_name_list)
    for i in range(10):
        data = {
            column_name_list[0]: i,
            column_name_list[1]: f"first_name_{i}",
            column_name_list[2]: f"last_name_{i}",
            column_name_list[3]: i,
            column_name_list[4]: f"email_address{i}",
        }
        data_rows_to_add_df = pd.DataFrame(data, index=[i])
        staff_df = pd.concat([staff_df, data_rows_to_add_df], ignore_index=True)

    return staff_df


@pytest.fixture
def department_df():
    department_name_list = ["department_id", 
                            "department_name", 
                            "location", 
                            ]
    department_df = pd.DataFrame(columns=department_name_list)
    for i in range(10):
        data = {
            department_name_list[0]: i,
            department_name_list[1]: f"department_name_{i}",
            department_name_list[2]: f"location_{i}",
        }

        data_rows_to_add_df = pd.DataFrame(data, index=[i])
        department_df = pd.concat(
            [department_df, data_rows_to_add_df], ignore_index=True
        )
    return department_df


@pytest.fixture
def counterparty_df():
    department_name_list = ["counterparty_id", 
                            "counterparty_legal_name", 
                            "legal_address_id"] #dropped before, "created_at", "last_updated"]
    department_df = pd.DataFrame(columns=department_name_list)
    for i in range(10):
        data = {
            department_name_list[0]: i,
            department_name_list[1]: f"counterparty_legal_name {i}",
            department_name_list[2]: i,
        }

        data_rows_to_add_df = pd.DataFrame(data, index=[i])
        department_df = pd.concat(
            [department_df, data_rows_to_add_df], ignore_index=True
        )
    return department_df

@pytest.fixture
def address_df():
    column_name_list = db_ref["address"]
    rows = []
    for i in range(10):
        row = {}
        for col_name in column_name_list:
            row[col_name] = i
        rows.append(row) 
    df = pd.DataFrame(rows, columns=column_name_list)
    return df

@pytest.fixture 
def modifed_tables(currency_df, design_df, payment_type_df, transaction_df, payment_df, purchase_order_df):
    return [{"currency": currency_df}, 
            {"design": design_df}, 
            {"payment_type": payment_type_df}, 
            {"transaction": transaction_df},
            {"payment": payment_df}, 
            {"purchase_order": purchase_order_df}]

@pytest.fixture
def merged_tables(address_df, department_df, staff_df, counterparty_df):
    return [{"address": address_df}, {"counterparty": counterparty_df}, 
            {"staff": staff_df}, {"department": department_df}]



class TestCombineTables:
    def test_table_names_merged(self, merged_tables, modifed_tables):
        results = combine_tables(merged_tables, modifed_tables)
        merged_names = sorted([table_name for table in merged_tables for table_name in list(table.keys())])
        modified_names = sorted([table_name for table in modifed_tables for table_name in list(table.keys())])

        result_names = sorted([table_name for table in results for table_name in list(table.keys())])
        assert result_names == sorted(merged_names + modified_names)
        
    def test_dfs__merged(self, merged_tables, modifed_tables):
        results = combine_tables(merged_tables, modifed_tables)
        merged_dfs = [df for table in merged_tables for df in list(table.values())]
        modified_dfs = [df for table in modifed_tables for df in list(table.values())]

        result_dfs = [df for table in results for df in list(table.values())]
        assert result_dfs == merged_dfs + modified_dfs


@pytest.fixture
def star_schema_tables():
    final_table = []
    star_schema_ref_copy = deepcopy(star_schema_ref)
    for table_name, column_names in star_schema_ref_copy.items():
        rows = []
        for i in range(10):
            row = {}
            for col_name in column_names:
                row[col_name] = f"{i}"
            rows.append(row)
        df = pd.DataFrame(rows, columns=column_names)
        final_table.append({table_name: df})
    
    return final_table

class TestCheckAgainstStarSchema:
    def test_correct_table_names_and_columns_return_true(self, star_schema_tables):
        assert check_against_star_schema(star_schema_tables)
    
    def test_different_length_error(self, star_schema_tables):
        star_schema_tables.append({"test": pd.DataFrame()})
        with pytest.raises(Exception) as exc_info:
            check_against_star_schema(star_schema_tables)
        assert str(exc_info.value) == "Star Schema check error: Tables do not match star schema length"

    def test_incorrect_columns_name_error(self, star_schema_tables):
        star_schema_tables.pop()
        star_schema_tables.append({"dim_currency": pd.DataFrame()})
        with pytest.raises(Exception) as exc_info:
            check_against_star_schema(star_schema_tables)
        assert str(exc_info.value) == "Star Schema check error: dim_currency columns do not match star_schema_reference"

    def test_incorrect_table_name_error(self, star_schema_tables):
        star_schema_tables.pop()
        star_schema_tables.append({"test": pd.DataFrame()})
        with pytest.raises(Exception) as exc_info:
            check_against_star_schema(star_schema_tables)
        assert str(exc_info.value) == "Star Schema check error: test not in star_schema_reference"

    def test_incorrect_columns_name_error(self, star_schema_tables):
        star_schema_tables.pop()
        duplicated_table = star_schema_tables.pop()
        star_schema_tables.append(duplicated_table)
        star_schema_tables.append(duplicated_table)
        with pytest.raises(Exception) as exc_info:
            check_against_star_schema(star_schema_tables)
        assert str(exc_info.value) == "Star Schema check error: table names do not match star_schema_reference"

@pytest.mark.skip
class TestReorderAllDfColumns:
    def test1(self):
        #place holder
        pass



"""
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
"""
