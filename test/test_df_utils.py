from datetime import timezone
import datetime
from src.utils.df_utils import (
    remove_dataframe_columns,
    add_prefix_to_table_name,
    merge_dataframes,
    reorder_dataframe,
    rename_dataframe_columns,
    currency_code_to_currency_name,
    convert_timestamp,
    add_index
)

from src.transform_lambda_pkg.transform_lambda.transform_data import db_ref, rename_col_names_ref, currency_dict

import pandas as pd
import pytest
from copy import deepcopy




@pytest.fixture
def dummy_df():
    data = {
        "currency_id": [4, 5, 6, 7],
        "currency_code": ["a", "b", "c", "d"],
        "created_at": [
            datetime.datetime.now(),
            datetime.datetime.now(),
            datetime.datetime.now(),
            datetime.datetime.now(),
        ],
        "last_updated": [
            datetime.datetime.now(),
            datetime.datetime.now(),
            datetime.datetime.now(),
            datetime.datetime.now(),
        ],
    }
    test_df = pd.DataFrame.from_dict(data)
    return test_df


@pytest.fixture
def dummy_df2():
    data = {
        "currency_id": [4, 5, 6, 7],
        "test_col1": [1, 2, 3, 4],
        "test_col2": [8, 9, 10, 11],
        "last_updated": [12, 13, 14, 15],
    }
    test_df = pd.DataFrame.from_dict(data)
    return test_df


@pytest.fixture
def dummy_df3():
    data = {
        "currency_id": [4, 5, 6, 7],
        "test_col3": [1, 2, 3, 4],
        "test_col4": [8, 9, 10, 11],
    }
    test_df = pd.DataFrame.from_dict(data)
    return test_df

@pytest.fixture
def dummy_df4():
    data = {
        "currency_id": [1, 2, 3, 10],
        "test_col9": [1, 2, 3, 4],
        "test_col10": [8, 9, 10, 11],
    }
    test_df = pd.DataFrame.from_dict(data)
    return test_df

@pytest.fixture
def address_df():
    column_name_list = db_ref["address"]
    rows = []
    for i in range(10):
        row = {}
        for col_name in column_name_list:
            row[col_name] = f"{i}"
        rows.append(row) 
    df = pd.DataFrame(rows, columns=column_name_list)
    return df

@pytest.fixture
def cols_to_rename_addr():
    rename_addr_cols = deepcopy(rename_col_names_ref['dim_counterparty'])
    # print(rename_addr_cols)
    return rename_addr_cols

@pytest.fixture
def currency_df():
    currency_name_list = db_ref["currency"]
    df = pd.DataFrame(columns=currency_name_list)
    for i in range(10):
        data = {
            currency_name_list[0]: i,
            currency_name_list[1]: list(currency_dict.keys())[i],
            currency_name_list[2]: f"{i}",
            currency_name_list[3]: f"{i}",
        }

        data_rows_to_add_df = pd.DataFrame(data, index=[i])
        df = pd.concat(
            [df, data_rows_to_add_df], ignore_index=True
        )
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
def sales_order_df():
    column_name_list = db_ref["sales_order"]
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

class TestRemoveDataFrameColumns:

    def test_empty_list_warns_user_and_returns_df_unchanged(self, dummy_df):
        columns_to_remove = []
        result = remove_dataframe_columns(dummy_df, columns_to_remove)

        assert result.equals(dummy_df)

    def test_nothing_removed_when_removing_exisitng_column(self, dummy_df):
        columns_to_remove = ["random"]
        result = remove_dataframe_columns(dummy_df, columns_to_remove)

        assert result.equals(dummy_df)

    def test_function_removes_specified_column(self, dummy_df):
        columns_to_remove = ["currency_id"]
        result = remove_dataframe_columns(dummy_df, columns_to_remove)

        assert not result.equals(dummy_df)
        assert columns_to_remove[0] not in result.columns

    def test_function_removes_specified_columns(self, dummy_df):
        columns_to_remove = ["currency_id", "created_at"]
        result = remove_dataframe_columns(dummy_df, columns_to_remove)

        assert not result.equals(dummy_df)
        for col in columns_to_remove:
            assert col not in result.columns

    def test_function_throws_error_if_all_columns_removed(self, dummy_df):
        columns_to_remove = dummy_df.columns

        with pytest.raises(Exception) as exc_info:
            remove_dataframe_columns(dummy_df, columns_to_remove)

        assert str(exc_info.value) == "all columns removed from dataframe"

    def test_original_df_not_mutatated(self, dummy_df):
        columns_to_remove = ["currency_id", "created_at"]
        result = remove_dataframe_columns(dummy_df, columns_to_remove)
        assert result is not dummy_df


class TestAddPrefixToTableName:

    def test_prefix_is_added_to_table_name(self, dummy_df):
        data = {"test": dummy_df}
        prefix = "dim_"
        result = add_prefix_to_table_name(data, prefix)

        assert list(result.keys())[0] == "dim_test"

    def test_dataframe_does_not_change(self, dummy_df):
        data = {"test": dummy_df}
        prefix = "dim_"
        result = add_prefix_to_table_name(data, prefix)

        assert result["dim_test"].equals(data["test"])

    def test_dataframe_not_mutated(self, dummy_df):
        data = {"test": dummy_df}
        prefix = "dim_"
        result = add_prefix_to_table_name(data, prefix)

        assert result["dim_test"] is not data["test"]

    def test_table_dict_not_mutated(self, dummy_df):
        data = {"test": dummy_df}
        data_copy = deepcopy(data)
        prefix = "dim_"
        add_prefix_to_table_name(data, prefix)

        assert data.keys() == data_copy.keys()
        assert data["test"].equals(data_copy["test"])


class TestMergeDataframes:
    def test_no_shared_merge_column(self, dummy_df, dummy_df2):
        merge_column = "test_col1"

        column_names = []

        with pytest.raises(Exception) as exc_info:
            merge_dataframes(dummy_df, dummy_df2, merge_column, column_names)

        assert str(exc_info.value) == "Merge column not in both dataframes"

    def test_column_names_doesnt_match_merge_column(self, dummy_df, dummy_df2):
        merge_column = "currency_id"

        column_names = ["currency_id", "currency_code", "test_col1", "non_existent"]

        with pytest.raises(Exception) as exc_info:
            merge_dataframes(dummy_df, dummy_df2, merge_column, column_names)

        assert str(exc_info.value) == "Shared column"

    def test_if_dataframes_share_column_throws_exception(self, dummy_df):
        merge_column = "currency_id"

        dummy_df_copy = dummy_df.copy()

        column_names = ["currency_id", "currency_code", "test_col1"]

        with pytest.raises(Exception) as exc_info:
            merge_dataframes(dummy_df, dummy_df_copy, merge_column, column_names)

        assert str(exc_info.value) == "Shared column"

    def test_dataframes_merged_all_columns(self, dummy_df, dummy_df3):
        merge_column = "currency_id"

        column_names = [
            "currency_id",
            "currency_code",
            "test_col3",
            "last_updated",
            "created_at",
            "test_col4",
        ]

        result = merge_dataframes(dummy_df, dummy_df3, merge_column, column_names)

        for col in column_names:
            assert col in result.columns

    def test_dataframes_merged_all_columns_and_values(self, dummy_df2, dummy_df3):
        merge_column = "currency_id"

        column_names = [
            "currency_id",
            "test_col1",
            "test_col2",
            "last_updated",
            "test_col3",
            "test_col4",
        ]

        data = {
            "currency_id": [4, 5, 6, 7],
            "test_col1": [1, 2, 3, 4],
            "test_col2": [8, 9, 10, 11],
            "last_updated": [12, 13, 14, 15],
            "test_col3": [1, 2, 3, 4],
            "test_col4": [8, 9, 10, 11],
        }

        test_df = pd.DataFrame.from_dict(data)

        result = merge_dataframes(dummy_df2, dummy_df3, merge_column, column_names)

        assert result.equals(test_df)

    def test_dataframes_not_mutated(self, dummy_df, dummy_df3):
        merge_column = "currency_id"

        column_names = [
            "currency_id",
            "currency_code",
            "test_col3",
            "last_updated",
            "created_at",
            "test_col4",
        ]

        dummy_df_copy = dummy_df.copy()

        dummy_df3_copy = dummy_df3.copy()

        merge_dataframes(dummy_df, dummy_df3, merge_column, column_names)

        assert dummy_df.equals(dummy_df_copy)
        assert dummy_df3.equals(dummy_df3_copy)

    def test_empty_dataframe_throws_error(self, dummy_df):
        merge_column = "currency_id"

        column_names = [
            "currency_id",
            "currency_code",
            "test_col3",
            "last_updated",
            "created_at",
            "test_col4",
        ]

        df_empty = pd.DataFrame({"A": []})

        with pytest.raises(Exception) as exc_info:
            merge_dataframes(dummy_df, df_empty, merge_column, column_names)

        assert str(exc_info.value) == "Merge column not in both dataframes"

        with pytest.raises(Exception) as exc_info:
            merge_dataframes(df_empty, dummy_df, merge_column, column_names)

        assert str(exc_info.value) == "Merge column not in both dataframes"

        # check dfs have same shape

        df_empty = pd.DataFrame({"currency_id": []})

        with pytest.raises(Exception) as exc_info:
            merge_dataframes(dummy_df, df_empty, merge_column, column_names)

        assert str(exc_info.value) == "Dataframes don't have the same number of values"

        with pytest.raises(Exception) as exc_info:
            merge_dataframes(df_empty, dummy_df, merge_column, column_names)

        assert str(exc_info.value) == "Dataframes don't have the same number of values"

        # check dfs have same shape

        df_empty = pd.DataFrame({"currency_id": [], "test_col3": [], "test_col4": []})

        with pytest.raises(Exception) as exc_info:
            merge_dataframes(dummy_df, df_empty, merge_column, column_names)

        assert str(exc_info.value) == "Dataframes don't have the same number of values"

        with pytest.raises(Exception) as exc_info:
            merge_dataframes(df_empty, dummy_df, merge_column, column_names)

        assert str(exc_info.value) == "Dataframes don't have the same number of values"

    def test_exception_thrown_when_no_shared_merge_values(self, dummy_df3, dummy_df4):
        column_names = ["test_col3", "test_col4", "test_col9", "test_col10", "currency_id"]

        merge_column = "currency_id"

        with pytest.raises(Exception) as exc_info:
            merge_dataframes(dummy_df3, dummy_df4, merge_column, column_names)

        assert str(exc_info.value) == "Merge failed: no shared values in merge column"

class TestReorderDataframe:

    def test_correctly_reorders_df(self, dummy_df2):
        column_names = ["test_col1", "test_col2", "currency_id", "last_updated"]
        data = {
            "test_col1": [1, 2, 3, 4],
            "test_col2": [8, 9, 10, 11],
            "currency_id": [4, 5, 6, 7],
            "last_updated": [12, 13, 14, 15],
        }
        test_df = pd.DataFrame.from_dict(data)

        result = reorder_dataframe(dummy_df2, column_names)
        assert result.equals(test_df)

    def test_unknown_column_raises_exception(self, dummy_df2):
        column_names = [
            "test_col1",
            "test_col2",
            "currency_id",
            "last_updated",
            "unknown",
        ]

        with pytest.raises(Exception) as exc_info:
            reorder_dataframe(dummy_df2, column_names)

        assert str(exc_info.value) == "Can't reorder df, column not in dataframe"

    def test_extra_column_raises_exception(self, dummy_df2):
        column_names = ["test_col1", "test_col2", "currency_id"]

        with pytest.raises(Exception) as exc_info:
            reorder_dataframe(dummy_df2, column_names)

        assert (
            str(exc_info.value)
            == "Can't reorder df, column in dataframe but not in list"
        )

class TestRenameDataframeColumns:
    def test_returns_df(self, address_df, cols_to_rename_addr):
        renamed_col_df = rename_dataframe_columns(address_df, cols_to_rename_addr)
        assert type(renamed_col_df) == type(address_df)
    
    def test_returns_error_if_df_is_empty(self, cols_to_rename_addr):
        df = pd.DataFrame()
        with pytest.raises(Exception) as exc_info: 
            rename_dataframe_columns(df, cols_to_rename_addr)
        
        assert str(exc_info.value) == "Column rename failed: has no columns"

        df = None
        
        with pytest.raises(Exception) as exc_info: 
            rename_dataframe_columns(df, cols_to_rename_addr)
            
        assert str(exc_info.value) == "Column rename failed: dataframe is None"

    def test_returns_error_if_cols_to_rename_is_not_a_dict(self, address_df):
        cols_to_rename = ["address_line_1"]
        
        with pytest.raises(Exception) as exc_info:
                rename_dataframe_columns(address_df, cols_to_rename)

        assert str(exc_info.value) == "Column rename failed: cols_to_rename must be a dictionary"

    def test_returns_error_if_col_to_rename_not_in_df(self, address_df, cols_to_rename_addr):
        
        cols_to_rename_addr["col name not in df"] = "renamed col"

        with pytest.raises(Exception) as exc_info:
                rename_dataframe_columns(address_df, cols_to_rename_addr)
                
        assert str(exc_info.value) == "Column rename failed: column name not in df"
        
        
    def test_returns_error_if_col_to_rename_and_renamed_value_not_strings(self, address_df, cols_to_rename_addr):
        
        cols_to_rename_addr["address_line_1"] = 12

        with pytest.raises(Exception) as exc_info:
                rename_dataframe_columns(address_df, cols_to_rename_addr)
                
        assert str(exc_info.value) == "Column rename failed: column name and new name must be strings"

        cols_to_rename_addr["address_line_1"] = "counterparty_legal_address_line_1"
        cols_to_rename_addr[12] = "counterparty_legal_address_line_1"

        with pytest.raises(Exception) as exc_info:
                rename_dataframe_columns(address_df, cols_to_rename_addr)
                
        assert str(exc_info.value) == "Column rename failed: column name and new name must be strings"
        
    def test_renames_columns(self, address_df, cols_to_rename_addr):
        column_names_before = list(address_df.columns)
        renamed_col_df = rename_dataframe_columns(address_df, cols_to_rename_addr)
        new_names_check = list(cols_to_rename_addr.values())
        new_col_names = list(renamed_col_df.columns)

        for new_col in new_col_names:
            if new_col in column_names_before and new_col not in new_names_check:
                continue
            assert new_col in new_names_check
    
    def test_raises_error_if_names_the_same(self, address_df, cols_to_rename_addr):
        
        old_col_names = list(cols_to_rename_addr.keys())
        
        for key in cols_to_rename_addr.keys():
            cols_to_rename_addr[key] = key

        with pytest.raises(Exception) as exc_info:
                rename_dataframe_columns(address_df, cols_to_rename_addr)
        
        assert str(exc_info.value) == "Column rename failed: new names cannot be the same as old names"

class TestCurrecncyCodeToCurrencyName:
    def test_returns_df(self, currency_df):
        result = currency_code_to_currency_name(currency_df)

        assert type(result) == type(currency_df)
    
    def test_input_df_modified(self, currency_df):
        result = currency_code_to_currency_name(currency_df)

        assert not result.equals(currency_df)

    def test_currency_df_passed_in(self, address_df):
        with pytest.raises(Exception) as exc_info:
            currency_code_to_currency_name(address_df)
        
        assert str(exc_info.value) == "Currency code: incorrect df"

    def test_new_currency_name_column_created(self, currency_df):
        result = currency_code_to_currency_name(currency_df)

        assert "currency_name" not in list(currency_df.columns)
        assert "currency_name" in list(result.columns)
    
    def test_if_df_has_incorrect_currency_code_records_error(self, currency_df):
        currency_df["currency_code"] = currency_df["currency_code"] + "x"

        result = currency_code_to_currency_name(currency_df)
        
        for val in result["currency_name"]:
            assert val == "Error"

    def test_code_converts_correctly(self, currency_df):
        result = currency_code_to_currency_name(currency_df)

        for i, row in result.iterrows():
            
            currency_code = row["currency_code"]
            currency_name = row["currency_name"]

            
            assert currency_name == currency_dict[currency_code]

class TestConvertTimestamp:
    def test_returns_df(self, purchase_order_df):
        result = convert_timestamp(purchase_order_df)

        assert type(result) == type(purchase_order_df)

    def test_input_df_modified(self, purchase_order_df):
        result = convert_timestamp(purchase_order_df)

        assert not result.equals(purchase_order_df)

    def test_new_columns_created(self, sales_order_df):
        result = convert_timestamp(sales_order_df)

        assert "last_updated_date" not in list(sales_order_df.columns)
        assert "last_updated_time" in list(result.columns)
        
        assert "created_at_date" not in list(sales_order_df.columns)
        assert "created_at_time" in list(result.columns)

    def test_timestamp_split_into_date_and_time(self, purchase_order_df):
        result = convert_timestamp(purchase_order_df)

        date = datetime.date(2000, 1, 1)
        time = datetime.time(0, 0)

        for val in result["last_updated_date"]:
            assert type(val) == type(date)

        for val in result["last_updated_time"]:
            assert type(val) == type(time)

        for val in result["created_at_date"]:
            assert type(val) == type(date)

        for val in result["created_at_time"]:
            assert type(val) == type(time)

    def test_values_split_correctly(self, sales_order_df):
        result = convert_timestamp(sales_order_df)

        date = datetime.date(2000, 1, 1)
        time = datetime.time(0, 0)

        for val in result["last_updated_date"]:
            assert val == date

        for val in result["last_updated_time"]:
            assert val == time

        for val in result["created_at_date"]:
            assert val == date

        for val in result["created_at_time"]:
            assert val == time

    def test_error_raised_if_df_does_not_have_needed_columns(self, currency_df):
        currency_df = currency_df.drop(["last_updated", "created_at"],
                         axis=1,
                         errors="ignore",)

        with pytest.raises(Exception) as exc_info:
            convert_timestamp(currency_df)

        assert str(exc_info.value) == "Datetime conversion error: df doesnt have last_updated/created_at"

class TestAddIndex:
    def test_returns_df(self, currency_df):
        result = add_index(currency_df, "payment_record_id")

        assert type(result) == type(currency_df)

    def test_new_column_added(self, currency_df):
        result = add_index(currency_df, "payment_record_id")

        assert len(list(result.columns)) == len(list(currency_df.columns)) + 1
    
    def test_new_column_called_index_name(self, currency_df):
        result = add_index(currency_df, "payment_record_id")

        assert "payment_record_id" in list(result.columns)

    def test_index_is_unique_and_incremented(self, currency_df):
        result = add_index(currency_df, "payment_record_id")

        num_rows = len(currency_df)

        for i in range(num_rows):
            assert result["payment_record_id"].loc[result.index[i]] == i

        assert list(result["payment_record_id"]) == list(set(result["payment_record_id"]))

     