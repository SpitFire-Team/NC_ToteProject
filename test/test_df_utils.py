from src.utils.df_utils import (
    remove_dataframe_columns,
    add_prefix_to_table_name,
    merge_dataframes,
    reorder_dataframe,
)
import pandas as pd
import pytest
import datetime
from copy import deepcopy


@pytest.fixture
def dummy_df():
    data = {
        "currency_id": [3, 2, 1, 0],
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


# def dummy_df2():
#     data = {
#         "currency_id": [4, 5, 6, 7],
#         "test_col1": [1, 2, 3, 4],
#         "test_col2": [8, 9, 10, 11],
#         "last_updated": [12, 13, 14, 15],
#     }
#     test_df = pd.DataFrame.from_dict(data)
