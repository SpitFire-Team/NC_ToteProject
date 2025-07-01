
from src.utils.df_utils import remove_dataframe_columns, add_prefix_to_table_name
import pandas as pd
import pytest
import datetime
from copy import deepcopy

@pytest.fixture
def dummy_df():
    data = {'currency_id': [3, 2, 1, 0], 
            'currency_code': ['a', 'b', 'c', 'd'],
            'created_at': [datetime.datetime.now(), datetime.datetime.now(), datetime.datetime.now(), datetime.datetime.now()],
            'last_updated':[datetime.datetime.now(),datetime.datetime.now(),datetime.datetime.now(),datetime.datetime.now()]}
    test_df = pd.DataFrame.from_dict(data)
    return test_df

class TestRemoveDataFrameColumns:

    def test_empty_list_warns_user_and_returns_df_unchanged(self, dummy_df):
        columns_to_remove = []
        result = remove_dataframe_columns(dummy_df, columns_to_remove)
        
        assert result.equals(dummy_df)
    
    def test_nothing_removed_when_removing_column_that_does_exist(self, dummy_df):
        columns_to_remove = ["random"]
        result = remove_dataframe_columns(dummy_df, columns_to_remove)
        
        assert result.equals(dummy_df)
        
    def test_function_removes_specified_column(self, dummy_df):
        columns_to_remove = ["currency_id"]
        result = remove_dataframe_columns(dummy_df, columns_to_remove)
        
        assert not result.equals(dummy_df)
        assert columns_to_remove[0] not in result.columns
        
        
    def test_function_removes_specified_columns(self, dummy_df):
        columns_to_remove = ["currency_id","created_at"]
        result = remove_dataframe_columns(dummy_df, columns_to_remove)
        
        assert not result.equals(dummy_df)
        for col in columns_to_remove:
            assert col not in result.columns
            
            
    def test_function_throws_error_if_all_columns_removed(self, dummy_df):
        columns_to_remove = dummy_df.columns
        
        with pytest.raises(Exception):
            result = remove_dataframe_columns(dummy_df, columns_to_remove)


    def test_original_df_not_mutatated(self, dummy_df):
        columns_to_remove = ["currency_id","created_at"]
        result = remove_dataframe_columns(dummy_df, columns_to_remove)
        assert not result is dummy_df
        

class TestAddPrefixToTableName:
    
    def test_prefix_is_added_to_table_name(self, dummy_df):
        data = {"test":dummy_df}
        prefix = "dim_"
        result = add_prefix_to_table_name(data, prefix)
        
        assert list(result.keys())[0] == "dim_test"

    def test_dataframe_does_not_change(self, dummy_df):
        data = {"test":dummy_df}
        prefix = "dim_"
        result = add_prefix_to_table_name(data, prefix)
        
        assert result["dim_test"].equals(data["test"])
        
    def test_dataframe_not_mutated(self, dummy_df):
        data = {"test":dummy_df}
        prefix = "dim_"
        result = add_prefix_to_table_name(data, prefix)
        
        assert result["dim_test"] is not data["test"]
        
            
    def test_table_dict_not_mutated(self, dummy_df):
        data = {"test":dummy_df}
        data_copy = deepcopy(data)
        prefix = "dim_"
        add_prefix_to_table_name(data, prefix)
    
        assert data.keys() == data_copy.keys()
        assert data["test"].equals(data_copy["test"])