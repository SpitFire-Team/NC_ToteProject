from src.transform_lambda_pkg.transform_lambda.modify_tables import (    dataframe_modification, create_modify_tables_datastructure, rename_table_and_remove_uneeded_df_columns, create_extra_columns)

from src.transform_lambda_pkg.transform_lambda.transform_data import star_schema_ref, db_ref, transform_table_names
from copy import deepcopy
import pandas as pd
import pytest
from pprint import pprint 
from datetime import datetime, timezone
import datetime


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
def tables_for_modify(currency_df, design_df, payment_type_df, transaction_df):
    return [{"currency": currency_df}, {"design": design_df}, 
            {"payment_type": payment_type_df}, {"transaction": transaction_df}]
   
@pytest.fixture 
def tables_for_extra_columns(currency_df, design_df, payment_type_df, transaction_df, payment_df, purchase_order_df):
    return [{"currency": currency_df}, 
            {"design": design_df}, 
            {"payment_type": payment_type_df}, 
            {"transaction": transaction_df},
            {"payment": payment_df}, 
            {"purchase_order": purchase_order_df}]

class TestDataframeModification:

    def test_dataframe_modification_returns_list_of_dicts(self, tables_for_modify):
        """
        This tests that the function returns a list of dictionaries as the return value.

        """
        assert type(dataframe_modification(tables_for_modify)) == list
        assert type(dataframe_modification(tables_for_modify)[0]) == dict


    def test_datafram_modification_has_correct_table_keys(self, currency_df, design_df):
        """
        This tests that the returned dictionaries have the correct table names as a key.
        """
        test_currency_list = [{"currency": currency_df}]
        test_design_list = [{"design": design_df}]
        assert "currency" in dataframe_modification(test_currency_list)[0].keys()
        assert "design" in dataframe_modification(test_design_list)[0].keys()



    def test_datafram_modification_has_correct_table_keys_for_multiple_items(self, tables_for_modify):
        """
        This tests that the returned dictionaries have the correct table names as a
        key when there are multiple dictionaries in the input list.
        """
        table_names = []
        for item in dataframe_modification(tables_for_modify):
            for key in item.keys():
                table_names.append(key)
        assert "currency" in table_names
        assert "design" in table_names


    def test_dataframe_moficiation_removes_create_at_column(self, design_df, tables_for_modify):
        """
        This tests that the returned dataframes have the "created_at" column removed.
        """
        
        assert "created_at" in list(design_df.columns.values)
        result = dataframe_modification(tables_for_modify)
        assert "created_at" not in list(result[1]["design"].columns.values)


    def test_dataframe_modification_removes_created_at_column_from_all_items_in_multi_item_list(self, tables_for_modify):
        """
        This tests that the returned dataframes have the "created_at" column removed
        when there are multiple dictionaries in the input list.
        """
        for item in tables_for_modify:
            target_dataframe = list(item.values())[0]
            assert "created_at" in list(target_dataframe.columns.values)
        result = dataframe_modification(tables_for_modify)
        for item in result:
            target_dataframe = list(item.values())[0]
            assert "created_at" not in list(target_dataframe.columns.values)


    def test_dataframe_moficiation_removes_last_updated_column(self, design_df, tables_for_modify):
        """
        This tests that the returned dataframes have the "last_updated" column removed.
        """
        assert "last_updated" in list(design_df.columns.values)
        result = dataframe_modification(tables_for_modify)
        assert "last_updated" not in list(result[1]["design"].columns.values)


    def test_dataframe_modification_removes_last_updated_column_from_all_items_in_multi_item_list(self, currency_df, design_df):
        """
        This tests that the returned dataframes have the "last_updated" column removed
        when there are multiple dictionaries in the input list.
        """
        test_dict_list = [{"design": design_df}, {"currency": currency_df}]
        for item in test_dict_list:
            target_dataframe = list(item.values())[0]
            assert "last_updated" in list(target_dataframe.columns.values)
        result = dataframe_modification(test_dict_list)
        for item in result:
            target_dataframe = list(item.values())[0]
            assert "last_updated" not in list(target_dataframe.columns.values)


    @pytest.mark.skip("Skipped to allow dummy data to be tested")
    def test_dataframe_modification_raises_error_if_missing_columns(self):
        """
        This tests that an appropriate 'KeyError' is raised if the function is passed a dataframe which
        does not contain the target columns "created_at" or "last_updated".
        """
        missing_string_1 = {
            "sales_order_id": [2, 3],
            "design_id": [3, 4],
            "staff_id": [19, 10],
            "counterparty_id": [8, 4],
            "units_sold": [42972, 65839],
            "unit_price": ["3.94", "2.91"],
            "currency_id": [2, 3],
            "agreed_delivery_date": ["2022-11-07", "2022-11-06"],
            "agreed_payment_date": ["2022-11-08", "2022-11-07"],
            "agreed_delivery_location_id": [8, 19],
        }
        missing_dataframe_1 = pd.DataFrame(missing_string_1)
        missing_values_table = [{"missing_values": missing_dataframe_1}]
        with pytest.raises(KeyError):
            dataframe_modification(missing_values_table)



class TestCreateModifyTablesDatastructure:
    def test_returns_list_of_dicts(self, tables_for_modify):
        table_names = ["currency", "design"]
        result = create_modify_tables_datastructure(tables_for_modify, table_names)

        assert type(result) == list

        for table in result:
            assert type(table) == dict
    
    def test_adds_required_tables_to_datastructure(self, tables_for_modify):
        table_names = ["currency", "design"]
        result = create_modify_tables_datastructure(tables_for_modify, table_names)
        result_tables = []

        for table in result:
            result_tables.append(list(table.keys())[0])
        
        for table_name in table_names:
            assert table_name in result_tables

        for table in result_tables:
            assert table in table_names

        assert len(table_names) == len(result_tables) 
    
    def test_adds_required_column_list(self, tables_for_modify):
        table_names = ["currency", "design"]

        result = create_modify_tables_datastructure(tables_for_modify, table_names)

        currency_columns1 = star_schema_ref["dim_currency"]
        design_columns1 = star_schema_ref["dim_design"]

        currency_columns2 = star_schema_ref[transform_table_names["currency"]]
        design_columns2 = star_schema_ref[transform_table_names["design"]]

        result_currency_columns = result[0]["col_list"]
        result_design_columns = result[1]["col_list"]

        assert result_currency_columns == currency_columns1
        assert result_currency_columns == currency_columns2

        assert result_design_columns == design_columns1
        assert result_design_columns == design_columns2

    def test_stores_correct_dataframes(self, tables_for_modify, currency_df, design_df):
        table_names = ["currency", "design"]

        result = create_modify_tables_datastructure(tables_for_modify, table_names)

        currency_df.equals(result[0]["currency"])
        design_df.equals(result[1]["design"])

    def test_unneeded_tables_not_added_to_datastructure(self, tables_for_modify):

        table_names = ["currency", "design"]
        result = create_modify_tables_datastructure(tables_for_modify, table_names)
        
        result_tables = []
        for item in result:
            result_tables.append(list(item.keys())[0])
           
        original_tables = [] 
        for item in tables_for_modify:
            original_tables.append(list(item.keys())[0])
        
        for item in result_tables:
            assert item in table_names and item in original_tables
        
       
        assert len(result_tables) <= len(original_tables)
        assert len(result_tables) <= len(table_names)
        
        
    def test_star_schema_not_mutated(self, tables_for_modify):
        table_names = ["currency", "design"]
        star_schema_ref_copy = deepcopy(star_schema_ref)

        create_modify_tables_datastructure(tables_for_modify, table_names)

        assert star_schema_ref == star_schema_ref_copy

    def test_dataframes_not_mutated(self, currency_df, design_df, payment_type_df, transaction_df):
        
        table_names = ["currency", "design"]
        tables = [{"currency": currency_df}, {"design": design_df}, 
            {"payment_type": payment_type_df}, {"transaction": transaction_df}]
        
        currency_df_copy = currency_df.copy()
        design_df_copy = design_df.copy()

        create_modify_tables_datastructure(tables, table_names)

        assert currency_df.equals(currency_df_copy)
        assert design_df.equals(design_df_copy)
      
    def test_raises_error_when_empty_data_passed_in(self):
        table_names = ["currency", "design"]
        tables = []
        
        with pytest.raises(Exception) as exc_info:
            create_modify_tables_datastructure(tables, table_names)
            
        assert str(exc_info.value) == "Tables should not be empty"
            
    def test_raises_error_when_tables_not_correct_type(self):
        table_names = ["currency", "design"]
        table = ["test", 1,2,3]
        
        with pytest.raises(Exception) as exc_info:
            create_modify_tables_datastructure(table, table_names)
            
        assert str(exc_info.value) == "Tables should a list of dictionaries"
        
        table = {}
        
        with pytest.raises(Exception) as exc_info:
            create_modify_tables_datastructure(table, table_names)
            
        assert str(exc_info.value) == "Tables should be type list"
                                                        
    def test_raises_error_when_table_names_is_empty(self, currency_df, design_df, payment_type_df, transaction_df):
        table_names = []
        tables = [{"currency": currency_df}, {"design": design_df}, 
            {"payment_type": payment_type_df}, {"transaction": transaction_df}]
        
        with pytest.raises(Exception) as exc_info:
            create_modify_tables_datastructure(tables, table_names)
            
        assert str(exc_info.value) == "Table names should not be empty"
        
    def test_raises_error_when_table_names_are_not_strings(self, currency_df, design_df, payment_type_df,transaction_df):
        table_names = [2,3,1,2]
        tables = [{"currency": currency_df}, {"design": design_df}, 
            {"payment_type": payment_type_df}, {"transaction": transaction_df}]
        
        with pytest.raises(Exception) as exc_info:
            create_modify_tables_datastructure(tables, table_names)
            
        assert str(exc_info.value) == "Table names should a list of strings"
        
        
class TestRenameTableAndRemoveUneededDfColumns:
    
    def test_returns_list_of_dictionaries(self, tables_for_modify):
        results = rename_table_and_remove_uneeded_df_columns(tables_for_modify)

        assert type(results) == list

        for table in results:
            assert type(table) == dict
            
    def test_tables_are_renamed(self, tables_for_modify):
        results = rename_table_and_remove_uneeded_df_columns(tables_for_modify)

        expected_table_names = sorted(["dim_currency", "dim_design", "dim_payment_type", "dim_transaction"])
        
        result_table_names = []
        
        for table in results: 
            result_table_names.append(list(table.keys())[0])
            
        assert sorted(result_table_names) == expected_table_names    
        
    def test_columns_are_removed(self, tables_for_modify):
        results = rename_table_and_remove_uneeded_df_columns(tables_for_modify)
        star_schema_ref_copy = deepcopy(star_schema_ref)
        
        for table in results:
            table_name = list(table.keys())[0]
            if table_name != "dim_currency":
                df = table[table_name]
                df_columns = list(df.columns)
                expected_columns = sorted(star_schema_ref_copy[table_name])
                assert sorted(df_columns) == expected_columns
       

    def test_tables_is_not_mutated(self, tables_for_modify):
        
        tables_copy = deepcopy(tables_for_modify)
        rename_table_and_remove_uneeded_df_columns(tables_for_modify)
        
        for i in range(len(tables_copy)):
            table_name_before = list(tables_copy[i].keys())[0] 
            table_name_after = list(tables_for_modify[i].keys())[0] 
            
            df_before = list(tables_copy[i].values())[0] 
            df_after = list(tables_for_modify[i].values())[0] 
            
            assert table_name_before == table_name_after
            assert df_before.equals(df_after)
            
        assert len(tables_copy) == len(tables_for_modify)
     


class TestCreateExtraColumns:

    def test_dataframe_modification_returns_list_of_dicts(self, tables_for_extra_columns):
        results = create_extra_columns(tables_for_extra_columns)

        assert type(results) == list

        for table in results:
            assert type(table) == dict
            
    def test_same_table_name(self, tables_for_extra_columns):
        before_names = [table_name for table in tables_for_extra_columns for table_name in list(table.keys())]
        print(before_names)
        results = create_extra_columns(tables_for_extra_columns)
        after_names = [table_name for table in results for table_name in list(table.keys())]
        
        assert len(results) == len(tables_for_extra_columns)
        assert before_names == after_names
        
    def test_original_tables_not_mutatated(self, tables_for_extra_columns):
        tables_copy = deepcopy(tables_for_extra_columns)
        create_extra_columns(tables_for_extra_columns)

        for i in range(len(tables_for_extra_columns)):
            original_df = list(tables_for_extra_columns[i].values())[0]
            copy_df = list(tables_copy[i].values())[0]
            assert original_df.equals(copy_df)
            
    def test_expected_columns_modified(self, tables_for_extra_columns):
        results = create_extra_columns(tables_for_extra_columns)

        for i in range(len(tables_for_extra_columns)):
            table_name = list(tables_for_extra_columns[i].keys())[0]
            
            original_df = list(tables_for_extra_columns[i].values())[0]
            results_df = list(results[i].values())[0]
            
            if table_name == "currency" or table_name == "payment" or table_name == "purchase_order":
                assert not original_df.equals(results_df)
            
    def test_payment_has_new_columns(self, tables_for_extra_columns):
        results = create_extra_columns(tables_for_extra_columns)
        payment_col_names = [column for table in results for key, df in table.items() if key == "payment" for column in df.columns]
        
        assert 'last_updated_date' in payment_col_names
        assert 'last_updated_time' in payment_col_names
        assert 'created_at_date' in payment_col_names
        assert 'created_at_time' in payment_col_names

    def test_purchase_order_has_new_columns(self, tables_for_extra_columns):
        results = create_extra_columns(tables_for_extra_columns)
        purchase_order_col_names = [column for table in results for key, df in table.items() if key == "purchase_order" for column in df.columns]
        
        assert 'last_updated_date' in purchase_order_col_names
        assert 'last_updated_time' in purchase_order_col_names
        assert 'created_at_date' in purchase_order_col_names
        assert 'created_at_time' in purchase_order_col_names
        
    def test_currency_has_new_columns(self, tables_for_extra_columns):
        results = create_extra_columns(tables_for_extra_columns)
        currency_order_col_names = [column for table in results for key, df in table.items() if key == "currency" for column in df.columns]
        
        assert 'currency_name' in currency_order_col_names
