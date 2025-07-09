import pytest
import pandas as pd
from copy import deepcopy


from src.transform_lambda_pkg.transform_lambda.merge_tables import (
    create_merged_datastructure,
    create_non_merged_datastructure,
    merge_tables
)

from src.transform_lambda_pkg.transform_lambda.transform_data import star_schema_ref, transform_table_names


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
            department_name_list[2]: f"legal_address_id {i}",
        }

        data_rows_to_add_df = pd.DataFrame(data, index=[i])
        department_df = pd.concat(
            [department_df, data_rows_to_add_df], ignore_index=True
        )
    return department_df

@pytest.fixture
def renamed_address_df():
    department_name_list = ["legal_address_id", 
                            "counterparty_legal_address_line_1", 
                            "counterparty_legal_address_line_2", 
                            "counterparty_legal_district", 
                            "counterparty_legal_city", 
                            "counterparty_legal_postal_code", 
                            "counterparty_legal_country", 
                            "counterparty_legal_phone_number"] #dropped before, "created_at", "last_updated"]
    department_df = pd.DataFrame(columns=department_name_list)
    for i in range(10):
        data = {
            department_name_list[0]: i,
            department_name_list[1]: f"counterparty_legal_address_line_1 {i}",
            department_name_list[2]: f"counterparty_legal_address_line_2 {i}",
            department_name_list[3]: f"counterparty_legal_district {i}",
            department_name_list[4]: f"counterparty_legal_city {i}",
            department_name_list[5]: f"counterparty_legal_postal_code {i}",
            department_name_list[6]: f"counterparty_legal_country {i}",
            department_name_list[7]: f"counterparty_legal_phone_number {i}",
        }

        data_rows_to_add_df = pd.DataFrame(data, index=[i])
        department_df = pd.concat(
            [department_df, data_rows_to_add_df], ignore_index=True
        )
    return department_df

@pytest.fixture
def currency_df():
    department_name_list = ["department_id", "department_name", "location", "manager"]
    department_df = pd.DataFrame(columns=department_name_list)
    for i in range(10):
        data = {
            department_name_list[0]: i,
            department_name_list[1]: f"department_name_{i}",
            department_name_list[2]: f"location_{i}",
            department_name_list[3]: f"manager_{i}",
        }

        data_rows_to_add_df = pd.DataFrame(data, index=[i])
        department_df = pd.concat(
            [department_df, data_rows_to_add_df], ignore_index=True
        )
    return department_df


@pytest.fixture
def df_column_name():
    column_names = [
        "staff_id",
        "first_name",
        "last_name",
        "department_name",
        "location",
        "email_address",
    ]
    return column_names

@pytest.fixture
def tables_for_md(department_df, staff_df):
    return [{"address": department_df}, {"counterparty": staff_df}, 
            {"staff": staff_df}, {"department": department_df}]

@pytest.fixture
def tables_not_for_md(department_df, staff_df):
    return [{"not_address": department_df}, {"not_counterparty": staff_df}, 
            {"not_staff": staff_df}, {"not_department": department_df}]


class TestCreateMergedDatastructure:
    def test_returns_list_of_dicts(self, tables_for_md):
        result = create_merged_datastructure(tables_for_md, star_schema_ref)

        assert type(result) == list

        for table in result:
            assert type(table) == dict
            assert len(table) == 2

        assert len(result) == 2
    
    def test_values_are_type_list(self, tables_for_md):
        result = create_merged_datastructure(tables_for_md, star_schema_ref)

        for table in result:
            keys = table.keys()
            for key in keys:
                assert type(table[key]) == list

    def test_storing_correct_df_for_merge(self, tables_for_md, staff_df, department_df):
        result = create_merged_datastructure(tables_for_md, star_schema_ref)

        assert result[0]["dim_counterparty"][0].equals(department_df) 
        assert result[0]["dim_counterparty"][1].equals(staff_df)

        assert result[1]["dim_staff"][0].equals(staff_df) 
        assert result[1]["dim_staff"][1].equals(department_df)    

    def test_storing_column_list_correctly(self, tables_for_md):
        columns_dim_counterparty = star_schema_ref["dim_counterparty"]
        columns_dim_staff = star_schema_ref["dim_staff"]

        result = create_merged_datastructure(tables_for_md, star_schema_ref)

        table_dim_counterparty = result[0]
        table_dim_staff = result[1]

        assert table_dim_counterparty["col_list"] == columns_dim_counterparty
        assert table_dim_staff["col_list"] == columns_dim_staff

    def test_star_schema_not_mutated(self, tables_for_md):
        star_schema_ref_copy = deepcopy(star_schema_ref)

        create_merged_datastructure(tables_for_md, star_schema_ref)

        assert star_schema_ref == star_schema_ref_copy

    def test_dataframes_not_mutated(self, staff_df, department_df):
        table = [{"address": department_df}, {"counterparty": staff_df}, 
            {"staff": staff_df}, {"department": department_df}]
        
        staff_df_copy = staff_df.copy()
        department_df_copy = department_df.copy()

        create_merged_datastructure(table, star_schema_ref)

        assert staff_df.equals(staff_df_copy)
        assert department_df.equals(department_df_copy)
        
    def test_tables_not_for_merge_not_added_to_datastructure(self, staff_df, department_df):
        no_counterparty_dfs = [{"not_address": department_df}, {"not_counterparty": staff_df}, 
            {"staff": staff_df}, {"department": department_df}]
        
        no_staff_dfs = [{"address": department_df}, {"counterparty": staff_df}, 
            {"not_staff": staff_df}, {"not_department": department_df}]
        
        one_counterparty_df = [{"address": department_df}, {"not_counterparty": staff_df}, 
            {"staff": staff_df}, {"department": department_df}]
        
        one_staff_df = [{"address": department_df}, {"counterparty": staff_df}, 
            {"not_staff": staff_df}, {"department": department_df}]
        
        three_counterparty_df = [{"address": department_df}, {"counterparty": staff_df}, {"counterparty": staff_df},
            {"staff": staff_df}, {"department": department_df}]
        
        three_staff_df = [{"address": department_df}, {"counterparty": staff_df}, 
            {"staff": staff_df}, {"department": department_df}, {"staff": staff_df}]

        with pytest.raises(Exception) as exc_info:
            create_merged_datastructure(no_counterparty_dfs, star_schema_ref)
            
        assert str(exc_info.value) == "dim_counterparty dfs not correctly added for merge. df count: 0"

        with pytest.raises(Exception) as exc_info:
            create_merged_datastructure(no_staff_dfs, star_schema_ref)
            
        assert str(exc_info.value) == "dim_staff dfs not correctly added for merge. df count: 0"

        with pytest.raises(Exception) as exc_info:
            create_merged_datastructure(one_counterparty_df, star_schema_ref)
            
        assert str(exc_info.value) == "dim_counterparty dfs not correctly added for merge. df count: 1"

        with pytest.raises(Exception) as exc_info:
            create_merged_datastructure(one_staff_df, star_schema_ref)
            
        assert str(exc_info.value) == "dim_staff dfs not correctly added for merge. df count: 1"

        with pytest.raises(Exception) as exc_info:
            create_merged_datastructure(three_counterparty_df, star_schema_ref)
            
        assert str(exc_info.value) == "dim_counterparty dfs not correctly added for merge. df count: 3"

        with pytest.raises(Exception) as exc_info:
            create_merged_datastructure(three_staff_df, star_schema_ref)
            
        assert str(exc_info.value) == "dim_staff dfs not correctly added for merge. df count: 3"

class TestCreateNonMergedDatastructure:
    def test_returns_list_of_dicts(self, tables_for_md):
        tables_to_add = ["address", "staff"]
        result = create_non_merged_datastructure(tables_for_md, tables_to_add)

        assert type(result) == list

        for table in result:
            assert type(table) == dict
    
    def test_adds_required_tables_to_datastructure(self, tables_for_md):
        tables_to_add = ["address", "staff"]

        result = create_non_merged_datastructure(tables_for_md, tables_to_add)

        result_tables = []

        for table in result:
            result_tables.append(list(table.keys())[0])
        
        for table_name in tables_to_add:
            assert table_name in result_tables

        for table in result_tables:
            assert table in tables_to_add

        assert len(tables_to_add) == len(result_tables) 
    
    def test_adds_required_column_list(self, tables_for_md):
        tables_to_add = ["address", "staff"]

        result = create_non_merged_datastructure(tables_for_md, tables_to_add)

        address_columns1 = star_schema_ref["dim_location"]
        staff_columns1 = star_schema_ref["dim_staff"]

        address_columns2 = star_schema_ref[transform_table_names["address"]]
        staff_columns2 = star_schema_ref[transform_table_names["staff"]]

        result_address_columns = result[0]["col_list"]
        result_staff_columns = result[1]["col_list"]

        assert result_address_columns == address_columns1
        assert result_address_columns == address_columns2

        assert result_staff_columns == staff_columns1
        assert result_staff_columns == staff_columns2

    def test_stores_correct_dataframes(self, tables_for_md, staff_df, department_df):
        tables_to_add = ["address", "staff"]

        result = create_non_merged_datastructure(tables_for_md, tables_to_add)

        department_df.equals(result[0]["address"])
        staff_df.equals(result[1]["staff"])

    def test_unneeded_tables_not_added_to_datastructure(self, tables_for_md):

        tables_to_add = ["address", "staff"]
        
        result = create_non_merged_datastructure(tables_for_md, tables_to_add)
        
        result_tables = []
        for item in result:
            result_tables.append(list(item.keys())[0])
           
        original_tables = [] 
        for item in tables_for_md:
            original_tables.append(list(item.keys())[0])
        
        for item in tables_to_add:
            assert item in result_tables and item in original_tables
        
        assert len(result_tables) <= len(original_tables)
        
        
    def test_star_schema_not_mutated(self, tables_for_md):
        tables_to_add = ["address", "staff"]
        star_schema_ref_copy = deepcopy(star_schema_ref)

        create_non_merged_datastructure(tables_for_md, tables_to_add)

        assert star_schema_ref == star_schema_ref_copy

    def test_dataframes_not_mutated(self, staff_df, department_df):
        
        tables_to_add = ["address", "staff"]
        table = [{"address": department_df}, {"counterparty": staff_df}, 
            {"staff": staff_df}, {"department": department_df}]
        
        staff_df_copy = staff_df.copy()
        department_df_copy = department_df.copy()

        create_non_merged_datastructure(table, tables_to_add)

        assert staff_df.equals(staff_df_copy)
        assert department_df.equals(department_df_copy)
      
    def test_raises_error_when_empty_data_passed_in(self):
        tables_to_add = ["address", "staff"]
        table = []
        
        with pytest.raises(Exception) as exc_info:
            create_non_merged_datastructure(table, tables_to_add)
            
        assert str(exc_info.value) == "Tables should not be empty"
            
    def test_raises_error_when_tables_not_correct_type(self):
        tables_to_add = ["address", "staff"]
        table = ["test", 1,2,3]
        
        with pytest.raises(Exception) as exc_info:
            create_non_merged_datastructure(table, tables_to_add)
            
        assert str(exc_info.value) == "Tables should a list of dictionaries"
        
    def test_raises_error_when_tables_not_correct_type(self):
        tables_to_add = ["address", "staff"]
        table = {}
        
        with pytest.raises(Exception) as exc_info:
            create_non_merged_datastructure(table, tables_to_add)
            
        assert str(exc_info.value) == "Tables should be type list"
                                                        
    def test_raises_error_when_table_names_is_empty(self):
        tables_to_add = []
        table = [{"address": department_df}, {"counterparty": staff_df}, 
            {"staff": staff_df}, {"department": department_df}]
        
        with pytest.raises(Exception) as exc_info:
            create_non_merged_datastructure(table, tables_to_add)
            
        assert str(exc_info.value) == "Table names should not be empty"
        
    def test_raises_error_when_table_names_is_empty(self):
        tables_to_add = [2,3,1,2]
        table = [{"address": department_df}, {"counterparty": staff_df}, 
            {"staff": staff_df}, {"department": department_df}]
        
        with pytest.raises(Exception) as exc_info:
            create_non_merged_datastructure(table, tables_to_add)
            
        assert str(exc_info.value) == "Table names should a list of strings"

@pytest.fixture
def mock_merge_ds(department_df, staff_df, renamed_address_df, counterparty_df):
    return_datastructure = [{"dim_counterparty": [renamed_address_df, counterparty_df], "col_list": star_schema_ref["dim_counterparty"]},
                            {"dim_staff": [department_df, staff_df], "col_list": star_schema_ref["dim_staff"]}]
    
    return return_datastructure
    

# [{"table1_name": df1, "col_list":[col1, col2]}, {"table2_name": df2, "col_list":[col1, col2]}]

class TestMergeTables:
    
    #"dim_staff": mock_df (with merge col - department_id)
    #"dim_counterparty": mock_df (with merge col - legal_address)
    
    def test_returns_list_of_dicts(self, mock_merge_ds):
        result = merge_tables(mock_merge_ds)

        assert type(result) == list

        for table in result:
            assert type(table) == dict