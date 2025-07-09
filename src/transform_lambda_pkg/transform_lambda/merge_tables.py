import pandas as pd
from src.utils.df_utils import merge_dataframes



def create_merged_datastructure(tables, star_schema_ref):
    """
    Creates a datastructure for tables that need to be merged to match star schema.

    inputs: list of dictionaries of tables names and dataframes, star schema reference datastructure

    Returns: list of dictionaries of star schema table names with dataframes to merge, column names matching star schema
    """

    return_data_structure = [
        {"dim_counterparty": [], "col_list": star_schema_ref["dim_counterparty"]},
        {"dim_staff": [], "col_list": star_schema_ref["dim_staff"]}
    ]

    dim_counterparty_dfs = []
    dim_staff_dfs = []
    
    for table in tables:
        table_name = list(table.keys())[0]
        if table_name == "address" or table_name == "counterparty":
            dim_counterparty_dfs.append(
                list(table.values())[0]
            )
        elif table_name == "staff" or table_name == "department":
            dim_staff_dfs.append(list(table.values())[0])

    return_data_structure[0]["dim_counterparty"] = dim_counterparty_dfs
    return_data_structure[1]["dim_staff"] = dim_staff_dfs

    if len(dim_counterparty_dfs) != 2:
        raise Exception(f"dim_counterparty dfs not correctly added for merge. df count: {len(dim_counterparty_dfs)}")
    
    elif len(dim_staff_dfs) != 2:
        raise Exception(f"dim_staff dfs not correctly added for merge. df count: {len(dim_staff_dfs)}")

    return return_data_structure

#Output: [{"table1_name": df1, "col_list":[col1, col2]}, {"table2_name": df2, "col_list":[col1, col2]}]
#using ds_merge_data - [{"table1_name": df1}, {"table2_name": df2}]




#inputs: [{"table1_name": df1, "col_list":[col1, col2]}, {"table2_name": df2, "col_list":[col1, col2]}]

    # Merge  - using ds_merge_data - outputs [{"dim/fact_table1_name": df1}, {"dim/fact_table2_name": df2}]
    
    # def merge_dataframes(df1, df2, merge_column, column_names):

def merge_tables(merge_datastructure):
    # loop through each dictionary in list
        # grab list of dfs to merge (value of dictorary [table name])
        # merge dataframes together - merged_df - merge_dataframes(df1, df2, merge_column, column_names)
        # create new dictionary with key: table name as in merge_datastructure, value: merged_df
        # save dictionary to list of merged tables
        # return new list of merged tables
        
    return_list = []
        
    for item in merge_datastructure:
        name = list(item.keys())[0]
        df1 = item[name][0]
        df2 = item[name][1]
        col_names = item["col_list"]
        
        if name == "dim_counterparty":
            merge_col = "legal_address_id" # rename address_df - address_id 
        elif name == "dim_staff":
            merge_col = "department_id"
        else:
            continue
        
        merge_df = merge_dataframes(df1,df2, merge_col, col_names)
        merge_table = {name: merge_df}
        
        return_list.append(merge_table)
        
    return return_list
        
    
        