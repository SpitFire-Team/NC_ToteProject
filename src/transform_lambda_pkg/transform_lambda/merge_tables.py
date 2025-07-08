import pandas as pd
from copy import deepcopy


def transform_staff_and_department_tables(staff_dataframe, department_dataframe):
    staff_df_copy = deepcopy(staff_dataframe)
    department_df_copy = deepcopy(department_dataframe)
    dim_staff_col_name_list = [
        "staff_id",
        "first_name",
        "last_name",
        "department_name",
        "location",
        "email_address",
    ]
    merge_df = pd.merge(staff_df_copy, department_df_copy, on="department_id")
    dim_staff_df = merge_df.drop(columns=["department_id", "manager"])
    dim_staff_df_reordered = dim_staff_df[dim_staff_col_name_list]
    return dim_staff_df_reordered


def create_merged_datastructure(tables, star_schema_ref):
    """
    Creates a datastructure for tables that need to be merged to match star schema.

    inputs: list of dictionaries of tables names and dataframes, star schema reference datastructure

    Returns: list of dictionaries of star schema table names with dataframes to merge, column names matching star schema
    """

    return_data_structure = [
        {"dim_counterparty": [], "col_list": star_schema_ref["dim_counterparty"]},
        {"dim_staff": [], "col_list": star_schema_ref["dim_staff"]},
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
