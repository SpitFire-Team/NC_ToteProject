import pandas as pd

# data = {'col_1': [3, 2, 1, 0], 'col_2': ['a', 'b', 'c', 'd']}

# test_df = pd.DataFrame.from_dict(data)

# column_list = ["col_1", "columns", "legal_address_id"]


def remove_dataframe_columns(df, column_list):
    """
    removes specified columns from df

    inputs: list of column names, pandas dataframe

    Returns: pandas dataframe with specified columns removed
    """

    modified_df = df.drop(
        column_list,
        axis=1,
        errors="ignore",
    )

    if len(modified_df.columns) == 0:
        raise Exception("all columns removed from dataframe")

    return modified_df


def add_prefix_to_table_name(table_dict, prefix):
    """
    Adds prefix to table name

    Inputs: dictionary  {table name (string): pandas dataframe}

    Returns: dictionary  {prefix + table name (string): pandas dataframe}
    """

    keys = list(table_dict.keys())
    return {prefix + keys[0]: table_dict[keys[0]].copy()}

def merge_dataframes(df1, df2, merge_column, column_names):
    if merge_column not in df1.columns or merge_column not in df2.columns:
        raise Exception("Merge column not in both dataframes")
    
    if df1.shape[0] != df2.shape[0]:
        raise Exception("Dataframes don't have the same number of values")
        
    for col in df1.columns:
        if col in df2.columns and col != merge_column:
            raise Exception("Shared column")
        
    try:    
        merge_df = pd.merge(df1, df2, on=merge_column)
    
    except:
        raise Exception("Merge failed")

    delete_columns = []

    for col in merge_df.columns:
        if col not in column_names:
            delete_columns.append(col)
        
    merge_df.drop(columns=delete_columns)

    for col in column_names:
        if col not in merge_df.columns:
            raise Exception("Necessary column not in dataframe")

    return merge_df


'''def transform_staff_and_department_tables(staff_dataframe, department_dataframe):
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
    return dim_staff_df_reordered'''