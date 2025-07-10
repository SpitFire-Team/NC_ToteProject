import pandas as pd
# from src.transform_lambda_pkg.transform_lambda.transform_data import rename_col_names_ref



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
    """
    Merges two dataframes and reorders based on list of column names

    Inputs: Pandas df 1 and 2
            merge_column (string)
            column_names (list of strings)

    Returns: merged dataframe
    """

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

    ## put in own util
    for col in merge_df.columns:
        if col not in column_names:
            delete_columns.append(col)

    merge_df = remove_dataframe_columns(merge_df, delete_columns)

    for col in column_names:
        if col not in merge_df.columns:
            raise Exception("Necessary column not in dataframe")
        
    ## put in own util - see ticket
        
    reordered_df = reorder_dataframe(merge_df, column_names)

    return reordered_df


def reorder_dataframe(df, list_column_names):
    """
    Reroders dataframe based on list of column names

    Inputs: Pandas dataframe
            column_names (list of strings)

    Returns: reordered dataframe
    """

    for col in list_column_names:
        if col not in df.columns:
            raise Exception("Can't reorder df, column not in dataframe")

    for col in df.columns:
        if col not in list_column_names:
            raise Exception("Can't reorder df, column in dataframe but not in list")


    return df[list_column_names]


def rename_dataframe_columns(df, cols_to_rename):
    """
    inputs: df to rename 
            cols_to_rename: dictionary {old_col_name (string): new_col_name (string)}  
    outputs: df with columns renamed
    
    """
    if type(cols_to_rename) is not dict:
        raise Exception("Column rename failed: cols_to_rename must be a dictionary")
    
    if df is None:
        raise Exception("Column rename failed: dataframe is None")
    
    if len(list(df.columns)) == 0:
        raise Exception("Column rename failed: has no columns")
    
    old_col_list = list(df.columns)
    
    for key, value in cols_to_rename.items():
        if type(key) is not str or type(value) is not str:
            raise Exception("Column rename failed: column name and new name must be strings")
        elif key not in old_col_list:
            raise Exception("Column rename failed: column name not in df")
        
    
    renamed_cols_df = df.rename(columns=cols_to_rename)
    
    new_col_list = list(renamed_cols_df.columns)
    
    if old_col_list == new_col_list: 
        raise Exception("Column rename failed: new names cannot be the same as old names")
        
    return renamed_cols_df






# function to split time stames into date and time - see_fact sales order


# function to rename columns
# def rename_columns_df(df, )

# function to rename df
