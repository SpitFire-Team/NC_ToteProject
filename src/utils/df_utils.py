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