

from src.transform_lambda_pkg.transform_lambda.transform_data import star_schema_ref, transform_table_names 
from src.utils.df_utils import remove_dataframe_columns, convert_timestamp, currency_code_to_currency_name

from copy import deepcopy

def dataframe_modification(list_of_dicts):
    """This function takes a list of dictionaries
    containing table name keys and dataframe values,
    removes columns 'last_updated'
    and 'created_at' from the dataframes,
    it then returns a list of dictionaries containing the modified dataframes.

    Arguments: A list of dictionaries in the format [{table_name: dataframe}, {table_name_2: dataframe_2}]
    Returns: A list of dictionaries in the format [{table_name: modified_dataframe}, {table_name_2: modified_dataframe_2}]
    """
    modified_list = []
    for item in list_of_dicts:
        table_name = list(item.keys())[0]
        target_dataframe = item[table_name]
        modified_dataframe = target_dataframe.drop(
            ["created_at", "last_updated"], axis=1, errors="ignore"
        )
        new_dict = {table_name: modified_dataframe}
        modified_list.append(new_dict)
    return modified_list



def create_modify_tables_datastructure(tables, table_names):

    if type(tables) != list:
        raise Exception("Tables should be type list")
    elif tables: 
        for item in tables:
            if type(item) != dict:
                raise Exception("Tables should a list of dictionaries")
    else: 
        raise Exception("Tables should not be empty")
    
    
    if type(table_names) != list:
        raise Exception("Table names should be type list")
    elif table_names: 
        for item in table_names:
            if type(item) != str:
                raise Exception("Table names should a list of strings")
    else: 
        raise Exception("Table names should not be empty")
    
    return_data_structure = []
    for table in tables:
        table_dict = {}

        table_name = list(table.keys())[0]
        if table_name in table_names:
            table_dict[table_name] = table[table_name]
            table_dict["col_list"] = star_schema_ref[transform_table_names[table_name]]

        if table_dict:
            return_data_structure.append(table_dict)

    return return_data_structure



def rename_table_and_remove_uneeded_df_columns(tables):
    star_schema_ref_copy = deepcopy(star_schema_ref)
    return_tables = []
    
    for table in tables:
        table_name = list(table.keys())[0]
        new_table_name = transform_table_names[table_name]
        df = table[table_name]
        remove_cols = [column for column in df.columns if column not in star_schema_ref_copy[new_table_name]]
        new_df = remove_dataframe_columns(df, remove_cols)
        
        return_tables.append({new_table_name: new_df})
        
    return return_tables



# think about columns that need to be added i.e currency name created from currency code - see transform data
#perhaps a function to add new columns as this happen with created_date and created_time from created_at time_stamp


# create extra columns - modify_tables - utils required: currency_code -> currency_name, one util(created_at -> created_date, 
# created_time, lasted_updated -> last_updated_date, last_updated_time)


def create_extra_columns(tables):
    return_tables = []
    for table in tables:
        table_name = list(table.keys())[0]
        df = list(table.values())[0]
        if table_name == "currency":
            updated_currency_table = {table_name: currency_code_to_currency_name(df)}
            return_tables.append(updated_currency_table)
        elif table_name == "payment" or table_name == "purchase_order": 
            updated_timestamp_table = {table_name: convert_timestamp(df)}
            return_tables.append(updated_timestamp_table)
        else:
            return_tables.append(table)
    
    return return_tables
            
            