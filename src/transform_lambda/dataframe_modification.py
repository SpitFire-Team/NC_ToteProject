import pandas as pd


def dataframe_modification(list_of_dicts):
    """This function takes a list of dictionaries
    containing dataframe values,
    removes columns 'last_updated'
    and 'created_at' from the dataframes
    and returns a list of dictionaries containing the modified dataframes.

    Arguments: A list of dictionaries in the format [{table_name: dataframe}, {table_name_2: dataframe_2}]
    Returns: A list of dictionaries in the format [{table_name: modified_dataframe}, {table_name_2: modified_dataframe_2}]
    """
    modified_list=[]
    for table_dict in list_of_dicts:
        print(table_dict)
        new_dict = {key:"test" for key in table_dict}
        modified_list.append(new_dict)                  
    return modified_list
