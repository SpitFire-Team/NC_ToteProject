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
        if table_name == "sales_order" or table_name == "payment":
            new_table_name = f"fact_{table_name}"
        elif table_name == "staff" or table_name == "department": 
            new_table_name = table_name
        else: 
            new_table_name = f"dim_{table_name}"
            
        new_dict = {new_table_name: modified_dataframe}
        modified_list.append(new_dict)
    return modified_list
