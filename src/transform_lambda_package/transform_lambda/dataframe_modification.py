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
            ["created_at", "last_updated", "legal_address_id"], 
            axis=1, 
            errors="ignore"
        )
           
        if table_name == "sales_order" or table_name == "payment":
            new_table_name = f"fact_{table_name}"
        elif table_name == "staff" or table_name == "department":
            new_table_name = table_name
        elif table_name == "address":
            new_table_name = "dim_location"
        else:
            new_table_name = f"dim_{table_name}"
            
        # Add placeholders only if this is the counterparty table
        if new_table_name == "dim_counterparty":
        # List of required columns with placeholder values # need to get these by joining with address table
            placeholders = {
                "counterparty_legal_address_line_1": "UNKNOWN",
                "counterparty_legal_address_line_2": "UNKNOWN",
                "counterparty_legal_district": "UNKNOWN",
                "counterparty_legal_city": "UNKNOWN",
                "counterparty_legal_postal_code": "UNKNOWN",
                "counterparty_legal_country": "UNKNOWN",
                "counterparty_legal_phone_number": "UNKNOWN"
            }
            for col, placeholder in placeholders.items():
                if col not in modified_dataframe.columns:
                    modified_dataframe[col] = placeholder
                else:
                    modified_dataframe[col] = modified_dataframe[col].fillna(placeholder)
                    
            modified_dataframe = modified_dataframe.drop(
                ["commercial_contact", "delivery_contact"], 
                axis=1, 
                errors="ignore"
            )
            
        if new_table_name == "dim_currency":
            placeholders = {
                "curreny_name": "UNKNOWN",
            }
            for col, placeholder in placeholders.items():
                if col not in modified_dataframe.columns:
                    modified_dataframe[col] = placeholder
                else:
                    modified_dataframe[col] = modified_dataframe[col].fillna(placeholder)

        new_dict = {new_table_name: modified_dataframe}
        modified_list.append(new_dict)
    return modified_list




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
        if table_name != "payment":
            modified_dataframe = target_dataframe.drop(
                ["created_at", "last_updated"], 
                axis=1, 
                errors="ignore"
            )
            
        if table_name == "currency":
            placeholders = {
                "currency_name": "UNKNOWN",
            }
            for col, placeholder in placeholders.items():
                if col not in modified_dataframe.columns:
                    modified_dataframe[col] = placeholder
                else:
                    modified_dataframe[col] = modified_dataframe[col].fillna(placeholder)

        if table_name == "sales_order":
            new_table_name = f"fact_{table_name}"
        elif table_name == "staff" or table_name == "department" or table_name == "address" or table_name == "counterparty" or table_name == "payment":
            new_table_name = table_name
        else:
            new_table_name = f"dim_{table_name}"
        
        new_dict = {new_table_name: modified_dataframe}
        modified_list.append(new_dict)
    return modified_list