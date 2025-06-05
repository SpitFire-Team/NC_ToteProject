from datetime import datetime


def convert_to_dict(query_results, col_names: tuple):
    """Converts SQL queries in list-of-lists form to a list of dictionaries (JSON)"""
    value_dicts = []
    for value in query_results:
        value_dict = {}
        for i, col_name in enumerate(col_names):
            value_dict[col_name] = value[i]
        value_dicts.append(value_dict)
    return value_dicts


def get_path_date_time_string():
    """returns current data_time as a string without / to use in file paths"""
    date_time = datetime.now()
    date_time_str = date_time.strftime("%d/%m/%Y_%H:%M")
    date_time_str = date_time_str.replace("/", "-")  # remove /, used for file path
    return date_time_str
