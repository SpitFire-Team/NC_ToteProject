from pprint import pprint
from datetime import datetime, timezone
from copy import deepcopy

def convert_to_dict(query_results, col_names : tuple):
    """Converts SQL queries in list-of-lists form to a list of dictionaries (JSON)"""
    value_dicts = []
    for value in query_results:
        value_dict= {}
        for i, col_name in enumerate(col_names):
            value_dict[col_name] = value[i]
        value_dicts.append(value_dict)
    return value_dicts

def get_path_date_time_string():
    """returns current data_time as a string without / to use in file paths"""

    date_time = datetime.now()  
    date_time_str = date_time.strftime('%d/%m/%Y_%H:%M')
    date_time_str = date_time_str.replace("/", "-") # remove /, used for file path
    return date_time_str




            
column_names = ('id', 'example_col', 'test_data')

db_values= {
    ('address',column_names): [(2, 'test_data2', '1030-01-01T00:00:00+00:00'), 
                                (3, 'test_data3', '1940-01-01T00:00:00+00:00'), 
                                (4, 'test_data4', '1001-01-01T00:00:00+00:00'), 
                                (5, 'test_data5', '1070-01-01T00:00:00+00:00'), 
                                (6, 'test_data6', '1005-01-01T00:00:00+00:00'), 
                                (7, 'test_data7', '1080-01-01T00:00:00+00:00'), 
                                (8, 'test_data8', '1031-01-01T00:00:00+00:00')], 
 
    ('payment', column_names): [(1, 'test_data1', '2000-01-01T00:00:00+00:00'), 
                                (2, 'test_data2', '2030-01-01T00:00:00+00:00'), 
                                (3, 'test_data3', '1940-01-01T00:00:00+00:00'), 
                                (4, 'test_data4', '2001-01-01T00:00:00+00:00'), 
                                (5, 'test_data5', '2070-01-01T00:00:00+00:00'), 
                                (6, 'test_data6', '2005-01-01T00:00:00+00:00'), 
                                (7, 'test_data7', '2080-01-01T00:00:00+00:00'), 
                                (8, 'test_data8', '2031-01-01T00:00:00+00:00')]}

