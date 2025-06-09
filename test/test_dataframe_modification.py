from src.transform_lambda.dataframe_modification import dataframe_modification
import pandas as pd
import json
from io import StringIO

string_1 ={"sales_order_id": [2,3],
  "created_at": ["2022-11-03 14:20:52.186000","2022-11-03 14:20:52.188000"],
  "last_updated": ["2022-11-03 14:20:52.186000","2022-11-03 14:20:52.188000"],
  "design_id": [3,4],
  "staff_id": [19,10],
  "counterparty_id": [8,4],
  "units_sold": [42972,65839],
  "unit_price": ["3.94","2.91"],
  "currency_id": [2,3],
  "agreed_delivery_date": ["2022-11-07","2022-11-06"],
  "agreed_payment_date": ["2022-11-08","2022-11-07"],
  "agreed_delivery_location_id": [8,19]}

dataframe_1 = pd.DataFrame(string_1)

test_dict_list= [{"sales_order": dataframe_1}]

def test_dataframe_modification_returns_list_of_dicts():
    test_dict_list= [{"sales_order": dataframe_1}]
    assert type(dataframe_modification(test_dict_list)) == list
    assert "sales_order" in dataframe_modification(test_dict_list)[0].keys()

def test_dataframe_moficiation_removes_create_at_collum():
    assert "created_at" in list(dataframe_1.columns.values) 
    result = dataframe_modification(test_dict_list)
    assert "created_at" not in list(result["sales_order"].columns.values) 
