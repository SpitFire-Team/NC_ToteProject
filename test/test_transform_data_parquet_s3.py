import pytest 
import pandas as pd
from pprint import pprint
from src.transform_lambda.transform_data_parquet_s3 import transform_data_to_parquet_on_s3

@pytest.fixture
def dummy_df():
    column_name_list = [
        "col_1",
        "col_2",
        "col_3",
        "col_4",
        "col_5",
    ]
    staff_df = pd.DataFrame(columns=column_name_list)
    for i in range(10):
        data = {
            column_name_list[0]: f"{column_name_list[0]} values",
            column_name_list[1]: f"{column_name_list[1]} values",
            column_name_list[2]: f"{column_name_list[2]} values",
            column_name_list[3]: f"{column_name_list[3]} values",
            column_name_list[4]: f"{column_name_list[4]} values",
        }
        data_rows_to_add_df = pd.DataFrame(data, index=[i])
        staff_df = pd.concat([staff_df, data_rows_to_add_df], ignore_index=True)

    return staff_df

@pytest.fixture
def dummy_dic_list(dummy_df):
    df_dict= {}
    list_dict=[]
    table_name_list= ["fact_sales_order" ,"dim_staff", "dim_location", "dim_design", "dim_date", "dim_currency", "dim_counterparty"]
    for table_name in table_name_list:
        df_dict[table_name]= dummy_df
        list_dict.append(df_dict)
    return list_dict

class TestTransformDataParquetS3:
    def test_1(self, dummy_dic_list):
        transform_data_to_parquet_on_s3(dummy_dic_list)
        pprint(dummy_dic_list)
        assert 1== 2 

