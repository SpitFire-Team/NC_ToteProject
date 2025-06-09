from src.utils.aws_utils import get_bucket_name, add_data_to_s3_bucket, make_s3_client
from src.utils.file_utils import get_path_date_time_string
from copy import deepcopy
import pandas as pd
from io import BytesIO


def transform_data_to_parquet_on_s3(s3_client, table_df_list, date_time_str):
    table_df_list_copy = deepcopy(table_df_list)

    for table_dict in table_df_list_copy:
        for table_name, table_df in table_dict.items():

            file_path = f"{table_name}/{date_time_str}.parquet"
            bucket_prefix = "processed-data"
            bucket_name = get_bucket_name(
                s3_client=s3_client, bucket_prefix=bucket_prefix
            )

            out_buffer = BytesIO()
            table_df.to_parquet(out_buffer, index=False)
            add_data_to_s3_bucket(
                s3_client,
                bucket_name=bucket_name,
                data=out_buffer.getvalue(),
                file_path=file_path,
            )


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


def dummy_dic_list(dummy_df):
    df_dict = {}
    list_dict = []
    table_name_list = [
        "fact_sales_order",
        "dim_staff",
        "dim_location",
        "dim_design",
        "dim_date",
        "dim_currency",
        "dim_counterparty",
    ]
    for table_name in table_name_list:
        df_dict[table_name] = dummy_df
    list_dict.append(df_dict)
    return list_dict


transform_data_to_parquet_on_s3(
    make_s3_client(), dummy_dic_list(dummy_df()), get_path_date_time_string()
)
