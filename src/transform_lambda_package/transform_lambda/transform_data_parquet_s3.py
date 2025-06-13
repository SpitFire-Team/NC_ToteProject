from copy import deepcopy
from io import BytesIO

# - uncomment for testing

from src.utils.aws_utils import get_bucket_name, add_data_to_s3_bucket

# - uncomment for testing


# - uncomment for deployment

# from utils.aws_utils import get_bucket_name, add_data_to_s3_bucket

# - uncomment for deployment


def transform_data_to_parquet_on_s3(s3_client, table_df_list, date_time_str):
    """This function takes a list of dictionaries containing table name keys and
    dataframe values, saves to parquet format in a buffer and saves to the processed
    data bucket in a folder with table name and file name with the last updated date
    and time

    Arguments: A list of dictionaries in the format [{table_name: dataframe}, {table_name_2: dataframe_2}]
    Returns: None
    """

    table_df_list_copy = deepcopy(table_df_list)
    count_uploaded = 0

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
            count_uploaded += 1
            
    return count_uploaded

