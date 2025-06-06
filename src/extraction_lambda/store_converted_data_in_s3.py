from src.utils.aws_utils import add_data_to_s3_bucket, get_bucket_name, make_s3_client
from src.utils.file_utils import convert_to_dict, get_path_date_time_string
from copy import deepcopy
import json


def transform_data_to_compatable_format(db_updated_values):
    """
    converts updated into json format (list of dictionaries)

    Returns dictionary of key = table_name values = converted value with table
    """
    transformed_values = {}

    for key, values in db_updated_values.items():
        # iterate though tables and convert values into json
        (table_name, column_names) = key
        value_json_compat = convert_to_dict(values, column_names)
        transformed_values[table_name] = value_json_compat
    return transformed_values


def input_updated_data_into_s3(s3_client, db_updated_values):
    """
    convert db_updated_values in json format seperated by table. uploads to table folder
    in the s3 bucket
            Parameters:
                    s3_client (boto3.client('s3')): a boto3 client for an s3 bucket
                    db_updated_values ({(<updated_table_name>, column_list) : all
                    updated data since last call})
            Returns:
                    none
    """
    db_values_copy = deepcopy(db_updated_values)

    date_time_str = get_path_date_time_string()
    transformed_data = transform_data_to_compatable_format(db_values_copy)

    bucket_prefix = "ingested-data"
    bucket_name = get_bucket_name(s3_client=s3_client, bucket_prefix=bucket_prefix)

    add_data_to_s3_bucket(
        s3_client=s3_client,
        bucket_name=bucket_name,
        data=date_time_str,
        file_path="last_ingestion.txt",
    )

    for table_name, transformed_values in transformed_data.items():
        file_path = f"{table_name}/{date_time_str}.json"

        json_compatable_data = json.dumps(transformed_values)

        add_data_to_s3_bucket(
            s3_client=s3_client,
            bucket_name=bucket_name,
            data=json_compatable_data,
            file_path=file_path,
        )


column_names = ("id", "example_col", "test_data")
db_values = {
    ("address", column_names): [
        (2, "test_data2", "1030-01-01T00:00:00+00:00"),
        (3, "test_data3", "1940-01-01T00:00:00+00:00"),
        (4, "test_data4", "1001-01-01T00:00:00+00:00"),
        (5, "test_data5", "1070-01-01T00:00:00+00:00"),
        (6, "test_data6", "1005-01-01T00:00:00+00:00"),
        (7, "test_data7", "1080-01-01T00:00:00+00:00"),
        (8, "test_data8", "1031-01-01T00:00:00+00:00"),
    ],
    ("payment", column_names): [
        (1, "test_data1", "2000-01-01T00:00:00+00:00"),
        (2, "test_data2", "2030-01-01T00:00:00+00:00"),
        (3, "test_data3", "1940-01-01T00:00:00+00:00"),
        (4, "test_data4", "2001-01-01T00:00:00+00:00"),
        (5, "test_data5", "2070-01-01T00:00:00+00:00"),
        (6, "test_data6", "2005-01-01T00:00:00+00:00"),
        (7, "test_data7", "2080-01-01T00:00:00+00:00"),
        (8, "test_data8", "2031-01-01T00:00:00+00:00"),
    ],
}

input_updated_data_into_s3(make_s3_client(), db_values)
