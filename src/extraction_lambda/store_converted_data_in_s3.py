from src.utils.aws_utils import add_data_to_s3_bucket  # src
from src.utils.file_utils import convert_to_dict, get_path_date_time_string  # src
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


def input_updated_data_into_s3(
    s3_client, db_updated_values, bucket
):  # delete '= None' once passing in db_updated values
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

    date_time_str = get_path_date_time_string()

    db_values_copy = deepcopy(db_updated_values)
    transformed_data = transform_data_to_compatable_format(db_values_copy)

    for table_name, transformed_values in transformed_data.items():
        file_path = f"{table_name}/{date_time_str}.json"
        bucket_name = bucket
        json_compatable_data = json.dumps(transformed_values, default=str)

        add_data_to_s3_bucket(
            s3_client=s3_client,
            bucket_name=bucket_name,
            data=json_compatable_data,
            file_path=file_path,
        )

    return date_time_str
