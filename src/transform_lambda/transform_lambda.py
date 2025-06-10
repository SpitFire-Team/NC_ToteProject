import boto3
from pprint import pprint
from src.transform_lambda.read_json_to_dataframe import read_json_to_dataframe
from src.transform_lambda.dataframe_modification import dataframe_modification
from src.utils.aws_utils import make_s3_client, get_bucket_name





def lambda_handler(event, context):

    date_time_str_last_ingestion = event[0]["last_ingested_str"] # date_time_str coming from step function and extraction lambda

    # date_time_str_last_ingestion = (
    #     "09-06-2025_07:46"  # example date time string for testing
    # )
    

    # s3 client
    s3_client = make_s3_client()

    """ uncomment for AWS implementation:
    s3_client = boto3.client(
        "s3"
    )"""

    # get full s3 bucket name
    bucket_prefix = "ingested-data"
    bucket_name = get_bucket_name(s3_client, bucket_prefix)

    # call read_json_to_datafram which reads s3 json datafiles and returns list of dicts mapping table names to Pandas dataframes
    ingested_data = read_json_to_dataframe(s3_client, bucket_name, date_time_str_last_ingestion)
    print("in func, ingested_data len >>>>", len(ingested_data))
    print("in func, ingested_data >>>>", ingested_data)

    # pass the list of dictionaries to dataframe_modification which removes unnecessary columns
    modified_data = dataframe_modification(ingested_data)
    print("in func, modified_data >>>>", modified_data)

    # from modified dataframes, select specifically the 'staff' and 'department' dataframes to be passed to create_dim_staff_table

    staff_and_department = []
    for dict in modified_data:
        for key in dict:
            if key == "staff" or key == "department":
                staff_and_department.append(dict)
    print ("in func, staff_and_department", staff_and_department)

    # create_dim_staff_table returns combined dim_staff table
    

    # add dim_staff table dictionart to list of dicts
    # remove staff and department from list of dicts
    # pass newly transformed list of dicts to transform_data_underscore_s3
    # return event: datetime string (for load lambda handler)

    return event
