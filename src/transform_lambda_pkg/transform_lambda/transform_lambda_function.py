# - uncomment for testing:

from src.transform_lambda_pkg.transform_lambda.read_json_to_dataframe import (
    read_json_to_dataframe,
)
from src.transform_lambda_pkg.transform_lambda.modify_tables import (
    dataframe_modification,
)
from src.transform_lambda_pkg.transform_lambda.merge_tables import (
    transform_staff_and_department_tables,
)
from src.transform_lambda_pkg.transform_lambda.transform_data_parquet_s3 import (
    transform_data_to_parquet_on_s3,
)
from src.utils.aws_utils import make_s3_client, get_bucket_name
from src.transform_lambda_pkg.transform_lambda.transfrom_data import star_schema_ref

# - uncomment for testing:


# - uncomment for deployment:

# import boto3

# from transform_lambda.read_json_to_dataframe import read_json_to_dataframe
# from transform_lambda.dataframe_modification import dataframe_modification
# from transform_lambda.create_dim_staff_table import (
#     transform_staff_and_department_tables,
# )
# from transform_lambda.transform_data_parquet_s3 import (
#     transform_data_to_parquet_on_s3,
# )
# from utils.aws_utils import get_bucket_name
# from transform_lambda.transform_data import star_schema_ref


# - uncomment for deployment:


def lambda_sudo():
    pass

    # Setup
    # get date_time_str
    # setup ingestion bucket

    # Get data
    # read json data from last ingestions and save to dataframe

    # Create Datastrucutres module
    # create star_scheme ref dictionary {star_schema_name: [col_list]} ***

    # create data structure for tables that need to merge - ds_merge_data [{"dim_counterparty":[df_counterparty, df_address],
    #                                                                                         "col_list":[col1, col2]}
    # create data structure for tables that don't need to merge - ds_non_merge_data  [{"table1_name": df1, "col_list":[col1, col2]}, {"table2_name": df2, "col_list":[col1, col2]}]

    # Transform Data
    # Merge  - using ds_merge_data - [{"dim/fact_table1_name": df1}, {"dim/fact_table2_name": df2}]
    
    # merged_tables ds  [{"dim/fact_table1_name": df1}, {"dim/fact_table2_name": df2}] - ready to go
    # modify_tables ds [{"dim/fact_table1_name": df1}, {"dim/fact_table2_name": df2}]
    
    # create extra columns - modify_tables - utils required: currency_code -> currency_name, one util(created_at -> created_date, created_time, lasted_updated -> last_updated_date, last_updated_time)
    # in main function, check for currency_code errors, log the errors and the currency codes that caused them
    
    # rename_table_and_remove_uneeded_df_columns
        # rename tables (address to location) 
        # Remove unneeded columns - [{"dim/fact_table1_name": df1}, {"dim/fact_table2_name": df2}]

    # Add merged and modified dfs - [{"dim/fact_table1_name": df1}, {"dim/fact_table2_name": df2}]
    
    

    # final check agaist star schema ref dictionary

    # Save data to parquet form in S3 bucket


def lambda_handler(event, context):

    date_time_str_last_ingestion = event[0]["last_ingested_str"]

    # - uncomment for testing:

    # s3_client = make_s3_client()

    # - uncomment for testing:

    # - uncomment for deployment:

    s3_client = boto3.client(
        "s3"
    )

    # - uncomment for deployment:

    # retreive full s3 bucket name
    bucket_prefix = "ingested-data"
    bucket_name = get_bucket_name(s3_client, bucket_prefix)

    # read_json_to_dataframe reads s3 json datafiles and returns list of dicts mapping table names to Pandas dataframes
    ingested_data = read_json_to_dataframe(
        s3_client, bucket_name, date_time_str_last_ingestion
    )

    # create data structure for tables that need to merge - ds_merge_data [{"dim_counterparty":[df_counterparty, df_address],
    #                                                                                         "col_list":[col1, col2]}

    print("ingested_data len >>>>", len(ingested_data))
    print("ingested_data >>>>", ingested_data)

    # pass the list of dicts to dataframe_modification which removes unnecessary columns
    modified_data = dataframe_modification(ingested_data)
    print("modified_data_len >>>>", len(modified_data))
    print("modified_data >>>>", modified_data)

    # print("combined_dim_staff >>>>", combined_dim_staff)

    # add dim_staff table dictionary to list of dicts
    modified_data.append({"dim_staff": combined_dim_staff})
    print("modified_data_len2 >>>>", len(modified_data))
    print("modified_data2 >>>>", modified_data)

    # remove staff and department from list of dicts
    for dict in modified_data:
        dict.pop("staff", None)
        dict.pop("department", None)

    # remove empty dicts
    modified_data = [dict for dict in modified_data if dict]

    print("modified_data_len3 >>>>", len(modified_data))
    print("modified_data3 >>>>", modified_data)
    for dict in modified_data:
        print("Updated modified data table >>>", dict.keys())

    # pass newly transformed list of dicts to transform_data_parquet_s3
    transformed_data = transform_data_to_parquet_on_s3(
        s3_client, modified_data, date_time_str_last_ingestion
    )
    print("transformed data >>>>", transformed_data)

    # return event: datetime string (for load lambda handler)
    return event


def merge_tables():
    # from the newly modified dataframes, select the 'staff' and 'department' dataframes to be passed to create_dim_staff_table
    for dict in modified_data:
        for key, df in dict.items():
            if key == "staff":
                staff_df = df
                # print("staff_df >>>>", staff_df)
            if key == "department":
                department_df = df
                # print("department_df >>>>", department_df)

    if staff_df is not None and department_df is not None:
        # create_dim_staff_table returns combined dim_staff dataframe
        combined_dim_staff = transform_staff_and_department_tables(
            staff_df, department_df
        )

    pass
