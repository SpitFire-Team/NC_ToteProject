# - uncomment for testing:

from src.transform_lambda_pkg.transform_lambda.read_json_to_dataframe import (
    read_json_to_dataframe,
)

from src.transform_lambda_pkg.transform_lambda.transform_data_parquet_s3 import (
    transform_data_to_parquet_on_s3,
)
from src.utils.aws_utils import make_s3_client, get_bucket_name
from src.transform_lambda_pkg.transform_lambda.transform_data import star_schema_ref, tables_for_modify

from pprint import pprint



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

from src.transform_lambda_pkg.transform_lambda.transform_data import star_schema_ref
from src.transform_lambda_pkg.transform_lambda.merge_tables import create_merged_datastructure, merge_tables

from src.transform_lambda_pkg.transform_lambda.modify_tables import create_modify_tables_datastructure, create_extra_columns, rename_table_and_remove_uneeded_df_columns

from copy import deepcopy

def combine_tables(merged_tables, modified_tables):
    return merged_tables + modified_tables

def check_against_star_schema(tables, star_schema_ref_copy):
    
    star_schema_table_names = list(star_schema_ref_copy.keys())
    

    
    if len(star_schema_ref_copy) != len(tables):
        pass
        # raise Exception("Star Schema check error: Tables do not match star schema length")
    table_names = []
    for table in tables: 
        table_name = list(table.keys())[0]
        table_names.append(table_name)
        try:
            star_schema_ref_copy[table_name]
        except KeyError:
            raise Exception(f"Star Schema check error: {table_name} not in star_schema_reference")
            
        table_df = list(table.values())[0]
        table_columns = list(table_df.columns) # list maybe should be removed
        
        if table_columns != star_schema_ref_copy[table_name]: # note for monday - i oon't think it should sort as order is important for db upload
            raise Exception(f"Star Schema check error: {table_name} columns do not match star_schema_reference")
    

    # print(table_names, "<<< table names")
    # print(star_schema_table_names, "<<< star schema table names")
    if sorted(table_names) != sorted(star_schema_table_names):

        raise Exception(f"Star Schema check error: table names do not match star_schema_reference")
        
    return True
    
    

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

    # combine merged and modified dfs - [{"dim/fact_table1_name": df1}, {"dim/fact_table2_name": df2}]
    
    

    # final check agaist star schema ref dictionary

    # Save data to parquet form in S3 bucket


def lambda_handler(event, context):
    date_time_str_last_ingestion = event[0]["last_ingested_str"]

       # - uncomment for testing:

    s3_client = make_s3_client()

    # - uncomment for testing:

    # - uncomment for deployment:

    # s3_client = boto3.client(
    #     "s3"
    # )
    
    # - uncomment for deployment:

    # retreive full s3 bucket name
    bucket_prefix = "ingested-data"
    bucket_name = get_bucket_name(s3_client, bucket_prefix)

    # read_json_to_dataframe reads s3 json datafiles and returns list of dicts mapping table names to Pandas dataframes
    tables = read_json_to_dataframe(
        s3_client, bucket_name, date_time_str_last_ingestion
    )
    
    star_schema_ref_copy = deepcopy(star_schema_ref)
    tables_for_modify_copy = deepcopy(tables_for_modify)

    merged_ds = create_merged_datastructure(tables, star_schema_ref_copy)

    merged_ds = merge_tables(merged_ds)
    
    modify_ds = create_modify_tables_datastructure(tables,tables_for_modify_copy, star_schema_ref_copy)
    
    modify_ds = create_extra_columns(modify_ds)
    
    modify_ds = rename_table_and_remove_uneeded_df_columns(modify_ds,star_schema_ref_copy)

    for table in modify_ds:
        for key, value in table.items():
            if key == "fact_payment":
                pprint(list(value.columns))
    
    final_tables = combine_tables(merged_ds, modify_ds)
        
    if check_against_star_schema(final_tables, star_schema_ref_copy):
        # pass newly transformed list of dicts to transform_data_parquet_s3
        transformed_data = transform_data_to_parquet_on_s3(
            s3_client, final_tables, date_time_str_last_ingestion
            )
        return [{"last_ingested_str": date_time_str_last_ingestion}]
    else:
        return [{"Transform error": "data does not match star schema"}]
    


lambda_handler([{"last_ingested_str":"05-08-2025_14:36"}],None)





def lambda_handler_old(event, context):

    date_time_str_last_ingestion = event[0]["last_ingested_str"]

    # - uncomment for testing:

    s3_client = make_s3_client()

    # - uncomment for testing:

    # - uncomment for deployment:

    # s3_client = boto3.client(
    #     "s3"
    # )

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




