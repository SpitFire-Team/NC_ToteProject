# - uncomment for testing:

from src.transform_lambda_pkg.transform_lambda.read_json_to_dataframe import (
    read_json_to_dataframe,
)

from src.transform_lambda_pkg.transform_lambda.transform_data_parquet_s3 import (
    transform_data_to_parquet_on_s3,
)
from src.utils.aws_utils import make_s3_client, get_bucket_name
from src.transform_lambda_pkg.transform_lambda.transform_data import star_schema_ref, tables_for_modify
from src.transform_lambda_pkg.transform_lambda.create_tables import create_dim_date

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
from src.utils.df_utils import reorder_dataframe
from copy import deepcopy

def combine_tables(merged_tables, modified_tables):
    return merged_tables + modified_tables

def print_all_tables(final_tables):
    print('\n \n \n')

    print("Final tables <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<< \n")

    for table in final_tables:
        for table_name, col in table.items():

            print(table_name, " ", list(col.columns), "\n")
    
    print("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<< \n \n \n")

def reorder_all_df_columns(tables: list, star_schema_ref_copy: dict):
    """
    
    """
    new_tables = []
    
    for table in tables:
        for table_name, df in table.items(): 
            reordered_df = reorder_dataframe(df, star_schema_ref_copy[table_name])
            new_tables.append({table_name: reordered_df})
            
    return new_tables


def check_against_star_schema(tables, star_schema_ref_copy):
    """
    """
    
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
    
    if sorted(table_names) != sorted(star_schema_table_names):
        pprint(table_names)
        
        pprint(star_schema_table_names)

        raise Exception(f"Star Schema check error: table names: {table_names} do not match star_schema_reference: {star_schema_table_names}")
        
    return True


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
    
    
    final_tables = combine_tables(merged_ds, modify_ds)
    
    dim_date_df = create_dim_date(final_tables)
    
    final_tables.append({"dim_date": dim_date_df})
    
    final_tables = reorder_all_df_columns(final_tables, star_schema_ref_copy)
        
    if check_against_star_schema(final_tables, star_schema_ref_copy):
        # pass newly transformed list of dicts to transform_data_parquet_s3
        transformed_data = transform_data_to_parquet_on_s3(
            s3_client, final_tables, date_time_str_last_ingestion
            )
        return [{"last_ingested_str": date_time_str_last_ingestion}]
    else:
        return [{"Transform error": "data does not match star schema"}]
    


lambda_handler([{"last_ingested_str":"05-08-2025_14:36"}],None)


