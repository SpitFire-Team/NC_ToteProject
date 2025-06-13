# - uncomment for testing

from src.transform_lambda_package.transform_lambda.read_json_to_dataframe import (
    read_json_to_dataframe,
)
from src.transform_lambda_package.transform_lambda.dataframe_modification import (
    dataframe_modification,
)

from src.transform_lambda_package.transform_lambda.transform_tables import (
    transform_staff_and_department_tables, 
    transform_dim_location_table, 
    transform_dim_counterparty_table, 
    transform_fact_payment_table
)
from src.transform_lambda_package.transform_lambda.transform_data_parquet_s3 import (
    transform_data_to_parquet_on_s3,
)
from src.utils.aws_utils import get_bucket_name, make_s3_client

# - uncomment for testing


# - uncomment for deployment

# from transform_lambda.read_json_to_dataframe import read_json_to_dataframe
# from transform_lambda.dataframe_modification import dataframe_modification
# import boto3

# from transform_lambda.transform_tables import (
#     transform_staff_and_department_tables, transform_dim_counterparty_table,transform_dim_location_table, transform_fact_payment_table, transform_dim_counterparty_table 
# )
# from transform_lambda.transform_data_parquet_s3 import (
#     transform_data_to_parquet_on_s3,
# )
# from utils.aws_utils import get_bucket_name

# - uncomment for deployment


def lambda_handler(event, context):

    date_time_str_last_ingestion = event[0]["last_ingested_str"]

    if date_time_str_last_ingestion == "no data ingested":
        return [{"last_ingested_str": "no data ingested"}]

    # - uncomment for testing:

    s3_client = make_s3_client()  # - uncomment for testing

    # - uncomment for testing:

    # - uncomment for deployment:

    # s3_client = boto3.client(
    #     "s3"
    # )

    # - uncomment for deployment

    # retreive full s3 bucket name
    bucket_prefix = "ingested-data"
    bucket_name = get_bucket_name(s3_client, bucket_prefix)

    # read_json_to_dataframe reads s3 json datafiles and returns list of dicts mapping table names to Pandas dataframes
    ingested_data = read_json_to_dataframe(
        s3_client, bucket_name, date_time_str_last_ingestion
    )

    if len(ingested_data) == 0:
        return [{"error": "no data ingested"}]

    # print("ingested_data len >>>>", len(ingested_data))
    # print("ingested_data >>>>", ingested_data)

    # pass the list of dicts to dataframe_modification which removes unnecessary columns
    modified_data = dataframe_modification(ingested_data)
    # print("modified_data_len >>>>", len(modified_data))
    # print("modified_data >>>>", modified_data)
    
    if len(modified_data) == 0:
        return [{"error": "error in modification"}]

    # from the newly modified dataframes, select the 'staff' and 'department' dataframes to be passed to create_dim_staff_table

    staff_df_exists = False
    department_df_exists = False
    counterparty_df_exists = False
    
    for dict in modified_data:
        for key, df in dict.items():
            print(key)
            if key == "staff":
                staff_df = df
                # print("staff_df >>>>", staff_df)
                staff_df_exists = True

            if key == "department":
                department_df = df
                # print("department_df >>>>", department_df)
                department_df_exists = True
            if key == "counterparty":
                counterparty_df = df
                # print("department_df >>>>", department_df)
                counterparty_df_exists = True
                print("here counterparty true")
                
            if key == "address":
                dim_location_df = transform_dim_location_table(df)
                modified_data.append({"dim_location": dim_location_df})
                
                if counterparty_df_exists:
                    dim_counterpart_df = transform_dim_counterparty_table(
                    df, counterparty_df
                    )
                    modified_data.append({"dim_counterparty": dim_counterpart_df})

            if key == "payment":
                payment_df_exists = True
                fact_payment_df = transform_fact_payment_table(df)
                modified_data.append({"fact_payment": fact_payment_df})
                print("here payment true")


    # create_dim_staff_table returns combined dim_staff dataframe
    if staff_df_exists and department_df_exists:
        combined_dim_staff = transform_staff_and_department_tables(
            staff_df, department_df
        )
        # print("combined_dim_staff >>>>", combined_dim_staff)
        modified_data.append({"dim_staff": combined_dim_staff})
        
        # remove staff and department from list of dicts
        for dict in modified_data:
            dict.pop("staff", None)
            dict.pop("department", None)
            dict.pop("counterparty", None)
            dict.pop("department", None)
            dict.pop("address", None)
            dict.pop("payment", None)
            # dict.pop("purchaise_order", None)
                           

    # remove empty dicts
    modified_data = [dict for dict in modified_data if dict]

    # print("modified_data_len3 >>>>", len(modified_data))
    # print("modified_data3 >>>>", modified_data)
    for dict in modified_data:
        pass
        # print("Updated modified data table >>>", dict.keys())

    # pass newly transformed list of dicts to transform_data_parquet_s3
    files_uploaded = transform_data_to_parquet_on_s3(
        s3_client, modified_data, date_time_str_last_ingestion
    )
    print(f"{files_uploaded} files uploaded to processed data data")

    # return event: datetime string (for load lambda handler)
    return event


event = [
  {
    "last_ingested_str": "12-06-2025_01:17"
  }
]

print(lambda_handler(event, {}))