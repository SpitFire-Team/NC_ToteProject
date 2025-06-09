

def transform_data_to_parquet_on_s3(table_df_list):
    for table in table_df_list:
        table_name= table.keys
        table_df= table.values
        
        print(table_name)
        print(table_df)

        pass