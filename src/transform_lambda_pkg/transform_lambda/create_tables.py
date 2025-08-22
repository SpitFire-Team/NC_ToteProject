import pandas as pd
from pprint import pprint

def create_dim_date(tables):
    for table in tables:
        table_name = list(table.keys())[0]
        if table_name == "fact_payment":
            fact_payment_df = table[table_name]
        elif table_name == "fact_sales_order":
            fact_sales_order_df = table[table_name]
        elif table_name == "fact_purchase_order":
            fact_purchase_order_df = table[table_name]
        
    df_columns = ["created_date", "last_updated_date", "agreed_delivery_date", "agreed_payment_date", "payment_date"]
    
    date_df = get_date_columns_from_dfs(fact_payment_df=fact_payment_df, 
                                        fact_sales_order_df=fact_sales_order_df, 
                                        fact_purchase_order_df = fact_purchase_order_df, 
                                        df_columns= df_columns)
    
    dim_date = seperate_dates(date_df)
    return dim_date

    

def get_date_columns_from_dfs(fact_payment_df:pd.DataFrame, 
                              fact_sales_order_df:pd.DataFrame, 
                              fact_purchase_order_df: pd.DataFrame, 
                              df_columns:list): 
    append_series = pd.Series()

    for column in df_columns:
        print(column)

        if column in ["agreed_delivery_date", "agreed_payment_date"]:
            s_sales_order = fact_sales_order_df[column] 
            s_purchase_order = fact_purchase_order_df[column]

            append_series = pd.concat([append_series, s_sales_order, s_purchase_order], ignore_index=True)
        elif column == "payment_date":
            print("only payment")
            print(column)

            s_payment = fact_payment_df[column]
            append_series = pd.concat([append_series, s_payment], ignore_index=True)
        else:
            s_payment = fact_payment_df[column]
            s_sales_order = fact_sales_order_df[column] 
            s_purchase_order = fact_purchase_order_df[column]

            append_series = pd.concat([append_series, s_payment, s_sales_order, s_purchase_order], ignore_index=True)
    
    append_series = append_series.drop_duplicates()
    append_series = pd.to_datetime(append_series)
    append_series = append_series.sort_values(ascending=True)
    append_series = append_series.reset_index(drop=True)
    
    return append_series.to_frame(name = "date")

def seperate_dates(dates_df):
    dim_date = dates_df.copy()
    dim_date["date"] = pd.to_datetime(dim_date["date"])

    dim_date["year"] = dim_date["date"].dt.year
    dim_date["month"] = dim_date["date"].dt.month
    dim_date["day"] = dim_date["date"].dt.day
    dim_date["day_of_week"] = dim_date["date"].dt.day_of_week+1
    dim_date["day_name"] = dim_date["date"].dt.day_name()
    dim_date["month_name"] = dim_date["date"].dt.month_name()
    dim_date["quarter"] = dim_date["date"].dt.quarter
    dim_date = dim_date.drop(columns = ["date"])
    dim_date = dim_date.reset_index(names = "date_id")
    return dim_date

def create_merged_datastructure(tables, star_schema_ref):
    return_data_structure = [
        {"dim_counterparty": [], "col_list": star_schema_ref["dim_counterparty"]},
        {"dim_staff": [], "col_list": star_schema_ref["dim_staff"]}
    ]
