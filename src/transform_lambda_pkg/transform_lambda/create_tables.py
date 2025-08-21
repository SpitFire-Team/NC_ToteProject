import pandas as pd

def create_dim_date(fact_payment_df, fact_sales_order_df, fact_purchase_order_df):
    # get date columns from both dfs
    # break down dates into dim_date_columns
    # create new dim date df
    
    df_columns = ["created_date", "last_updated_date", "agreed_delivery_date", "agreed_payment_date", "payment_date"]
    
    date_df = get_date_columns_from_dfs(fact_payment_df=fact_payment_df, 
                                        fact_sales_order_df=fact_sales_order_df, 
                                        fact_purchase_order_df = fact_purchase_order_df, 
                                        df_columns= df_columns)

    dim_date_columns = ["date_id", "year", "month", "day", "day_of_week", "day_name", "month_name", "quarter"]
    
    dim_date = seperate_dates(date_df, dim_date_columns)
    
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
    append_series = append_series.sort_values(ascending=True)
    append_series = append_series.reset_index(drop=True)
    
    return append_series.to_frame(name = "date")

def seperate_dates(dates_df, dim_date_df, dim_columns):
    # dim_date = dates_df[]
    pass
    