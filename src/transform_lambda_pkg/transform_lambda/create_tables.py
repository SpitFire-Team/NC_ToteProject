import pandas as pd

def create_dim_date(fact_payment_df, fact_sales_order_df):
    # get date columns from both dfs
    # break down dates into dim_date_columns
    # create new dim date df
    
    df_columns = ["created_date", "last_updated_date", "agreed_delivery_date", "agreed_payment_date"]

    dim_date_columns = ["date_id", "year", "month", "day", "day_of_week", "day_name", "month_name", "quarter"]

    dim_date = pd.DataFrame(columns=dim_date_columns)

    append_series = pd.Series()

    for column in df_columns:
        temp_series = pd.Series()
        s_sales_order = fact_sales_order_df[column] 

        if column != "agreed_delivery_date": 
            if column != "agreed_payment_date":
                s_payment = fact_payment_df[column]
    
                temp_series = pd.concat([s_payment, s_sales_order], ignore_index=True)

                append_series = pd.concat([temp_series, append_series], ignore_index=True)

                print(append_series.to_string(), "inside")
        
        else:
            append_series = pd.concat([append_series, s_sales_order], ignore_index=True)
    print(append_series.to_string(), "outside")

