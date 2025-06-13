import pandas as pd
from copy import deepcopy
from pprint import pprint

def transform_staff_and_department_tables(staff_dataframe, department_dataframe):
    staff_df_copy = deepcopy(staff_dataframe)
    department_df_copy = deepcopy(department_dataframe)
    dim_staff_col_name_list = [
        "staff_id",
        "first_name",
        "last_name",
        "department_name",
        "location",
        "email_address",
    ]
    merge_df = pd.merge(staff_df_copy, department_df_copy, on="department_id")
    dim_staff_df = merge_df.drop(columns=["department_id", "manager"], 
                                axis = 1, 
                                errors="ignore")
    dim_staff_df_reordered = dim_staff_df[dim_staff_col_name_list]
    return dim_staff_df_reordered



def transform_dim_counterparty_table(address_df, counterparty_df):
    address_df_copy = deepcopy(address_df)
    counterparty_df_copy = deepcopy(counterparty_df)
    dim_counterparty_col_name_list = [
        "counterparty_id",
        "counterparty_legal_name",
        "counterparty_legal_address_line_1",
        "counterparty_legal_address_line_2",
        "counterparty_legal_district",
        "counterparty_legal_city",
        "counterparty_legal_postal_code",
        "counterparty_legal_country",
        "counterparty_legal_phone_number",
    ]
    address_df_renamed = address_df_copy.rename(columns={  "address_line_1" : "counterparty_legal_address_line_1",
                                                           "address_line_2" : "counterparty_legal_address_line_2",
                                                           "district" :  "counterparty_legal_district",
                                                           "city" : "counterparty_legal_city",
                                                           "postal_code" : "counterparty_legal_postal_code",
                                                           "country" : "counterparty_legal_country",
                                                           "phone" : "counterparty_legal_phone_number"})
    
    counterparty_df_renamed = counterparty_df_copy.rename(columns={'legal_address_id': 'address_id'}) 
   
                                                           
    merged_dim_counterparty_df = pd.merge(address_df_renamed, counterparty_df_renamed, on="address_id")
    dim_counterparty_df = merged_dim_counterparty_df.drop(columns=["commercial_contact", "delivery_contact", "legal_address_id", "address_id"],
                                                            axis = 1, 
                                                            errors="ignore")
    
   
    
    dim_counterparty_df_reordered = dim_counterparty_df[dim_counterparty_col_name_list]
    
    # pprint(dim_counterparty_df_reordered)
    return dim_counterparty_df_reordered


def transform_dim_location_table(address_df):
    address_df_copy = deepcopy(address_df)
    address_df_renamed = address_df_copy.rename(columns={"address_id" : "location_id"})                                                           
    return address_df_renamed

def transform_fact_payment_table(payment_df):
    payment_df = deepcopy(payment_df)
    fact_payment_col_name_list = [
        "payment_record_id", #create
        "payment_id", #payment
        "created_date", #convert from created_at timestamp
        "created_time", #convert from created_at timestamp
        "last_updated_date", #convert from last updated time_stamp
        "last_updated_time", #convert from last updated time_stamp
        "transaction_id", #paymnet
        "counterparty_id", #payment
        "payment_amount",  #payment
        "curreny_id", #payment
        "payment_type_id", #payment
        "paid", #payment 
        "payment_date" #payment
    ]
    # merge_df = pd.merge(staff_df_copy, department_df_copy, on="department_id")
  
    fact_payment_df = payment_df.drop(columns=["company_ac_number", "counterparty_ac_number"], 
                                    axis = 1, 
                                    errors="ignore")
   
    # pprint(fact_payment_df)
    
    # fact_payment_df["created_date"] = fact_payment_df["created_at"].dt.date    
    # fact_payment_df["created_time"] = fact_payment_df["created_at"].dt.time

    # fact_payment_df["last_updated_date"] = fact_payment_df["last_updated"].dt.date    
    # fact_payment_df["last_updated_time"] = fact_payment_df["last_updated"].dt.time

    fact_payment_df_reordered = fact_payment_df[fact_payment_col_name_list]
    pprint(fact_payment_df_reordered)
    return fact_payment_df_reordered
