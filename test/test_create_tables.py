
from src.transform_lambda_pkg.transform_lambda.create_tables import seperate_dates, create_dim_date, get_date_columns_from_dfs

import pandas as pd
from datetime import datetime, date, time, timedelta
import pytest

def create_fact_payment_df():
    data = {
        "payment_record_id": [1, 2, 3],
        "payment_id": [101, 102, 103],
        "created_date": [pd.Timestamp(date(2025, 8, 1)), pd.Timestamp(date(2025, 8, 2)), pd.Timestamp(date(2025, 8, 3))],
        "created_time": [time(9, 15), time(10, 30), time(14, 45)],
        "last_updated_date": [pd.Timestamp(date(2025, 8, 5)), pd.Timestamp(date(2025, 8, 5)), pd.Timestamp(date(2025, 8, 6))],
        "last_updated_time": [time(16, 10), time(12, 5), time(17, 20)],
        "transaction_id": [5001, 5002, 5003],
        "counterparty_id": [2001, 2002, 2003],
        "payment_amount": [1500.75, 2500.00, 300.5],
        "currency_id": [1, 1, 2],
        "payment_type_id": [10, 11, 10],
        "paid": [True, False, True],
        "payment_date": [pd.Timestamp(date(2025, 8, 7)), pd.Timestamp(date(2025, 8, 8)), pd.Timestamp(date(2025, 8, 9))]
    }
    return pd.DataFrame(data)

def create_fact_sales_order_df():
    data = {
        "sales_record_id": [1, 2, 3],
        "sales_order_id": [201, 202, 203],
        "created_date": [pd.Timestamp(date(2025, 7, 25)), pd.Timestamp(date(2025, 7, 26)), pd.Timestamp(date(2025, 7, 27))],
        "created_time": [time(8, 0), time(9, 30), time(11, 45)],
        "last_updated_date": [pd.Timestamp(date(2025, 7, 28)), pd.Timestamp(date(2025, 7, 29)), pd.Timestamp(date(2025, 7, 30))],
        "last_updated_time": [time(15, 15), time(16, 20), time(14, 5)],
        "sales_staff_id": [3001, 3002, 3003],
        "counterparty_id": [2001, 2004, 2005],
        "units_sold": [10, 20, 15],
        "unit_price": [100.0, 150.0, 200.0],
        "currency_id": [1, 1, 2],
        "design_id": [4001, 4002, 4003],
        "agreed_payment_date": [pd.Timestamp(date(2025, 8, 10)), pd.Timestamp(date(2025, 8, 11)), pd.Timestamp(date(2025, 8, 12))],
        "agreed_delivery_date": [pd.Timestamp(date(2025, 8, 15)), pd.Timestamp(date(2025, 8, 16)), pd.Timestamp(date(2025, 8, 17))],
        "agreed_delivery_location_id": [6001, 6002, 6003]
    }
    return pd.DataFrame(data)

def create_fact_purchase_order_df():
    data = {
        "purchase_record_id": [1, 2, 3],
        "purchase_order_id": [501, 502, 503],
        "created_date": [pd.Timestamp(date(2025, 7, 20)), pd.Timestamp(date(2025, 7, 21)), pd.Timestamp(date(2025, 7, 22))],
        "created_time": [time(9, 0), time(10, 30), time(14, 15)],
        "last_updated_date": [pd.Timestamp(date(2025, 7, 25)), pd.Timestamp(date(2025, 7, 26)), pd.Timestamp(date(2025, 7, 27))],
        "last_updated_time": [time(15, 0), time(16, 45), time(13, 30)],
        "staff_id": [4001, 4002, 4003],
        "counterparty_id": [1001, 1002, 1003],
        "item_code": ["A100", "B200", "C300"],
        "item_quantity": [50, 30, 20],
        "item_unit_price": [25.0, 40.0, 15.0],
        "currency_id": [1, 1, 2],
        "agreed_delivery_date": [pd.Timestamp(date(2025, 8, 5)), pd.Timestamp(date(2025, 8, 6)), pd.Timestamp(date(2025, 8, 7))],
        "agreed_payment_date": [pd.Timestamp(date(2025, 8, 1)), pd.Timestamp(date(2025, 8, 2)), pd.Timestamp(date(2025, 8, 3))],
        "agreed_delivery_location_id": [7001, 7002, 7003],
    }
    return pd.DataFrame(data)

def create_date_df():
    data = {
        "date": sorted(list(set([
            # From fact_payment_df
            pd.Timestamp(date(2025, 8, 1)),
            pd.Timestamp(date(2025, 8, 2)),
            pd.Timestamp(date(2025, 8, 3)),
            pd.Timestamp(date(2025, 8, 5)),
            pd.Timestamp(date(2025, 8, 6)),
            pd.Timestamp(date(2025, 8, 7)),
            pd.Timestamp(date(2025, 8, 8)),
            pd.Timestamp(date(2025, 8, 9)),
            
            # From fact_sales_order_df
            pd.Timestamp(date(2025, 7, 25)),
            pd.Timestamp(date(2025, 7, 26)),
            pd.Timestamp(date(2025, 7, 27)),
            pd.Timestamp(date(2025, 7, 28)),
            pd.Timestamp(date(2025, 7, 29)),
            pd.Timestamp(date(2025, 7, 30)),
            pd.Timestamp(date(2025, 8, 10)),
            pd.Timestamp(date(2025, 8, 11)),
            pd.Timestamp(date(2025, 8, 12)),
            pd.Timestamp(date(2025, 8, 15)),
            pd.Timestamp(date(2025, 8, 16)),
            pd.Timestamp(date(2025, 8, 17)),

            # From fact_purchase_order_df
            pd.Timestamp(date(2025, 7, 20)),
            pd.Timestamp(date(2025, 7, 21)),
            pd.Timestamp(date(2025, 7, 22)),
        ])))
    }
    return pd.DataFrame(data)





@pytest.fixture
def fact_payment_df():
    return create_fact_payment_df()

@pytest.fixture
def fact_sales_order_df():
    return create_fact_sales_order_df()

@pytest.fixture
def date_df():
    return create_date_df()

@pytest.fixture
def fact_purchase_order_df(): 
    return create_fact_purchase_order_df()

class TestGetDateColumnsFromDfs:
    def test_print(self, fact_payment_df, fact_sales_order_df, date_df, fact_purchase_order_df):
        df_columns = ["created_date", "last_updated_date", "agreed_delivery_date", "agreed_payment_date", "payment_date"]


        result_df = get_date_columns_from_dfs(fact_payment_df=fact_payment_df, 
                                              fact_sales_order_df=fact_sales_order_df, 
                                              fact_purchase_order_df=fact_purchase_order_df, 
                                              df_columns=df_columns)
        
        assert result_df.equals(date_df)
    
class TestSeperateDates:
    def test_1(self, date_df):
        seperate_dates(date_df)
        pass

class TestCreateDimDate:
    def test_all_fixtures(self, fact_payment_df,fact_sales_order_df,fact_purchase_order_df):
        dim_date = create_dim_date(fact_payment_df, fact_sales_order_df, fact_purchase_order_df)
        print(dim_date.to_string())
        pass 