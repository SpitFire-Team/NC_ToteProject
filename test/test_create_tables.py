from src.transform_lambda_pkg.transform_lambda.create_tables import create_dim_date

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

@pytest.fixture
def fact_payment_df():
    return create_fact_payment_df()

@pytest.fixture
def fact_sales_order_df():
    return create_fact_sales_order_df()

class TestCreateDimDate:
    def test_print(self, fact_payment_df, fact_sales_order_df):

        create_dim_date(fact_payment_df, fact_sales_order_df)
        
        assert 1 == 2