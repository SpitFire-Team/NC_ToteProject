import pandas as pd
import psycopg2
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv
load_dotenv()

user = os.getenv("WH_USER")
password = os.getenv("WH_PASSWORD")
host = os.getenv("WH_HOST")
port = 5432
database = os.getenv("WH_DATABASE")
connection_string= 'postgresql://project_team_09:W0fAc1kiQ1BA5uA@nc-data-eng-project-dw-prod.chpsczt8h1nu.eu-west-2.rds.amazonaws.com/postgres'

# def get_connection():
#     return create_engine(
#         url="postgres://{0}:{1}@{2}:{3}/{4}".format(
#             user, password, host, port, database
#         )
#     )

# print(get_connection())
db = create_engine(connection_string)
conn = db.connect()

data = {
    "sales_order_id": [2, 3],
    "created_at": ["2022-11-03 14:20:52.186000", "2022-11-03 14:20:52.188000"],
    "last_updated": ["2022-11-03 14:20:52.186000", "2022-11-03 14:20:52.188000"],
    "design_id": [3, 4],
    "staff_id": [19, 10],
    "counterparty_id": [8, 4],
    "units_sold": [42972, 65839],
    "unit_price": ["3.94", "2.91"],
    "currency_id": [2, 3],
    "agreed_delivery_date": ["2022-11-07", "2022-11-06"],
    "agreed_payment_date": ["2022-11-08", "2022-11-07"],
    "agreed_delivery_location_id": [8, 19],
}

df = pd.DataFrame(data)
test_dict_list = [{"sales_order": df}]

df.to_sql('sales_order', con=conn, if_exists= 'append', index = False)


