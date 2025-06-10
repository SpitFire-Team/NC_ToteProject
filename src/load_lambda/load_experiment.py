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


def staff_df():
    column_name_list = [
        "staff_id",
        "first_name",
        "last_name",
        "department_name",
        "location",
        "email_address"
    ]
    staff_df = pd.DataFrame(columns=column_name_list)
    for i in range(10,20):
        data = {
            column_name_list[0]: i,
            column_name_list[1]: f"first_name_{i}",
            column_name_list[2]: f"last_name_{i}",
            column_name_list[3]: f"department_name{i}",
            column_name_list[4]: f"location{i}",
            column_name_list[5]: f"email_address{i}",
        }
        data_rows_to_add_df = pd.DataFrame(data, index=[i])
        staff_df = pd.concat([staff_df, data_rows_to_add_df], ignore_index=True)
    return staff_df


staff_data = staff_df()

df = pd.DataFrame(staff_data)
test_dict_list = [{"test_dim_staff": df}]
#Create a for loop, that take out the table name, and take out the data frame. 



df.to_sql('dim_staff', con=conn, if_exists= 'append', index = False)
#Puts inside the wharehouse 