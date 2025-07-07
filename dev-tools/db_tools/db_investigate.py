from src.extraction_lambda_pkg.extraction_lambda.extraction_lambda_function import db_connection 
import pg8000.native
from pprint import pprint
from src.utils.file_utils import convert_to_dict
from dotenv import load_dotenv
import pg8000
import os


### this is just a helper that allow quick and dirty quieres. If not 
#### useful can be removed. or useful stuff added to other util files


def connect_to_db():
    load_dotenv(override=True)
    return pg8000.native.Connection (
        user = os.getenv("DB_USER"),
        password = os.getenv("DB_PASSWORD"),
        host = os.getenv("DB_HOST"),
        port = int(os.getenv("DB_PORT")),
        database = os.getenv("DB_DATABASE")
    )
def close_db_connection(conn):
    conn.close()

conn= connect_to_db()
sql_query= conn.run('''SELECT * FROM sales_order LIMIT 10''')
# pprint(conn.columns)

def run_query(query):
    """run_query will run a given sql query using a pg8000 db connection"""
    conn= connect_to_db()
    conn.run(query)
    column_names = [col['name'] for col in conn.columns]
    list_of_dicts= convert_to_dict(sql_query, [col['name'] for col in conn.columns])
    return list_of_dicts
