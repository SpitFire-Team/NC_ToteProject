
from sqlalchemy import create_engine, Table, MetaData
import os
from dotenv import load_dotenv
from src.transform_lambda_pkg.transform_lambda.transform_data import star_schema_ref
from copy import deepcopy


load_dotenv()


def wh_connection_engine():
    try:
        user = os.getenv("WH_USER")
        password = os.getenv("WH_PASSWORD")
        host = os.getenv("WH_HOST")
        database = os.getenv("WH_NAME")
        connection_string = f"postgresql://{user}:{password}@{host}/{database}"
        db = create_engine(connection_string)
        conn = db.connect()
        return conn, db
    except Exception as e:
        raise Exception(f"Database connection failed: {e}")
    
def print_db(table_name, conn, engine):
    metadata= MetaData()
    table = Table(table_name, metadata, autoload_with=engine)
    query = table.select()
    results = conn.execute(query)
    for row in results:
        print(row)
        print("done")

def delete_db_table_values(table_name, conn, engine):
    metadata= MetaData()
    table = Table(table_name, metadata, autoload_with=engine)
    query = table.delete()

    results = conn.execute(query)

    conn.commit()

def delete_all_values_in_db(conn, engine):
    star_schema_ref_copy = deepcopy(star_schema_ref)

    for table in star_schema_ref_copy.keys():
        delete_db_table_values(table, conn, engine)

def print_all_tables(conn, engine):
    star_schema_ref_copy = deepcopy(star_schema_ref)

    for table in star_schema_ref_copy.keys():
        print_db(table, conn, engine)
conn,engine = wh_connection_engine()
delete_all_values_in_db(conn, engine)
#print_all_tables(conn, engine)