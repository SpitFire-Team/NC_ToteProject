
from sqlalchemy import create_engine, Table, MetaData
import os
from dotenv import load_dotenv


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
    
def print_db(conn, engine):
    metadata= MetaData()
    table = Table("dim_counterparty", metadata, autoload_with=engine)
    query = table.select()
    results = conn.execute(query)
    for row in results:
        print(row)
        print("done")

    
conn,engine = wh_connection_engine()
print_db(conn=conn, engine=engine)