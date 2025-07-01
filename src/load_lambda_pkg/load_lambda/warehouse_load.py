from dotenv import load_dotenv
from sqlalchemy import create_engine
import os

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
        return conn
    except Exception as e:
        raise Exception(f"Database connection failed: {e}")


def load_to_warehouse_loop(dict_list, conn):
    """
    #     This function will load transformed data into appropriate tables in
    #     a pre-prepared data warehouse
    #     Inputs:
    #     A list of dictionaries in the format [{table_name: dataframe}, {table_name_2: dataframe_2}]
    #"""
    try:
        for item in dict_list:
            table_name = list(item.keys())[0]
            df = item[table_name]
            df.to_sql(table_name, con=conn, if_exists="append", index=False)
    except Exception as e:
        raise Exception(f"Could not append to table: {e}")


## below is updated version modified for deploying lambdas branch.


# def load_to_warehouse_loop(dict_list, conn):
#     """
#     #     This function will load transformed data into appropriate tables in
#     #     a pre-prepared data warehouse
#     #     Inputs:
#     #     A list of dictionaries in the format [{table_name: dataframe}, {table_name_2: dataframe_2}]
#     #"""
#     items_loaded = 0
#     # pprint(dict_list)
#     inspector = inspect(conn)

#     for item in dict_list:
#         table_name = list(item.keys())[0]
#         df = item[table_name]
#         if inspector.has_table(table_name, schema='project_team_09'):
#             df.to_sql(
#                 name=table_name,
#                 con=conn,
#                 if_exists="append",
#                 index=False,
#                 schema="project_team_09"
#             )
#             items_loaded += 1
#         else:
#             print(f"Table '{table_name}' does not exist. Skipping.")


#     return f"items loaded {items_loaded}"
#     # except Exception as e:
#     #     raise Exception(f"Could not append to table: {e}")
