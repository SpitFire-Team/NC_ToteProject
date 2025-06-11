from dotenv import load_dotenv
import pg8000
import os
from sqlalchemy import create_engine

load_dotenv()


def wh_connection_engine():
    user = os.getenv("WH_USER")
    password = os.getenv("WH_PASSWORD")
    host = os.getenv("WH_HOST")
    database = os.getenv("WH_NAME")
    connection_string = f"postgresql://{user}:{password}@{host}/{database}"
    db = create_engine(connection_string)
    conn = db.connect()
    return conn


def wh_connection():
    """
    Grabs environment variables and uses them to create a database connection.
    Returns:
        pg8000 database connection.
    Raises:
        exception if the connection fails due to interface errors.
    """
    load_dotenv()

    user = "project_team_09"
    password = "W0fAc1kiQ1BA5uA"
    host = "nc-data-eng-project-dw-prod.chpsczt8h1nu.eu-west-2.rds.amazonaws.com"
    port = 5432
    database = "postgres"

    try:
        conn = pg8000.connect(
            user=user,
            password=password,
            host=host,
            port=port,
            database=database,
        )
        return conn
    except pg8000.InterfaceError as e:
        raise Exception(f"Database connection failed: {e}")
