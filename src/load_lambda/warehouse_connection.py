from dotenv import load_dotenv
import pg8000
import os

def db_connection():
    """
    Grabs environment variables and uses them to create a database connection.
    Returns:
        pg8000 database connection.
    Raises:
        exception if the connection fails due to interface errors.
    """
    load_dotenv()

    user = os.getenv("WH_USER")
    password = os.getenv("WH_PASSWORD")
    host = os.getenv("WH_HOST")
    port = int(os.getenv("WH_PORT"))
    database = os.getenv("WH_DATABASE")

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