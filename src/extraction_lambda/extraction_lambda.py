from dotenv import load_dotenv
import pg8000
import os
import boto3
from datetime import datetime
from get_items_from_database import set_latest_updated_time, check_database_updates

def db_connection():
    """
    Grabs environment variables and uses them to create a database connection.
    Returns:
        pg8000 database connection.
    Raises:
        exception if the connection fails due to interface errors.
    """
    load_dotenv()

    user = os.getenv("USER")
    password = os.getenv("PASSWORD")
    host = os.getenv("HOST")
    port = int(os.getenv("PORT"))
    database = os.getenv("DATABASE")

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

def lambda_handler(event, context):
    try:
        client = boto3.client("s3")
        
        #code to grab bucket name
        bucket = "random_name"

        conn = db_connection()

        latest_updated_time = set_latest_updated_time(bucket, client)

    except Exception as e:
        raise Exception(f"Exception: {e}")

    finally:

        conn.close()
