from dotenv import load_dotenv
import pg8000
import os
from datetime import datetime
import boto3
import json
import io


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
        conn = db_connection()
        cursor = conn.cursor()

    except Exception as e:
        raise Exception(f"Exception: {e}")

    finally:
        cursor.close()
        conn.close()
