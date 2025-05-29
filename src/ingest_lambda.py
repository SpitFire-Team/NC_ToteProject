from dotenv import load_dotenv
import pg8000
import os
from datetime import datetime

def db_connection():
    '''
    Grabs environment variables and uses them to create a database connection.
    Returns:
        pg8000 database connection.
    Raises:
        exception if the connection fails due to interface errors.
    '''
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
        raise Exception(f"Database connection failed: {e}")
    
    finally:
        cursor.close()
        conn.close()


def set_time():
    
    return datetime(1980, 1 , 1)

def get_last_checked_time_from_s3(bucket_name, key):
    response = s3.object(Bucket=bucket_name, Key=key)
    return response



def check_update(cursor):
     
    last_checked_time = set_time()

    return datetime
        
