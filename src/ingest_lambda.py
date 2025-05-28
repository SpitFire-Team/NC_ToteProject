from dotenv import load_dotenv
import pg8000
import os

load_dotenv()

user = os.getenv("USER")
password = os.getenv("PASSWORD")
host = os.getenv("HOST")
port = os.getenv("PORT")
database = os.getenv("DATABASE")

def db_connection():
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
