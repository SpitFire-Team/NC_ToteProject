from dotenv import load_dotenv
import pg8000
import os

load_dotenv()

user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
host = os.getenv("DB_HOST")
port = int(os.getenv("DB_PORT", 5432))
database = os.getenv("DB_NAME")

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
    
db_connection = db_connection()
cursor = db_connection.cursor()
cursor.execute( 
    'SELECT * FROM Sale_order'
)
print(cursor.fetchall())

cursor.close()
db_connection.close()
