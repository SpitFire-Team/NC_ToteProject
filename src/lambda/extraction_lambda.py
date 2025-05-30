from dotenv import load_dotenv
import pg8000
import os
from datetime import datetime
import boto3
import json
import io

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
    
def set_latest_updated_time(bucket):
    s3 =  boto3.client("s3")
    objects = s3.list_objects(Bucket_name = bucket)
    if len(objects) == 0:
        return datetime.datetime(1970, 1, 1, 0, 0, 0, 0, tzinfo=datetime.timezone.utc)
    currenttime = objects["Contents"][0]["LastModified"]
    for object in objects["Contents"]:
        if object["LastModified"] > currenttime:
            currenttime = object["LastModified"]
    return currenttime

def check_database_updates(cursor, table, last_checked_time):
    last_checked_time = set_latest_updated_time("ingestion bucket")
    result = cursor.execute('''SELECT * FROM {table} WHERE last_updated > {last_checked_time}''')
    return result

def data_transform(results):
    buffer = io.StringIO()
    for result in results:
        buffer.write(json.dumps(result) + "\n")
    return buffer

def query_all_databases(cursor):
    tables = ["counterparty", "currency", "department", "design", "staff",
              "sales_order", "address","payment", "purchase_order",
              "payment_type", "transaction"]
    last_checked_time = set_latest_updated_time("ingestion bucket")
    results = []
    for table in tables:
        results.append(check_database_updates(cursor, table, last_checked_time))
    return results

def input_into_s3(buffer):
    timestamp = datetime.now()
    s3 = boto3.client("s3")
    s3.put_object(
        Bucket="extract_bucket",
        Key=timestamp,
        Body=buffer.getvalue())
        
def lambda_handler(event, context):
    try:
        conn = db_connection()
        cursor = conn.cursor()

    except Exception as e:
        raise Exception(f"Exception: {e}")
    
    finally:
        cursor.close()
        conn.close()
    
    

        


    
    


