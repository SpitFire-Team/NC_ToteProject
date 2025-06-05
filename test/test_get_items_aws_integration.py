import boto3
import pytest
import time
import psycopg2
import os
from dotenv import load_dotenv
from datetime import datetime, timezone
# from src.extraction_lambda.extraction_lambda import db_connection
from src.extraction_lambda.get_items_from_database import (
    set_latest_updated_time,
    check_database_updates,
    query_all_tables
)

# load in variables from .env file. Below is configured to find .env in project root, while file is run from test/dir (specifically when root is one level above test dir). Test .dir location with: print("loading .env from:", dotenv_path)
dotenv_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '.env'))
load_dotenv(dotenv_path, override=True) # print("check DB_NAME. DB_NAME from .env is:", os.getenv("DB_NAME"))

# access DB and AWS variables from .env file
DB_USER=os.getenv("DB_USER")
DB_PASSWORD=os.getenv("DB_PASSWORD")
DB_HOST=os.getenv("DB_HOST")
DB_PORT=os.getenv("DB_PORT")
DB_NAME=os.getenv("DB_NAME")
AWS_ACCESS_KEY=os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_KEY=os.getenv("AWS_SECRET_KEY")
# print(f"Check connection details: user={DB_USER}")

@pytest.fixture
def real_db_connection():
    totesys_db_connection = psycopg2.connect(
    user=DB_USER,
    password=DB_PASSWORD,
    host=DB_HOST,
    port=DB_PORT,
    database=DB_NAME
)
    cursor = totesys_db_connection.cursor()
    cursor.execute("SELECT current_database();")
    print("Using database:", cursor.fetchone())
    cursor.execute("SELECT * FROM staff;")
    rows = cursor.fetchall()
    print(rows)

    return totesys_db_connection # or yield?
    
class TestRealTotesysDatabaseConnection:
    def test_real_database_returns_multiple_records_for_staff_table(self, real_db_connection):
        last_updated_time = datetime(2020, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

        result = check_database_updates(real_db_connection, "staff", last_updated_time)

        assert len(result[1]) == 20
    
    def test_real_database_returns_multiple_records_for_paymemt_table(self, real_db_connection):
        last_updated_time = datetime(2020, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

        result = check_database_updates(real_db_connection, "payment", last_updated_time)
        assert len(result[1]) == 20540
    

# access AWS bucket
# real_bucket_name = <namehere>

