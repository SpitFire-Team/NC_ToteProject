from dotenv import load_dotenv
import pg8000
import os
import boto3
import re
import pprint
from datetime import datetime

from src.extraction_lambda.get_items_from_database import (
    set_latest_updated_time,
    check_database_updates,
)


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


def filter_buckets(
    bucket_list,
):  # can be improved later to filter for any bucket prefix
    """
    Takes a list of buckets and returns a list of buckets that match the prefix
    Arguments:
        list of buckets
    Returns:
        a list of buckets filtered by the prefix
    """
    pattern = r"^ingested-data-bucket-"

    ingestion_buckets = []

    for bucket in bucket_list:
        if re.match(pattern, bucket["Name"]):
            ingestion_buckets.append(bucket)

    return ingestion_buckets


def find_latest_ingestion_bucket(client):
    """
    Takes all the s3 buckets and returns the name of the latest created ingestion bucket
    Arguments:
        a boto3 client
    Returns:
        the latest created ingestion bucket
    """
    list_of_all_buckets = client.list_buckets()

    list_of_ingestion_buckets = filter_buckets(list_of_all_buckets["Buckets"])

    if not list_of_ingestion_buckets:
        return None  # change to appropriate error in the future. Logging required

    latest_creation = list_of_ingestion_buckets[0]

    for bucket in list_of_ingestion_buckets:
        if bucket["CreationDate"] > latest_creation["CreationDate"]:
            latest_creation = bucket

    return latest_creation["Name"]


def lambda_handler(event, context):
    # create a client
    # code to grab bucket name using prefix
    # use with bucket name to grab last updated time
    # use latest time with query all tables
    #

    try:
        client = boto3.client("s3")

        bucket = "random_name"

        conn = db_connection()

        latest_updated_time = set_latest_updated_time(bucket, client)

    except Exception as e:
        raise Exception(f"Exception: {e}")

    finally:

        conn.close()
