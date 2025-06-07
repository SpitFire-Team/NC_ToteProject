from datetime import datetime, timezone

"""
Import the necessary libraries to work with data and time.
"""


def set_latest_updated_time(bucket, client):
    """
    vars:
        - s3 bucket from lambda handler
        - client from lambda handler
    This function checks the contents of an s3 bucket.
    It determines the latest LastModified timestamp by looping through all objects in the s3 bucket.
    Function returns the latest LastModified time.
    Or returns a default unix time (1970) if no objects are found.

    """

    s3_files = client.list_objects(Bucket=bucket)  # check the s3 bucket for objects

    if "Contents" not in s3_files:
        return datetime(
            1970, 1, 1, 0, 0, 0, 0, tzinfo=timezone.utc
        )  # if theres no objects set the time to 1970

    last_updated = s3_files["Contents"][0]["LastModified"].astimezone(
        timezone.utc
    )  # if there is an item, set the curret time to the earliest object

    for object in s3_files["Contents"]:
        if (
            object["LastModified"].astimezone(timezone.utc) > last_updated
        ):  # loop through the objects checking if any have a later date than the current latest time
            last_updated = object["LastModified"]

    return last_updated.astimezone(timezone.utc)


def check_database_updates(conn, table, last_updated_time):
    """
    Vars:
        - conn - DB connection from extraction_lambda
        - table - database table to query. Passed from query_all_tables function
        - last_updated_time - last_updated time from set_latest_updated_time

    This function checks the given table for new updates since the last_updated_time.
    First, it converts the Pythonic time for PostgreSQL format and creates a cursor.
    A PostgreSQL query is executed to compare if the last_updated column is greater than the last checked time.
    Results are returned as a list of dictionaries.
    """

    last_updated_time_str = (
        last_updated_time.isoformat()
    )  # converting time format for postgres

    cursor = conn.cursor()  # creating a cursor to query/ interact with the db

    if table == "transaction":
        table_name = '"transaction"'  # transaction is a reserved SQL keyword, so must be double quoted
    else:
        table_name = table

    # because of use of sqllite3 and pyscopg2, we've had to remove the placeholder to test (placeholders are different). Risk of SQL injection
    cursor.execute(
        f"SELECT * FROM {table_name} WHERE last_updated > '{last_updated_time_str}'"
    )

    # Convert SQL output from list of tuples to a list of dictionaries with column names and values:
    columns = [obj[0] for obj in cursor.description]
    rows = cursor.fetchall()

    # results = [dict(zip(columns, row)) for row in rows] - this was needed to output data in true JSON format (zipped dictionary)

    cursor.close()

    return columns, rows


def query_all_tables(conn, last_updated_time):
    """
    Queries a list of whitelisted tables (in the totesys database) for new rows/record updated after the last_updated_time.
    Uses check_database_updates() to query individual tables.
    Returns a dictionary containing lists of dictionaries (JSON ready)
    """

    # list of approved tables we need check
    whitelisted_tables = [
        "counterparty",
        "currency",
        "department",
        "design",
        "staff",
        "sales_order",
        "address",
        "payment",
        "purchase_order",
        "payment_type",
        "transaction",
    ]

    results = {}  # empty dictionary to store the results

    for table in whitelisted_tables:
        try:
            columns, rows = check_database_updates(conn, table, last_updated_time)
            if rows:
                results[(table, tuple(columns))] = rows

        except Exception:
            print(f"Unable to query table: {table}")

    # the below approach orginally output data in true JSON format and was removed to implement the format required by transform_and_store_data function

    # for table in tables:
    #     try:
    #         returned_table_data = check_database_updates(conn, table, last_updated_time)
    #         if returned_table_data: # i.e. if returned data is not empty
    #             results[table] = returned_table_data

    #     except Exception as e:
    #         print(f"Unable to query table: {table}")

    return results
