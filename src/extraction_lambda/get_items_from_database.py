from datetime import datetime, timezone
"""
* Import the necessary libraries to work with data and time.""" 


def set_latest_updated_time(bucket, client):  # COMPLETED!!!!!
    """ set_latest_updated_time. vars: bucket, client

    This function checks the S3 buckets for objects. If no objects exist, it returns the default Unix time. It finds the most recent object between the objects looping through all the objects and compares if it has any later time than the current latest. It returns the latest time."""
    
    s3_files = client.list_objects(Bucket=bucket)  # check the s3 bucket for objects

    if "Contents" not in s3_files:
        return datetime(
            1970, 1, 1, 0, 0, 0, 0, tzinfo=timezone.utc
        )  # if theres no objects set the time to 1970

    last_updated = s3_files["Contents"][0]["LastModified"].astimezone(
        timezone.utc
    )  # if there is an item set the curret time to the earliest object

    for object in s3_files["Contents"]:
        if (
            object["LastModified"].astimezone(timezone.utc) > last_updated
        ):  # loop through the objects checking if any have a later date than the current latest time
            last_updated = object["LastModified"]

    return last_updated.astimezone(timezone.utc)


def check_database_updates(
    conn, table, last_checked_time
):  # passes in the latest checked time POSSIBLY COMPLETED NEEDS TESTING
    """ check_database_updates. vars: conn, table, last_checked_time

        This function checks the database for the latest update time.
        First, it converts the Pythonic time for PostgreSQL format and creates a cursor. A following PostgreSQL statement is executed to compare if the last_updated column is greater than the last checked time. Results are retrieved inside a result variable which is returned and the cursor is closed."""
    
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

    if table not in whitelisted_tables:
        return None  # change to an appropriate exception

    last_checked_time_str = (
        last_checked_time.isoformat()
    )  # converting time format for postgres
    cursor = conn.cursor()  # creating a cursor to query the db

    cursor.execute(
        f"SELECT * FROM {table} WHERE last_updated > ?", (last_checked_time_str,)
    )
    results = cursor.fetchall()
    cursor.close()

    return results


# ^queries a single table, checking if the last_updated column is greater than the last checked time


def query_all_databases(cursor, last_checked_time):  
    # NOT COMPLETED
    """ query_all_databases. vars: cursor, last_checked_time
        Interact through a specific list of tables inside the database and check each for the most recent updates. This function runs the PostgreSQL query defined above by check_database_updates( ) inside the selected tables and inserts the output of the function above into a result variable that is then returned """

    tables = [
        "counterparty",
        "currency",
        "department",
        "design",
        "staff",
        "sales_order",
        "address",
        "payment",
        "purchase_order",  # list of all tables we need check
        "payment_type",
        "transaction",
    ]

    results = []  # empty list where we will store the results

    for table in tables:
        results.append(
            check_database_updates(cursor, table, last_checked_time)
        )  # running the query through each table

    return results