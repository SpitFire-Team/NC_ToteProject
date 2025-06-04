from datetime import datetime, timezone
"""
Import the necessary libraries to work with data and time.
""" 


def set_latest_updated_time(bucket, client): 
    """ 
    This function checks the contents of an s3 bucket.
    Determines the latest LastModified timestamp by looping through all objects in the s3 bucket. The most recent object is determined by comparing if any objects timestamp is more recent than the current latest timestamp. 
    Function returns the latest LastModified time.
    or returns a default unix time (1970) if no objects are found.

    """
    
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

def query_all_tables(last_updated_time):  
    # NOT COMPLETED
    """ 
    Check through a specific list of tables (in the totesys database) for new updates since the last_checked_time.
    Returns a list of results from each table query.
    This function runs the PostgreSQL query defined above by check_database_updates( ) inside the selected tables and inserts the output of the function above into a result variable that is then returned 
    """

    # list of all tables we need check
    tables = [
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

    results = []  # empty list where we will store the results

    for table in tables:
        results.append(
            check_database_updates(table, last_updated_time)
        )  # running the query through each table

    return results

def check_database_updates(conn, table, last_updated_time):

    # passes in the latest checked time POSSIBLY COMPLETED NEEDS TESTING
    """ 
    Vars: 
        - conn - DB connection from extraction_lambda
        - table - database table to query. Passed from query_all_tables function
        - last_updated_time - last_updated time from set_latest_updated_time

    This function checks the database for the latest update time.
    First, it converts the Pythonic time for PostgreSQL format and creates a cursor. 
    A following PostgreSQL statement is executed to compare if the last_updated column is greater than the last checked time. Results are retrieved inside a result variable which is returned and the cursor is closed.
    """
    
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
    ] # these are the relevent tables to check in the TOTESYS database 

    if table not in whitelisted_tables:
        return None  # change to an appropriate exception

    last_updated_time_str = (
        last_updated_time.isoformat()
    )  # converting time format for postgres
    cursor = conn.cursor()  # creating a cursor to query the db

    # change beow to remove f string to protect against SQL injection?
    cursor.execute(
        f"SELECT * FROM {table} WHERE last_updated > ?", (last_updated_time_str,)
    )
    results = cursor.fetchall()
    cursor.close()

    return results

# add exception for when table name is not a whitelisted name
# ^queries a single table, checking if the last_updated column is greater than the last checked time
