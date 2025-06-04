from datetime import datetime, timezone
"""
Import the necessary libraries to work with data and time.
""" 


def set_latest_updated_time(bucket, client): 
    """ 
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
    Results are returned as a list.
    The cursor is closed.
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

    # Nice to have - if table not in whitelisted_tables:
    #     return appropriate exception

    last_updated_time_str = (
        last_updated_time.isoformat()
    )  # converting time format for postgres

    cursor = conn.cursor()  # creating a cursor to query/ interact with the db

    # nice to have - change below to remove f string to protect against SQL injection?

    if table == "transaction":
        table_name = '"transaction"' # transaction is a reserved SQL keyword, so must be double quoted
    else:
        table_name = table

    cursor.execute(
        f"SELECT * FROM {table_name} WHERE last_updated > ?", (last_updated_time_str,)
    )
    
    # Approach for converting output from list of tuples to a list of dictionarie with column names and values:
        # columns (variable)
        # rows (variable (results=curser.fetchall()))
        # results = dictionary zip the two tuples together 
        # results = [dict(zip(columns, row)) for row in rows]

    results = cursor.fetchall()
    cursor.close()

    return results

def query_all_tables(conn, last_updated_time):  
    """ 
    Queries a list of whitelisted tables (in the totesys database) for new rows/record updated after the last_updated_time.
    Uses check_database_updates() to query individual tables.
    Returns a dict with table names as keys with new updates as result.
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

    results = {} # empty dictionary where we will store the results
    
    for table in tables:
        try:
            if len(check_database_updates(conn, table, last_updated_time)) != 0:
                results[table] = check_database_updates(conn, table, last_updated_time)
            else:
                pass
        except Exception as e:
            print(f"Unable to query given table: {table}")
     
        # logic for list output (may still be needed):
        # results.append(
        #     check_database_updates(conn, table, last_updated_time)
        # )  # running the query through each table

    return results