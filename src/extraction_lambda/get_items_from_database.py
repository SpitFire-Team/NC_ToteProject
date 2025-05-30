from datetime import datetime, timezone

def set_latest_updated_time(bucket, client): #COMPLETED!!!!!
    s3_files = client.list_objects(Bucket = bucket) #check the s3 bucket for objects
   
    if 'Contents' not in s3_files:
        return datetime(1970, 1, 1, 0, 0, 0, 0, tzinfo=timezone.utc) #if theres no objects set the time to 1970
    
    last_updated = s3_files["Contents"][0]["LastModified"].astimezone(timezone.utc) #if there is an item set the curret time to the earliest object

    for object in s3_files["Contents"]:
        if object["LastModified"].astimezone(timezone.utc) > last_updated: #loop through the objects checking if any have a later date than the current latest time
            last_updated = object["LastModified"]

    return last_updated.astimezone(timezone.utc)

def check_database_updates(conn, table, last_checked_time): #passes in the latest checked time POSSIBLY COMPLETED NEEDS TESTING
    #list of whitelisted tables for security??? 
    last_checked_time_str = last_checked_time.isoformat()#converting time format for postgres
    cursor = conn.cursor()#creating a cursor to query the db

    cursor.execute(f"SELECT * FROM {table} WHERE last_updated > ?", (last_checked_time_str,))
    results = cursor.fetchall()
    cursor.close()

    return results
#^queries a single table, checking if the last_updated column is greater than the last checked time 

def query_all_databases(cursor, last_checked_time): #NOT COMPLETED
    tables = ["counterparty", "currency", "department", "design", "staff",
              "sales_order", "address","payment", "purchase_order", #list of all tables we need check
              "payment_type", "transaction"]
    
    results = [] #empty list where we will store the results

    for table in tables:
        results.append(check_database_updates(cursor, table, last_checked_time)) #running the query through each table

    return results