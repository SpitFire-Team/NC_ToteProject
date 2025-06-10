from src.load_lambda.warehouse_connection import wh_connection

conn = wh_connection()

def load_to_warehouse(conn):
    """
    This function will load transformed data into appropriate tables in
    a pre-prepared data warehouse
    Inputs: 
    A list of dictionaries in the format [{table_name: dataframe}, {table_name_2: dataframe_2}]
    """
    cursor = conn.cursor()  # creating a cursor to query/ interact with the db

    #Attempt to add data into the table.
    create_table = cursor.execute(
    """INSERT INTO dim_staff 
    (staff_id,
    first_name,
    last_name,
    department_name, 
    location, 
    email_address)
    VALUES 
    (888,
    'Test',
    'Test',
    'Test',
    'Test',
    'test@test.com');"""
    )

    #Maintain to check if we are getting something inside the table. 
    results = cursor.execute(
    "SELECT * FROM dim_staff;"
    )
    test_result = cursor.fetchall()
    print(test_result)

    cursor.close()
    return results

load_to_warehouse(conn)


