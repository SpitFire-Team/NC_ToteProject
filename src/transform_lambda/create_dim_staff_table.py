import pandas as pd 


def transform_staff_and_department_tables(staff_dataframe, department_dataframe):

# CREATE Dim_staff_dataframe with the following columns (staff_id, first_name, last_name, department_name,location and email_address)
# From department_dataframe populate the following columns (department_name, location)

    #return new_dataframe
    dim_staff_col_name_list = ["staff_id", "first_name", "last_name", "department_name","location", "email_address"]
    dim_staff_df= pd.DataFrame(columns=dim_staff_col_name_list)
    # From staff_dataframe populate the following columns (staff_id, first_name, last_name, email_address)
    merge_df= pd.merge(staff_dataframe,department_dataframe, on= "department_id")
    print(merge_df)

    return dim_staff_df




