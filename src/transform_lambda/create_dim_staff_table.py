import pandas as pd
from copy import deepcopy


def transform_staff_and_department_tables(staff_dataframe, department_dataframe):
    staff_df_copy = deepcopy(staff_dataframe)
    department_df_copy = deepcopy(department_dataframe)
    dim_staff_col_name_list = [
        "staff_id",
        "first_name",
        "last_name",
        "department_name",
        "location",
        "email_address",
    ]
    merge_df = pd.merge(staff_df_copy, department_df_copy, on="department_id")
    dim_staff_df = merge_df.drop(columns=["department_id", "manager"])
    dim_staff_df_reordered = dim_staff_df[dim_staff_col_name_list]
    return dim_staff_df_reordered
