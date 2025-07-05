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

def create_merged_datastructure(tables_for_merge, star_schema_ref):
    #[{"table1_name": df1}, {"table2_name": df2}]

    return_data_structure = [{"dim_counterparty": [], "col_list":star_schema_ref["dim_counterparty"]},
                             {"dim_staff" : [], "col_list":star_schema_ref["dim_staff"]}]
    
    for table_name in tables_for_merge.keys():
        if table_name == "address" or table_name  == "counterparty":
            return_data_structure[0]["dim_counterparty"].append(list(table_name.values())[0])
        elif table_name == "staff" or table_name == "department":
            return_data_structure[1]["dim_staff"].append(list(table_name.values())[0])
        
    return return_data_structure
