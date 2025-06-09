import pytest
import pandas as pd


from src.transform_lambda.create_dim_staff_table import transform_staff_and_department_tables

@pytest.fixture 
def staff_df():
    column_name_list = ["staff_id","first_name", "last_name", "department_id", "email_address"]
    staff_df= pd.DataFrame(columns=column_name_list)
    for i in range(10):
        data= {column_name_list[0]: i, 
               column_name_list[1]: f"first_name_{i}",
               column_name_list[2]: f"last_name_{i}",
               column_name_list[3]: i,
               column_name_list[4]: f"email_address{i}"
               }
        data_rows_to_add_df=pd.DataFrame(data, index= [i])
        staff_df= pd.concat([staff_df, data_rows_to_add_df], ignore_index=True)
       
    return staff_df  
    



@pytest.fixture 
def department_df():
    department_name_list = ["department_id", "department_name", "location", "manager"]
    department_df= pd.DataFrame(columns=department_name_list)
    for i in range(10):
        data= {department_name_list[0]: i, 
               department_name_list[1]: f"department_name_{i}",
               department_name_list[2]: f"location_{i}",
               department_name_list[3]: f"manager_{i}",
               }
        
        data_rows_to_add_df=pd.DataFrame(data, index= [i])
        department_df= pd.concat([department_df, data_rows_to_add_df], ignore_index=True)
    return department_df

@pytest.fixture 
def df_column_name():
    column_names= ["staff_id", "first_name", "last_name", "department_name","location", "email_address"]
    return column_names



class TestTransformStaffAndDepartmentTables:
    def test_function_returns_dataframe(self,staff_df, department_df):
        data_frame= transform_staff_and_department_tables(staff_df, department_df)
        assert type(data_frame) is type(staff_df)

    def test_dataframe_has_correct_names(self,staff_df, department_df,df_column_name):
        data_frame= transform_staff_and_department_tables(staff_df, department_df)
        assert data_frame.columns.tolist() == df_column_name


    def test_function_pulls_required_data_from_staff_table(self, staff_df, department_df):
        data_frame= transform_staff_and_department_tables(staff_df, department_df)
        assert 1==2