import pandas as pd 


def transform_staff():
    # Load raw data into a DataFrame
    raw_staff_data = [ ]

df = pd.DataFrame(raw_staff_data)

#Renaming the columns 

df.rename(columns={
    "id": "staff_id",
    "department_id": "department_id"  
}, inplace=True)


#  Create full_name field

df["full_name"] = df["first_name"] + " " + df["last_name"]

# Remove sensitive or unused columns like password.

df.drop (columns=["password"], inplace=True)

def transform_currency():
    pass 