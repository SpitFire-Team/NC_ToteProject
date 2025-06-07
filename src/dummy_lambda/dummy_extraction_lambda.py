
from src.utils.file_utils import get_path_date_time_string
from datetime import datetime


def lambda_handler(event, context):
    
    datetime_London = datetime.now()
    date_time_str = datetime_London.strftime("%d/%m/%Y_%H:%M")
    date_time_str = date_time_str.replace("/", "-")  # remove /, used for file path
     


    last_ingested_data = [{"last_ingested_str": date_time_str}]
    return last_ingested_data
    