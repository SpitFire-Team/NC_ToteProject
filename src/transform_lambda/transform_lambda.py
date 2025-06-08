
def lambda_handler(event, context):
    
    date_time_str_last_ingestion = event[0]["last_ingested_str"]
    
    return event