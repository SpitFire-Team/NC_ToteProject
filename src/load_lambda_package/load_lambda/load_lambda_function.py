def lambda_handler(event, context):
    
    date_time_str_last_ingestion = event[0]["last_ingested_str"]
    
    if date_time_str_last_ingestion == "no data ingested":
        return [{"last_ingested_str": "no data ingested"}]
    
    return event
