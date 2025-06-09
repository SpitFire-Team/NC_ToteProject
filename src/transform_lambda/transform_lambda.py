def lambda_handler(event, context):

    # date_time_str_last_ingestion = event[0]["last_ingested_str"] # date_time_str coming from step function and extraction lambda

    date_time_str_last_ingestion = (
        "09-06-2025_07:46"  # example date time string for testing
    )

    return event
