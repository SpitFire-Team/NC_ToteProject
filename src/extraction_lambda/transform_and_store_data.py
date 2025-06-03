import json
import io
from datetime import datetime


def data_transform(
    results,
):  # taking in results array from database query NOT COMPLETED
    buffer = io.StringIO()  # creating a buffer to store json

    for result in results:
        buffer.write(
            json.dumps(result) + "\n"
        )  # looping through each item in results and writing it to the buffer

    return buffer


def input_into_s3(
    buffer, client
):  # taking in the buffer and a boto client NOT COMPLETED
    timestamp = datetime.now()  # getting the current date and time

    client.put_object(
        Bucket="extract_bucket",
        Key=timestamp,  # storing the buffer in the bucket using the timestamp as a name
        Body=buffer.getvalue(),
    )
