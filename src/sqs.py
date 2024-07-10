from botocore.exceptions import ClientError
import json
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
import hashlib
import boto3
import os
from datetime import datetime

# Set up environment variables for dummy AWS credentials
os.environ["AWS_ACCESS_KEY_ID"] = "dummy_access_key"
os.environ["AWS_SECRET_ACCESS_KEY"] = "dummy_secret_key"

# Initialize SQS client with the local endpoint and a region
sqs = boto3.client("sqs", endpoint_url="http://localhost:4566",
                   region_name="us-east-1")

# Function to read messages from the SQS queue
def read_messages_from_sqs(max_messages: int = 10) -> list:
    messages = []
    try:
        # Request messages from the SQS queue
        response = sqs.receive_message(
            QueueUrl=SQS_QUEUE_URL,
            MaxNumberOfMessages=max_messages
        )
        # Check if the response contains messages
        if "Messages" in response:
            messages = response["Messages"]
            # Delete each message after it's read from the queue
            for message in messages:
                sqs.delete_message(
                    QueueUrl=SQS_QUEUE_URL,
                    ReceiptHandle=message["ReceiptHandle"]
                )
    except ClientError as e:
        # Print any errors that occur while reading messages from the queue
        print(f"Error reading messages from SQS: {e}")

    return messages
def mask_ip(ip: str) -> str:
    return hashlib.md5(ip.encode()).hexdigest()


def mask_device_id(device_id: str) -> str:
    return hashlib.md5(device_id.encode()).hexdigest()


def mask_data(data: Dict[str, Any]) -> Dict[str, Any]:
    masked_data = {
        "user_id": data["user_id"],
        "app_version": data["app_version"],
        "device_type": data["device_type"],
        "masked_ip": mask_ip(data["ip"]),
        "locale": data["locale"],
        "masked_device_id": mask_device_id(data["device_id"]),
        # Added this line to include the create_date field
        "create_date": datetime.utcnow().date()
    }
    return masked_data

# Function to mask PII data in the input dictionary
def mask_pii_data(data: Dict[str, Any]) -> Dict[str, Any]:
    # Extract values from the input data
    user_id = data.get("user_id")
    device_type = data.get("device_type")
    ip = data.get("ip")
    device_id = data.get("device_id", "unknown")
    locale = data.get("locale", "unknown")
    app_version = data.get("app_version")

    print(f"Raw data: {data}")

    # Check for required fields in the input data
    if user_id is None or device_type is None or ip is None or app_version is None:
        print(f"Invalid data: {data}")
        return None

    # Mask the IP and device_id using SHA-256 hashing
    masked_ip = hashlib.sha256(ip.encode("utf-8")).hexdigest()
    masked_device_id = hashlib.sha256(device_id.encode("utf-8")).hexdigest()

    # Create a dictionary with the masked data
    masked_data = {
        "user_id": user_id,
        "device_type": device_type,
        "masked_ip": masked_ip,
        "masked_device_id": masked_device_id,
        "locale": locale,
        "app_version": app_version,
    }

    print(f"Masked data: {masked_data}")

    # Return the masked data dictionary along with the current timestamp as create_date
    return {
        "user_id": user_id,
        "device_type": device_type,
        "masked_ip": masked_ip,
        "masked_device_id": masked_device_id,
        "locale": locale,
        "app_version": app_version,
        "create_date": datetime.now(),
    }

import psycopg2
from psycopg2.extras import execute_values

# Function to convert a list of 'Record' objects to a list of tuples
def convert_records_to_tuples(records):
    tuple_list = []
    for record in records:
        # Print the attributes of the 'Record' object
        print(f"Record: {record.__dict__}")
        # Add the record's attributes as a tuple to the list
        tuple_list.append((record.user_id, record.device_type, record.masked_ip,
                          record.masked_device_id, record.locale, record.app_version, record.create_date))
    return tuple_list

# Function to insert records into the 'user_logins' table in the Postgres database
def insert_to_postgres(connection_params, records):
    # SQL query to insert records into the 'user_logins' table
    insert_query = """
    INSERT INTO user_logins (
        user_id,
        device_type,
        masked_ip,
        masked_device_id,
        locale,
        app_version,
        create_date
    ) VALUES %s;
"""

    # Connect to the Postgres database
    with psycopg2.connect(**connection_params) as conn:
        with conn.cursor() as cur:
            # Convert the Record objects to tuples
            converted_records = convert_records_to_tuples(records)

            # Print the records being inserted
            print(f"Inserting records: {converted_records}")
            # Execute the insert query with the records as tuples
            execute_values(cur, insert_query, converted_records)
        # Commit the transaction
        conn.commit()

        # Define a dataclass for storing the processed record information
@dataclass
class Record:
    user_id: str
    device_type: str
    masked_ip: str
    masked_device_id: str
    locale: str
    app_version: str
    create_date: str

# Create a Record object from the masked data dictionary
def create_record(masked_data: Dict[str, Any]) -> Optional[Record]:
    if masked_data is None:
        return None

    # Convert the app version to an integer by removing the dots
    app_version = int(masked_data["app_version"].replace(".", ""))

    # Return a Record object with the masked data and converted app version
    return Record(
        user_id=masked_data["user_id"],
        device_type=masked_data["device_type"],
        masked_ip=masked_data["masked_ip"],
        masked_device_id=masked_data["masked_device_id"],
        locale=masked_data["locale"],
        app_version=app_version,
        create_date=masked_data["create_date"],
    )

# Process the messages read from the SQS queue
def process_messages(messages: List[Dict[str, Any]]) -> List[Record]:
    records = []

    # Iterate through each message and extract the data
    for message in messages:
        data = json.loads(message["Body"])
        print(f"Loaded message data: {data}")
        # Mask the PII data in the message
        masked_data = mask_pii_data(data)
        # Create a Record object from the masked data
        record = create_record(masked_data)

        if record is not None:
            records.append(record)

    return records

# Main function for running the ETL process
def main():
    # Read messages from the SQS queue
    messages = read_messages_from_sqs()
    print(f"Messages: {messages}")

    # Process the messages and create a list of Record objects
    records = process_messages(messages)
    print(f"Records: {records}")

    # Insert the records into the PostgreSQL database
    insert_to_postgres(POSTGRES_CONNECTION, records)


# Run the main function when the script is executed
if __name__ == "__main__":
    main()