import json
import os
import base64
import boto3

sqs = boto3.client("sqs")
kinesis = boto3.client("kinesis")

# Read from Lambda environment variables (CORRECT way)
DLQ_URL = os.environ["DLQ_URL"]
STREAM_NAME = os.environ["KINESIS_STREAM_NAME"]

def lambda_handler(event, context):
    """
    Replay Lambda:
    - Reads failed batches from SQS DLQ
    - Extracts original Kinesis records
    - Re-publishes them to Kinesis
    """

    response = sqs.receive_message(
        QueueUrl=DLQ_URL,
        MaxNumberOfMessages=5,
        WaitTimeSeconds=2
    )

    messages = response.get("Messages", [])

    if not messages:
        print("No messages in DLQ")
        return {"status": "no_messages"}

    replayed = 0

    for msg in messages:
        body = json.loads(msg["Body"])

        # Lambda DLQ for Kinesis contains "records"
        records = body.get("records", [])

        for record in records:
            # Kinesis data is base64-encoded
            payload = json.loads(
                base64.b64decode(record["data"]).decode("utf-8")
            )

            kinesis.put_record(
                StreamName=STREAM_NAME,
                Data=json.dumps(payload),
                PartitionKey=payload.get("quake_id", "replay")
            )

            replayed += 1

        # Delete message ONLY after successful replay
        sqs.delete_message(
            QueueUrl=DLQ_URL,
            ReceiptHandle=msg["ReceiptHandle"]
        )

    return {
        "status": "replayed",
        "records_replayed": replayed
    }
