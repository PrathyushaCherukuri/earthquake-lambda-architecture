import json
import os
import boto3

sqs = boto3.client("sqs")
kinesis = boto3.client("kinesis")

DLQ_URL = os.environ["DLQ_URL"]
STREAM_NAME = os.environ["KINESIS_STREAM_NAME"]

def lambda_handler(event, context):
    """
    Replay Lambda:
    - Reads messages from SQS DLQ
    - Replays ONLY eligible messages back to Kinesis
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
    skipped = 0

    for msg in messages:
        body = json.loads(msg["Body"])

        reason = body.get("reason")
        payload = body.get("payload")

        # --------------------
        # DO NOT replay noise (BY DESIGN)
        # --------------------
        if reason == "low_magnitude_noise":
            print(f"Skipping replay for low magnitude quake")
            skipped += 1

            # Delete message (we intentionally drop it)
            sqs.delete_message(
                QueueUrl=DLQ_URL,
                ReceiptHandle=msg["ReceiptHandle"]
            )
            continue

        # --------------------
        # Replay valid payload
        # --------------------
        if payload:
            kinesis.put_record(
                StreamName=STREAM_NAME,
                Data=json.dumps(payload),
                PartitionKey=payload.get("feature", {}).get("id", "replay")
            )
            replayed += 1

        # Delete only after successful replay
        sqs.delete_message(
            QueueUrl=DLQ_URL,
            ReceiptHandle=msg["ReceiptHandle"]
        )

    return {
        "status": "completed",
        "records_replayed": replayed,
        "records_skipped": skipped
    }
