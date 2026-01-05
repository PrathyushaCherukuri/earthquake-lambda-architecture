import json
import boto3

sqs = boto3.client("sqs")
kinesis = boto3.client("kinesis")

DLQ_URL = "<TO_BE_SET_LATER>"
STREAM_NAME = "<TO_BE_SET_LATER>"

def lambda_handler(event, context):
    """
    Replay Lambda:
    - Reads failed messages from DLQ
    - Re-publishes them to Kinesis
    """

    messages = sqs.receive_message(
        QueueUrl=DLQ_URL,
        MaxNumberOfMessages=10
    ).get("Messages", [])

    if not messages:
        return {"status": "no_messages"}

    for msg in messages:
        body = json.loads(msg["Body"])

        kinesis.put_record(
            StreamName=STREAM_NAME,
            Data=json.dumps(body),
            PartitionKey=body.get("quake_id", "replay")
        )

        sqs.delete_message(
            QueueUrl=DLQ_URL,
            ReceiptHandle=msg["ReceiptHandle"]
        )

    return {
        "status": "replayed",
        "count": len(messages)
    }
