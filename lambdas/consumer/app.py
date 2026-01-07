import json
import boto3
import os
import base64
import time
from datetime import datetime

# --------------------
# AWS clients
# --------------------
dynamodb = boto3.resource("dynamodb")
s3 = boto3.client("s3")
sns = boto3.client("sns")

# --------------------
# Environment variables
# --------------------
DDB_TABLE_NAME = os.environ["DDB_TABLE_NAME"]
SNS_TOPIC_ARN = os.environ["SNS_TOPIC_ARN"]
RAW_S3_PREFIX = os.environ["RAW_S3_PREFIX"]
SERVING_S3_PREFIX = os.environ["SERVING_S3_PREFIX"]

# Fixed S3 bucket
S3_BUCKET = "prathyusha-project"

# DynamoDB table
table = dynamodb.Table(DDB_TABLE_NAME)

# --------------------
# Lambda handler
# --------------------
def lambda_handler(event, context):

    for record in event["Records"]:

        payload = json.loads(
            base64.b64decode(record["kinesis"]["data"]).decode("utf-8")
        )

        feature = payload.get("feature", {})
        props = feature.get("properties", {})
        coords = feature.get("geometry", {}).get("coordinates", [None, None, None])

        quake_id = feature.get("id")
        mag = props.get("mag")

        if not quake_id:
            continue

        now = datetime.utcnow()
        epoch_ms = int(time.time() * 1000)

        # --------------------
        # 1) Write latest state to DynamoDB
        # --------------------
        table.put_item(
            Item={
                "quake_id": quake_id,
                "updated_at": epoch_ms,
                "payload": json.dumps(payload)
            }
        )

        # --------------------
        # 2) Write RAW event to S3
        # --------------------
        raw_key = (
            f"{RAW_S3_PREFIX}"
            f"dt={now.date()}/hour={now.hour}/"
            f"{quake_id}.json"
        )

        s3.put_object(
            Bucket=S3_BUCKET,
            Key=raw_key,
            Body=json.dumps(payload),
            ContentType="application/json"
        )

        # --------------------
        # 3) Write SERVING event to S3
        # --------------------
        serving_record = {
            "quake_id": quake_id,
            "mag": props.get("mag"),
            "place": props.get("place"),
            "title": props.get("title"),
            "event_time_ms": props.get("time"),
            "updated_ms": props.get("updated"),
            "lon": coords[0],
            "lat": coords[1],
            "depth_km": coords[2],
            "pipeline_type": "stream"
        }

        serving_key = (
            f"{SERVING_S3_PREFIX}"
            f"dt={now.date()}/hour={now.hour}/"
            f"{quake_id}.json"
        )

        s3.put_object(
            Bucket=S3_BUCKET,
            Key=serving_key,
            Body=json.dumps(serving_record),
            ContentType="application/json"
        )

        # --------------------
        # 4) Publish SNS alert
        # --------------------
        if mag is not None and mag >= 4.5:
            sns.publish(
                TopicArn=SNS_TOPIC_ARN,
                Subject=f"Earthquake Alert | M {mag}",
                Message=json.dumps({
                    "quake_id": quake_id,
                    "magnitude": mag,
                    "time": now.isoformat()
                })
            )

    return {"status": "ok"}
