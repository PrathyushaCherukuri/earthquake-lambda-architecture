import os
import json
import base64
import time
import uuid
import boto3
from datetime import datetime, timezone
from botocore.exceptions import ClientError

# --------------------
# AWS clients
# --------------------
s3 = boto3.client("s3")
ddb = boto3.client("dynamodb")
sns = boto3.client("sns")
sqs = boto3.client("sqs")

# --------------------
# Environment variables
# --------------------
RAW_BUCKET = os.environ["RAW_BUCKET"]
RAW_PREFIX = os.environ.get("RAW_PREFIX", "raw/earthquakes")

SERVING_PREFIX = os.environ.get(
    "SERVING_PREFIX", "serving/earthquakes_stream"
)

DDB_TABLE = os.environ["DDB_TABLE"]
SNS_TOPIC_ARN = os.environ["SNS_TOPIC_ARN"]
DLQ_URL = os.environ["DLQ_URL"]

ALERT_MAG_THRESHOLD = float(os.environ.get("ALERT_MAG_THRESHOLD", 4.0))
MIN_VALID_MAG = float(os.environ.get("MIN_VALID_MAG", 0.5))

# --------------------
# Helper functions
# --------------------
def _safe_int(x, default=0):
    try:
        return int(x)
    except Exception:
        return default


def _safe_float(x, default=0.0):
    try:
        return float(x)
    except Exception:
        return default


def send_to_dlq(reason, record):
    sqs.send_message(
        QueueUrl=DLQ_URL,
        MessageBody=json.dumps({
            "reason": reason,
            "record": record
        })
    )

# --------------------
# Core processing logic
# --------------------
def process_record(obj, allow_alerts=True):
    """
    Core business logic.
    Reused for:
    - Streaming (Kinesis)
    - Replay
    """

    feature = obj.get("feature", {})
    props = feature.get("properties", {})
    geom = feature.get("geometry", {})
    coords = geom.get("coordinates", [None, None, None])

    quake_id = feature.get("id")
    if not quake_id:
        send_to_dlq("missing_quake_id", obj)
        return "dlq", None

    mag = _safe_float(props.get("mag"))
    if mag < MIN_VALID_MAG:
        send_to_dlq("magnitude_below_threshold", obj)
        return "dlq", None

    updated_ms = _safe_int(props.get("updated"))
    event_time_ms = _safe_int(props.get("time"))

    try:
        ddb.update_item(
            TableName=DDB_TABLE,
            Key={"quake_id": {"S": quake_id}},
            UpdateExpression="""
                SET #updated = :u,
                    event_time_ms = :t,
                    mag = :mag,
                    #place = :place,
                    #title = :title,
                    #url = :url,
                    lon = :lon,
                    lat = :lat,
                    depth_km = :dep
            """,
            ConditionExpression="attribute_not_exists(#updated) OR #updated < :u",
            ExpressionAttributeNames={
                "#updated": "updated",
                "#place": "place",
                "#title": "title",
                "#url": "url",
            },
            ExpressionAttributeValues={
                ":u": {"N": str(updated_ms)},
                ":t": {"N": str(event_time_ms)},
                ":mag": {"N": str(mag)},
                ":place": {"S": str(props.get("place", ""))},
                ":title": {"S": str(props.get("title", ""))},
                ":url": {"S": str(props.get("url", ""))},
                ":lon": {"N": str(coords[0] or 0)},
                ":lat": {"N": str(coords[1] or 0)},
                ":dep": {"N": str(coords[2] or 0)},
            },
        )

        if allow_alerts and mag >= ALERT_MAG_THRESHOLD:
            sns.publish(
                TopicArn=SNS_TOPIC_ARN,
                Subject=f"Earthquake Alert | M {mag}",
                Message=(
                    f"🚨 Earthquake Alert 🚨\n\n"
                    f"Magnitude: {mag}\n"
                    f"Location: {props.get('place')}\n"
                    f"Event Time (ms): {event_time_ms}\n"
                    f"URL: {props.get('url')}"
                ),
            )

        return "processed", flatten_for_serving(obj)

    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            return "skipped", None

        send_to_dlq("ddb_error", obj)
        return "dlq", None

# --------------------
# Flatten record for serving layer
# --------------------
def flatten_for_serving(obj):
    feature = obj["feature"]
    props = feature["properties"]
    coords = feature["geometry"]["coordinates"]

    return {
        "quake_id": feature["id"],
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

# --------------------
# Streaming entry point
# --------------------
def lambda_handler(event, context):
    records = event.get("Records", [])
    if not records:
        return {"status": "ok", "message": "No records"}

    raw_events = []
    serving_events = []

    stats = {
        "processed": 0,
        "skipped": 0,
        "dlq": 0
    }

    for r in records:
        try:
            raw = base64.b64decode(r["kinesis"]["data"]).decode("utf-8")
            obj = json.loads(raw)
            raw_events.append(obj)

            result, serving_row = process_record(obj, allow_alerts=True)
            stats[result] += 1

            if serving_row:
                serving_events.append(serving_row)

        except Exception:
            send_to_dlq("parse_error", r)
            stats["dlq"] += 1

    now = datetime.now(timezone.utc)

    # --------------------
    # Write RAW events (immutable log)
    # --------------------
    raw_key = (
        f"{RAW_PREFIX}/dt={now:%Y-%m-%d}/hour={now:%H}/"
        f"batch_{int(time.time())}_{uuid.uuid4().hex}.jsonl"
    )

    s3.put_object(
        Bucket=RAW_BUCKET,
        Key=raw_key,
        Body="\n".join(json.dumps(x) for x in raw_events).encode("utf-8"),
        ContentType="application/json",
    )

    # --------------------
    # Write STREAM serving output
    # --------------------
    if serving_events:
        serving_key = (
            f"{SERVING_PREFIX}/dt={now:%Y-%m-%d}/hour={now:%H}/"
            f"stream_{int(time.time())}_{uuid.uuid4().hex}.jsonl"
        )

        s3.put_object(
            Bucket=RAW_BUCKET,
            Key=serving_key,
            Body="\n".join(json.dumps(x) for x in serving_events).encode("utf-8"),
            ContentType="application/json",
        )

    return {
        "status": "ok",
        "records": len(records),
        **stats,
        "raw_s3_key": raw_key,
        "stream_s3_written": len(serving_events)
    }
