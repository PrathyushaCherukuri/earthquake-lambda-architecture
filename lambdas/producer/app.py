import json
import os
import time
import urllib.request
import boto3

# --------------------
# AWS client
# --------------------
kinesis = boto3.client("kinesis")

# --------------------
# Environment variables
# --------------------
STREAM_NAME = os.environ["KINESIS_STREAM_NAME"]

USGS_URL = os.environ.get(
    "USGS_URL",
    "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson"
)

# --------------------
# Fetch data from USGS
# --------------------
def fetch_usgs():
    with urllib.request.urlopen(USGS_URL, timeout=10) as resp:
        return json.loads(resp.read().decode("utf-8"))

# --------------------
# Lambda entry point
# --------------------
def lambda_handler(event, context):
    data = fetch_usgs()
    features = data.get("features", [])

    if not features:
        return {
            "status": "ok",
            "message": "No earthquake data found",
            "records_sent": 0
        }

    records = []

    for f in features:
        quake_id = f.get("id", "unknown")

        payload = {
            "source": "usgs",
            "ingested_at_epoch_ms": int(time.time() * 1000),
            "feature": f
        }

        records.append({
            "Data": json.dumps(payload).encode("utf-8"),
            "PartitionKey": quake_id
        })

    sent = 0
    failed = 0

    # Kinesis PutRecords max = 500 records per call
    for i in range(0, len(records), 500):
        batch = records[i:i + 500]

        response = kinesis.put_records(
            StreamName=STREAM_NAME,
            Records=batch
        )

        sent += len(batch)
        failed += response.get("FailedRecordCount", 0)

    return {
        "status": "ok",
        "records_sent": sent,
        "failed": failed
    }
