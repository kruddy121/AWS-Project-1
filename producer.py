import boto3
import json
import time
import random
from datetime import datetime, timezone

kinesis = boto3.client("kinesis", region_name="us-east-1")

STREAM_NAME = "streaming-security-events"

users = [
    "user_101",
    "user_102",
    "user_103",
    "user_104",
    "user_105"
]

devices = [
    "macOS",
    "Windows",
    "iPhone",
    "Android"
]

locations = [
    "Maryland",
    "Virginia",
    "Washington DC",
    "Pennsylvania",
    "New York"
]

event_types = [
    "login_success",
    "logout",
    "failed_login",
    "password_reset",
    "account_locked"
]

event_weights = [
    45,  # login_success
    25,  # logout
    18,  # failed_login
    8,   # password_reset
    4    # account_locked
]


def generate_ip():
    return f"192.168.{random.randint(0, 10)}.{random.randint(1, 254)}"


def get_risk_level(event_type):
    if event_type == "account_locked":
        return "high"
    elif event_type == "failed_login":
        return "medium"
    elif event_type == "password_reset":
        return "medium"
    else:
        return "low"


def generate_event():
    event_type = random.choices(
        event_types,
        weights=event_weights,
        k=1
    )[0]

    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "user_id": random.choice(users),
        "ip_address": generate_ip(),
        "event_type": event_type,
        "device": random.choice(devices),
        "location": random.choice(locations),
        "risk_level": get_risk_level(event_type)
    }


while True:
    event = generate_event()

    response = kinesis.put_record(
        StreamName=STREAM_NAME,
        Data=json.dumps(event),
        PartitionKey=event["user_id"]
    )

    print("Sent event:")
    print(json.dumps(event, indent=2))
    print("Shard ID:", response["ShardId"])
    print("-" * 40)

    time.sleep(5)