from kafka import KafkaConsumer
from dotenv import load_dotenv
import boto3
import pandas as pd
import json
import io
import os
from datetime import datetime

# Load environment variables from .env file
load_dotenv()

# Connect to Redpanda
consumer = KafkaConsumer(
    'security-events',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='earliest',
    group_id='security-group'
)

# Connect to AWS S3 using environment variables
s3 = boto3.client(
    's3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY'),
    aws_secret_access_key=os.getenv('AWS_SECRET_KEY'),
    region_name='ap-south-1'
)

BUCKET_NAME = 'smart-security-datalake'
buffer = []
BATCH_SIZE = 10

def upload_to_s3(records):
    df = pd.DataFrame(records)
    parquet_buffer = io.BytesIO()
    df.to_parquet(parquet_buffer, index=False)
    parquet_buffer.seek(0)

    filename = f"processed/alerts_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=filename,
        Body=parquet_buffer.getvalue()
    )
    print(f"✅ Uploaded {len(records)} alerts → s3://{BUCKET_NAME}/{filename}")

print("👂 Consumer started — listening for events...")

# Listen for messages forever
for message in consumer:
    event = message.value
    print(f"Received → {event['sensor_id']} at {event['location']} | Alert: {event['alert_level']}")

    if event['alert_level'] == 'HIGH':
        buffer.append(event)
        print(f"  🔴 HIGH alert added to buffer ({len(buffer)}/{BATCH_SIZE})")

        if len(buffer) >= BATCH_SIZE:
            upload_to_s3(buffer)
            buffer.clear()