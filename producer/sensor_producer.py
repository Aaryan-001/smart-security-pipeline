from kafka import KafkaProducer
import json
import time
import random
import datetime

# Connect to Redpanda (which speaks Kafka language)
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# This function creates fake sensor readings
def generate_sensor_data():
    return {
        "timestamp": datetime.datetime.now().isoformat(),
        "sensor_id": random.choice(["CAM_01", "CAM_02", "MOTION_01", "TEMP_01"]),
        "location": random.choice(["entrance", "parking", "server_room", "lobby"]),
        "motion_detected": random.choice([True, False]),
        "temperature": round(random.uniform(20.0, 45.0), 2),
        "alert_level": random.choice(["LOW", "MEDIUM", "HIGH"])
    }

print("🚀 Producer started — sending sensor data...")

# Keep sending data every 2 seconds forever
while True:
    data = generate_sensor_data()
    producer.send('security-events', value=data)  # sends to Redpanda
    print(f"Sent → {data}")
    time.sleep(2)