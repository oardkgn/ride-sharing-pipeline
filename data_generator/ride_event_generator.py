from faker import Faker
import json
import random
import uuid
from datetime import datetime, timedelta
from kafka import KafkaProducer
import time

fake = Faker()

# --- NEW: driver and passenger pools ---
DRIVER_POOL = [str(uuid.uuid4()) for _ in range(200)]
PASSENGER_POOL = [str(uuid.uuid4()) for _ in range(1000)]

STATUSES = ["requested", "accepted", "completed", "cancelled"]

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

def generate_event():
    start_time = datetime.now() - timedelta(minutes=random.randint(1, 60))
    end_time = start_time + timedelta(minutes=random.randint(3, 30))

    ride = {
        "ride_id": str(uuid.uuid4()),
        "driver_id": random.choice(DRIVER_POOL),       # reused drivers
        "passenger_id": random.choice(PASSENGER_POOL), # reused passengers
        "status": random.choice(STATUSES),
        "distance_km": round(random.uniform(1, 15), 2),
        "price": round(random.uniform(5, 40), 2),
        "start_time": start_time.isoformat(),
        "end_time": end_time.isoformat(),
        "created_at": datetime.now().isoformat(),
    }
    return ride

while True:
    event = generate_event()
    producer.send("ride_events", value=event)
    print("Sent:", event)
    time.sleep(1)
