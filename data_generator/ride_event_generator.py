import json
import random
import time
from datetime import datetime, timedelta
from uuid import uuid4

from faker import Faker
from kafka import KafkaProducer

fake = Faker()

KAFKA_BROKER = "localhost:9092"   # sends events INTO Kafka
TOPIC = "ride_events"             # stream name


def create_ride_event():
    """
    This function builds ONE realistic ride event.
    - Every event has a unique ride_id (like Uber)
    - Random start/end time, distance, surge price
    - Feels like real mobility data
    """
    ride_id = str(uuid4())
    passenger_id = str(uuid4())
    driver_id = str(uuid4())

    start_time = datetime.utcnow() - timedelta(
        seconds=random.randint(0, 300)
    )
    end_time = start_time + timedelta(minutes=random.randint(5, 30))

    # Fake GPS within a city range
    start_lat = 41.0 + random.uniform(-0.05, 0.05)
    start_lon = 29.0 + random.uniform(-0.05, 0.05)
    end_lat = 41.0 + random.uniform(-0.05, 0.05)
    end_lon = 29.0 + random.uniform(-0.05, 0.05)

    distance_km = random.uniform(2, 20)
    price = round((10 + distance_km * 2) * random.choice([1,1,1,1.2,1.5]), 2)

    event = {
        "ride_id": ride_id,
        "passenger_id": passenger_id,
        "driver_id": driver_id,
        "status": random.choice(["requested","accepted","in_progress","completed"]),
        "distance_km": round(distance_km,2),
        "price": price,
        "start_time": start_time.isoformat(),
        "end_time": end_time.isoformat(),
        "created_at": datetime.utcnow().isoformat(),
    }
    return event


def main():
    """
    Continuously push events to Kafka.
    - Spark Streaming will consume these later
    - This is your live data source
    """
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    print(f"Streaming events to '{TOPIC}'... (CTRL+C to stop)")
    while True:
        event = create_ride_event()
        producer.send(TOPIC, value=event)
        print("EVENT:", event["start_time"], event["status"], event["price"])
        time.sleep(1)  # 1 event/second → like a busy app


if __name__ == "__main__":
    main()
