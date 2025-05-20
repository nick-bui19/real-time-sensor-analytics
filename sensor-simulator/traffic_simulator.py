import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer

# Connect to Kafka
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Simulate 3 locations
locations = ['intersection_A', 'intersection_B', 'intersection_C']

def simulate_traffic():
    while True:
        message = {
            "timestamp": datetime.utcnow().isoformat(),
            "location": random.choice(locations),
            "sensor_type": "traffic",
            "value": random.randint(20, 120)  # cars per minute
        }

        print(f"Sending: {message}")
        producer.send("traffic-topic", message)
        time.sleep(random.uniform(1.5, 3))  # random delay between messages

if __name__ == "__main__":
    simulate_traffic()