from kafka import KafkaProducer
import json
import time
import uuid
from datetime import datetime, timedelta

# Kafka configuration
topic = 'flights'
bootstrap_servers = ['kafka:9092']
producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Generate sample flight events
def generate_event():
    return {
        'flight_id': str(uuid.uuid4()),
        'route_id': 'route-' + str(uuid.uuid4())[:8],
        'scheduled_departure': (datetime.utcnow() + timedelta(hours=1)).isoformat(),
        'scheduled_arrival': (datetime.utcnow() + timedelta(hours=2)).isoformat(),
        'max_passenger_capacity': 180
    }

if __name__ == '__main__':
    while True:
        event = generate_event()
        producer.send(topic, event)
        print(f"Sent event: {event}")
        time.sleep(5)