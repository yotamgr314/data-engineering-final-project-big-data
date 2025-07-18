import json, os, time, random
from datetime import datetime, timedelta
from faker import Faker
from kafka import KafkaProducer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC      = os.getenv("KAFKA_TOPIC", "raw_users")

fake  = Faker()
prod  = KafkaProducer(bootstrap_servers=BOOTSTRAP,
                      value_serializer=lambda v: json.dumps(v).encode("utf-8"))

def gen_event():
    now = datetime.utcnow()
    # מדמה latency אקראית 0‑48 שעות
    event_time = now - timedelta(hours=random.randint(0, 48))
    return {
        "event_ts": event_time.isoformat(timespec="seconds") + "Z",
        "user_id" : fake.uuid4(),
        "name"    : fake.name(),
        "country" : fake.country_code(representation="alpha-2")
    }

if __name__ == "__main__":
    while True:
        prod.send(TOPIC, gen_event())
        prod.flush()
        time.sleep(0.3)
