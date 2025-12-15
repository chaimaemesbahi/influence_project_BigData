# producer_kafka.py
import json, time, random
from confluent_kafka import Producer

p = Producer({'bootstrap.servers': 'localhost:9092'})

users = [f"user{i}" for i in range(1, 51)]
actions = ["LIKE", "SHARE", "COMMENT", "FOLLOW"]

def produce_one():
    u1 = random.choice(users)
    u2 = random.choice([u for u in users if u!=u1])
    msg = {
        "from": u1,
        "to": u2,
        "action": random.choice(actions),
        "timestamp": int(time.time())
    }
    p.produce("social_events", json.dumps(msg).encode('utf-8'))
    p.flush()

if __name__ == "__main__":
    while True:
        produce_one()
        time.sleep(0.2)  # 5 événements/sec
