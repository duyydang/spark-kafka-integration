# Test kafka send message
from kafka import KafkaProducer
import json
import time

kafka_broker = ['localhost:9091', 'localhost:9092', 'localhost:9093']
kafka_topic = 'test-url-1204'

producer = KafkaProducer(bootstrap_servers=kafka_broker,
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))

# Send messages
messages = [
    {"sslsni": "value1", "subscriberid": "id1",
        "hour_key": 1, "count": 10, "up": 5, "down": 5},
    {"sslsni": "value2", "subscriberid": "id2",
        "hour_key": 2, "count": 15, "up": 8, "down": 7}
]

for message in messages:
    producer.send(kafka_topic, value=message)
    print(f"Sent message: {message}")
    time.sleep(1)  # Optionally wait between messages

# Flush and close producer
producer.flush()
producer.close()
