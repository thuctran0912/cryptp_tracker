from kafka import KafkaProducer
import json
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv('config/config.env')

class CryptoKafkaProducer:
    def __init__(self):
        kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:29092').split(',')
        self.topic = os.getenv('KAFKA_TOPIC', 'crypto_prices')
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

    def send_message(self, message):
        self.producer.send(self.topic, value=message)
        print(f"Message sent to Kafka: {message}")

    def close(self):
        self.producer.close()