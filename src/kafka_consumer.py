from kafka import KafkaConsumer
import json
import os
from datetime import datetime
from dotenv import load_dotenv
from .snowflake_handler import SnowflakeHandler

load_dotenv('config/config.env')

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:29092').split(',')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'crypto_prices')

class CryptoKafkaConsumer:
    def __init__(self, process_message):
        self.consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        self.process_message = process_message
        self.snowflake_handler = SnowflakeHandler()
        self.snowflake_handler.create_table_if_not_exists()

    def consume_messages(self):
        for message in self.consumer:
            self.process_message(message.value)
            self.store_in_snowflake(message.value)

    def store_in_snowflake(self, data):
        timestamp = datetime.fromtimestamp(int(data['t'])/1000)
        self.snowflake_handler.insert_crypto_price(timestamp, data['s'], data['p'])

    def close(self):
        self.consumer.close()
        self.snowflake_handler.close()

def main():
    def process_message(message):
        print(f"Processed message: {message}")

    consumer = CryptoKafkaConsumer(process_message)
    try:
        consumer.consume_messages()
    except KeyboardInterrupt:
        print("Shutting down consumer...")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()