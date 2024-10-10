import sys
import os
from datetime import datetime
import threading

print("Current working directory:", os.getcwd())
print("__file__:", __file__)

# Add the project root directory to the Python path
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, project_root)

print("Python path:")
for path in sys.path:
    print(path)

try:
    from src.websocket_client import FinnhubWebsocketClient
    from src.kafka_producer import CryptoKafkaProducer
    from src.kafka_consumer import CryptoKafkaConsumer
    from src.snowflake_handler import SnowflakeHandler
    print("All modules imported successfully")
except ImportError as e:
    print(f"Failed to import a module: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

def run_websocket_client(producer):
    def on_message(message):
        producer.send_message(message)

    client = FinnhubWebsocketClient(on_message)
    client.connect()
    client.run()

def run_kafka_consumer(snowflake_handler):
    def process_message(message):
        print(f"Processed message: {message}")
        timestamp = datetime.fromtimestamp(int(message['t'])/1000)
        symbol = message['s']
        price = message['p']
        snowflake_handler.insert_crypto_price(timestamp, symbol, price)

    consumer = CryptoKafkaConsumer(process_message)
    consumer.consume_messages()

if __name__ == "__main__":
    print("Starting main execution")
    try:
        producer = CryptoKafkaProducer()
        snowflake_handler = SnowflakeHandler()
        
        # Create the table if it doesn't exist
        snowflake_handler.create_table_if_not_exists()

        websocket_thread = threading.Thread(target=run_websocket_client, args=(producer,))
        consumer_thread = threading.Thread(target=run_kafka_consumer, args=(snowflake_handler,))

        websocket_thread.start()
        consumer_thread.start()

        websocket_thread.join()
        consumer_thread.join()
    except Exception as e:
        print(f"An error occurred during execution: {e}")
        import traceback
        traceback.print_exc()
    finally:
        print("Shutting down...")
        try:
            producer.close()
            snowflake_handler.close()
        except Exception as e:
            print(f"Error during shutdown: {e}")