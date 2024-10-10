import sys
import os
from datetime import datetime

# Add the project root directory to the Python path
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, project_root)

from src.snowflake_handler import SnowflakeHandler

def test_snowflake_handler():
    print("Initializing SnowflakeHandler...")
    handler = SnowflakeHandler()

    print("Creating table if not exists...")
    handler.create_table_if_not_exists()

    print("Inserting test data...")
    test_timestamp = datetime.now()
    test_symbol = "TEST_BTC"
    test_price = 50000.0
    handler.insert_crypto_price(test_timestamp, test_symbol, test_price)

    print("Fetching latest prices...")
    latest_prices = handler.get_latest_prices(limit=5)
    print(latest_prices)

    print("Closing connection...")
    handler.close()

    print("Test completed successfully!")

if __name__ == "__main__":
    try:
        test_snowflake_handler()
    except Exception as e:
        print(f"An error occurred during testing: {e}")
        import traceback
        traceback.print_exc()