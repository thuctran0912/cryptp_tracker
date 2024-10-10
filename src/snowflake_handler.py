import os
from dotenv import load_dotenv
import snowflake.connector
import pandas as pd
from datetime import datetime

class SnowflakeHandler:
    def __init__(self):
        load_dotenv('config/config.env')
        self.conn = snowflake.connector.connect(
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSWORD')
        )
        self.cursor = self.conn.cursor()

    def create_table_if_not_exists(self):
        self.cursor.execute("""
        CREATE OR REPLACE TABLE TEST.TEST.crypto_prices (
            timestamp TIMESTAMP,
            symbol STRING,
            price FLOAT
        )
        """)

    def insert_crypto_price(self, timestamp, symbol, price):
        self.cursor.execute("""
        INSERT INTO TEST.TEST.crypto_prices (timestamp, symbol, price)
        VALUES (%s, %s, %s)
        """, (timestamp, symbol, price))
        self.conn.commit()

    def get_latest_prices(self, limit=10):
        query = f"""
        SELECT * FROM TEST.TEST.crypto_prices
        ORDER BY timestamp DESC
        LIMIT {limit}
        """
        self.cursor.execute(query)
        columns = [desc[0] for desc in self.cursor.description]
        data = self.cursor.fetchall()
        return pd.DataFrame(data, columns=columns)

    def close(self):
        self.cursor.close()
        self.conn.close()

if __name__ == "__main__":
    # This block allows you to test the SnowflakeHandler directly
    handler = SnowflakeHandler()
    handler.create_table_if_not_exists()
    handler.insert_crypto_price(datetime.now(), 'BTC', 50000.0)
    print(handler.get_latest_prices())
    handler.close()