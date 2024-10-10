import csv
import os
from datetime import datetime

class CSVHandler:
    def __init__(self):
        self.file_path = 'crypto_prices.csv'
        self._create_csv_if_not_exists()

    def _create_csv_if_not_exists(self):
        if not os.path.exists(self.file_path):
            with open(self.file_path, 'w', newline='') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(['timestamp', 'symbol', 'price'])

    def store_crypto_price(self, data):
        with open(self.file_path, 'a', newline='') as csvfile:
            writer = csv.writer(csvfile)
            timestamp = datetime.fromtimestamp(int(data['t'])/1000).strftime('%Y-%m-%d %H:%M:%S')
            writer.writerow([timestamp, data['s'], data['p']])
        print(f"Stored in CSV: {data['s']} - {data['p']} at {timestamp}")

    def close(self):
        # No need to close anything for CSV
        pass
