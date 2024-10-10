import websocket
import json
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv('config/config.env')

class FinnhubWebsocketClient:
    def __init__(self, on_message_callback):
        self.on_message_callback = on_message_callback
        self.ws = None

    def on_message(self, ws, message):
        data = json.loads(message)
        if data['type'] == 'trade':
            for trade in data['data']:
                self.on_message_callback(trade)

    def on_error(self, ws, error):
        print(f"Error: {error}")

    def on_close(self, ws):
        print("WebSocket connection closed")

    def on_open(self, ws):
        print("WebSocket connection opened")
        symbols = ["BINANCE:BTCUSDT", "BINANCE:ETHUSDT"]
        for symbol in symbols:
            ws.send(json.dumps({"type": "subscribe", "symbol": symbol}))

    def connect(self):
        api_key = os.getenv('FINNHUB_API_KEY')
        self.ws = websocket.WebSocketApp(f"wss://ws.finnhub.io?token={api_key}",
                                         on_message=self.on_message,
                                         on_error=self.on_error,
                                         on_close=self.on_close,
                                         on_open=self.on_open)

    def run(self):
        self.ws.run_forever()

    def close(self):
        if self.ws:
            self.ws.close()