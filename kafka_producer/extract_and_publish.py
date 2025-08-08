import json
import yfinance as yf
from kafka import KafkaProducer
import time
import pandas as pd
from datetime import datetime


KAFKA_TOPIC = "stock_topic"
KAFKA_SERVER = "localhost:9092"
SYMBOLS = ["AAPL", "GOOG", "MSFT", "AMZN", "TSLA"]

print("[DEBUG] Starting Producer...")

# Initialize Kafka Producer
try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    print("[DEBUG] Kafka producer initialized")
except Exception as e:
    print(f"[ERROR] Failed to initialize Kafka producer: {e}")
    exit(1)


def fetch_stock(symbol):
    try:
        ticker = yf.Ticker(symbol)
        data = ticker.history(period="1d", interval="1m")

        if len(data) < 2:
            return None

        latest = data.iloc[-1]
        # timestamp = data.index[-1].strftime('%Y-%m-%d %H:%M:%S')
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')


        if pd.isna(latest["Close"]) or latest["Volume"] == 0:
            return None

        return {
            "symbol": str(symbol),
            "timestamp": str(timestamp),
            "Open": float(latest["Open"]),
            "High": float(latest["High"]),
            "Low": float(latest["Low"]),
            "Close": float(latest["Close"]),
            "Volume": int(latest["Volume"])
        }

    except Exception as e:
        print(f"[{symbol}] Error fetching data: {e}")
        return None


while True:
    print("[DEBUG] Fetching stock data...")
    for symbol in SYMBOLS:
        data = fetch_stock(symbol)
        if data:
            try:
                producer.send(KAFKA_TOPIC, value=data)
                print(f"[Kafka] Published: {data}")
            except Exception as e:
                print(f"[Kafka] Error publishing {symbol}: {e}")
        else:
            print(f"[{symbol}] No valid data.")
    time.sleep(60)  # Wait 1 minute before next fetch
