import json
import yfinance as yf
from kafka import KafkaProducer
from datetime import datetime
import pandas as pd
import sys

KAFKA_TOPIC = "stock_topic"
KAFKA_SERVER = "localhost:9092"
SYMBOLS = ["AAPL", "GOOG", "MSFT", "AMZN", "TSLA"]

print("[DEBUG] Script started")

# Initialize Kafka producer
try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        retries=3  # retry up to 3 times if connection fails
    )
    print("[DEBUG] Kafka producer initialized")
except Exception as e:
    print(f"[ERROR] Failed to initialize Kafka producer: {e}")
    sys.exit(1)  # Exit if Kafka is not available


def fetch_stock(symbol):
    """Fetch the latest stock data for the given symbol."""
    print(f"[DEBUG] Fetching stock data for {symbol}")
    try:
        ticker = yf.Ticker(symbol)
        data = ticker.history(period="1d", interval="1m")

        if data.empty:
            print(f"[{symbol}] No data returned from yfinance.")
            return None

        latest = data.iloc[-1]
        timestamp = latest.name.strftime('%Y-%m-%d %H:%M:%S')

        if pd.isna(latest["Close"]) or latest["Volume"] == 0:
            print(f"[{symbol}] Skipped due to missing/zero Volume: {latest.to_dict()}")
            return None

        return {
            "symbol": symbol,
            "timestamp": timestamp,
            "Open": float(latest["Open"]),
            "High": float(latest["High"]),
            "Low": float(latest["Low"]),
            "Close": float(latest["Close"]),
            "Volume": int(latest["Volume"])
        }

    except Exception as e:
        print(f"[{symbol}] Error fetching data: {e}")
        return None


def fetch_and_publish():
    """Fetch and publish data for all symbols."""
    published_count = 0
    for symbol in SYMBOLS:
        data = fetch_stock(symbol)
        if data:
            try:
                producer.send(KAFKA_TOPIC, value=data)
                print(f"[Kafka] Published: {data}")
                published_count += 1
            except Exception as e:
                print(f"[Kafka] Error publishing {symbol}: {e}")
        else:
            print(f"[{symbol}] No valid data to publish")
    return published_count


if __name__ == "__main__":
    print("[DEBUG] Starting batch publish...")
    count = fetch_and_publish()
    producer.flush()
    producer.close()
    print(f"[DEBUG] Batch publish complete. Total messages sent: {count}")
