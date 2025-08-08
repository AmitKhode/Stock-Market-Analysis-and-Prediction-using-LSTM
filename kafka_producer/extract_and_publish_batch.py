import json
import yfinance as yf
from kafka import KafkaProducer
from datetime import datetime
import pandas as pd

KAFKA_TOPIC = "stock_topic"
KAFKA_SERVER = "localhost:9092"

print("[DEBUG] Script started")

try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    print("[DEBUG] Kafka producer initialized")
except Exception as e:
    print(f"[ERROR] Failed to initialize Kafka producer: {e}")
    producer = None


def fetch_stock(symbol):
    print(f"[DEBUG] Fetching stock data for {symbol}")
    try:
        ticker = yf.Ticker(symbol)
        data = ticker.history(period="1d", interval="1m")

        if len(data) < 1:
            print(f"[{symbol}] Not enough data to extract latest price.")
            return None

        latest = data.iloc[-1]
        timestamp = data.index[-1].strftime('%Y-%m-%d %H:%M:%S')

        if pd.isna(latest["Close"]) or latest["Volume"] == 0:
            print(f"[{symbol}] Skipped due to missing or zero Volume: {latest.to_dict()}")
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


def fetch_and_publish():
    print("[DEBUG] Entered fetch_and_publish")
    symbols = ["AAPL", "GOOG", "MSFT", "AMZN", "TSLA"]

    for symbol in symbols:
        data = fetch_stock(symbol)
        if data:
            try:
                producer.send(KAFKA_TOPIC, value=data)
                print(f"[Kafka] Published data for {symbol}: {data}")
            except Exception as e:
                print(f"[Kafka] Error publishing {symbol}: {e}")
        else:
            print(f"[{symbol}] No data to publish")


if __name__ == "__main__":
    print("[DEBUG] In __main__ block")
    if producer:
        fetch_and_publish()  # ⬅️ Run only once
        producer.flush()     # Ensure all messages are sent before exiting
        print("[DEBUG] Producer finished and exited cleanly")
    else:
        print("[ERROR] Kafka producer not available, exiting")
