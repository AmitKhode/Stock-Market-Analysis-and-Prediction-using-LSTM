# predict.py

import os
import joblib
import numpy as np
import pandas as pd
import argparse
from tensorflow.keras.models import load_model
from pyspark.sql import SparkSession

SEQUENCE_LENGTH = 60
SYMBOLS = ["AAPL", "GOOG", "TSLA", "AMZN","MSFT"]

def predict_stock(symbol, spark):
    ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    MODEL_PATH = os.path.join(ROOT_DIR, "ml_models", "lstm_models", "saved_models")
    SCALER_PATH = os.path.join(ROOT_DIR, "ml_models", "lstm_models", "saved_scalers")
    DATA_PATH = "hdfs://localhost:9000/user/amitk/stock_data_cleaned/"
    OUTPUT_PATH = os.path.join(ROOT_DIR, "Predictions", f"{symbol}_predictions.csv")

    try:
        model_file = os.path.join(MODEL_PATH, f"model_{symbol}.h5")
        scaler_file = os.path.join(SCALER_PATH, f"scaler_{symbol}.pkl")

        if not os.path.exists(model_file) or not os.path.exists(scaler_file):
            print(f"[⚠️] Model or scaler missing for {symbol}")
            return

        model = load_model(model_file)
        scaler = joblib.load(scaler_file)

        df = spark.read.parquet(f"{DATA_PATH}/symbol={symbol}")
        pdf = df.orderBy("timestamp").select("timestamp", "Close").toPandas()

        if len(pdf) < SEQUENCE_LENGTH:
            print(f"[⚠️] Not enough data for {symbol}")
            return

        last_sequence = pdf["Close"].values[-SEQUENCE_LENGTH:]
        scaled_sequence = scaler.transform(last_sequence.reshape(-1, 1))
        X = np.array(scaled_sequence).reshape((1, SEQUENCE_LENGTH, 1))

        prediction = model.predict(X)
        predicted_price = scaler.inverse_transform(prediction)[0][0]
        latest_timestamp = pd.to_datetime(pdf["timestamp"].iloc[-1])

        # Save prediction
        out_df = pd.DataFrame({
            "timestamp": [latest_timestamp],
            "Predicted": [predicted_price]
        })
        out_df.to_csv(OUTPUT_PATH, index=False)

        print(f"[✓] {symbol} Prediction on {latest_timestamp.date()}: {predicted_price:.2f}")
        print(f"    → Saved to: {OUTPUT_PATH}")

    except Exception as e:
        print(f"[✗] Error processing {symbol}: {e}")

def main():
    parser = argparse.ArgumentParser(description="LSTM Stock Price Prediction")
    parser.add_argument("--stock", type=str, help="Stock symbol (e.g., AAPL). Leave empty to run all.")

    args = parser.parse_args()
    spark = SparkSession.builder.appName("LSTM_Predict").master("local[*]").getOrCreate()

    if args.stock:
        predict_stock(args.stock.upper(), spark)
    else:
        print("[ℹ️] Running prediction for all symbols...")
        for sym in SYMBOLS:
            predict_stock(sym, spark)

    spark.stop()

if __name__ == "__main__":
    main()
