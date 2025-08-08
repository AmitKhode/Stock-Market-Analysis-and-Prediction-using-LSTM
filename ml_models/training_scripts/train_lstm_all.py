import sys
import os
import numpy as np
import pandas as pd
import joblib

from sklearn.preprocessing import MinMaxScaler
from tensorflow.keras.callbacks import EarlyStopping

from pyspark.sql import SparkSession

# Ensure root dir is in path
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.append(ROOT_DIR)

from ml_models.lstm_model import build_lstm_model

# Initialize Spark
spark = SparkSession.builder \
    .appName("TrainLSTMAll") \
    .master("local[*]") \
    .getOrCreate()

# Paths
DATA_PATH = "hdfs://localhost:9000/user/amitk/stock_data_cleaned/"
MODEL_SAVE_PATH = os.path.join(ROOT_DIR, "ml_models", "lstm_models", "saved_models")
SCALER_SAVE_PATH = os.path.join(ROOT_DIR, "ml_models", "lstm_models", "saved_scalers")

os.makedirs(MODEL_SAVE_PATH, exist_ok=True)
os.makedirs(SCALER_SAVE_PATH, exist_ok=True)

# Parameters
sequence_length = 60
batch_size = 32
epochs = 10

def prepare_sequences(data, seq_len):
    x, y = [], []
    for i in range(len(data) - seq_len):
        x.append(data[i:i + seq_len])
        y.append(data[i + seq_len])
    return np.array(x), np.array(y)

# Get list of symbols
symbols = [row['symbol'] for row in spark.read.parquet(DATA_PATH).select("symbol").distinct().collect()]

# Train LSTM model per symbol
for symbol in symbols:
    print(f"🔄 Processing symbol: {symbol}")
    try:
        df = spark.read.parquet(f"{DATA_PATH}/symbol={symbol}")
        pdf = df.orderBy("timestamp").select("Close").toPandas()

        if len(pdf) <= sequence_length:
            print(f"⚠️ Not enough data for {symbol}, skipping.")
            continue

        # Normalize
        scaler = MinMaxScaler()
        scaled_data = scaler.fit_transform(pdf)

        # Create sequences
        X, y = prepare_sequences(scaled_data, sequence_length)
        X = X.reshape((X.shape[0], X.shape[1], 1))

        # Build and train model
        model = build_lstm_model(input_shape=(sequence_length, 1))
        early_stop = EarlyStopping(monitor='loss', patience=3)
        model.fit(X, y, epochs=epochs, batch_size=batch_size, verbose=1, callbacks=[early_stop])

        # Save model and scaler
        model_path = os.path.join(MODEL_SAVE_PATH, f"model_{symbol}.h5")
        scaler_path = os.path.join(SCALER_SAVE_PATH, f"scaler_{symbol}.pkl")

        model.save(model_path)
        joblib.dump(scaler, scaler_path)

        print(f"✅ Saved model and scaler for {symbol}")

    except Exception as e:
        print(f"❌ Failed for {symbol}: {str(e)}")
