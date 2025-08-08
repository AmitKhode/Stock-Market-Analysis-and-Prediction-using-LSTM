from pyspark.sql import SparkSession
import os
import pandas as pd

# --- Config ---
symbols = ["AAPL", "GOOG", "TSLA", "AMZN"]
HDFS_BASE_PATH = "hdfs://localhost:9000/user/amitk/stock_data_cleaned"
LOCAL_OUTPUT_DIR = "/home/amitk/Desktop/stock-market-etl/spark_output"
os.makedirs(LOCAL_OUTPUT_DIR, exist_ok=True)

# --- Spark Session ---
spark = SparkSession.builder \
    .appName("ExportCleanedParquetToCSV") \
    .master("local[*]") \
    .getOrCreate()

for symbol in symbols:
    try:
        hdfs_path = f"{HDFS_BASE_PATH}/symbol={symbol}"
        local_path = os.path.join(LOCAL_OUTPUT_DIR, f"{symbol}_cleaned.csv")

        # Load from HDFS
        df = spark.read.parquet(hdfs_path)
        df = df.orderBy("timestamp")
        new_data = df.toPandas()

        if os.path.exists(local_path):
            # Load existing CSV and append new unique rows
            existing_data = pd.read_csv(local_path)
            combined = pd.concat([existing_data, new_data])
            combined.drop_duplicates(subset=["timestamp"], keep="last", inplace=True)
            combined.sort_values("timestamp", inplace=True)
            combined.to_csv(local_path, index=False)
            print(f"[+] Appended and deduplicated data for {symbol}")
        else:
            # First-time export
            new_data.to_csv(local_path, index=False)
            print(f"[✓] Created new CSV for {symbol}")

    except Exception as e:
        print(f"[✗] Failed for {symbol}: {e}")

spark.stop()
