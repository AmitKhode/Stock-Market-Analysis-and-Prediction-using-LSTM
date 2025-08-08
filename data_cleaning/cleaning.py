from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp
from pyspark.sql.types import IntegerType

# Start Spark session
spark = SparkSession.builder \
    .appName("IncrementalStockDataCleaning") \
    .master("local[*]") \
    .getOrCreate()

# Load raw data
raw_df = spark.read.parquet("hdfs://localhost:9000/user/amitk/stock_data/")
raw_df = raw_df.withColumn("timestamp", to_timestamp(col("timestamp")))

# Load existing cleaned data (if exists)
try:
    cleaned_df_existing = spark.read.parquet("hdfs://localhost:9000/user/amitk/stock_data_cleaned/")
    cleaned_df_existing = cleaned_df_existing.select("symbol", "timestamp").dropDuplicates()
    print("✅ Loaded existing cleaned data.")
except Exception as e:
    print("⚠️ No existing cleaned data found. This is likely the first run.")
    cleaned_df_existing = None

# Convert raw "Volume" to integer
raw_df = raw_df.withColumn("Volume", col("Volume").cast(IntegerType()))

# Drop rows with nulls and duplicates in raw data
raw_df = raw_df.dropna().dropDuplicates(["symbol", "timestamp"])

# Filter only new records
if cleaned_df_existing:
    # Left anti join to get new records
    new_data = raw_df.join(cleaned_df_existing, on=["symbol", "timestamp"], how="left_anti")
else:
    new_data = raw_df

# Show how many new rows were found
print(f"🆕 New rows to append: {new_data.count()}")

# If there's new data, append it to cleaned dataset
if new_data.count() > 0:
    new_data.write \
        .partitionBy("symbol") \
        .mode("append") \
        .parquet("hdfs://localhost:9000/user/amitk/stock_data_cleaned/")
    print("✅ Appended new cleaned data.")
else:
    print("✅ No new data to clean.")

