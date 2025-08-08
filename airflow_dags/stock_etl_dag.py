from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'stock_pipeline_dag',
    default_args=default_args,
    description='Stock ETL pipeline: producer → cleaner → consumer → trainer',
    schedule_interval='@daily',
    start_date=datetime(2025, 8, 1),
    catchup=False,
)

# 1️⃣ Producer - fetch & publish stock data to Kafka
producer_task = BashOperator(
    task_id='run_producer',
    bash_command='python3.10 /home/amitk/Desktop/stock-market-etl/kafka_producer/extract_and_publish2.py',
    dag=dag,
)

# 2️⃣ Cleaner - clean raw data
cleaner_task = BashOperator(
    task_id='run_cleaner',
    bash_command='python3.10 /home/amitk/Desktop/stock-market-etl/data_cleaning/cleaning.py',
    dag=dag,
)

# 3️⃣ Consumer - Spark job to process and store
consumer_task = BashOperator(
    task_id='run_consumer',
    bash_command='spark-submit   --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0   spark_consumer_HDFS.py',
    dag=dag,
)

# 4️⃣ Trainer - Train LSTM models on cleaned data
trainer_task = BashOperator(
    task_id='run_model_trainer',
    bash_command='python3.10 /home/amitk/Desktop/stock-market-etl/ml_models/training_scripts/train_lstm_all.py',
    dag=dag,
)

# DAG dependencies
producer_task >> consumer_task >> cleaner_task  
