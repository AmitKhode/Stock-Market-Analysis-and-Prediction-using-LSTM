from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'amitk',
    'start_date': days_ago(1),
    'depends_on_past': False
}

with DAG(
    dag_id='stock_market_etl',
    default_args=default_args,
    description='ETL pipeline for stock market using Kafka, Spark, and prediction',
    schedule_interval='@hourly',
    catchup=False
) as dag:

    run_producer = BashOperator(
        task_id='run_producer',
        bash_command='cd /home/amitk/Desktop/stock-market-etl/kafka_producer && python3.10 extract_and_publish.py'
    )

    run_consumer = BashOperator(
        task_id='run_consumer',
        bash_command='cd /home/amitk/Desktop/stock-market-etl/spark_consumer && spark-submit spark_consumer_HDFS.py'
    )

    run_prediction = BashOperator(
        task_id='run_prediction',
        bash_command='cd /home/amitk/Desktop/stock-market-etl && python3.10 predict.py'
    )

    export_csv = BashOperator(
        task_id='export_csv',
        bash_command='cd /home/amitk/Desktop/stock-market-etl && python3.10 export_cleaned_data.py'
    )

    run_producer >> run_consumer >> export_csv >> run_prediction
