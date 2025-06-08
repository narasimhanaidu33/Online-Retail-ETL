# dags/online_retail_etl.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    'owner': 'retail_team',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'online_retail_etl',
    default_args=default_args,
    description='ETL pipeline for Online Retail data',
    schedule_interval='@daily',
    catchup=False,
    tags=['retail', 'etl'],
)

extract_task = SparkSubmitOperator(
    task_id='extract_retail_data',
    application='/opt/airflow/spark_jobs/extract/retail_extractor.py',
    conn_id='spark_default',
    dag=dag,
    application_args=[],  # Add any arguments here
    verbose=True
)

# Placeholder for future transform and load tasks
transform_task = DummyOperator(task_id='transform_placeholder', dag=dag)
load_task = DummyOperator(task_id='load_placeholder', dag=dag)

extract_task >> transform_task >> load_task