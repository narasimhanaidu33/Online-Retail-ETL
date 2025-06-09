from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.dummy import DummyOperator  # Add this import

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

create_tables = PostgresOperator(
    task_id="create_tables",
    postgres_conn_id="postgres_default",
    sql="sql/migrations/001_initial_schema.sql",
    dag=dag
)

load_to_db = SparkSubmitOperator(
    task_id="load_to_database",
    application="/opt/airflow/spark_jobs/load/retail_loader.py",
    conn_id="spark_default",
    dag=dag
)

transform_task = DummyOperator(task_id='transform_placeholder', dag=dag)

extract_task >> transform_task >> create_tables >> load_to_db
