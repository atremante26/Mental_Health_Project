from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Default arguments applied to all tasks
default_args = {
    'owner': 'andrew',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    dag_id='cdc_data_ingestion',
    description='Ingest and clean weekly CDC mental health data',
    default_args=default_args,
    start_date=datetime(2025, 6, 27),
    schedule_interval='@weekly',
    catchup=False
) as dag:

    run_cdc_ingestion = BashOperator(
        task_id='run_cdc_ingestion_script',
        bash_command='python /opt/airflow/pipeline/cdc/ingest_cdc.py'
    )

    run_cdc_ingestion
