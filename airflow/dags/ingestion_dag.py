from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pipeline.ingestion.ingest_cdc import CDCIngestor
from pipeline.ingestion.ingest_reddit import RedditIngestor
from pipeline.ingestion.ingest_trends import GoogleTrendsIngestor
from pipeline.snowflake.load_snowflake import (
    load_cdc_to_snowflake,
    load_reddit_to_snowflake,
    load_trends_to_snowflake
)

default_args = {
    'owner': 'andrew',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

def run_cdc():
    CDCIngestor().run("cdc", "cdc_suite", save_s3=True, save_local=False)

def run_reddit():
    RedditIngestor().run("reddit", "reddit_suite", save_s3=True, save_local=False)

def run_trends():
    GoogleTrendsIngestor().run("trends", "trends_suite", save_s3=True, save_local=False)

with DAG(
    dag_id="ingestion_dag",
    default_args=default_args,
    start_date=datetime(2025, 7, 21),
    schedule_interval="@weekly",
    catchup=False,
    tags=["mental_health"],
) as dag:
    
    ingest_cdc_task = PythonOperator(
        task_id='ingest_cdc',
        python_callable=run_cdc
    )

    ingest_reddit_task = PythonOperator(
        task_id='ingest_reddit',
        python_callable=run_reddit
    )

    ingest_trends_task = PythonOperator(
        task_id='ingest_trends',
        python_callable=run_trends
    )

    load_cdc_task = PythonOperator(
        task_id='load_cdc_to_snowflake',
        python_callable=load_cdc_to_snowflake
    )

    load_reddit_task = PythonOperator(
        task_id='load_reddit_to_snowflake',
        python_callable=load_reddit_to_snowflake
    )

    load_trends_task = PythonOperator(
        task_id='load_trends_to_snowflake',
        python_callable=load_trends_to_snowflake
    )

    ingest_cdc_task >> load_cdc_task
    ingest_reddit_task >> load_reddit_task
    ingest_trends_task >> load_trends_task
