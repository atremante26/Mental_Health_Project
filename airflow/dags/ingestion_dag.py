from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pipeline.ingestion.ingest_cdc import CDCIngestor
from pipeline.ingestion.ingest_reddit import RedditIngestor
from pipeline.ingestion.ingest_trends import GoogleTrendsIngestor

default_args = {
    'owner': 'andrew',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

def run_cdc():
    CDCIngestor().run("cdc", save_s3=True, save_local=False)

def run_reddit():
    RedditIngestor().run("reddit", save_s3=True, save_local=False)

def run_trends():
    GoogleTrendsIngestor().run("trends", save_s3=True, save_local=False)

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
