import os
import snowflake.connector
from datetime import datetime
from dotenv import load_dotenv
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
from pathlib import Path

load_dotenv()

# Create the connection
def snowflake_connection():
    # Determine the correct key path based on environment
    env_key_path = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH")
    
    if Path('/opt/airflow').exists():
        # Running in Docker - use Docker path
        key_path = '/opt/airflow/keys/rsa_key.p8'
    else:
        # Running locally - use env variable
        key_path = env_key_path
    
    with open(key_path, 'rb') as key:
        p_key = serialization.load_pem_private_key(
            key.read(),
            password=None,
            backend=default_backend()
        )

    pkb = p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )

    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
        private_key=pkb
    )

# Run SQL queries
def run_sql_from_file(filepath: str, date: str):
    with open(filepath, "r") as f:
        sql_content = f.read().replace("{{ ds_nodash }}", date)
        statements = [stmt.strip() for stmt in sql_content.split(';') if stmt.strip()]
        with snowflake_connection().cursor() as cur:
            for statement in statements:
                cur.execute(statement)

# Loaders for each dataset
def load_reddit_to_snowflake():
    today = datetime.today().strftime("%Y-%m-%d")
    run_sql_from_file("pipeline/snowflake/reddit_sql/reddit_processed_load.sql", today)

def load_cdc_to_snowflake():
    today = datetime.today().strftime("%Y-%m-%d")
    run_sql_from_file("pipeline/snowflake/cdc_sql/cdc_processed_load.sql", today)

def load_trends_to_snowflake():
    today = datetime.today().strftime("%Y-%m-%d")
    run_sql_from_file("pipeline/snowflake/trends_sql/trends_processed_load.sql", today)

def load_news_to_snowflake():
    today = datetime.today().strftime("%Y-%m-%d")
    run_sql_from_file("pipeline/snowflake/news_sql/news_processed_load.sql", today)

if __name__ == "__main__":
    load_cdc_to_snowflake()
    load_reddit_to_snowflake()
    load_trends_to_snowflake()
