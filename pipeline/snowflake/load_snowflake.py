import os
import snowflake.connector
from datetime import datetime
from dotenv import load_dotenv
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

load_dotenv()

# Create the connection
def snowflake_connection():
    with open(os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH"), 'rb') as key:
        private_key = serialization.load_pem_private_key(
            key.read(),
            password=None,
            backend=default_backend()
        )
    
    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        private_key=private_key,
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        role=os.getenv("SNOWFLAKE_ROLE"),
    )

# Run SQL queries
def run_sql_from_file(filepath: str, date: str):
    with open(filepath, "r") as f:
        sql_content = f.read().replace("{{ ds_nodash }}", date)
        print(f"=== DATE BEING SUBSTITUTED: {date} ===")
        print(f"=== FINAL SQL CONTENT: ===")
        print(sql_content)
        
        # Split by semicolon into individual statements
        statements = [stmt.strip() for stmt in sql_content.split(';') if stmt.strip()]
        
        with snowflake_connection().cursor() as cur:
            for statement in statements: 
                cur.execute(statement)

# Loaders for each dataset
def load_reddit_to_snowflake():
    today = datetime.today().strftime("%Y%m%d")
    run_sql_from_file("pipeline/snowflake/reddit_sql/reddit_processed_load.sql", today)

def load_cdc_to_snowflake():
    today = datetime.today().strftime("%Y%m%d")
    run_sql_from_file("pipeline/snowflake/cdc_sql/cdc_processed_load.sql", today)

def load_trends_to_snowflake():
    today = datetime.today().strftime("%Y%m%d")
    run_sql_from_file("pipeline/snowflake/trends_sql/trends_processed_load.sql", today)

if __name__ == "__main__":
    load_cdc_to_snowflake()
    load_reddit_to_snowflake()
    load_trends_to_snowflake()
