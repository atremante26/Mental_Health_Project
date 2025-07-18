import os
import snowflake.connector
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# Create the connection
def snowflake_connection():
    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        role=os.getenv("SNOWFLAKE_ROLE"),
    )

# Run SQL with substitution
def run_sql_from_file(filepath: str, date: str):
    with open(filepath, "r") as f:
        sql = f.read().replace("{{ ds_nodash }}", date)
        with snowflake_connection().cursor() as cur:
            cur.execute(sql)

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
