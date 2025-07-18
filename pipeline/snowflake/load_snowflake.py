import os
import snowflake.connector
from datetime import datetime


def get_snowflake_connection():
    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
        role=os.getenv("SNOWFLAKE_ROLE"),
    )

def run_sql(sql: str):
    conn = get_snowflake_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
    finally:
        conn.close()

def load_reddit_to_snowflake():
    today = datetime.today().strftime("%Y%m%d")
    with open("sql/load_reddit.sql") as f:
        run_sql(f.read().replace("{{ ds_nodash }}", today))

def load_cdc_to_snowflake():
    today = datetime.today().strftime("%Y%m%d")
    with open("sql/load_cdc.sql") as f:
        run_sql(f.read().replace("{{ ds_nodash }}", today))

def load_trends_to_snowflake():
    today = datetime.today().strftime("%Y%m%d")
    with open("sql/load_trends.sql") as f:
        run_sql(f.read().replace("{{ ds_nodash }}", today))
