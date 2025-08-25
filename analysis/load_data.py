import logging
import sys
import pandas as pd
from pipeline.snowflake.load_snowflake import snowflake_connection

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_from_snowflake(path):
    # Read SQL from file
    with open(path, 'r') as f:
        query = f.read()

    # Execute against Snowflake
    with snowflake_connection() as conn:
        df = pd.read_sql(query, conn)
    
    return df

if __name__ == '__main__':
    load_from_snowflake('pipeline/snowflake/cdc_sql/cdc_extract.sql')
    load_from_snowflake('pipeline/snowflake/reddit_sql/reddit_extract.sql')
    load_from_snowflake('pipeline/snowflake/trends_sql/trends_extract.sql')
    load_from_snowflake('pipeline/snowflake/static/tech_survey_extract.sql')
    load_from_snowflake('pipeline/snowflake/static/who_suicide_extract.sql')
    load_from_snowflake('pipeline/snowflake/static/mental_health_care_extract.sql')
    load_from_snowflake('pipeline/snowflake/static/suicide_demographics_extract.sql')
