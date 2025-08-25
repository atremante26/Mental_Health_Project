import logging
import sys
import pandas as pd
from pipeline.snowflake.load_snowflake import snowflake_connection

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_from_snowflake(sql_path):
    try:
        with open(sql_path, 'r') as f:
            query = f.read()
        
        with snowflake_connection() as conn:
            df = pd.read_sql(query, conn)
        
        logger.info(f"Loaded {len(df)} rows from {sql_path}")
        return df
    
    except FileNotFoundError:
        logger.error(f"SQL file not found: {sql_path}")
        return None
    except Exception as e:
        logger.error(f"Error loading data from {sql_path}: {e}")
        return None
    
if __name__ == '__main__':
    cdc_df = load_from_snowflake('pipeline/snowflake/cdc_sql/cdc_extract.sql')
    reddit_df = load_from_snowflake('pipeline/snowflake/reddit_sql/reddit_extract.sql')
    trends_df = load_from_snowflake('pipeline/snowflake/trends_sql/trends_extract.sql')
    tech_survey_df = load_from_snowflake('pipeline/snowflake/static/tech_survey_extract.sql')
    who_suicide_df = load_from_snowflake('pipeline/snowflake/static/who_suicide_extract.sql')
    mental_health_care_df = load_from_snowflake('pipeline/snowflake/static/mental_health_care_extract.sql')
    suicide_demographics_df = load_from_snowflake('pipeline/snowflake/static/suicide_demographics_extract.sql')
