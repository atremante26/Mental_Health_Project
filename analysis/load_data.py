import logging
import pandas as pd
from pipeline.snowflake.load_snowflake import snowflake_connection
from config.data_paths import SQL_PATHS

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_dataset(dataset_name):
    """Load a specific dataset by name using SQL files"""
    try:
        if dataset_name not in SQL_PATHS:
            raise ValueError(f"Unknown dataset: {dataset_name}")
        
        sql_path = SQL_PATHS[dataset_name]
        return load_from_sql_file(sql_path)
    
    except Exception as e:
        logger.error(f"Error loading {dataset_name}: {e}")
        return None

def load_from_sql_file(sql_path):
    """Load data from SQL file"""
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
        logger.error(f"Error executing query from {sql_path}: {e}")
        return None