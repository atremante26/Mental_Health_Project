import os
import sys
os.chdir('/Users/Andrew/Desktop/Computer Science/Mental_Health_Project')
sys.path.append('.')

import logging
import pandas as pd
from pathlib import Path
from pipeline.snowflake.load_snowflake import snowflake_connection
from config.data_paths import SQL_PATHS

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Get root directory
PROJECT_ROOT = Path(__file__).parent.parent

def load_dataset(dataset_name):
    """Load a specific dataset by name using SQL files"""
    try:
        if dataset_name not in SQL_PATHS:
            raise ValueError(f"Unknown dataset: {dataset_name}")
        
        sql_path = PROJECT_ROOT / SQL_PATHS[dataset_name]
        print("sql_path: ", sql_path)
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
    
if __name__ == "__main__":
    # Testing
    data = load_dataset('tech_survey')