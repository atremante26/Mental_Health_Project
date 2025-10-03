import os
import sys
import logging
import pandas as pd
from pathlib import Path

_current_file = Path(__file__).resolve()  # Absolute path to load_data.py
PROJECT_ROOT = _current_file.parent.parent  # Up from analysis/ to project root
sys.path.insert(0, str(PROJECT_ROOT))

from pipeline.snowflake.load_snowflake import snowflake_connection
from analysis.config.data_paths import SQL_PATHS

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_dataset(dataset_name):
    """Load a specific dataset by name using SQL files"""
    try:
        if dataset_name not in SQL_PATHS:
            raise ValueError(f"Unknown dataset: {dataset_name}")
        
        sql_path = (PROJECT_ROOT / SQL_PATHS[dataset_name]).resolve()

        print(f"DEBUG - PROJECT_ROOT: {PROJECT_ROOT}")
        print(f"DEBUG - SQL_PATHS[{dataset_name}]: {SQL_PATHS[dataset_name]}")
        print(f"DEBUG - Resolved sql_path: {sql_path}")
        print(f"DEBUG - File exists: {sql_path.exists()}")
        print(f"DEBUG - Is file: {sql_path.is_file()}")

        if not sql_path.exists():
            raise FileNotFoundError(f"SQL file not found: {sql_path}")
        
        return load_from_sql_file(sql_path)
    
    except Exception as e:
        logger.error(f"Error loading {dataset_name}: {e}")
        return None

def load_from_sql_file(sql_path):
    """Load data from SQL file"""
    print("DEBUG - load_from_sql_file called")
    try:
        print(f"DEBUG - Opening file: {sql_path}")
        with open(sql_path, 'r') as f:
            query = f.read()
        
        print(f"DEBUG - Read {len(query)} characters from SQL file")
        print(f"DEBUG - Connecting to Snowflake...")
        
        with snowflake_connection() as conn:
            print(f"DEBUG - Executing query...")
            df = pd.read_sql(query, conn)
        
        logger.info(f"Loaded {len(df)} rows")
        return df
    
    except Exception as e:  # Catch ALL exceptions, not just FileNotFoundError
        print(f"DEBUG - Exception type: {type(e).__name__}")
        print(f"DEBUG - Exception message: {str(e)}")
        logger.error(f"Error in load_from_sql_file: {e}")
        return None
    
if __name__ == "__main__":
    # Testing
    data = load_dataset('tech_survey')