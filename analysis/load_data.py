import os
import sys
import logging
import pandas as pd
from pathlib import Path

# Get absolute path to project root
_current_file = Path(__file__).resolve()
PROJECT_ROOT = _current_file.parent.parent
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
            raise ValueError(f"Unknown dataset: {dataset_name}. Available datasets: {list(SQL_PATHS.keys())}")
        
        sql_path = (PROJECT_ROOT / SQL_PATHS[dataset_name]).resolve()
        
        if not sql_path.exists():
            raise FileNotFoundError(f"SQL file not found: {sql_path}")
        
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
        
        logger.info(f"Loaded {len(df)} rows from {sql_path.name}")
        return df
    
    except Exception as e:
        logger.error(f"Error executing query from {sql_path}: {e}")
        return None

if __name__ == "__main__":
    data = load_dataset('tech_survey')
    if data is not None:
        print(f"Successfully loaded {len(data)} rows")
        print(f"Columns: {list(data.columns)}")