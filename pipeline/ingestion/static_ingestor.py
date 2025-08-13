import logging
import pandas as pd
from great_expectations.data_context import get_context
from great_expectations.core.batch import BatchRequest
from abc import ABC, abstractmethod
from dotenv import load_dotenv
import boto3
import os
import sys
from pathlib import Path

if os.path.exists('.env.local'):
    load_dotenv('.env.local')
else:
    load_dotenv()

project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))
from pipeline.snowflake.load_snowflake import snowflake_connection

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class StaticIngestor(ABC):
    def __init__(self):

        # S3 config
        self.S3_BUCKET = "mental-health-project-pipeline"
        self.s3 = boto3.client(
            "s3",
            aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
            region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
        )

    @abstractmethod
    def load_data(self) -> pd.DataFrame:
        """Fetch or load raw data from a data source"""
        pass

    @abstractmethod
    def process_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and transform raw data"""
        pass

    def load_static_to_s3(self, df: pd.DataFrame, file_name: str):
        """Load processed data to AWS S3"""
        try:
            processed_key = f"static_data/processed/{file_name.lower()}.json"
            json_buffer = df.to_json(orient='records', date_format='iso')

            self.s3.put_object(
            Bucket=self.S3_BUCKET,
            Key=processed_key,
            Body=json_buffer
            )

            logger.info(f"Saved processed data to s3://{self.S3_BUCKET}/{processed_key}")
        except Exception as e:
            logger.error(f"Failed to save processed data to S3: {e}")


    def load_static_to_snowflake(self, df: pd.DataFrame, table_name: str):
        """Load processed data to Snowflake"""
        try:
            with snowflake_connection() as conn:
                cursor = conn.cursor()
                
                # Create table
                columns = []
                for col in df.columns:
                    if df[col].dtype == 'object':
                        columns.append(f'"{col}" VARCHAR(16777216)')
                    elif df[col].dtype in ['int64', 'int32']:
                        columns.append(f'"{col}" INTEGER')
                    elif df[col].dtype in ['float64', 'float32']:
                        columns.append(f'"{col}" FLOAT')
                    elif df[col].dtype == 'bool':
                        columns.append(f'"{col}" BOOLEAN')
                    elif 'datetime' in str(df[col].dtype):
                        columns.append(f'"{col}" VARCHAR(16777216)')
                    else:
                        columns.append(f'"{col}" VARCHAR(16777216)')
                
                # Create table SQL statement
                create_sql = f"""
                CREATE OR REPLACE TABLE STATIC.{table_name} (
                    {', '.join(columns)}
                )
                """
                cursor.execute(create_sql)
                
                data_tuples = []
                for _, row in df.iterrows():
                    tuple_row = []
                    for value in row:
                        if pd.isna(value):
                            tuple_row.append(None)
                        elif hasattr(value, 'strftime'): 
                            tuple_row.append(value.strftime('%Y-%m-%d %H:%M:%S'))
                        else:
                            tuple_row.append(value)
                    data_tuples.append(tuple(tuple_row))
                
                # Bulk insert
                placeholders = ', '.join(['%s'] * len(df.columns))
                insert_sql = f"INSERT INTO STATIC.{table_name} VALUES ({placeholders})"
                
                cursor.executemany(insert_sql, data_tuples)
                conn.commit()
                
            logger.info(f"Loaded {len(df)} rows to STATIC.{table_name}")
        except Exception as e:
            logger.error(f"Failed to load {table_name} to Snowflake: {e}")
            raise
    
    def validate(self, df: pd.DataFrame, gx_suite: str):
        # Initialize GE context
        context = get_context()

        # Create validator
        validator = context.sources.pandas_default.read_dataframe(df)
        try:
            validator = context.get_validator(
                validator=validator,
                expectation_suite_name=gx_suite
            )
        except Exception as e:
            logger.error(f"[GE Suite Error] Could not load suite '{gx_suite}': {e}")
            raise

        # Run validation
        results = validator.validate()

        if not results["success"]:
            logger.error(f"[Validation Failed] Suite: {gx_suite} — details:\n{results}")
            raise ValueError(f"[Validation Failed] Suite: {gx_suite}")

        logger.info(f"[Validation Passed] Suite: {gx_suite}")
        return results
    
    def run(self, file_name: str, gx_suite: str, table_name: str):
        """Main loading, processing, and saving logic"""
        # Load data
        raw_df = self.load_data()

        if raw_df.empty:
            logger.warning(f"No data found for {table_name}")
            return None

        # Process data
        processed_df = self.process_data(raw_df)

        # Validate data with Great Expectations
        self.validate(processed_df, gx_suite)

        # Save data to S3
        self.load_static_to_s3(processed_df, file_name)

        # Save data to Snowflake
        self.load_static_to_snowflake(processed_df, table_name)

        logger.info(f"Completed {table_name}")

class MentalHealthInTechSurveyIngestor(StaticIngestor):
    def __init__(self):
        super().__init__()

    def load_data(self):
        try:
            response = self.s3.get_object(
                Bucket=self.S3_BUCKET,
                Key='static_data/raw/mental_health_in_tech_survey.csv'
            )
            df = pd.read_csv(response['Body'])
            return df
        except Exception as e:
            logger.error(f"Failed to load Mental Health in Tech Survey data from S3: {e}")
            return pd.DataFrame() 
    
    def process_data(self, df: pd.DataFrame):
        # Keep only standard Male/Female responses
        df['Gender'] = df['Gender'].str.lower().str.strip()
        
        # Keep only rows that start with 'm' or 'f' 
        mask = df['Gender'].str.startswith('m') | df['Gender'].str.startswith('f')
        df = df[mask].copy()
        
        # Standardize to Male/Female
        df.loc[df['Gender'].str.startswith('m'), 'Gender'] = 'Male'
        df.loc[df['Gender'].str.startswith('f'), 'Gender'] = 'Female'

        # Remove age outliers
        df = df[(df['Age'] >= 16) & (df['Age'] <= 80)]

        # Remove free text
        df = df.drop(columns=['comments'])

        # Convert column to datetime type
        df['survey_date'] = pd.to_datetime(df['Timestamp']).dt.date

        logger.info(f"Processed Mental Health in Tech Survey data: {len(df)} rows")
        
        return df

class WHOSuicideStatisticsIngestor(StaticIngestor):
    def __init__(self):
        super().__init__()
    
    def load_data(self):
        try:
            response = self.s3.get_object(
                Bucket=self.S3_BUCKET,
                Key='static_data/raw/who_suicide_statistics.csv'
            )
            df = pd.read_csv(response['Body'])
            return df
        except Exception as e:
            logger.error(f"Failed to load WHO Suicide Statistics data from S3: {e}")
            return pd.DataFrame() 

    def process_data(self, df: pd.DataFrame):
        # Reformat and clean sex column
        gender_mapping = {"male": "Male", "female": "Female"}
        df["sex"] = df["sex"].str.lower().str.strip() 
        df["sex"] = df["sex"].map(gender_mapping)

        # Fill NA values in suicide_no with 0
        df["suicides_no"] = df["suicides_no"].fillna(0)

        # Clean country name
        df["country"] = df["country"].str.strip()

        # Suicide rate per 100k population
        df["suicide_rate_per_100k"] = (df["suicides_no"] / df["population"]) * 100000

        # Data validation
        df = df[df["year"] > 1900] 
        df = df[df["population"] > 0]

        logger.info(f"Processed WHO Suicide Statistics data: {len(df)} rows")

        return df
    
class MentalHealthCareInLast4WeeksIngestor(StaticIngestor):
    def __init__(self):
        super().__init__()

    def load_data(self):
        try:
            response = self.s3.get_object(
                Bucket=self.S3_BUCKET,
                Key='static_data/raw/mental_health_care_in_the_last_4_weeks.csv'
            )
            df = pd.read_csv(response['Body'])
            return df
        except Exception as e:
            logger.error(f"Failed to load Mental Health Care in Last 4 Weeks data from S3: {e}")
            return pd.DataFrame() 

    def process_data(self, df: pd.DataFrame):
        # Drop Suppression Flag (mostly NaN as you noted)
        df = df.drop(columns=['Suppression Flag'], errors='ignore')

        # Convert date columns to proper datetime
        df['Time Period Start Date'] = pd.to_datetime(df['Time Period Start Date'])
        df['Time Period End Date'] = pd.to_datetime(df['Time Period End Date'])

        # Clean categorical columns
        df['State'] = df['State'].str.strip()
        df['Group'] = df['Group'].str.strip()
        df['Subgroup'] = df['Subgroup'].str.strip()

        logger.info(f"Processed Mental Health Care in Last 4 Weeks data: {len(df)} rows")

        return df

class SuicideByDemographicsIngestor(StaticIngestor):
    def __init__(self):
        super().__init__()
    
    def load_data(self):
        try:
            response = self.s3.get_object(
                Bucket=self.S3_BUCKET,
                Key='static_data/raw/death_rates_for_suicide_by_sex_race_hispanic_origin_and_age_united_states.csv'
            )
            df = pd.read_csv(response['Body'])
            return df
        except Exception as e:
            logger.error(f"Failed to load Death Rates for Suicide by Demographic data from S3: {e}")
            return pd.DataFrame()
        
    def process_data(self, df: pd.DataFrame):
        # Clean column names
        df.columns = df.columns.str.lower().str.strip()

        # Drop redundant 'num' columns
        df = df.drop(columns=['unit_num', 'stub_name_num', 'stub_label_num', 
                             'year_num', 'age_num'], errors='ignore')

        # Clean categorical columns
        df['indicator'] = df['indicator'].str.strip() 
        df['unit'] = df['unit'].str.strip() 
        df['stub_name'] = df['stub_name'].str.strip()
        df['stub_label'] = df['stub_label'].str.strip() 
        df['age'] = df['age'].str.strip()

        # Improve column names
        df['demographic_category'] = df['stub_name']
        df['demographic_value'] = df['stub_label'] 

        # Data validation
        df = df[df['year'] >= 1950]  

        logger.info(f"Processed Death Rates for Suicide by Demographic data: {len(df)} rows")

        return df
