from pipeline.snowflake.load_snowflake import run_sql_from_file
import logging
from datetime import datetime
import pandas as pd
from abc import ABC, abstractmethod
from dotenv import load_dotenv
import boto3
import os
load_dotenv()

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

    @abstractmethod
    def load_static_to_snowflake(self):
        """Load static data sources to Snowflake"""
        pass

    def run(self, name: str):
        """Main loading, processing, and saving logic"""
        # Load data
        raw_df = self.load_data()
        processed_df = self.process_data(raw_df)
        



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
    
    def load_static_to_snowflake(self):
        today = datetime.today().strftime("%Y-%m-%d")
        run_sql_from_file()

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
    
    def load_static_to_snowflake(self):
        today = datetime.today().strftime("%Y-%m-%d")
        run_sql_from_file()
    
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
    
    def load_static_to_snowflake(self):
        today = datetime.today().strftime("%Y-%m-%d")
        run_sql_from_file()

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
    
    def load_static_to_snowflake(self):
        today = datetime.today().strftime("%Y-%m-%d")
        run_sql_from_file()

        




