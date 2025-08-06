from pipeline.ingestion import BaseIngestor
import logging
import pandas as pd
import boto3
import os

logger = logging.getLogger(__name__)

s3 = boto3.client(
    "s3",
    aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
    region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
)

class MentalHealthInTechSurveyIngestor(BaseIngestor):
    def __init__(self):
        super().__init__()

    def load_data(self):
        try:
            response = self.s3.get_object(
                Bucket='mental-health-project-pipeline',
                Key='static_data/raw/mental_health_in_tech_survey.csv'
            )
            df = pd.read_csv(response['Body'])
            return df
        except Exception as e:
            logger.error(f"Failed to load mental health in tech survey data from S3: {e}")
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
        
        return df

class WHOSuicideStatisticsIngestor(BaseIngestor):
    def __init__(self):
        super().__init__()
    
    def load_data(self):
        try:
            response = self.s3.get_object(
                Bucket='mental-health-project-pipeline',
                Key='static_data/raw/who_suicide_statistics.csv'
            )
            df = pd.read_csv(response['Body'])
            return df
        except Exception as e:
            logger.error(f"Failed to load WHO suicide statistics from S3: {e}")
            return pd.DataFrame() 

    def process_data(self, df: pd.DataFrame):
        # Reformat sex column
        gender_mapping = {"male": "Male", "female": "Female"}
        df["sex"] = df["sex"].map(gender_mapping)

        # Fill NA values in suicide_no with 0
        df["suicides_no"] = df["suicides_no"].fillna(0)

        return df
    
class MentalHealthCareInLast4WeeksIngestor(BaseIngestor):
    def __init__(self):
        super().__init__()

    def load_data(self):
        try:
            response = self.s3.get_object(
                Bucket='mental-health-project-pipeline',
                Key='static_data/raw/mental_health_care_in_the_last_4_weeks.csv'
            )
            df = pd.read_csv(response['Body'])
            return df
        except Exception as e:
            logger.error(f"Failed to load mental health care in last 4 weeks from S3: {e}")
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

        return df
        


