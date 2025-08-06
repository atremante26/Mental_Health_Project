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
            logger.error(f"Failed to load mental health in tech survey data: {e}")
            return pd.DataFrame() 
    
    def process_data(self, df: pd.DataFrame):
        pass