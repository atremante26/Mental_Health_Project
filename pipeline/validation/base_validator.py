import os
import boto3
import datetime
from abc import ABC, abstractmethod
from dotenv import load_dotenv
load_dotenv()

class BaseValidator(ABC):
    def __init__(self):

       # S3 config
        self.S3_BUCKET = "mental-health-project-pipeline"
        self.s3 = boto3.client(
            "s3",
            aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
            region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
        )
        self.today = datetime.datetime.today().strftime("%Y-%m-%d")

    @abstractmethod
    def validate(self):
        """Validate data using Great Expectations"""
        pass