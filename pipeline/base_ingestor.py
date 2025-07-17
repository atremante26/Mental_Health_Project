import os
import json
import boto3
import logging
from abc import ABC, abstractmethod
from datetime import datetime
from io import BytesIO
from typing import Optional
import pandas as pd
from dotenv import load_dotenv
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BaseIngestor(ABC):
    def __init__(self):
        self.today = datetime.today().strftime("%Y-%m-%d")

        # Local dev directories
        self.local_raw_dir = "data/raw"
        self.local_processed_dir = "data/processed"
        os.makedirs(self.local_raw_dir, exist_ok=True)
        os.makedirs(self.local_processed_dir, exist_ok=True)

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

    def save_raw(self, df: pd.DataFrame, name: str) -> str:
        path = os.path.join(self.local_raw_dir, f"{name}_raw_{self.today}.json")
        df.reset_index().to_json(path, orient="records", indent=2)
        logger.info(f"Saved raw data locally to {path}")
        return path

    def save_processed(self, df: pd.DataFrame, name: str) -> str:
        path = os.path.join(self.local_processed_dir, f"{name}_processed_{self.today}.json")
        with open(path, "w") as f:
            json.dump(df.to_dict(orient="records"), f, indent=2)
        logger.info(f"Saved processed data locally to {path}")
        return path

    def upload(self, df: pd.DataFrame, name: str, s3_folder: str, is_processed: bool = False) -> None:
        suffix = "processed" if is_processed else "raw"
        filename = f"{name}_{suffix}_{self.today}.json"
        s3_key = f"{s3_folder}/{filename}"

        buffer = BytesIO()
        df.reset_index().to_json(buffer, orient="records", indent=2)
        buffer.seek(0)

        self.s3.upload_fileobj(buffer, self.S3_BUCKET, s3_key)
        logger.info(f"Uploaded {suffix} data to s3://{self.S3_BUCKET}/{s3_key}")

    def run(self, name: str, save_s3: bool = True, save_local: bool = False):
        """Main ingestion pipeline logic"""
        # Load data
        raw_df = self.load_data()

        # Save raw (local + S3)
        if save_local:
            self.save_raw(raw_df, name)
        if save_s3:
            s3_raw_folder_name = f"{name}_raw"
            self.upload(raw_df, name, s3_raw_folder_name, is_processed=False)

        # Process data
        processed_df = self.process_data(raw_df)

        # Save processed (local + S3)
        if save_local:
            self.save_processed(processed_df, name)
        if save_s3:
            s3_processed_folder_name = f"{name}_processed"
            self.upload(processed_df, name, s3_processed_folder_name, is_processed=True)