import os
import pandas as pd
import boto3
from datetime import datetime
from dotenv import load_dotenv

# Load AWS credentials if running locally
load_dotenv()

# Constants
CSV_URL = "https://data.cdc.gov/api/views/8pt5-q6wp/rows.csv?accessType=DOWNLOAD"
BASE_DIR = os.path.dirname(__file__)
RAW_DIR = os.path.join(BASE_DIR, "../../data/raw")
PROCESSED_DIR = os.path.join(BASE_DIR, "../../data/processed")
os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(PROCESSED_DIR, exist_ok=True)

# AWS S3 Config
S3_BUCKET = "mental-health-project-pipeline"
s3 = boto3.client(
    "s3",
    aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
    region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
)

def upload_to_s3(local_path, s3_key):
    try:
        s3.upload_file(local_path, S3_BUCKET, s3_key)
    except Exception as e:
        print(f"S3 upload failed: {e}")

def clean_cdc_data():
    today = datetime.today().strftime("%Y-%m-%d")

    # Download
    df = pd.read_csv(CSV_URL)

    # Save raw CSV locally
    raw_filename = f"cdc_raw_{today}.csv"
    raw_local_path = os.path.join(RAW_DIR, raw_filename)
    df.to_csv(raw_local_path, index=False)
    upload_to_s3(raw_local_path, f"raw/{raw_filename}")

    # Filter
    df = df[(df['Group'] == 'National Estimate') & (df['Subgroup'] == 'United States')]
    df = df[['Time Period Start Date', 'Indicator', 'Value']]

    # Pivot
    df = df.pivot(index='Time Period Start Date', columns='Indicator', values='Value')
    df = df.rename_axis(None, axis=1).reset_index()
    df = df.rename(columns={'Time Period Start Date': 'date'})
    df = df.rename(columns={
        'Symptoms of Anxiety Disorder': 'anxiety',
        'Symptoms of Depressive Disorder': 'depression',
        'Symptoms of Anxiety or Depressive Disorder': 'anxiety_or_depression'
    })
    df = df.dropna()

    for col in ['anxiety', 'depression', 'anxiety_or_depression']:
        if col in df:
            df[col] = df[col].round(1)

    # Step 5: Save processed JSON
    processed_filename = f"cdc_timeseries_{today}.json"
    processed_local_path = os.path.join(PROCESSED_DIR, processed_filename)
    df.to_json(processed_local_path, orient="records", indent=2)
    upload_to_s3(processed_local_path, f"processed/{processed_filename}")

if __name__ == "__main__":
    clean_cdc_data()
