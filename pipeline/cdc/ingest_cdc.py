import os
import pandas as pd

# Download source
CSV_URL = "https://data.cdc.gov/api/views/8pt5-q6wp/rows.csv?accessType=DOWNLOAD"

# Define output paths
BASE_DIR = os.path.dirname(__file__)
RAW_DIR = os.path.join(BASE_DIR, "../../data/raw")
PROCESSED_DIR = os.path.join(BASE_DIR, "../../data/processed")

# Ensure directories exist
os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(PROCESSED_DIR, exist_ok=True)

def clean_cdc_data():
    df = pd.read_csv(CSV_URL)

    # Apply filter
    df = df[(df['Group'] == 'National Estimate') & (df['Subgroup'] == 'United States')]

    # Select columns
    df = df[['Time Period Start Date', 'Indicator', 'Value']]

    # Pivot
    df = df.pivot(index='Time Period Start Date', columns='Indicator', values='Value')
    df = df.rename_axis(None, axis=1).reset_index()
    df = df.rename(columns={'Time Period Start Date': 'date'})

    # Rename
    df = df.rename(columns={
        'Symptoms of Anxiety Disorder': 'anxiety',
        'Symptoms of Depressive Disorder': 'depression',
        'Symptoms of Anxiety or Depressive Disorder': 'anxiety_or_depression'
    })

    # Drop missing
    df = df.dropna()

    # Round values
    for col in ['anxiety', 'depression', 'anxiety_or_depression']:
        if col in df:
            df[col] = df[col].round(1)

    # Save to JSON
    df.to_json(os.path.join(PROCESSED_DIR, "cdc_timeseries.json"), orient="records", indent=2)


if __name__ == "__main__":
    clean_cdc_data()
