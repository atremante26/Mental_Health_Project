import os
import json
from datetime import datetime
from pytrends.request import TrendReq
import pandas as pd

# Configure pytrends
pytrends = TrendReq(hl='en-US', tz=360)

# Define mental health terms to track (split into groups of ≤5)
keyword_groups = [
    ["mental health", "depression", "anxiety", "therapy", "suicide prevention"],
    ["bipolar disorder", "OCD", "ADHD", "CPTSD", "BPD"]
]

# Collect and combine all raw data
raw_data_frames = []

for group in keyword_groups:
    try:
        pytrends.build_payload(group, timeframe='today 3-m', geo='US')
        group_data = pytrends.interest_over_time()
        if not group_data.empty:
            raw_data_frames.append(group_data)
    except Exception as e:
        print(f"Failed for group {group}: {e}")

# Merge all results
if not raw_data_frames:
    raise ValueError("No data returned from Google Trends.")

raw_combined = pd.concat(raw_data_frames, axis=1)
raw_combined = raw_combined.loc[:, ~raw_combined.columns.duplicated()]

# Use today’s date for file naming
today = datetime.today().strftime("%Y-%m-%d")

# Ensure folders exist
raw_dir = "data/raw"
processed_dir = "data/processed"
os.makedirs(raw_dir, exist_ok=True)
os.makedirs(processed_dir, exist_ok=True)

# Save raw data
raw_path = os.path.join(raw_dir, f"trends_raw_{today}.json")
raw_combined.reset_index().to_json(raw_path, orient="records", indent=2)

# Drop 'isPartial' if present
if 'isPartial' in raw_combined.columns:
    raw_combined = raw_combined.drop(columns=['isPartial'])

# Process data to long format
processed = raw_combined.reset_index().melt(id_vars='date', var_name='keyword', value_name='interest')
processed['date'] = processed['date'].dt.strftime('%Y-%m-%d')

# Save processed data
processed_path = os.path.join(processed_dir, f"trends_processed_{today}.json")
with open(processed_path, "w") as f:
    json.dump(processed.to_dict(orient="records"), f, indent=2)