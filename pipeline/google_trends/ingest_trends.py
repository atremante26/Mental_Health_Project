from base_ingestor import BaseIngestor
import logging
from pytrends.request import TrendReq
import pandas as pd

logger = logging.getLogger(__name__)

class GoogleTrendsIngestor(BaseIngestor):
    def __init__(self):
        super().__init__()
        self.pytrends = TrendReq(hl='en-US', tz=360)
        self.keyword_groups = [
            ["mental health", "depression", "anxiety", "therapy", "suicide prevention"],
            ["bipolar disorder", "OCD", "ADHD", "CPTSD", "BPD"]
        ]
    
    def load_data(self):
        # Collect and combine all raw data
        raw_data_frames = []

        for group in self.keyword_groups:
            try:
                self.pytrends.build_payload(group, timeframe='today 3-m', geo='US')
                group_data = self.pytrends.interest_over_time()
                if not group_data.empty:
                    raw_data_frames.append(group_data)
            except:
                logger.info(f"Failed to load Google Trends data for group {group} on {self.today}")

        # Merge all results
        if not raw_data_frames:
            logger.info("No data returned from Google Trends")
            return pd.DataFrame()

        raw_combined = pd.concat(raw_data_frames, axis=1)
        raw_combined = raw_combined.loc[:, ~raw_combined.columns.duplicated()]
        return raw_combined

    def process_data(self, df):
        # Drop 'isPartial' if present
        if 'isPartial' in df.columns:
            df = df.drop(columns=['isPartial'])

        # Process data to long format
        processed = df.reset_index().melt(id_vars='date', var_name='keyword', value_name='interest')
        processed['date'] = processed['date'].dt.strftime('%Y-%m-%d')
        return processed
    
if __name__ == "__main__":
    trends_ingestor = GoogleTrendsIngestor()
    trends_ingestor.run("trends", True, True)