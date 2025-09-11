from pipeline.ingestion import BaseIngestor
import logging
from pytrends.request import TrendReq
import pandas as pd
import time
import random

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
        all_data = []
        
        for group in self.keyword_groups:
            for attempt in range(3):  # Retry up to 3 times
                try:
                    logger.info(f"Attempting to load Google Trends data for group {group} (attempt {attempt + 1})")
                    
                    # Add random delay to avoid rate limiting
                    time.sleep(random.uniform(2, 5))
                    
                    # Build payload with date range
                    self.pytrends.build_payload(
                        group, 
                        cat=0, 
                        timeframe='today 12-m',  # Last 12 months
                        geo='CA', # Try region with less traffic (Canada)
                        gprop=''
                    )
                    
                    # Get interest over time
                    data = self.pytrends.interest_over_time()
                    
                    if not data.empty:
                        all_data.append(data)
                        logger.info(f"Successfully loaded data for group {group}")
                        break
                    else:
                        logger.warning(f"Empty data returned for group {group}")
                        
                except Exception as e:
                    logger.warning(f"Attempt {attempt + 1} failed for group {group}: {e}")
                    if attempt < 2:  # Not the last attempt
                        time.sleep(random.uniform(5, 10))  # Longer delay before retry
                    continue
        
        if not all_data:
            logger.error("No data returned from Google Trends")
            return pd.DataFrame()
        
        # Combine all data
        combined_data = pd.concat(all_data, axis=1)
        return combined_data

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
    trends_ingestor.run("trends", "trends_suite", True, True)