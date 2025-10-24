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
        # Simplified - just basic TrendReq without custom retry params
        self.pytrends = TrendReq(hl='en-US', tz=360)
        # Reduced to just one group of 5 keywords
        self.keyword_groups = [
            ["mental health", "depression", "anxiety", "therapy", "counseling"]
        ]
    
    def load_data(self):
        all_data = []
        
        for group in self.keyword_groups:
            for attempt in range(3):  # Retry up to 3 times
                try:
                    logger.info(f"Attempting to load Google Trends data for group {group} (attempt {attempt + 1})")
                    
                    # Longer initial delay to be respectful
                    delay = random.uniform(10, 20)
                    logger.info(f"Waiting {delay:.1f} seconds before request...")
                    time.sleep(delay)
                    
                    # Build payload with shorter timeframe
                    self.pytrends.build_payload(
                        group, 
                        cat=0, 
                        timeframe='today 3-m',  # Last 3 months instead of 12
                        geo='US',
                        gprop=''
                    )
                    
                    # Get interest over time
                    data = self.pytrends.interest_over_time()
                    
                    if not data.empty:
                        all_data.append(data)
                        logger.info(f"Successfully loaded {len(data)} rows for group {group}")
                        break
                    else:
                        logger.warning(f"Empty data returned for group {group}")
                        
                except Exception as e:
                    logger.warning(f"Attempt {attempt + 1} failed for group {group}: {str(e)}")
                    if attempt < 2:  # Not the last attempt
                        retry_delay = random.uniform(30, 60)  # Much longer delay before retry
                        logger.info(f"Waiting {retry_delay:.1f} seconds before retry...")
                        time.sleep(retry_delay)
                    else:
                        logger.error(f"All attempts failed for group {group}")
                    continue
        
        if not all_data:
            logger.error("No data returned from Google Trends after all attempts")
            return pd.DataFrame()
        
        # Combine all data
        combined_data = pd.concat(all_data, axis=1)
        logger.info(f"Combined data shape: {combined_data.shape}")
        return combined_data

    def process_data(self, df):
        # Drop 'isPartial' if present
        if 'isPartial' in df.columns:
            df = df.drop(columns=['isPartial'])

        # Process data to long format
        processed = df.reset_index().melt(id_vars='date', var_name='keyword', value_name='interest')
        processed['date'] = processed['date'].dt.strftime('%Y-%m-%d')
        
        logger.info(f"Processed {len(processed)} data points")
        return processed
    
if __name__ == "__main__":
    trends_ingestor = GoogleTrendsIngestor()
    trends_ingestor.run("trends", "trends_suite", True, True)