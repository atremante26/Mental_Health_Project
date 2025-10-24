from pipeline.ingestion import BaseIngestor
import logging
import pandas as pd
import requests
import xml.etree.ElementTree as ET
from pathlib import Path
from urllib.parse import urlencode

# Configure logging
logger = logging.getLogger(__name__)

# US Population for calculating crude rates
POPULATION_BY_YEAR = {
    2018: 327_167_434,  
    2019: 328_239_523,  
    2020: 331_449_281,  
    2021: 331_893_745,  
    2022: 333_287_557,  
    2023: 334_914_895, 
    2024: 336_673_595,  
    2025: 342_694_430,  
    2026: 344_500_000,  # Projection
    2027: 346_300_000,  # Projection
    2028: 348_100_000,  # Projection
    2029: 349_900_000,  # Projection
    2030: 351_700_000   # Projection
}

class CDCIngestor(BaseIngestor):
    def __init__(self):
        super().__init__()
        self.api_url = "https://wonder.cdc.gov/controller/datarequest/D176"
        self.xml_request_path = Path(__file__).parent / "config/cdc_wonder_request.xml"  

    def load_data(self) -> pd.DataFrame:
        try:
            logger.info("Loading CDC WONDER request XML...")
            with open(self.xml_request_path, 'r') as f:
                xml_request = f.read()
            
            logger.info("Sending request to CDC WONDER API...")
            
            # Key insight from the repo: send XML as form-encoded data with 'request_xml' parameter
            data = {
                'request_xml': xml_request,
                'accept_datause_restrictions': 'true'
            }
            
            headers = {
                'Content-Type': 'application/x-www-form-urlencoded',
                'User-Agent': 'Mozilla/5.0'
            }
            
            response = requests.post(
                self.api_url,
                data=urlencode(data),
                headers=headers,
                timeout=120
            )
            
            logger.info(f"Response status: {response.status_code}")

            # Add these debug lines
            print(f"DEBUG: Response content:\n{response.text[:1000]}")
            
            response.raise_for_status()
            
            # Parse XML response
            root = ET.fromstring(response.content)
            
            # Extract data rows
            data_rows = []
            for row in root.findall('.//r'):
                row_data = {}
                for cell in row.findall('./c'):
                    label = cell.get('l', '')
                    value = cell.get('v', '')
                    row_data[label] = value
                data_rows.append(row_data)
            
            df = pd.DataFrame(data_rows)
            logger.info(f"Fetched {len(df)} records from CDC WONDER on {self.today}")
            return df
            
        except Exception as e:
            logger.error(f"Failed to fetch CDC WONDER data: {e}")
            raise
                    
    def process_data(self, df):
        processed_df = df.copy()
        
        # Parse month column (format: "Jan., 2018")
        if 'Month' in processed_df.columns:
            processed_df['date'] = pd.to_datetime(
                processed_df['Month'].str.replace('.', ''), 
                format='%b, %Y',
                errors='coerce'
            )
        
        # Convert deaths to numeric
        if 'Deaths' in processed_df.columns:
            processed_df['deaths'] = pd.to_numeric(
                processed_df['Deaths'].str.replace(',', ''), 
                errors='coerce'
            )
        
        # Get population for each row based on year
        processed_df['year'] = processed_df['date'].dt.year
        processed_df['population'] = processed_df['year'].map(
            lambda year: POPULATION_BY_YEAR.get(year, POPULATION_BY_YEAR[max(POPULATION_BY_YEAR.keys())])
        )
        processed_df['monthly_population'] = processed_df['population'] / 12

        # Calculate crude suicide rate per 100,000
        processed_df['suicide_rate'] = (
            (processed_df['deaths'] / processed_df['monthly_population']) * 100000
        ).round(1)
        
        # Select final columns
        processed_df = processed_df[['date', 'suicide_rate']]
        
        # Remove rows with missing data
        processed_df = processed_df.dropna(subset=['date'])
        
        # Sort by date
        processed_df = processed_df.sort_values('date')
        
        logger.info(f"Processed {len(processed_df)} data points")
        logger.info(f"Date range: {processed_df['date'].min()} to {processed_df['date'].max()}")
        
        return processed_df

    
if __name__ == "__main__":
    cdc_ingestor = CDCIngestor()
    cdc_ingestor.run("cdc", "cdc_suite", True, True)