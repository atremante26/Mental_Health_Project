from pipeline.ingestion import BaseIngestor
import logging
import pandas as pd

# Configure logging
logger = logging.getLogger(__name__)

class CDCIngestor(BaseIngestor):
    def __init__(self):
        super().__init__()
        self.csv_url = "https://data.cdc.gov/api/views/8pt5-q6wp/rows.csv?accessType=DOWNLOAD"

    def load_data(self) -> pd.DataFrame:
        try:
            df = pd.read_csv(self.csv_url)
        except:
            logger.info(f"Failed to read CSV data on {self.today} from {self.csv_url}")
        
        return df

    def process_data(self, df):
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
            'Symptoms of Anxiety Disorder or Depressive Disorder': 'anxiety_or_depression'
        })
        df = df.dropna()

        for col in ['anxiety', 'depression', 'anxiety_or_depression']:
            if col in df:
                df[col] = df[col].round(1)
        return df
    
if __name__ == "__main__":
    cdc_ingestor = CDCIngestor()
    cdc_ingestor.run("cdc", True, True)