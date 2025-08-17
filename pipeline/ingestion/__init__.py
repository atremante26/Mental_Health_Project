from .base_ingestor import BaseIngestor
from .ingest_trends import GoogleTrendsIngestor
from .ingest_reddit import RedditIngestor
from .ingest_cdc import CDCIngestor
from .static_ingestor import (
    StaticIngestor,
    MentalHealthInTechSurveyIngestor,
    WHOSuicideStatisticsIngestor,
    MentalHealthCareInLast4WeeksIngestor,
    SuicideByDemographicsIngestor
)
from .validator import Validator