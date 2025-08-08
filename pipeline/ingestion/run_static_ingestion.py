from static_ingestor import (
    MentalHealthInTechSurveyIngestor,
    WHOSuicideStatisticsIngestor, 
    MentalHealthCareInLast4WeeksIngestor,
    SuicideByDemographicsIngestor
)

def run_all_static_sources():
    MentalHealthInTechSurveyIngestor().run("TECH_SURVEY")
    WHOSuicideStatisticsIngestor().run("WHO_SUICIDE")
    MentalHealthCareInLast4WeeksIngestor().run("MENTAL_HEALTH_CARE")
    SuicideByDemographicsIngestor().run("SUICIDE_DEMOGRAPHICS")

if __name__ == "__main__":
    run_all_static_sources()