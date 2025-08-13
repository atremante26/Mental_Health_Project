from static_ingestor import (
    MentalHealthInTechSurveyIngestor,
    WHOSuicideStatisticsIngestor, 
    MentalHealthCareInLast4WeeksIngestor,
    SuicideByDemographicsIngestor
)

def run_all_static_sources():
    MentalHealthInTechSurveyIngestor().run("tech_survey_processed", "TECH_SURVEY")
    WHOSuicideStatisticsIngestor().run("who_suicide_processed", "WHO_SUICIDE")
    MentalHealthCareInLast4WeeksIngestor().run("mental_health_care_processed", "MENTAL_HEALTH_CARE")
    SuicideByDemographicsIngestor().run("suicide_demographics_processed", "SUICIDE_DEMOGRAPHICS")

if __name__ == "__main__":
    run_all_static_sources()