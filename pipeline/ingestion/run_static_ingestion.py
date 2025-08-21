from static_ingestor import (
    MentalHealthInTechSurveyIngestor,
    WHOSuicideStatisticsIngestor, 
    MentalHealthCareInLast4WeeksIngestor,
    SuicideByDemographicsIngestor
)

def run_all_static_sources():
    MentalHealthInTechSurveyIngestor().run("tech_survey_processed", "tech_survey_suite", "TECH_SURVEY")
    WHOSuicideStatisticsIngestor().run("who_suicide_processed", "who_suicide_suite", "WHO_SUICIDE")
    MentalHealthCareInLast4WeeksIngestor().run("mental_health_care_processed", "mental_health_care_suite", "MENTAL_HEALTH_CARE")
    SuicideByDemographicsIngestor().run("suicide_demographics_processed", "suicide_demographics_suite", "SUICIDE_DEMOGRAPHICS")

if __name__ == "__main__":
    run_all_static_sources()