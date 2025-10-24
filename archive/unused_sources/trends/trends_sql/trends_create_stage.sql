CREATE OR REPLACE STAGE google_trends_stage
    URL = 's3://mental-health-project-pipeline/trends_processed/'
    STORAGE_INTEGRATION = s3_int;

CREATE OR REPLACE FILE FORMAT google_trends_json_format
  TYPE = 'JSON'
  STRIP_OUTER_ARRAY = TRUE;