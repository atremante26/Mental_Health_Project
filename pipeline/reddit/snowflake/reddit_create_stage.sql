CREATE OR REPLACE STAGE reddit_stage
    URL = 's3://mental-health-project-pipeline/reddit-processed/'
    STORAGE_INTEGRATION = s3_int;

CREATE OR REPLACE FILE FORMAT reddit_json_format
  TYPE = 'JSON'
  STRIP_OUTER_ARRAY = TRUE;