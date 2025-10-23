-- Create stage for NEWS data in S3
CREATE OR REPLACE STAGE news_stage
    URL = 's3://mental-health-project-pipeline/news_processed/'
    STORAGE_INTEGRATION = s3_int;

CREATE OR REPLACE FILE FORMAT news_json_format
    TYPE = 'JSON'
    STRIP_OUTER_ARRAY = TRUE;