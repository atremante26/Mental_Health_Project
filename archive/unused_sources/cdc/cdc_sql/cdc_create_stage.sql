-- Create the storage integration 
-- NOTE: Replace <your-account-id> and <your-role-name> with actual values
CREATE OR REPLACE STORAGE INTEGRATION s3_int
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::<your-account-id>:role/<your-role-name>'
  STORAGE_ALLOWED_LOCATIONS = ('s3://mental-health-project-pipeline/');

-- Create the external stage to the processed CDC folder in S3
CREATE OR REPLACE STAGE cdc_stage
  URL = 's3://mental-health-project-pipeline/cdc-processed/'
  STORAGE_INTEGRATION = s3_int;

-- Create the file format for JSON
CREATE OR REPLACE FILE FORMAT cdc_json_format
  TYPE = 'JSON'
  STRIP_OUTER_ARRAY = TRUE;