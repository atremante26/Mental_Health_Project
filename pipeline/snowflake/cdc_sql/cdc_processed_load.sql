-- Create a temporary table to hold new dates
CREATE OR REPLACE TEMP TABLE TEMP_DATES(date DATE);

-- Extract dates from the incoming JSON into the temp table
COPY INTO TEMP_DATES
FROM (
  SELECT TO_DATE($1:date::STRING)
  FROM @cdc_stage/cdc_processed_{{ ds_nodash }}.json (file_format => cdc_json_format)
);

-- Delete any existing rows in the main table that match incoming dates
DELETE FROM MENTAL_HEALTH.CDC.CDC_PROCESSED
WHERE date IN (SELECT date FROM TEMP_DATES);

-- Load the new data
COPY INTO MENTAL_HEALTH.CDC.CDC_PROCESSED
FROM @cdc_stage/cdc_processed_{{ ds_nodash }}.json
FILE_FORMAT = (FORMAT_NAME = 'cdc_json_format')
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

-- Remove any corrupted/incomplete rows
DELETE FROM MENTAL_HEALTH.CDC.CDC_PROCESSED
WHERE date IS NULL;

-- Clean up
DROP TABLE IF EXISTS TEMP_DATES;
