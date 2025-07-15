-- Delete rows from the CDC_PROCESSED table that match dates in the new file
DELETE FROM MENTAL_HEALTH.CDC.CDC_PROCESSED
WHERE date IN (
  SELECT $1:date::DATE
  FROM @cdc_stage/cdc-processed/cdc_processed_{{ ds_nodash }}.json
  (FILE_FORMAT => 'cdc_json_format')
);

-- Load the new data into the CDC_PROCESSED table
COPY INTO MENTAL_HEALTH.CDC.CDC_PROCESSED
FROM @cdc_stage/cdc-processed/cdc_processed_{{ ds_nodash }}.json
FILE_FORMAT = (FORMAT_NAME = 'cdc_json_format')
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

-- Remove any rows where the date failed to load (safety check)
DELETE FROM MENTAL_HEALTH.CDC.CDC_PROCESSED WHERE date IS NULL;