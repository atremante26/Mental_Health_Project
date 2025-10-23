-- Use NEWS Schema
USE SCHEMA MENTAL_HEALTH.NEWS;

-- Create a temporary table to hold new dates
CREATE OR REPLACE TEMP TABLE TEMP_NEWS_DATES(date DATE);

-- Extract dates from the incoming JSON into the temp table
COPY INTO TEMP_NEWS_DATES
FROM (
  SELECT TO_DATE($1:date::STRING)
  FROM @news_stage/news_processed_{{ ds_nodash }}.json (file_format => news_json_format)
);

-- Delete any existing rows in the main table that match incoming dates
DELETE FROM MENTAL_HEALTH.NEWS.NEWS_PROCESSED
WHERE date IN (SELECT date FROM TEMP_NEWS_DATES);

-- Load the new data
COPY INTO MENTAL_HEALTH.NEWS.NEWS_PROCESSED
FROM @news_stage/news_processed_{{ ds_nodash }}.json
FILE_FORMAT = (FORMAT_NAME = 'news_json_format')
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

-- Remove any corrupted/incomplete rows
DELETE FROM MENTAL_HEALTH.NEWS.NEWS_PROCESSED
WHERE date IS NULL;

-- Clean up
DROP TABLE IF EXISTS TEMP_NEWS_DATES;