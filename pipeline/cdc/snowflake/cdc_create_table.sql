-- Create table for CDC processed data
CREATE OR REPLACE TABLE MENTAL_HEALTH.CDC.CDC_PROCESSED (
    date DATE,
    anxiety FLOAT,
    depression FLOAT,
    anxiety_or_depression FLOAT
);