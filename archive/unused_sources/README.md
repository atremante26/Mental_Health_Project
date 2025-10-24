# Archived Data Sources

This directory contains data source integrations that were attempted but not used in production.

## CDC WONDER API
**Status:** Archived  
**Reason:** API consistently returned 429/500 errors. CDC WONDER does not have reliable programmatic access.  
**Replacement:** News API for tracking mental health media coverage  
**Files:** `cdc/`

## Google Trends API
**Status:** Archived  
**Reason:** Rate limiting issues (429 errors) despite implementing delays and retry logic.  
**Potential Future:** Could revisit with proxies or monthly ingestion schedule  
**Files:** `trends/`

## Active Sources
- Reddit (community discussions)
- News API (media coverage)