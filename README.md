# MindPulseAI: Real-Time Analytics for Mental Health

> A data platform that tracks evolving mental health trends across online discussions and public health data, using machine learning to identify patterns in anxiety and depression.

[![Status](badge)] [![Python](badge)] [![AWS](badge)]

## Table of Contents
- [Overview](#overview)
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Data Pipeline](#data-pipeline)
- [ML Models](#ml-models)
- [Infrastructure](#infrastructure)
- [Setup & Installation](#setup--installation)
- [Technical Decisions](#technical-decisions)
- [Challenges & Solutions](#challenges--solutions)
- [Current Status & Roadmap](#current-status--roadmap)
- [Cost Breakdown](#cost-breakdown)

## Overview

Mental health data exists across multiple scattered sources - social media discussions, public health surveys, and search trends. This platform brings these sources together into a unified data warehouse, validates data quality, and uses machine learning to identify patterns that would be invisible in any single source.

### Currently Completed

- ✅ **Automated Data Pipeline**: Weekly ingestion from 3 live APIs (Reddit, CDC, Google Trends)
- ✅ **Data Validation**: 9 Great Expectations suites ensuring data quality
- ✅ **Data Warehouse**: 7 datasets in Snowflake (3 live, 4 static)
- ✅ **Serverless Architecture**: Runs on AWS ECS Fargate (cost-friendly)
- ✅ **CI/CD Pipeline**: Automated deployment via GitHub Actions
- ✅ **ML Models**: 4 models validated on production data

### Active Development

- 🚧 Integrating ML models into Airflow for batch predictions
- 🚧 API endpoints for model inference
- 🚧 Interactive dashboard frontend

## Architecture

The platform follows a serverless, event-driven architecture for cost efficiency and scalability.

### Data Flow
```mermaid
graph TB
    A[EventBridge Weekly Cron] --> B[AWS ECS Fargate Task]
    B --> C[Airflow DAG Orchestration]
    
    C --> D1[Reddit API Ingestion]
    C --> D2[CDC Survey Ingestion]
    C --> D3[Google Trends Ingestion]
    
    D1 --> E[Great Expectations Validation]
    D2 --> E
    D3 --> E
    
    E --> F[AWS S3 Raw Data]
    F --> G[AWS S3 Processed Data]
    G --> H[Snowflake Data Warehouse]
    
    H --> I[ML Analysis Notebooks]
    I --> J1[Clustering]
    I --> J2[Forecasting]
    I --> J3[LLM Insights]
    I --> J4[Recommendations]
    
    K[GitHub Push] --> L[GitHub Actions]
    L --> M[Docker Build]
    M --> N[Amazon ECR]
    N --> B
    
    O[CloudWatch Logs] -.-> B

