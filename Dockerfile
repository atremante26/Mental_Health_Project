# Use Python image with Airflow compatibility
FROM python:3.11-slim

# Set working directory
WORKDIR /opt/airflow

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy pipeline code
COPY pipeline/ ./pipeline/
COPY airflow/dags/ ./dags/
COPY gx/ ./gx/

# Create necessary directories for Airflow
RUN mkdir -p logs plugins temp

# Set environment variables for Airflow
ENV AIRFLOW_HOME=/opt/airflow
ENV PYTHONPATH=/opt/airflow
ENV AIRFLOW__CORE__DAGS_FOLDER=/opt/airflow/dags
ENV AIRFLOW__CORE__LOAD_EXAMPLES=false
ENV AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=sqlite:////tmp/airflow.db
ENV AIRFLOW__CORE__EXECUTOR=SequentialExecutor

# Initialize Airflow database
RUN airflow db init

# Set the default command to run a specific DAG
CMD ["sh", "-c", "airflow dags test ingestion_dag $(date +%Y-%m-%d)"]