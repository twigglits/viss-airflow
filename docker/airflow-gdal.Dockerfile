FROM apache/airflow:2.9.3

# Install system deps (GDAL CLI tools)
USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
    gdal-bin \
 && rm -rf /var/lib/apt/lists/*

# Install Airflow providers needed by the DAGs
USER airflow
RUN pip install --no-cache-dir \
    apache-airflow-providers-postgres

# Keep defaults from base image
