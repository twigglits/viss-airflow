FROM apache/airflow:2.9.3

# Install system deps (GDAL CLI tools)
USER root
RUN rm -f /etc/apt/sources.list.d/mariadb*.list \
 && apt-get update \
 && apt-get install -y --no-install-recommends \
    gdal-bin \
    curl \
 && rm -rf /var/lib/apt/lists/*

# Install Airflow providers needed by the DAGs
USER airflow
COPY viss-airflow/requirements.txt /opt/airflow/requirements.txt
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt

# Keep defaults from base image
