FROM apache/airflow:2.9.3

# Install system deps (GDAL CLI tools)
USER root
ENV DEBIAN_FRONTEND=noninteractive
ENV ACCEPT_EULA=Y
RUN rm -f /etc/apt/sources.list.d/mariadb*.list \
 && echo "ttf-mscorefonts-installer msttcorefonts/accepted-mscorefonts-eula boolean true" | debconf-set-selections \
 && apt-get update \
 && apt-get upgrade -y -o Dpkg::Options::="--force-confold" -o Dpkg::Options::="--force-confdef" \
 && apt-get install -y --no-install-recommends \
    gdal-bin \
    curl \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

# Install Airflow providers needed by the DAGs
USER airflow
COPY viss-airflow/requirements.txt /opt/airflow/requirements.txt
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt

# Keep defaults from base image
