# viss-airflow

An open source repository to host Airflow DAGs that transform publicly available population statistics into a standardized format.

This repo is preconfigured to run Apache Airflow via Docker Compose, giving you an Airflow Web UI at http://localhost:8081.

## Prerequisites

- Docker and Docker Compose
- Linux/macOS (Windows WSL2 should also work)

## Quick Start

1. Create the `.env` file with your UID (recommended to match host permissions):

   ```bash
   echo "AIRFLOW_UID=$(id -u)" > .env
   ```

   If you skip this step, the default `AIRFLOW_UID=50000` from the included `.env` will be used.

2. Initialize Airflow metadata DB and create an admin user (one-time):

   ```bash
   docker compose up airflow-init
   ```

3. Start the Airflow services (webserver, scheduler, triggerer, postgres):

   ```bash
   docker compose up -d
   ```

4. Open the Airflow Web UI:

   - URL: http://localhost:8081
   - Username: `airflow`
   - Password: `airflow`

5. Stop the stack when done:

   ```bash
   docker compose down
   ```

To view logs:

```bash
docker compose logs -f airflow-webserver
docker compose logs -f airflow-scheduler
```

## Repository Structure

```
.
├── dags/                    # Place your DAG .py files here
│   └── example_viss_dag.py  # Sample DAG to verify the setup
├── plugins/                 # Custom plugins (optional)
├── logs/                    # Airflow logs (mounted volume)
├── requirements.txt         # Python deps installed into containers
├── docker-compose.yaml      # Airflow stack definition
├── .env                     # AIRFLOW_UID for file permissions
└── README.md
```

## Adding DAGs

- Add your DAG files under `dags/`. Airflow will auto-detect changes within ~1 minute.
- Use the Airflow Web UI to trigger runs manually, or set a `schedule_interval` in your DAG.

## Installing Providers or Python Libraries

- Add packages to `requirements.txt`. They will be installed into the Airflow containers on startup.
- Example:

  ```
  apache-airflow-providers-postgres>=5.10.0
  pandas>=2.0
  ```

## Common Commands

- Restart services after changing dependencies:

  ```bash
  docker compose down && docker compose up -d --build
  ```

- Remove containers, networks, and volumes (CAUTION: removes Postgres data):

  ```bash
  docker compose down -v
  ```

## Troubleshooting

- If the UI is not accessible, check container health and logs:

  ```bash
  docker compose ps
  docker compose logs --since=1m airflow-webserver
  ```

- Ensure ports `8081` (web) and `5432` (Postgres) are free on your host.
- If port 8081 is busy, edit `docker-compose.yaml` and change the mapping under `airflow-webserver` from `"8081:8080"` to another free port (e.g., `"8090:8080"`).
- If you change your UID, regenerate `.env` and recreate containers.

## Geotiff ETL Pipeline

Currently it's a two step process. First step is to convert the downloaded .tif file to Web Mercator. (Not sure yet what the compression argument does yet, whether lossy or lossless)

```bash
gdalwarp -t_srs EPSG:3857 -r bilinear -multi \
  -srcnodata -99999 -dstnodata -99999 \
  -co COMPRESS=LZW \
  /home/jeannaude/epi/viss-frontend/frontend/zaf_pop_2025_CN_100m_R2025A_v1.tif \
  /home/jeannaude/epi/viss-frontend/frontend/zaf_pop_2025_CN_100m_R2025A_v1_3857.tif
```

Second step is to convert the .tif file to a COG. (Not sure yet what the compression argument does yet, whether lossy or lossless.)
```bash
gdal_translate -of COG \
  -co COMPRESS=ZSTD \
  -co NUM_THREADS=ALL_CPUS \
  -co OVERVIEW_RESAMPLING=AVERAGE \
  /home/jeannaude/epi/viss-frontend/frontend/zaf_pop_2025_CN_100m_R2025A_v1_3857.tif \
  /home/jeannaude/epi/viss-frontend/frontend/zaf_pop_2025_CN_100m_R2025A_v1_cog.tif
  ```

We can then vailidate if it's a healthy COG with:
```bash
gdalinfo /home/jeannaude/epi/viss-frontend/frontend/zaf_pop_2025_CN_100m_R2025A_v1_cog.tif | \
  grep -i -E 'coordinate|tiling|overviews|compression|NoData'
```

(Optional)

```bash
docker run --rm -v /home/jeannaude/epi/viss-frontend/frontend:/data osgeo/gdal:alpine-small-latest \
  gdalwarp -t_srs EPSG:3857 -r bilinear -multi \
  -srcnodata -99999 -dstnodata -99999 \
  -co COMPRESS=LZW \
  /data/zaf_pop_2025_CN_100m_R2025A_v1.tif \
  /data/zaf_pop_2025_CN_100m_R2025A_v1_3857.tif
  

docker run --rm -v /home/jeannaude/epi/viss-frontend/frontend:/data osgeo/gdal:alpine-small-latest \
  gdal_translate -of COG \
  -co COMPRESS=ZSTD \
  -co NUM_THREADS=ALL_CPUS \
  -co OVERVIEW_RESAMPLING=AVERAGE \
  /data/zaf_pop_2025_CN_100m_R2025A_v1_3857.tif \
  /data/zaf_pop_2025_CN_100m_R2025A_v1_cog.tif
  

docker run --rm -v /home/jeannaude/epi/viss-frontend/frontend:/data osgeo/gdal:alpine-small-latest \
  gdalinfo /data/zaf_pop_2025_CN_100m_R2025A_v1_cog.tif | \
  grep -i -E 'coordinate|tiling|overviews|compression|NoData'
```
