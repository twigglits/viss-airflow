from datetime import datetime
from pathlib import Path
import os
import hashlib
import mimetypes
import requests

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Inputs (adjust as needed)
RAW_URL = "https://data.worldpop.org/GIS/Population/Global_2015_2030/R2025A/2025/ZAF/v1/100m/constrained/zaf_pop_2025_CN_100m_R2025A_v1.tif"

# Storage paths inside containers (use a mounted volume in compose for persistence)
# Prefer mounting ./data -> /opt/airflow/data in docker-compose; /tmp works but is ephemeral
DATA_DIR = Path(os.environ.get("AIRFLOW_DATA_DIR", "/opt/airflow/data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)

RAW_PATH = str(DATA_DIR / "zaf_pop_2025_CN_100m_R2025A_v1.tif")
WM_PATH  = str(DATA_DIR / "zaf_pop_2025_CN_100m_R2025A_v1_3857.tif")
COG_PATH = str(DATA_DIR / "zaf_pop_2025_CN_100m_R2025A_v1_cog.tif")

# Target Postgres connection (configure in Airflow Admin -> Connections)
PG_CONN_ID = "viss_data_db"


def _download_raw(**context):
    url = RAW_URL
    dest = Path(RAW_PATH)
    dest.parent.mkdir(parents=True, exist_ok=True)
    # If exists, skip; set force=True via DAG run conf to re-download
    force = context.get("dag_run").conf.get("force", False) if context.get("dag_run") else False
    if dest.exists() and not force:
        print(f"Exists, skipping: {dest}")
        return str(dest)
    with requests.get(url, stream=True, timeout=60) as r:
        r.raise_for_status()
        tmp = dest.with_suffix(dest.suffix + ".partial")
        with open(tmp, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)
        os.replace(tmp, dest)
    print(f"Downloaded: {dest} ({dest.stat().st_size} bytes)")
    return str(dest)


def _ensure_tools():
    """Optional: gently check that GDAL tools exist; fail with clear message if not."""
    import shutil
    missing = [t for t in ("gdalwarp", "gdal_translate", "gdalinfo") if shutil.which(t) is None]
    if missing:
        raise RuntimeError(f"GDAL tools missing: {missing}. Install GDAL in the Airflow worker or use DockerOperator.")


def _ensure_requests():
    try:
        import requests  # noqa
    except Exception as e:
        raise RuntimeError("Python package `requests` is required in the worker env") from e


def _ensure_env():
    """Top-level callable for PythonOperator: validate tools and Python deps."""
    _ensure_tools()
    _ensure_requests()


def _ensure_db_objects_exist():
    """Create the table to track raster objects if it doesn't already exist."""
    hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    sql = """
    CREATE TABLE IF NOT EXISTS raster_objects (
        id SERIAL PRIMARY KEY,
        stage TEXT NOT NULL,
        filename TEXT NOT NULL,
        run_id TEXT NOT NULL,
        sha256 TEXT,
        size_bytes BIGINT,
        content_type TEXT,
        lo_oid OID NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    """
    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql)
        conn.commit()


def _store_file_as_large_object(stage: str, file_path: str, run_id: str):
    """Store a local file into Postgres as a Large Object and record metadata.

    Uses psycopg2 large object API via PostgresHook connection.
    """
    hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    file_path = Path(file_path)
    filename = file_path.name
    content_type = mimetypes.guess_type(filename)[0] or "application/octet-stream"

    hasher = hashlib.sha256()
    size = 0

    with hook.get_conn() as conn, conn.cursor() as cur:
        # Create the large object and stream-write to avoid loading whole file in memory
        lo = conn.lobject(0, 'w')
        with open(file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(1024 * 1024), b''):
                lo.write(chunk)
                hasher.update(chunk)
                size += len(chunk)
        oid = lo.oid
        lo.close()

        cur.execute(
            """
            INSERT INTO raster_objects (stage, filename, run_id, sha256, size_bytes, content_type, lo_oid)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (stage, filename, run_id, hasher.hexdigest(), size, content_type, oid),
        )
        conn.commit()


def _store_raw(**context):
    ti = context.get("ti")
    run_id = ti.run_id if ti else "unknown"
    _ensure_db_objects_exist()
    _store_file_as_large_object("raw", RAW_PATH, run_id)


def _store_wm(**context):
    ti = context.get("ti")
    run_id = ti.run_id if ti else "unknown"
    _ensure_db_objects_exist()
    _store_file_as_large_object("warped_web_mercator", WM_PATH, run_id)


def _store_cog(**context):
    ti = context.get("ti")
    run_id = ti.run_id if ti else "unknown"
    _ensure_db_objects_exist()
    _store_file_as_large_object("cog", COG_PATH, run_id)


default_args = {
    "owner": "airflow",
    "retries": 1,
}

with DAG(
    dag_id="worldpop_ingest_cog",
    description="Download WorldPop raw TIFF -> warp to EPSG:3857 -> COG -> validate",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["worldpop", "raster", "cog"],
) as dag:

    ensure_env = PythonOperator(
        task_id="ensure_env",
        python_callable=_ensure_env,
    )

    download_raw = PythonOperator(
        task_id="download_raw",
        python_callable=_download_raw,
    )

    store_raw = PythonOperator(
        task_id="store_raw",
        python_callable=_store_raw,
    )

    warp_web_mercator = BashOperator(
        task_id="warp_web_mercator",
        bash_command=(
            "gdalwarp -t_srs EPSG:3857 -r bilinear -multi "
            "-srcnodata -99999 -dstnodata -99999 "
            "-co COMPRESS=LZW "
            f"{RAW_PATH} {WM_PATH}"
        ),
    )

    to_cog = BashOperator(
        task_id="to_cog",
        bash_command=(
            "gdal_translate -of COG "
            "-co COMPRESS=ZSTD "
            "-co NUM_THREADS=ALL_CPUS "
            "-co OVERVIEW_RESAMPLING=AVERAGE "
            f"{WM_PATH} {COG_PATH}"
        ),
    )

    validate_cog = BashOperator(
        task_id="validate_cog",
        bash_command=(
            "set -euo pipefail; "
            f"gdalinfo {COG_PATH} | "
            r"grep -i -E Coordinate"
        ),
    )

    store_wm = PythonOperator(
        task_id="store_wm",
        python_callable=_store_wm,
    )

    store_cog = PythonOperator(
        task_id="store_cog",
        python_callable=_store_cog,
    )

    # Chaining: ensure proper order and persist each artifact to Postgres
    ensure_env >> download_raw >> store_raw >> warp_web_mercator >> store_wm >> to_cog >> store_cog >> validate_cog