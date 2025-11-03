from datetime import datetime
from pathlib import Path
import os
import hashlib
import mimetypes
import requests

from airflow import DAG
from airflow.operators.python import PythonOperator, get_current_context, ShortCircuitOperator
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
YEARS = [y for y in range(2015, 2026)]


def _download_file(url: str, dest_path: str):
    ctx = get_current_context()
    dest = Path(dest_path)
    dest.parent.mkdir(parents=True, exist_ok=True)
    force = ctx.get("dag_run").conf.get("force", False) if ctx.get("dag_run") else False
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


def _cog_exists_in_db(filename: str) -> bool:
    hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1
            FROM raster_objects
            WHERE stage = 'cog' AND filename = %s
            ORDER BY id DESC
            LIMIT 1
            """,
            (filename,),
        )
        return cur.fetchone() is not None


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


def _store_lo(stage: str, file_path: str):
    ctx = get_current_context()
    ti = ctx.get("ti")
    run_id = ti.run_id if ti else "unknown"
    _ensure_db_objects_exist()
    _store_file_as_large_object(stage, file_path, run_id)


def _year_paths(year: int):
    raw = str(DATA_DIR / f"zaf_pop_{year}_CN_100m_R2025A_v1.tif")
    wm = str(DATA_DIR / f"zaf_pop_{year}_CN_100m_R2025A_v1_3857.tif")
    cog = str(DATA_DIR / f"zaf_pop_{year}_CN_100m_R2025A_v1_cog.tif")
    return raw, wm, cog


def _year_url(year: int) -> str:
    return f"https://data.worldpop.org/GIS/Population/Global_2015_2030/R2025A/{year}/ZAF/v1/100m/constrained/zaf_pop_{year}_CN_100m_R2025A_v1.tif"


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

    for year in YEARS:
        raw_path, wm_path, cog_path = _year_paths(year)
        url = _year_url(year)

        def _check_missing(year: int, expected_cog: str) -> bool:
            _ensure_db_objects_exist()
            exists = _cog_exists_in_db(Path(expected_cog).name)
            if exists:
                print(f"COG already in DB for {year}: {expected_cog} -> skipping year")
                return False
            return True

        check_missing = ShortCircuitOperator(
            task_id=f"check_missing_{year}",
            python_callable=lambda y=year, c=cog_path: _check_missing(y, c),
        )

        download_raw = PythonOperator(
            task_id=f"download_raw_{year}",
            python_callable=_download_file,
            op_kwargs={"url": url, "dest_path": raw_path},
        )

        store_raw = PythonOperator(
            task_id=f"store_raw_{year}",
            python_callable=_store_lo,
            op_kwargs={"stage": "raw", "file_path": raw_path},
        )

        warp_web_mercator = BashOperator(
            task_id=f"warp_web_mercator_{year}",
            bash_command=(
                "gdalwarp -overwrite -t_srs EPSG:3857 -r bilinear -multi "
                "-srcnodata -99999 -dstnodata -99999 "
                "-co COMPRESS=LZW "
                "{raw} {wm}"
            ).format(raw=raw_path, wm=wm_path),
        )

        store_wm = PythonOperator(
            task_id=f"store_wm_{year}",
            python_callable=_store_lo,
            op_kwargs={"stage": "warped_web_mercator", "file_path": wm_path},
        )

        to_cog = BashOperator(
            task_id=f"to_cog_{year}",
            bash_command=(
                "set -euo pipefail; "
                "rm -f {cog}; "
                "gdal_translate -of COG "
                "-co COMPRESS=ZSTD "
                "-co NUM_THREADS=ALL_CPUS "
                "-co OVERVIEW_RESAMPLING=AVERAGE "
                "{wm} {cog}"
            ).format(wm=wm_path, cog=cog_path),
        )

        store_cog = PythonOperator(
            task_id=f"store_cog_{year}",
            python_callable=_store_lo,
            op_kwargs={"stage": "cog", "file_path": cog_path},
        )

        validate_cog = BashOperator(
            task_id=f"validate_cog_{year}",
            bash_command=(
                "set -euo pipefail; "
                "gdalinfo {cog} | "
                r"grep -i -E Coordinate"
            ).format(cog=cog_path),
        )

        ensure_env >> check_missing >> download_raw >> store_raw >> warp_web_mercator >> store_wm >> to_cog >> store_cog >> validate_cog