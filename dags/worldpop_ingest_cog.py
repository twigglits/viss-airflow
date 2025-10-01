from datetime import datetime
from pathlib import Path
import os
import requests

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# Inputs (adjust as needed)
RAW_URL = "https://data.worldpop.org/GIS/Population/Global_2015_2030/R2025A/2025/ZAF/v1/100m/constrained/zaf_pop_2025_CN_100m_R2025A_v1.tif"

# Local targets (matching your current project layout)
RAW_PATH = "/home/jeannaude/epi/viss-frontend/frontend/zaf_pop_2025_CN_100m_R2025A_v1.tif"
WM_PATH  = "/home/jeannaude/epi/viss-frontend/frontend/zaf_pop_2025_CN_100m_R2025A_v1_3857.tif"
COG_PATH = "/home/jeannaude/epi/viss-frontend/frontend/zaf_pop_2025_CN_100m_R2025A_v1_cog.tif"


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
        python_callable=lambda: (_ensure_tools(), _ensure_requests()),
    )

    download_raw = PythonOperator(
        task_id="download_raw",
        python_callable=_download_raw,
        provide_context=True,
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