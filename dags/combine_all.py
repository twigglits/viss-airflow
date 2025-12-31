from datetime import datetime
from pathlib import Path
import os
from typing import List

from airflow import DAG
from airflow.operators.python import PythonOperator, get_current_context


# Years to process (inclusive)
YEAR_START = int(os.environ.get("COMBINE_YEAR_START", "2015"))
YEAR_END = int(os.environ.get("COMBINE_YEAR_END", "2025"))
YEARS: List[int] = list(range(YEAR_START, YEAR_END + 1))

# Where to look for country folders/files. Defaults to the same data dir used by other DAGs.
INPUT_ROOT = Path(os.environ.get("COMBINE_INPUT_ROOT", os.environ.get("AIRFLOW_DATA_DIR", "/opt/airflow/data")))
OUTPUT_ROOT = Path(os.environ.get("COMBINE_OUTPUT_ROOT", str(INPUT_ROOT)))
OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)

RAW_GLOB = os.environ.get("COMBINE_RAW_GLOB", "*.tif*")


def _ensure_tools():
    import shutil

    missing = [t for t in ("gdalbuildvrt", "gdal_translate", "gdalinfo") if shutil.which(t) is None]
    if missing:
        raise RuntimeError(
            f"GDAL tools missing: {missing}. Install GDAL in the Airflow worker or use DockerOperator."
        )


def _find_raw_tiffs_for_year(year: int) -> List[str]:
    """Find raw.tiff files for a given year.

    Expected layouts supported:
    - <INPUT_ROOT>/<year>/<country>/raw.tiff
    - any nested path where a path segment equals the year and the filename is raw.tiff

    If your data uses a different layout, set COMBINE_INPUT_ROOT and/or adjust this matcher.
    """

    year_s = str(year)

    year_root = INPUT_ROOT / year_s
    search_root = year_root if year_root.exists() else INPUT_ROOT

    candidates = list(search_root.rglob(RAW_GLOB))
    out: List[str] = []
    for p in candidates:
        name_l = p.name.lower()
        if name_l.endswith("_cog.tif") or name_l.endswith("_cog.tiff"):
            continue
        if name_l.endswith("_3857.tif") or name_l.endswith("_3857.tiff"):
            continue

        if year_root.exists():
            out.append(str(p))
            continue

        # Match by year being a full directory segment in the path
        if year_s in {part for part in p.parts}:
            out.append(str(p))
            continue

        # Fallback: allow year in parent directory names (e.g., "2015_something")
        if any(year_s in part for part in p.parts):
            out.append(str(p))

    # Deterministic order
    out.sort()
    return out


def _combine_year(year: int):
    _ensure_tools()

    ctx = get_current_context()
    force = ctx.get("dag_run").conf.get("force", False) if ctx.get("dag_run") else False

    inputs = _find_raw_tiffs_for_year(year)
    if not inputs:
        exists = INPUT_ROOT.exists()
        year_root = INPUT_ROOT / str(year)
        year_exists = year_root.exists()
        raise RuntimeError(
            "No inputs found for year "
            f"{year}. Searched for {RAW_GLOB} under: {INPUT_ROOT} "
            f"(exists={exists}, year_dir={year_root}, year_dir_exists={year_exists})."
        )

    out_tif = OUTPUT_ROOT / f"combined_tiff_{year}.tiff"
    vrt = OUTPUT_ROOT / f"combined_tiff_{year}.vrt"

    if out_tif.exists() and not force:
        print(f"Output exists, skipping (use dag_run.conf.force=true to overwrite): {out_tif}")
        return str(out_tif)

    # Build VRT and translate to GeoTIFF mosaic
    # Note: gdalbuildvrt handles differing extents/resolutions better than a naive merge.
    import subprocess

    if vrt.exists():
        vrt.unlink()

    buildvrt_cmd = ["gdalbuildvrt", "-overwrite", str(vrt), *inputs]
    print("Running:", " ".join(buildvrt_cmd))
    subprocess.run(buildvrt_cmd, check=True)

    translate_cmd = [
        "gdal_translate",
        "-of",
        "GTiff",
        "-co",
        "BIGTIFF=IF_NEEDED",
        "-co",
        "COMPRESS=LZW",
        str(vrt),
        str(out_tif),
    ]
    print("Running:", " ".join(translate_cmd))
    subprocess.run(translate_cmd, check=True)

    # Basic validation
    info_cmd = ["gdalinfo", str(out_tif)]
    subprocess.run(info_cmd, check=True, stdout=subprocess.DEVNULL)

    try:
        vrt.unlink()
    except Exception:
        pass

    return str(out_tif)


with DAG(
    dag_id="combine_all",
    description="Combine all country raw.tiff files per year into combined_tiff_<year>.tiff",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    concurrency=1,
    max_active_runs=1,
    default_args={"owner": "airflow", "retries": 1},
    tags=["raster", "combine", "mosaic"],
) as dag:
    for year in YEARS:
        PythonOperator(
            task_id=f"combine_{year}",
            python_callable=_combine_year,
            op_kwargs={"year": year},
        )
