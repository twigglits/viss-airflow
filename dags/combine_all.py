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

COG_SUFFIXES = ("_cog.tif", "_cog.tiff")
COG_GLOB = os.environ.get("COMBINE_COG_GLOB", "*_cog.tif*")


def _ensure_tools():
    import shutil

    missing = [t for t in ("gdalbuildvrt", "gdal_translate", "gdalinfo") if shutil.which(t) is None]
    if missing:
        raise RuntimeError(
            f"GDAL tools missing: {missing}. Install GDAL in the Airflow worker or use DockerOperator."
        )


def _find_cogs_for_year(year: int) -> List[str]:
    """Find per-country WorldPop population COGs for a given year.

    We only want to mosaic the *COG outputs* produced by the ingest pipeline, i.e.
    filenames like:

      <iso3>_pop_<year>_CN_100m_R2025A_v1_cog.tif

    This matcher intentionally does NOT recurse into subdirectories to avoid picking
    up unrelated TIFFs.
    """

    year_s = str(year)
    year_token = f"_pop_{year_s}_"

    candidates = list(INPUT_ROOT.glob(COG_GLOB))
    out: List[str] = []
    for p in candidates:
        if not p.is_file():
            continue
        name_l = p.name.lower()
        if not name_l.endswith(COG_SUFFIXES):
            continue
        if name_l.endswith(".partial"):
            continue
        if year_token not in name_l:
            continue
        out.append(str(p))

    out.sort()
    return out


def _combine_year(year: int):
    _ensure_tools()

    ctx = get_current_context()
    force = ctx.get("dag_run").conf.get("force", False) if ctx.get("dag_run") else False

    inputs = _find_cogs_for_year(year)
    if not inputs:
        exists = INPUT_ROOT.exists()
        raise RuntimeError(
            "No inputs found for year "
            f"{year}. Searched for COGs matching {COG_GLOB} under: {INPUT_ROOT} "
            f"(exists={exists})."
        )

    out_tif = OUTPUT_ROOT / f"combined_pop_{year}_cog.tif"
    vrt = OUTPUT_ROOT / f"combined_pop_{year}.vrt"

    if out_tif.exists() and not force:
        print(f"Output exists, skipping (use dag_run.conf.force=true to overwrite): {out_tif}")
        return str(out_tif)

    # Build VRT and translate to a COG mosaic.
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
        "COG",
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
    description="Combine all country population COGs per year into combined_pop_<year>_cog.tif",
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
