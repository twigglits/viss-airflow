from __future__ import annotations

from datetime import datetime
from pathlib import Path
import os
from typing import Dict, List, Tuple

import numpy as np
import requests
import rasterio

from airflow import DAG
from airflow.operators.python import PythonOperator, get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook


# Output/storage root inside containers (mounted in docker-compose)
DATA_ROOT = Path(os.environ.get("AIRFLOW_DATA_DIR", "/opt/airflow/data"))
DATA_ROOT.mkdir(parents=True, exist_ok=True)

# Target Postgres connection (configured in Airflow Admin -> Connections)
PG_CONN_ID = "viss_data_db"

# Country/year selection (override via env)
ISO3 = os.environ.get("AGEPYR_ISO3", "SUR").upper()
YEAR_START = int(os.environ.get("AGEPYR_YEAR_START", "2015"))
YEAR_END = int(os.environ.get("AGEPYR_YEAR_END", "2025"))
YEARS: List[int] = list(range(YEAR_START, YEAR_END + 1))

WRITE_CSV_DEFAULT = os.environ.get("AGEPYR_WRITE_CSV", "false").strip().lower() in {"1", "true", "yes", "y"}

# WorldPop constants
RELEASE = os.environ.get("AGEPYR_WORLDPOP_RELEASE", "R2025A")
VERSION = os.environ.get("AGEPYR_WORLDPOP_VERSION", "v1")
RESOLUTION = os.environ.get("AGEPYR_WORLDPOP_RES", "100m")
TYPE = os.environ.get("AGEPYR_WORLDPOP_TYPE", "CN")


def _ensure_tools():
    import shutil

    missing = [t for t in ("gdalinfo",) if shutil.which(t) is None]
    if missing:
        raise RuntimeError(
            f"GDAL tools missing: {missing}. Install GDAL in the Airflow worker or use DockerOperator."
        )


def _ensure_db_tables_exist():
    hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    sql = """
    CREATE TABLE IF NOT EXISTS age_pyramid_5yr (
        iso3 TEXT NOT NULL,
        year INTEGER NOT NULL,
        age_bin TEXT NOT NULL,
        pop DOUBLE PRECISION NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        PRIMARY KEY (iso3, year, age_bin)
    );
    """
    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql)
        conn.commit()


def _age_codes_worldpop() -> List[str]:
    # WorldPop: 00 (0-12mo), 01 (1-4y), then 5-year steps up to 85, and 90 (90+)
    codes = ["00", "01"]
    codes.extend([f"{a:02d}" for a in range(5, 90, 5)])  # 05..85
    codes.append("90")
    return codes


def _worldpop_agesex_url(iso3: str, year: int, gender: str, age_code: str) -> str:
    iso3u = iso3.upper()
    iso3l = iso3.lower()
    return (
        f"https://data.worldpop.org/GIS/AgeSex_structures/Global_2015_2030/{RELEASE}/"
        f"{year}/{iso3u}/{VERSION}/{RESOLUTION}/constrained/"
        f"{iso3l}_{gender}_{age_code}_{year}_{TYPE}_{RESOLUTION}_{RELEASE}_{VERSION}.tif"
    )


def _download(url: str, dest: Path) -> Path:
    dest.parent.mkdir(parents=True, exist_ok=True)

    ctx = get_current_context()
    force = ctx.get("dag_run").conf.get("force", False) if ctx.get("dag_run") else False

    if dest.exists() and not force:
        return dest

    with requests.get(url, stream=True, timeout=120) as r:
        r.raise_for_status()
        tmp = dest.with_suffix(dest.suffix + ".partial")
        with open(tmp, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)
        os.replace(tmp, dest)

    return dest


def _rio_sum_tif(file_path: Path, nodata: float = -99999.0, bidx: int = 1) -> float:
    total = 0.0
    with rasterio.Env(GDAL_CACHEMAX=512):
        with rasterio.open(str(file_path)) as src:
            nd = float(nodata) if nodata is not None else src.nodata
            for _, window in src.block_windows(bidx):
                arr = src.read(bidx, window=window, masked=False)
                if nd is not None:
                    mask = arr == nd
                    if mask.any():
                        arr = np.where(mask, 0.0, arr)
                else:
                    mask = src.dataset_mask(window=window) == 0
                    if mask.any():
                        arr = np.where(mask, 0.0, arr)
                total += float(arr.sum(dtype=np.float64))
    return total


def _rebin_to_5yr(age_totals: Dict[str, float]) -> List[Tuple[str, float]]:
    # Target bins: 0-4, 5-9, 10-14, ..., 75-79, 80+
    out: List[Tuple[str, float]] = []

    def get(code: str) -> float:
        return float(age_totals.get(code, 0.0) or 0.0)

    out.append(("0-4", get("00") + get("01")))

    for start in range(5, 80, 5):
        end = start + 4
        code = f"{start:02d}"  # WorldPop code matches start age for 5-year bins
        out.append((f"{start}-{end}", get(code)))

    out.append(("80+", get("80") + get("85") + get("90")))

    return out


def _upsert_age_pyramid_5yr(iso3: str, year: int, bins: List[Tuple[str, float]]):
    hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    with hook.get_conn() as conn, conn.cursor() as cur:
        for age_bin, pop in bins:
            cur.execute(
                """
                INSERT INTO age_pyramid_5yr (iso3, year, age_bin, pop)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (iso3, year, age_bin)
                DO UPDATE SET pop = EXCLUDED.pop
                """,
                (iso3, int(year), str(age_bin), float(pop)),
            )
        conn.commit()


def _write_csv(iso3: str, year: int, bins: List[Tuple[str, float]]) -> str:
    out_dir = DATA_ROOT / "age_pyramids_5yr" / iso3.lower()
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{iso3.lower()}_age_pyramid_5yr_{year}.csv"
    lines = ["age_group,pop"]
    for age_bin, pop in bins:
        lines.append(f"{age_bin},{pop}")
    out_path.write_text("\n".join(lines) + "\n")
    return str(out_path)


def build_age_pyramid_year(iso3: str, year: int):
    _ensure_tools()
    _ensure_db_tables_exist()

    ctx = get_current_context()
    write_csv = (
        ctx.get("dag_run").conf.get("write_csv", WRITE_CSV_DEFAULT) if ctx.get("dag_run") else WRITE_CSV_DEFAULT
    )

    age_codes = _age_codes_worldpop()
    iso3l = iso3.lower()

    # Download and compute totals for total-sex ('t') age rasters
    age_totals: Dict[str, float] = {}
    for code in age_codes:
        url = _worldpop_agesex_url(iso3, year, gender="t", age_code=code)
        dest = DATA_ROOT / "agesex" / iso3l / str(year) / Path(url).name
        try:
            _download(url, dest)
        except Exception as e:
            raise RuntimeError(f"Failed to download age structure file: {url} -> {e}")

        total = _rio_sum_tif(dest)
        age_totals[code] = float(total)

    bins_5yr = _rebin_to_5yr(age_totals)

    _upsert_age_pyramid_5yr(iso3, year, bins_5yr)
    csv_path = None
    if write_csv:
        csv_path = _write_csv(iso3, year, bins_5yr)
        print(f"Wrote 5-year age pyramid CSV: {csv_path}")

    print(f"Stored 5-year age pyramid bins in Postgres: iso3={iso3} year={year} bins={len(bins_5yr)}")
    return csv_path


with DAG(
    dag_id="worldpop_age_pyramid_5yr",
    description="Compute age pyramid (5-year bins) from WorldPop AgeSex_structures for a country and store to Postgres + CSV",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "airflow", "retries": 1},
    tags=["worldpop", "age_pyramid", "demography"],
) as dag:
    for year in YEARS:
        PythonOperator(
            task_id=f"age_pyramid_{ISO3.lower()}_{year}",
            python_callable=build_age_pyramid_year,
            op_kwargs={"iso3": ISO3, "year": year},
        )
