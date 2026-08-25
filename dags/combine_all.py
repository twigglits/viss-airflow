from datetime import datetime, timedelta
from pathlib import Path
import os
from typing import List
import hashlib
import mimetypes

import numpy as np
import rasterio

from airflow import DAG
from airflow.operators.python import PythonOperator, get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook
# Per-task ceiling. Without one, a wedged task holds an executor slot
# forever -- and at core.parallelism=2 that is half the install.
# mosaics every country COG for one year.
TASK_TIMEOUT = timedelta(minutes=int(os.environ.get("COMBINE_TASK_TIMEOUT_MIN", "480")))


# Years to process (inclusive)
YEAR_START = int(os.environ.get("COMBINE_YEAR_START", "2015"))
YEAR_END = int(os.environ.get("COMBINE_YEAR_END", "2026"))
YEARS: List[int] = list(range(YEAR_START, YEAR_END + 1))

# Where to look for country folders/files. Defaults to the same data dir used by other DAGs.
INPUT_ROOT = Path(os.environ.get("COMBINE_INPUT_ROOT", os.environ.get("AIRFLOW_DATA_DIR", "/opt/airflow/data")))
OUTPUT_ROOT = Path(os.environ.get("COMBINE_OUTPUT_ROOT", str(INPUT_ROOT)))
OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)

# Mosaic from the Web-Mercator intermediates, not the per-country COGs.
# worldpop_ingest_all builds a COG with `gdal_translate -of COG {wm} {cog}` and
# then keeps only the COG -- as a Postgres large object. No `*_cog.tif` for a
# country survives on disk, so globbing for them finds nothing and the mosaic
# comes out empty. `_3857.tif` is the exact input that COG was translated from:
# same pixels, same CRS, only the internal tiling/overviews differ, and
# gdalbuildvrt does not care about those.
COG_SUFFIXES = ("_3857.tif", "_3857.tiff")
COG_GLOB = os.environ.get("COMBINE_COG_GLOB", "*_3857.tif*")

# Connection used by DAGs to store raster objects in Postgres within this stack.
# docker-compose.yml provides AIRFLOW_CONN_VISS_DATA_DB, which maps to conn_id: viss_data_db
PG_CONN_ID = os.environ.get("PG_CONN_ID", os.environ.get("COMBINE_PG_CONN_ID", "viss_data_db"))


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


def _ensure_db_objects_exist():
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
        cur.execute("ALTER TABLE raster_objects ADD COLUMN IF NOT EXISTS exact_total DOUBLE PRECISION")
        cur.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS raster_objects_unique_idx
            ON raster_objects(stage, filename, sha256)
            """
        )
        conn.commit()


def _cog_exists_in_db(filename: str) -> bool:
    hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1 FROM raster_objects
            WHERE stage = 'cog' AND filename = %s
            ORDER BY id DESC LIMIT 1
            """,
            (filename,),
        )
        return cur.fetchone() is not None


def _cog_exact_total_exists(filename: str) -> bool:
    hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1
            FROM raster_objects
            WHERE stage = 'cog' AND filename = %s AND exact_total IS NOT NULL
            LIMIT 1
            """,
            (filename,),
        )
        return cur.fetchone() is not None


def _set_cog_exact_total(filename: str, total: float):
    hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            UPDATE raster_objects
            SET exact_total = %s
            WHERE stage = 'cog' AND filename = %s
            """,
            (float(total), filename),
        )
        conn.commit()


def _store_file_as_large_object(stage: str, file_path: str, run_id: str):
    hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    file_path_p = Path(file_path)
    filename = file_path_p.name
    content_type = mimetypes.guess_type(filename)[0] or "application/octet-stream"

    hasher = hashlib.sha256()
    size = 0
    with open(file_path_p, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            hasher.update(chunk)
            size += len(chunk)
    digest = hasher.hexdigest()

    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT lo_oid FROM raster_objects
            WHERE stage = %s AND filename = %s
            """,
            (stage, filename),
        )
        old_rows = cur.fetchall() or []
        if old_rows:
            # Unlink old large objects before dropping the rows that reference
            # them. Two traps here, both previously live:
            #   1. Swallowing a failed lo_unlink and deleting the row anyway
            #      orphans the LO -- nothing references it and only vacuumlo
            #      can reclaim it. That is how a raster DB grows to a terabyte.
            #   2. A raised error inside psycopg2 aborts the whole transaction,
            #      so `except: pass` could not work as intended anyway -- every
            #      later statement in this block would fail.
            # So: ask which OIDs still exist, unlink exactly those, and let a
            # genuine failure abort the task instead of leaking silently.
            old_oids = [int(r[0]) for r in old_rows]
            cur.execute(
                "SELECT oid FROM pg_largeobject_metadata WHERE oid = ANY(%s)",
                (old_oids,),
            )
            live_oids = {int(r[0]) for r in cur.fetchall()}
            for old_oid in old_oids:
                if old_oid in live_oids:
                    cur.execute("SELECT lo_unlink(%s)", (old_oid,))
                else:
                    print(f"lo_unlink: oid {old_oid} already gone; dropping stale row")
            cur.execute(
                """
                DELETE FROM raster_objects
                WHERE stage = %s AND filename = %s
                """,
                (stage, filename),
            )

        lo = conn.lobject(0, "w")
        try:
            with open(file_path_p, "rb") as f:
                for chunk in iter(lambda: f.read(1024 * 1024), b""):
                    lo.write(chunk)
            oid = lo.oid
        finally:
            try:
                lo.close()
            except Exception:
                pass

        cur.execute(
            """
            INSERT INTO raster_objects (stage, filename, run_id, sha256, size_bytes, content_type, lo_oid)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (stage, filename, sha256) DO NOTHING
            """,
            (stage, filename, run_id, digest, size, content_type, oid),
        )
        conn.commit()


def _store_combined_cog(year: int):
    ctx = get_current_context()
    force = ctx.get("dag_run").conf.get("force", False) if ctx.get("dag_run") else False
    ti = ctx.get("ti")
    run_id = ti.run_id if ti else "unknown"

    out_tif = OUTPUT_ROOT / f"combined_pop_{year}_cog.tif"
    _ensure_db_objects_exist()

    filename = out_tif.name
    if not force and _cog_exists_in_db(filename):
        print(f"Combined COG already in DB (use dag_run.conf.force=true to overwrite): {filename}")
        return

    if not out_tif.exists():
        raise FileNotFoundError(f"Combined COG missing: {out_tif}")

    _store_file_as_large_object("cog", str(out_tif), run_id)


def _rio_calc_combined_cog_exact_total(year: int, nodata: float = -99999.0, bidx: int = 1):
    ctx = get_current_context()
    force = ctx.get("dag_run").conf.get("force", False) if ctx.get("dag_run") else False

    out_tif = OUTPUT_ROOT / f"combined_pop_{year}_cog.tif"
    if not out_tif.exists():
        raise FileNotFoundError(f"Combined COG missing: {out_tif}")

    _ensure_db_objects_exist()

    filename = out_tif.name
    if not force and _cog_exact_total_exists(filename):
        print(f"rio-calc: exact_total already present for cog:{filename}; skipping")
        return

    print(f"rio-calc: computing exact_total for cog:{filename} from {out_tif}")
    total = 0.0
    valid_count = 0

    with rasterio.Env(GDAL_CACHEMAX=512):
        with rasterio.open(out_tif) as src:
            if bidx < 1 or bidx > src.count:
                raise ValueError(f"rio-calc: invalid bidx {bidx}; raster has {src.count} band(s)")

            nd = float(nodata) if nodata is not None else src.nodata
            for _, window in src.block_windows(bidx):
                arr = src.read(bidx, window=window, masked=False)
                if nd is not None:
                    mask = arr == nd
                    if mask.any():
                        arr = np.where(mask, 0.0, arr)
                        valid_count += int((~mask).sum())
                    else:
                        valid_count += int(arr.size)
                else:
                    mask = src.dataset_mask(window=window) == 0
                    if mask.any():
                        arr = np.where(mask, 0.0, arr)
                        valid_count += int((~mask).sum())
                    else:
                        valid_count += int(arr.size)
                total += float(arr.sum(dtype=np.float64))

    print(f"rio-calc: computed exact_total={total} valid_count={valid_count} for cog:{filename}")
    _set_cog_exact_total(filename, float(total))
    print(f"rio-calc: stored exact_total for cog:{filename}")


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

    def _run(cmd: List[str]):
        print("Running:", " ".join(cmd))
        try:
            subprocess.run(cmd, check=True, text=True, capture_output=True)
        except subprocess.CalledProcessError as e:
            if e.stdout:
                print("STDOUT:\n", e.stdout)
            if e.stderr:
                print("STDERR:\n", e.stderr)
            raise

    if vrt.exists():
        vrt.unlink()

    buildvrt_cmd = ["gdalbuildvrt", "-overwrite", str(vrt), *inputs]
    _run(buildvrt_cmd)

    translate_cmd = [
        "gdal_translate",
        "-of",
        "COG",
        "-co",
        "BIGTIFF=YES",
        "-co",
        "NUM_THREADS=ALL_CPUS",
        # ZSTD beats LZW by ~25% on this Float32 mosaic at comparable speed; a
        # PREDICTOR only hurts here (the data is long runs of NoData and 0.0).
        # Measured -- see dags/recompress_zstd.py for the full codec table.
        "-co",
        "COMPRESS=ZSTD",
        "-co",
        "LEVEL=9",
        str(vrt),
        str(out_tif),
    ]
    _run(translate_cmd)

    # Basic validation
    info_cmd = ["gdalinfo", str(out_tif)]
    _run(info_cmd)

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
    default_args={"owner": "airflow", "retries": 1, "execution_timeout": TASK_TIMEOUT},
    tags=["raster", "combine", "mosaic"],
) as dag:
    for year in YEARS:
        combine = PythonOperator(
            task_id=f"combine_{year}",
            python_callable=_combine_year,
            op_kwargs={"year": year},
        )

        store = PythonOperator(
            task_id=f"store_{year}",
            python_callable=_store_combined_cog,
            op_kwargs={"year": year},
        )

        rio_calc = PythonOperator(
            task_id=f"rio_calc_{year}",
            python_callable=_rio_calc_combined_cog_exact_total,
            op_kwargs={"year": year},
        )

        combine >> store >> rio_calc
