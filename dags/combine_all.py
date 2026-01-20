from datetime import datetime
from pathlib import Path
import os
from typing import List
import hashlib
import mimetypes

from airflow import DAG
from airflow.operators.python import PythonOperator, get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook


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
        for (old_oid,) in old_rows:
            try:
                cur.execute("SELECT lo_unlink(%s)", (int(old_oid),))
            except Exception:
                pass
        if old_rows:
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
    if not out_tif.exists():
        raise FileNotFoundError(f"Combined COG missing: {out_tif}")

    _ensure_db_objects_exist()
    if not force and _cog_exists_in_db(out_tif.name):
        print(f"Combined COG already in DB (use dag_run.conf.force=true to overwrite): {out_tif.name}")
        return

    _store_file_as_large_object("cog", str(out_tif), run_id)


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
        "-co",
        "COMPRESS=LZW",
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
    default_args={"owner": "airflow", "retries": 1},
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

        combine >> store
