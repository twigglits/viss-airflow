from datetime import datetime
from pathlib import Path
import os
import hashlib
import mimetypes
import requests
from typing import Iterable, List

from airflow import DAG
from airflow.operators.python import PythonOperator, get_current_context, ShortCircuitOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Years to ingest (inclusive)
YEARS: List[int] = list(range(2015, 2026))

# Full ISO 3166-1 alpha-3 list (static)
ISO3_CODES: List[str] = [
    'ABW','AFG','AGO','AIA','ALA','ALB','AND','ARE','ARG','ARM','ASM','ATA','ATF','ATG','AUS','AUT','AZE',
    'BDI','BEL','BEN','BES','BFA','BGD','BGR','BHR','BHS','BIH','BLM','BLR','BLZ','BMU','BOL','BRA','BRB','BRN','BTN','BVT','BWA',
    'CAF','CAN','CCK','CHE','CHL','CHN','CIV','CMR','COD','COG','COK','COL','COM','CPV','CRI','CUB','CUW','CXR','CYM','CYP','CZE',
    'DEU','DJI','DMA','DNK','DOM','DZA',
    'ECU','EGY','ERI','ESH','ESP','EST','ETH',
    'FIN','FJI','FLK','FRA','FRO',
    'GAB','GBR','GEO','GGY','GHA','GIB','GIN','GLP','GMB','GNB','GNQ','GRC','GRD','GRL','GTM','GUF','GUM','GUY',
    'HKG','HMD','HND','HRV','HTI','HUN',
    'IDN','IMN','IND','IOT','IRL','IRN','IRQ','ISL','ISR','ITA',
    'JAM','JEY','JOR','JPN',
    'KAZ','KEN','KGZ','KHM','KIR','KNA','KOR','KWT',
    'LAO','LBN','LBR','LBY','LCA','LIE','LKA','LSO','LTU','LUX','LVA',
    'MAC','MAF','MAR','MCO','MDA','MDG','MDV','MEX','MHL','MKD','MLI','MLT','MMR','MNE','MNG','MNP','MOZ','MRT','MSR','MTQ','MUS','MWI','MYS',
    'MYT','NAM','NCL','NER','NFK','NGA','NIC','NIU','NLD','NOR','NPL','NRU','NZL',
    'OMN',
    'PAK','PAN','PCN','PER','PHL','PLW','PNG','POL','PRI','PRK','PRT','PRY','PSE','PYF',
    'QAT',
    'REU','ROU','RUS','RWA',
    'SAU','SDN','SEN','SGP','SGS','SHN','SJM','SLB','SLE','SLV','SMR','SOM','SPM','SRB','SSD','STP','SUR','SVK','SVN','SWE','SWZ','SXM','SYC','SYR',
    'TCA','TCD','TGO','THA','TJK','TKL','TKM','TLS','TON','TTO','TUN','TUR','TUV','TWN','TZA',
    'UGA','UKR','UMI','URY','USA','UZB',
    'VAT','VCT','VEN','VGB','VIR','VNM','VUT',
    'WLF','WSM',
    'YEM',
    'ZAF','ZMB','ZWE',
]

# Optional: reduce the list via env (comma-separated ISO3 codes)
_ENV_COUNTRIES = os.getenv("WORLDPOP_COUNTRIES")
if _ENV_COUNTRIES:
    WANT = {c.strip().upper() for c in _ENV_COUNTRIES.split(',') if c.strip()}
    ISO3_CODES = [c for c in ISO3_CODES if c in WANT]

# Storage paths inside containers (use a mounted volume in compose for persistence)
DATA_ROOT = Path(os.environ.get("AIRFLOW_DATA_DIR", "/opt/airflow/data"))
DATA_ROOT.mkdir(parents=True, exist_ok=True)

# Target Postgres connection (configure in Airflow Admin -> Connections)
PG_CONN_ID = "viss_data_db"


def _year_url(cc3: str, year: int) -> str:
    cc3u = cc3.upper()
    cc3l = cc3.lower()
    return (
        f"https://data.worldpop.org/GIS/Population/Global_2015_2030/R2025A/{year}/{cc3u}/v1/100m/constrained/"
        f"{cc3l}_pop_{year}_CN_100m_R2025A_v1.tif"
    )


def _year_paths(cc3: str, year: int):
    cc3l = cc3.lower()
    raw = str(DATA_ROOT / f"{cc3l}_pop_{year}_CN_100m_R2025A_v1.tif")
    wm = str(DATA_ROOT / f"{cc3l}_pop_{year}_CN_100m_R2025A_v1_3857.tif")
    cog = str(DATA_ROOT / f"{cc3l}_pop_{year}_CN_100m_R2025A_v1_cog.tif")
    return raw, wm, cog


def _ensure_tools():
    import shutil
    missing = [t for t in ("gdalwarp", "gdal_translate", "gdalinfo") if shutil.which(t) is None]
    if missing:
        raise RuntimeError(f"GDAL tools missing: {missing}. Install GDAL in the Airflow worker or use DockerOperator.")


def _ensure_requests():
    try:
        import requests  # noqa: F401
    except Exception as e:
        raise RuntimeError("Python package `requests` is required in the worker env") from e


def _ensure_env():
    _ensure_tools()
    _ensure_requests()


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
        # Ensure uniqueness on (stage, filename, sha256) to avoid duplicates
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


def _download_file(url: str, dest_path: str):
    ctx = get_current_context()
    dest = Path(dest_path)
    dest.parent.mkdir(parents=True, exist_ok=True)
    force = ctx.get("dag_run").conf.get("force", False) if ctx.get("dag_run") else False
    if dest.exists() and not force:
        # Compare remote size via HEAD Content-Length to local size
        local_size = dest.stat().st_size
        try:
            h = requests.head(url, timeout=30, allow_redirects=True)
            if h.status_code >= 400:
                # Cannot validate; proceed to re-download per user policy
                print(f"HEAD status {h.status_code}; will re-download to ensure integrity: {url}")
            else:
                remote_len = h.headers.get("Content-Length") or h.headers.get("content-length")
                if remote_len is not None:
                    try:
                        remote_size = int(remote_len)
                        if remote_size == local_size:
                            print(f"Exists and size matches remote ({local_size} bytes), skipping download: {dest}")
                            return str(dest)
                        else:
                            print(f"Size mismatch (local {local_size} != remote {remote_size}); re-downloading: {dest}")
                    except ValueError:
                        print("Invalid Content-Length header; re-downloading to ensure integrity")
                else:
                    print("No Content-Length header; re-downloading to ensure integrity")
        except Exception as e:
            print(f"HEAD failed ({e}); re-downloading to ensure integrity: {url}")

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


def _store_file_as_large_object(stage: str, file_path: str, run_id: str):
    hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    file_path = Path(file_path)
    filename = file_path.name
    content_type = mimetypes.guess_type(filename)[0] or "application/octet-stream"

    # First pass: compute sha256 and size to check for duplicates before creating a LO
    hasher = hashlib.sha256()
    size = 0
    with open(file_path, 'rb') as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b''):
            hasher.update(chunk)
            size += len(chunk)
    digest = hasher.hexdigest()

    with hook.get_conn() as conn, conn.cursor() as cur:
        # Check if an identical record already exists
        cur.execute(
            """
            SELECT id FROM raster_objects
            WHERE stage = %s AND filename = %s AND sha256 = %s
            LIMIT 1
            """,
            (stage, filename, digest),
        )
        row = cur.fetchone()
        if row:
            # Duplicate detected; skip storing
            return

        # Create the large object and stream file into it
        lo = conn.lobject(0, 'w')
        try:
            with open(file_path, 'rb') as f:
                for chunk in iter(lambda: f.read(1024 * 1024), b''):
                    lo.write(chunk)
            oid = lo.oid
        finally:
            try:
                lo.close()
            except Exception:
                pass

        # Insert metadata row with ON CONFLICT safety (backstop)
        cur.execute(
            """
            INSERT INTO raster_objects (stage, filename, run_id, sha256, size_bytes, content_type, lo_oid)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (stage, filename, sha256) DO NOTHING
            """,
            (stage, filename, run_id, digest, size, content_type, oid),
        )
        conn.commit()


def _store_lo(stage: str, file_path: str):
    ctx = get_current_context()
    ti = ctx.get("ti")
    run_id = ti.run_id if ti else "unknown"
    _ensure_db_objects_exist()
    _store_file_as_large_object(stage, file_path, run_id)


# DAG factory
for CC3 in ISO3_CODES:
    cc3u = CC3.upper()
    cc3l = CC3.lower()

    with DAG(
        dag_id=f"worldpop_ingest_cog_{cc3l}",
        description=f"WorldPop ingest (raw -> 3857 -> COG) for {cc3u} years {YEARS[0]}–{YEARS[-1]}",
        start_date=datetime(2025, 1, 1),
        schedule=None,
        catchup=False,
        concurrency=1,
        max_active_runs=1,
        default_args={"owner": "airflow", "retries": 1},
        tags=["worldpop", "raster", "cog", cc3u],
    ) as dag:

        ensure_env = PythonOperator(
            task_id="ensure_env",
            python_callable=_ensure_env,
        )

        for year in YEARS:
            raw_path, wm_path, cog_path = _year_paths(cc3l, year)
            url = _year_url(cc3u, year)

            # Short-circuit if COG already exists in DB
            def _check_missing(y: int, expected_cog: str) -> bool:
                _ensure_db_objects_exist()
                exists = _cog_exists_in_db(Path(expected_cog).name)
                if exists:
                    print(f"COG already in DB for {cc3u} {y}: {expected_cog} -> skipping year")
                    return False
                # HEAD check: if remote missing, skip this year entirely
                try:
                    h = requests.head(_year_url(cc3u, y), timeout=30, allow_redirects=True)
                    if h.status_code >= 400:
                        print(f"Remote missing for {cc3u} {y}: status={h.status_code} -> skipping year")
                        return False
                except Exception as e:
                    print(f"HEAD failed for {cc3u} {y}: {e} -> skipping year")
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
                    "-co BIGTIFF=IF_NEEDED -co COMPRESS=LZW "
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
