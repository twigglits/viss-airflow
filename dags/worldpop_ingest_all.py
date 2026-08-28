from datetime import datetime, timedelta
from pathlib import Path
import os
import hashlib
import mimetypes
import requests
from typing import Iterable, List

import numpy as np
import rasterio

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator, get_current_context
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from recompress_zstd import recompress
# Per-task ceiling. Without one, a wedged task holds an executor slot forever
# -- and slots are the scarce resource that caps how many countries run at once.
#
# 180m was too tight: RUS and USA warps were killed at exactly 3h on the first
# run under this setting. Their reprojected outputs are 1.2 GB and 2.7 GB
# (RUS is 416797 x 123684 px), so hours of real work is normal, not a hang.
# 8h clears the two largest countries with headroom; every other country
# finishes in minutes, so the ceiling only ever bites on a genuine wedge.
TASK_TIMEOUT = timedelta(minutes=int(os.environ.get("WORLDPOP_TASK_TIMEOUT_MIN", "480")))

# Years to ingest (inclusive)
YEARS: List[int] = list(range(2015, 2027))

# Full ISO 3166-1 alpha-3 list (static)
ISO3_CODES: List[str] = [
    'ABW','AFG','AGO','AIA','ALA','ALB','AND','ARE','ARG','ARM','ASM','ATG','AUS','AUT','AZE',
    'BDI','BEL','BEN','BES','BFA','BGD','BGR','BHR','BHS','BIH','BLM','BLR','BLZ','BMU','BOL','BRA','BRB','BRN','BTN','BWA',
    'CAF','CAN','CCK','CHE','CHL','CHN','CIV','CMR','COD','COG','COK','COL','COM','CPV','CRI','CUB','CUW','CXR','CYM','CYP','CZE',
    'DEU','DJI','DMA','DNK','DOM','DZA',
    'ECU','EGY','ERI','ESH','ESP','EST','ETH',
    'FIN','FJI','FLK','FRA','FRO',
    'GAB','GBR','GEO','GGY','GHA','GIB','GIN','GLP','GMB','GNB','GNQ','GRC','GRD','GRL','GTM','GUF','GUM','GUY',
    'HKG','HND','HRV','HTI','HUN',
    'IDN','IMN','IND','IRL','IRN','IRQ','ISL','ISR','ITA',
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


def _env_int(name: str, default: int) -> int:
    val = os.getenv(name)
    if val is None or val == "":
        return default
    try:
        return int(val)
    except ValueError:
        raise ValueError(f"Invalid integer for {name}: {val!r}")


DEFAULT_DAG_CONCURRENCY = _env_int("WORLDPOP_DAG_CONCURRENCY", 1)
DEFAULT_MAX_ACTIVE_RUNS = _env_int("WORLDPOP_MAX_ACTIVE_RUNS", 1)
DEFAULT_MAX_ACTIVE_TASKS = _env_int("WORLDPOP_MAX_ACTIVE_TASKS", 1)


GDAL_NUM_THREADS = _env_int("WORLDPOP_GDAL_NUM_THREADS", 14)
GDAL_CACHEMAX_MB = _env_int("WORLDPOP_GDAL_CACHEMAX_MB", 8192)
GDAL_WM_MB = _env_int("WORLDPOP_GDAL_WM_MB", 4096)
VSI_CACHE_SIZE_BYTES = _env_int("WORLDPOP_VSI_CACHE_SIZE_BYTES", 268435456)


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
        cur.execute("ALTER TABLE raster_objects ADD COLUMN IF NOT EXISTS exact_total DOUBLE PRECISION")
        # Ensure uniqueness on (stage, filename, sha256) to avoid duplicates
        cur.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS raster_objects_unique_idx
            ON raster_objects(stage, filename, sha256)
            """
        )
        conn.commit()


def _usable(path: str) -> bool:
    """True when the file exists and holds bytes.

    A task killed mid-write (timeout, OOM, container restart) leaves a 0-byte
    file. Plain exists() treats that as done and the pipeline never recovers.
    """
    p = Path(path)
    return p.exists() and p.stat().st_size > 0


def _raw_exact_total_exists(filename: str) -> bool:
    hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1
            FROM raster_objects
            WHERE stage = 'raw' AND filename = %s AND exact_total IS NOT NULL
            LIMIT 1
            """,
            (filename,),
        )
        return cur.fetchone() is not None


def _raw_exists_in_db(filename: str) -> bool:
    hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1
            FROM raster_objects
            WHERE stage = 'raw' AND filename = %s
            ORDER BY id DESC
            LIMIT 1
            """,
            (filename,),
        )
        return cur.fetchone() is not None


def _set_raw_exact_total(filename: str, total: float):
    hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            UPDATE raster_objects
            SET exact_total = %s
            WHERE stage = 'raw' AND filename = %s
            """,
            (float(total), filename),
        )
        conn.commit()


def _rio_calc_exact_total(file_path: str, filename: str, nodata: float = -99999.0, bidx: int = 1):
    ctx = get_current_context()
    force = ctx.get("dag_run").conf.get("force", False) if ctx.get("dag_run") else False

    _ensure_db_objects_exist()
    if not force and _raw_exact_total_exists(filename):
        print(f"rio-calc: exact_total already present for raw:{filename}; skipping")
        return

    p = Path(file_path)
    if not p.exists():
        raise FileNotFoundError(f"rio-calc: RAW file missing: {file_path}")

    print(f"rio-calc: computing exact_total for raw:{filename} from {file_path}")
    total = 0.0
    valid_count = 0
    with rasterio.Env(GDAL_CACHEMAX=512):
        with rasterio.open(file_path) as src:
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

    print(f"rio-calc: computed exact_total={total} valid_count={valid_count} for raw:{filename}")
    _set_raw_exact_total(filename, float(total))
    print(f"rio-calc: stored exact_total for raw:{filename}")


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
        # Overwrite semantics: unlink and remove any existing rows for (stage, filename)
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



def _recompress(paths: Iterable[str]):
    """Losslessly re-encode this year's rasters to ZSTD in place.

    WorldPop ships raw as LZW+PREDICTOR=2, and every warp written before the
    `-co COMPRESS=ZSTD` flag went into warp_web_mercator is plain LZW. Both are
    25-40% larger than plain ZSTD at bit-identical pixels -- see
    recompress_zstd.py for the measured codec table. recompress() verifies the
    rewrite block-by-block (including overviews) and only then replaces the
    original, so a file that fails verification is reported and left alone.

    A file already in the target state costs one rasterio.open(), so running
    this every year is cheap and it backfills whatever is already on disk.
    """
    checked = before_total = after_total = 0
    for path in paths:
        path = Path(path)
        if not _usable(str(path)):
            continue
        status, before, after = recompress(path)
        checked += 1
        before_total += before
        after_total += after
        if status in ("CORRUPT", "FAILED"):
            print(f"WARNING: recompress {status} for {path.name}, left as-is")
        elif status == "ok":
            print(f"recompressed {path.name}: {before} -> {after} bytes")
    print(f"recompress: {checked} file(s) checked, {before_total - after_total} bytes saved")


def _recompress_country(cc3l: str):
    """End-of-country sweep. The per-year tasks only know the three paths this
    DAG builds; the glob also catches leftovers from earlier releases and from
    years outside the current YEARS range."""
    _recompress(sorted(DATA_ROOT.glob(f"{cc3l}_pop_*.tif")))


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
        concurrency=DEFAULT_DAG_CONCURRENCY,
        max_active_tasks=DEFAULT_MAX_ACTIVE_TASKS,
        max_active_runs=DEFAULT_MAX_ACTIVE_RUNS,
        default_args={"owner": "airflow", "retries": 1, "execution_timeout": TASK_TIMEOUT},
        tags=["worldpop", "raster", "cog", cc3u],
    ) as dag:

        ensure_env = PythonOperator(
            task_id="ensure_env",
            python_callable=_ensure_env,
        )

        year_tails = []

        for year in YEARS:
            raw_path, wm_path, cog_path = _year_paths(cc3l, year)
            url = _year_url(cc3u, year)

            def _check_missing(y: int, expected_raw: str, expected_wm: str, expected_cog: str) -> str:
                _ensure_db_objects_exist()
                raw_name = Path(expected_raw).name
                cog_name = Path(expected_cog).name

                if not _usable(expected_raw):
                    print(f"Local RAW missing or empty for {cc3u} {y}: {expected_raw} -> start at download_raw")
                    return f"download_raw_{y}"

                if not _raw_exists_in_db(raw_name):
                    print(f"RAW not in DB for {cc3u} {y}: {raw_name} -> start at store_raw")
                    return f"store_raw_{y}"

                if not _raw_exact_total_exists(raw_name):
                    print(f"rio-calc exact_total missing for {cc3u} {y}: raw:{raw_name} -> start at rio_calc")
                    return f"rio_calc_{y}"

                # exists() is not enough: a gdalwarp killed mid-write leaves a
                # 0-byte file behind, and trusting it here skips the warp that
                # would rebuild it, so to_cog fails on an empty input -- forever.
                # Size is the cheap proxy for "this file is real".
                if not _usable(expected_wm):
                    print(f"Local WM missing or empty for {cc3u} {y}: {expected_wm} -> start at warp_web_mercator")
                    return f"warp_web_mercator_{y}"

                exists = _cog_exists_in_db(cog_name)
                if exists:
                    print(f"COG already in DB for {cc3u} {y}: {expected_cog} and exact_total present -> skipping year")
                    return f"skip_{y}"

                print(f"COG missing in DB for {cc3u} {y}: {expected_cog} -> start at to_cog")
                return f"to_cog_{y}"

            check_missing = BranchPythonOperator(
                task_id=f"check_missing_{year}",
                python_callable=lambda y=year, r=raw_path, w=wm_path, c=cog_path: _check_missing(y, r, w, c),
            )

            skip_year = EmptyOperator(task_id=f"skip_{year}")

            download_raw = PythonOperator(
                task_id=f"download_raw_{year}",
                python_callable=_download_file,
                op_kwargs={"url": url, "dest_path": raw_path},
            )

            store_raw = PythonOperator(
                task_id=f"store_raw_{year}",
                python_callable=_store_lo,
                op_kwargs={"stage": "raw", "file_path": raw_path},
                trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
            )

            rio_calc = PythonOperator(
                task_id=f"rio_calc_{year}",
                python_callable=_rio_calc_exact_total,
                op_kwargs={"file_path": raw_path, "filename": Path(raw_path).name},
                trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
            )

            warp_web_mercator = BashOperator(
                task_id=f"warp_web_mercator_{year}",
                bash_command=(
                    "gdalwarp -overwrite -t_srs EPSG:3857 -r bilinear -multi "
                    "--config GDAL_NUM_THREADS {gdal_num_threads} "
                    "-wo NUM_THREADS={gdal_num_threads} "
                    "--config GDAL_CACHEMAX {gdal_cachemax_mb} "
                    "-wm {gdal_wm_mb} "
                    "--config VSI_CACHE TRUE "
                    "--config VSI_CACHE_SIZE {vsi_cache_size_bytes} "
                    "--config GDAL_DISABLE_READDIR_ON_OPEN YES "
                    "-srcnodata -99999 -dstnodata -99999 "
                    # GTiff driver spells the level ZSTD_LEVEL (the COG driver
                    # spells it LEVEL) -- passing the wrong one is silently ignored.
                    "-co BIGTIFF=YES -co COMPRESS=ZSTD -co ZSTD_LEVEL=9 "
                    "{raw} {wm}"
                ).format(
                    raw=raw_path,
                    wm=wm_path,
                    gdal_num_threads=GDAL_NUM_THREADS,
                    gdal_cachemax_mb=GDAL_CACHEMAX_MB,
                    gdal_wm_mb=GDAL_WM_MB,
                    vsi_cache_size_bytes=VSI_CACHE_SIZE_BYTES,
                ),
                trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
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
                trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
            )

            store_cog = PythonOperator(
                task_id=f"store_cog_{year}",
                python_callable=_store_lo,
                op_kwargs={"stage": "cog", "file_path": cog_path},
                trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
            )


            validate_cog = BashOperator(
                task_id=f"validate_cog_{year}",
                bash_command=(
                    "set -euo pipefail; "
                    "gdalinfo {cog} | "
                    r"grep -i -E Coordinate"
                ).format(cog=cog_path),
                trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
            )

            # ALL_DONE, not the usual NONE_FAILED_*: this must also run when the
            # year was skipped outright (nothing new, but old LZW files on disk
            # still get backfilled) and when the warp or COG step failed (the
            # raw that did land is still worth shrinking).
            recompress_year = PythonOperator(
                task_id=f"recompress_{year}",
                python_callable=_recompress,
                op_kwargs={"paths": [raw_path, wm_path, cog_path]},
                trigger_rule=TriggerRule.ALL_DONE,
            )

            ensure_env >> check_missing
            check_missing >> [skip_year, download_raw, store_raw, rio_calc, warp_web_mercator, to_cog]

            download_raw >> store_raw
            store_raw >> rio_calc
            rio_calc >> warp_web_mercator
            warp_web_mercator >> to_cog
            to_cog >> store_cog >> validate_cog

            [skip_year, validate_cog] >> recompress_year
            year_tails.append(recompress_year)

        recompress_country = PythonOperator(
            task_id="recompress_country",
            python_callable=_recompress_country,
            op_kwargs={"cc3l": cc3l},
            trigger_rule=TriggerRule.ALL_DONE,
        )
        year_tails >> recompress_country
