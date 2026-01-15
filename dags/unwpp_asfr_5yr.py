from __future__ import annotations

from datetime import datetime
import os
import time
import re
from typing import List, Tuple

import requests

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


PG_CONN_ID = "viss_data_db"

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

_ENV_COUNTRIES = os.getenv("ASFR_COUNTRIES")
if _ENV_COUNTRIES:
    WANT = {c.strip().upper() for c in _ENV_COUNTRIES.split(',') if c.strip()}
    ISO3_CODES = [c for c in ISO3_CODES if c in WANT]
else:
    _ISO3_ENV = (os.getenv("ASFR_ISO3") or "").strip().upper()
    if _ISO3_ENV:
        ISO3_CODES = [_ISO3_ENV]
YEAR_START = int(os.environ.get("ASFR_YEAR_START", "2015"))
YEAR_END = int(os.environ.get("ASFR_YEAR_END", "2025"))
YEARS: List[int] = list(range(YEAR_START, YEAR_END + 1))

UN_BASE = os.environ.get("UN_DP_BASE", "https://population.un.org/dataportalapi").rstrip("/")
UN_TOKEN = os.environ.get("UN_DP_BEARER_TOKEN", "").strip()

# WPP source tag for traceability
UN_SOURCE = os.environ.get("ASFR_SOURCE", "UN_WPP")
UN_RELEASE = os.environ.get("ASFR_RELEASE", "2024")

# Rate limiting: UN API docs mention 5 requests / 10 seconds per IP.
# We keep a conservative delay between calls.
REQUEST_DELAY_SECONDS = float(os.environ.get("UN_DP_REQUEST_DELAY_SECONDS", "2.5"))
REQUEST_TIMEOUT_SECONDS = float(os.environ.get("UN_DP_REQUEST_TIMEOUT_SECONDS", "60"))
ASFR_DEBUG = os.environ.get("ASFR_DEBUG", "").strip().lower() in ("1", "true", "yes", "y")

ASFR_VARIANT_CANDIDATES: dict[str, List[str]] = {
    # Store standardized variant_short_name keys on output; pick the first available underlying variant.
    "LOWER95": ["LOWER95", "LOWER80", "LOW"],
    "MEDIAN": ["MEDIAN", "EST", "ESTIMATE", "MAIN"],
    "UPPER95": ["UPPER95", "UPPER80", "HIGH"],
}


def _auth_headers() -> dict:
    headers: dict = {"Accept": "application/json"}
    if not UN_TOKEN:
        return headers
    tok = UN_TOKEN
    if tok.lower().startswith("bearer "):
        tok = tok.split(" ", 1)[1].strip()
    headers["Authorization"] = f"Bearer {tok}"
    return headers


def _ensure_db_tables_exist():
    hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    with hook.get_conn() as conn, conn.cursor() as cur:
        # Create table in the desired shape if it doesn't exist.
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS asfr_5yr (
                iso3 TEXT NOT NULL,
                year INTEGER NOT NULL,
                variant_short_name TEXT NOT NULL,
                age_min INTEGER NOT NULL,
                age_max INTEGER NOT NULL,
                asfr DOUBLE PRECISION NOT NULL,
                source TEXT NOT NULL,
                release TEXT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (iso3, year, variant_short_name, age_min, age_max, source, release)
            );
            """
        )

        # Idempotent migration from older schema (without variant_short_name).
        cur.execute(
            """
            SELECT 1
            FROM information_schema.columns
            WHERE table_name = 'asfr_5yr' AND column_name = 'variant_short_name'
            """
        )
        has_variant = cur.fetchone() is not None
        if not has_variant:
            cur.execute("ALTER TABLE asfr_5yr ADD COLUMN variant_short_name TEXT")
            cur.execute("UPDATE asfr_5yr SET variant_short_name = 'MEDIAN' WHERE variant_short_name IS NULL")
            cur.execute("ALTER TABLE asfr_5yr ALTER COLUMN variant_short_name SET NOT NULL")

        # Ensure primary key includes variant_short_name.
        cur.execute(
            """
            SELECT constraint_name
            FROM information_schema.table_constraints
            WHERE table_name = 'asfr_5yr' AND constraint_type = 'PRIMARY KEY'
            """
        )
        pk = cur.fetchone()
        if pk is not None:
            pk_name = pk[0]
            cur.execute(
                """
                SELECT kcu.column_name
                FROM information_schema.key_column_usage kcu
                WHERE kcu.table_name = 'asfr_5yr' AND kcu.constraint_name = %s
                ORDER BY kcu.ordinal_position
                """,
                (pk_name,),
            )
            pk_cols = [r[0] for r in cur.fetchall()]
            if "variant_short_name" not in pk_cols:
                cur.execute(f"ALTER TABLE asfr_5yr DROP CONSTRAINT {pk_name}")
                cur.execute(
                    """
                    ALTER TABLE asfr_5yr
                    ADD CONSTRAINT asfr_5yr_pkey
                    PRIMARY KEY (iso3, year, variant_short_name, age_min, age_max, source, release)
                    """
                )
        conn.commit()


def _get_json(url: str) -> list:
    r = requests.get(url, headers=_auth_headers(), timeout=REQUEST_TIMEOUT_SECONDS)
    if r.status_code == 401:
        raise RuntimeError(
            "UN API returned 401 Unauthorized. If your IP/environment requires auth, set UN_DP_BEARER_TOKEN."
        )
    if r.status_code == 429:
        raise RuntimeError("UN API rate limit hit (429). Increase UN_DP_REQUEST_DELAY_SECONDS.")
    r.raise_for_status()

    payload = r.json()
    # The API sometimes wraps results; normalize to a list of records.
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        # common patterns: {"data": [...]}, {"items": [...]}, {"records": [...]}
        for k in ("data", "items", "records", "result"):
            v = payload.get(k)
            if isinstance(v, list):
                return v
        raise RuntimeError(f"Unexpected JSON object shape from UN API (keys={list(payload.keys())})")
    if isinstance(payload, str):
        snippet = payload[:200].replace("\n", " ")
        raise RuntimeError(f"Unexpected JSON string payload from UN API: {snippet}")
    raise RuntimeError(f"Unexpected JSON type from UN API: {type(payload)}")


def _resolve_location_id(iso3: str) -> int:
    url = f"{UN_BASE}/api/v1/locations/{iso3}"
    rows = _get_json(url)
    for row in rows:
        if str(row.get("iso3", "")).upper() == iso3.upper():
            return int(row["id"])
    raise RuntimeError(f"LocationId not found for iso3={iso3}")


def _resolve_indicator_id(short_name: str) -> Tuple[int, str]:
    url = f"{UN_BASE}/api/v1/Indicators/{short_name}"
    rows = _get_json(url)
    for row in rows:
        if str(row.get("shortName", "")) == short_name:
            return int(row["id"]), str(row.get("name") or short_name)
    raise RuntimeError(f"Indicator not found: {short_name}")


def _get_first_present(d: dict, keys: Tuple[str, ...]):
    for k in keys:
        if k in d and d[k] is not None:
            return d[k]
    return None


def _parse_age_bin(r: dict) -> Tuple[int, int] | None:
    age_min_raw = _get_first_present(
        r,
        (
            "ageStart",
            "AgeStart",
            "age_start",
            "ageGrpStart",
            "AgeGrpStart",
            "ageGroupStart",
        ),
    )
    age_max_raw = _get_first_present(
        r,
        (
            "ageEnd",
            "AgeEnd",
            "age_end",
            "ageGrpEnd",
            "AgeGrpEnd",
            "ageGroupEnd",
        ),
    )

    # Some responses omit ageEnd but provide an ageLabel like "40-44".
    if age_min_raw is None:
        age_label = _get_first_present(r, ("ageLabel", "AgeLabel"))
        if isinstance(age_label, str):
            m = re.match(r"^\s*(\d+)\s*[-–]\s*(\d+)\s*$", age_label)
            if m:
                return int(m.group(1)), int(m.group(2))
        return None

    try:
        age_min = int(age_min_raw)
    except Exception:
        return None

    if age_max_raw is None:
        # Treat as single-year bin when ageEnd is missing.
        return age_min, age_min

    try:
        age_max = int(age_max_raw)
    except Exception:
        return None

    return age_min, age_max


def _fetch_asfr_rows(location_id: int, indicator_id: int, year: int) -> List[Tuple[str, int, int, float]]:
    url = (
        f"{UN_BASE}/api/v1/data/indicators/{indicator_id}/locations/{location_id}/start/{year}/end/{year}"
    )
    rows = _get_json(url)

    dict_rows = [r for r in rows if isinstance(r, dict)]
    if not dict_rows:
        return []

    # Group by (sexId, variantShortName) so we can select specific uncertainty bounds.
    groups: dict[tuple[int, str], list[dict]] = {}
    for r in dict_rows:
        sex_raw = _get_first_present(r, ("sexId", "SexID", "sex_id", "SexId"))
        var_raw = _get_first_present(r, ("variantShortName", "VariantShortName"))
        if sex_raw is None or var_raw is None:
            continue
        try:
            sex_id = int(sex_raw)
        except Exception:
            continue
        if not isinstance(var_raw, str) or not var_raw.strip():
            continue
        # Normalize to avoid mismatches like 'Median' vs 'MEDIAN'.
        variant = var_raw.strip().upper()
        groups.setdefault((sex_id, variant), []).append(r)

    if not groups:
        first_row = dict_rows[0]
        print("UN ASFR response had no groupable rows (missing sexId/variantShortName)")
        print(f"UN ASFR first row keys={list(first_row.keys())}")
        print(f"UN ASFR first row sample={first_row}")
        return []

    if ASFR_DEBUG and year in (2024, 2025):
        group_summaries: List[Tuple[int, int, str, List[Tuple[int, int]]]] = []
        for (sex_id, variant), rs in groups.items():
            uniq = set()
            for r in rs:
                ab = _parse_age_bin(r)
                if ab is None:
                    continue
                a0, a1 = ab
                if a0 < 15 or a1 > 49:
                    continue
                uniq.add((a0, a1))
            bins = sorted(list(uniq), key=lambda t: (t[0], t[1]))
            group_summaries.append((len(bins), sex_id, variant, bins))

        group_summaries.sort(key=lambda t: (t[0], 1 if t[1] == 2 else (0 if t[1] == 3 else -1)), reverse=True)
        print(f"ASFR_DEBUG year={year}: group summaries (top 10)")
        for bins_count, sex_id, variant, bins in group_summaries[:10]:
            head = bins[:10]
            tail = bins[-10:] if len(bins) > 10 else []
            print(
                f"  sexId={sex_id} variant={variant} bins_15_49={bins_count} "
                f"head={head} tail={tail}"
            )

    sex_ids = {k[0] for k in groups.keys()}
    selected_sex_id: int | None = None
    if 2 in sex_ids:
        selected_sex_id = 2
    elif 3 in sex_ids:
        selected_sex_id = 3
        print(
            f"WARNING: No female (sexId=2) ASFR rows returned for year={year}; using sexId=3 (both sexes)"
        )
    else:
        print(f"WARNING: No usable sexId (2 or 3) ASFR rows returned for year={year}; skipping")
        return []

    assert selected_sex_id is not None

    chosen_underlying: dict[str, str] = {}
    for out_variant, candidates in ASFR_VARIANT_CANDIDATES.items():
        found = None
        for cand in candidates:
            if (selected_sex_id, cand) in groups:
                found = cand
                break
        if found is None:
            available = sorted(list({v for (sid, v) in groups.keys() if sid == selected_sex_id}))
            print(
                "WARNING: Missing required ASFR variant; skipping year="
                f"{year} sexId={selected_sex_id} missing_bucket={out_variant} candidates={candidates} available_variants={available}"
            )
            return []
        chosen_underlying[out_variant] = found

    out: List[Tuple[str, int, int, float]] = []
    for out_variant, underlying_variant in chosen_underlying.items():
        rs = groups[(selected_sex_id, underlying_variant)]
        for r in rs:
            val_raw = _get_first_present(r, ("value", "Value", "val", "Val"))
            ab = _parse_age_bin(r)
            if ab is None or val_raw is None:
                continue
            age_min, age_max = ab
            try:
                val = float(val_raw)
            except Exception:
                continue
            if age_min < 15 or age_max > 49:
                continue
            out.append((out_variant, age_min, age_max, val))

    print(
        f"Selected ASFR variants for year={year} sexId={selected_sex_id}: "
        + ", ".join([f"{k}<-{v}" for k, v in chosen_underlying.items()])
    )

    out.sort(key=lambda t: (t[0], t[1]))
    return out


def ingest_asfr_country(iso3: str):
    iso3 = str(iso3).upper().strip()
    _ensure_db_tables_exist()

    location_id = _resolve_location_id(iso3)
    time.sleep(REQUEST_DELAY_SECONDS)

    indicator_id, _indicator_name = _resolve_indicator_id("ASFR5")
    time.sleep(REQUEST_DELAY_SECONDS)

    hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    with hook.get_conn() as conn, conn.cursor() as cur:
        for year in YEARS:
            bins = _fetch_asfr_rows(location_id, indicator_id, year)
            if not bins:
                print(f"WARNING: Skipping ASFR ingest for iso3={iso3} year={year} (no female data)")
                time.sleep(REQUEST_DELAY_SECONDS)
                continue

            # Overwrite semantics: remove any prior outputs for this iso3/year so stale bins don't linger.
            cur.execute(
                "DELETE FROM asfr_5yr WHERE iso3 = %s AND year = %s",
                (iso3, int(year)),
            )

            for variant_short_name, age_min, age_max, asfr in bins:
                cur.execute(
                    """
                    INSERT INTO asfr_5yr (iso3, year, variant_short_name, age_min, age_max, asfr, source, release)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (iso3, year, variant_short_name, age_min, age_max, source, release)
                    DO UPDATE SET asfr = EXCLUDED.asfr
                    """,
                    (
                        iso3,
                        int(year),
                        str(variant_short_name),
                        int(age_min),
                        int(age_max),
                        float(asfr),
                        UN_SOURCE,
                        UN_RELEASE,
                    ),
                )
            conn.commit()
            print(f"Stored ASFR bins in Postgres: iso3={iso3} year={year} bins={len(bins)}")

            time.sleep(REQUEST_DELAY_SECONDS)


for CC3 in ISO3_CODES:
    cc3u = CC3.upper()
    cc3l = CC3.lower()

    with DAG(
        dag_id=f"unwpp_asfr_5yr_{cc3l}_{YEAR_START}_{YEAR_END}",
        description=(
            f"Ingest ASFR 5-year fertility rates for {cc3u} {YEAR_START}-{YEAR_END} "
            "from UN Data Portal API into Postgres"
        ),
        start_date=datetime(2025, 1, 1),
        schedule=None,
        catchup=False,
        max_active_runs=1,
        default_args={"owner": "airflow", "retries": 2},
        tags=["un", "wpp", "asfr", "demography", cc3u],
    ) as dag:
        PythonOperator(
            task_id=f"ingest_asfr_{cc3l}_{YEAR_START}_{YEAR_END}",
            python_callable=ingest_asfr_country,
            op_kwargs={"iso3": cc3u},
        )