from __future__ import annotations

from datetime import datetime
import os
import time
from typing import List, Tuple

import requests

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


PG_CONN_ID = "viss_data_db"

ISO3 = os.environ.get("ASFR_ISO3", "SUR").upper()
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
    sql = """
    CREATE TABLE IF NOT EXISTS asfr_5yr (
        iso3 TEXT NOT NULL,
        year INTEGER NOT NULL,
        age_min INTEGER NOT NULL,
        age_max INTEGER NOT NULL,
        asfr DOUBLE PRECISION NOT NULL,
        source TEXT NOT NULL,
        release TEXT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        PRIMARY KEY (iso3, year, age_min, age_max, source, release)
    );
    """
    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql)
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


def _fetch_asfr_rows(location_id: int, indicator_id: int, year: int) -> List[Tuple[int, int, float]]:
    url = (
        f"{UN_BASE}/api/v1/data/indicators/{indicator_id}/locations/{location_id}/start/{year}/end/{year}"
    )
    rows = _get_json(url)

    dict_rows = [r for r in rows if isinstance(r, dict)]
    if not dict_rows:
        return []

    # Decide which sex to ingest.
    sex_ids = set()
    for r in dict_rows:
        s = _get_first_present(r, ("sexId", "SexID", "sex_id", "SexId"))
        if s is not None:
            try:
                sex_ids.add(int(s))
            except Exception:
                pass

    # Prefer female (2). If only both sexes (3) is available, fall back.
    preferred_sex_id = 2 if 2 in sex_ids else (3 if 3 in sex_ids else None)

    out: List[Tuple[int, int, float]] = []

    for r in dict_rows:
        # Sex filter (only if the response includes sexId)
        sex_raw = _get_first_present(r, ("sexId", "SexID", "sex_id", "SexId"))
        if preferred_sex_id is not None and sex_raw is not None:
            try:
                if int(sex_raw) != preferred_sex_id:
                    continue
            except Exception:
                continue

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
        val_raw = _get_first_present(r, ("value", "Value", "val", "Val"))

        if age_min_raw is None or age_max_raw is None or val_raw is None:
            continue

        try:
            age_min = int(age_min_raw)
            age_max = int(age_max_raw)
            val = float(val_raw)
        except Exception:
            continue

        # Keep fertility ages 15-49.
        if age_min < 15 or age_max > 49:
            continue

        out.append((age_min, age_max, val))

    out.sort(key=lambda t: t[0])
    if not out:
        first_row = dict_rows[0]
        print(
            "UN ASFR response contained 0 parsed bins. "
            f"sex_ids={sorted(sex_ids)} preferred_sex_id={preferred_sex_id}"
        )
        print(f"UN ASFR first row keys={list(first_row.keys())}")
        print(f"UN ASFR first row sample={first_row}")
    return out


def ingest_asfr_sur_2015_2025():
    if ISO3 != "SUR":
        raise RuntimeError("This DAG is currently scoped to SUR only. Override ASFR_ISO3 if needed.")
    if YEAR_START != 2015 or YEAR_END != 2025:
        raise RuntimeError("This DAG is currently scoped to years 2015-2025 only. Override ASFR_YEAR_START/END if needed.")

    _ensure_db_tables_exist()

    location_id = _resolve_location_id(ISO3)
    time.sleep(REQUEST_DELAY_SECONDS)

    indicator_id, _indicator_name = _resolve_indicator_id("ASFR5")
    time.sleep(REQUEST_DELAY_SECONDS)

    hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    with hook.get_conn() as conn, conn.cursor() as cur:
        for year in YEARS:
            bins = _fetch_asfr_rows(location_id, indicator_id, year)
            if not bins:
                raise RuntimeError(f"No ASFR rows returned for iso3={ISO3} year={year}")

            # Overwrite semantics: remove any prior outputs for this iso3/year so stale bins don't linger.
            cur.execute(
                "DELETE FROM asfr_5yr WHERE iso3 = %s AND year = %s",
                (ISO3, int(year)),
            )

            for age_min, age_max, asfr in bins:
                cur.execute(
                    """
                    INSERT INTO asfr_5yr (iso3, year, age_min, age_max, asfr, source, release)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (iso3, year, age_min, age_max, source, release)
                    DO UPDATE SET asfr = EXCLUDED.asfr
                    """,
                    (
                        ISO3,
                        int(year),
                        int(age_min),
                        int(age_max),
                        float(asfr),
                        UN_SOURCE,
                        UN_RELEASE,
                    ),
                )
            conn.commit()
            print(f"Stored ASFR bins in Postgres: iso3={ISO3} year={year} bins={len(bins)}")

            time.sleep(REQUEST_DELAY_SECONDS)


with DAG(
    dag_id="unwpp_asfr_5yr_sur_2015_2025",
    description="Ingest ASFR 5-year fertility rates for Suriname (SUR) 2015-2025 from UN Data Portal API into Postgres",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "airflow", "retries": 2},
    tags=["un", "wpp", "asfr", "demography"],
) as dag:
    PythonOperator(
        task_id="ingest_asfr_sur_2015_2025",
        python_callable=ingest_asfr_sur_2015_2025,
    )