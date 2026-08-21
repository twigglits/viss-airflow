"""
Our World in Data (OWID) - Infectious Disease Surveillance
==========================================================

Ingests comprehensive COVID-19 and infectious disease surveillance data from
the Our World in Data GitHub repository into PostgreSQL for VISS SEIRS model
calibration and validation.

Data source
-----------
- URL: https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/owid-covid-data.csv
- License: Creative Commons Attribution 4.0 International (CC-BY 4.0)
- Auth: None required
- Format: CSV (single large file, ~50 MB)
- Update frequency: Daily (during active pandemic), now weekly/monthly
- Maintained by: Our World in Data (University of Oxford)

What data is ingested
---------------------
The OWID COVID dataset is the most comprehensive open pandemic surveillance
dataset available.  For each country-day observation, the DAG stores:

  - Case counts (total, new, smoothed)
  - Death counts (total, new, smoothed)
  - Reproduction rate (R_t) -- directly calibrates SEIRS beta parameter
  - Testing data (total tests, positive rate)
  - Vaccination data (total, people vaccinated, people fully vaccinated,
    boosters, doses per 100)
  - Stringency index (Oxford COVID-19 Government Response Tracker)
  - Hospital & ICU data (patients, admissions per million)
  - Excess mortality metrics

Why it matters
--------------
1. **R_t (reproduction rate)** is the single most important parameter for
   calibrating SEIRS transmission dynamics.  OWID provides daily R_t
   estimates from Johns Hopkins CSSE data.
2. **Vaccination coverage** directly modulates the S->R transition in the
   SEIRS model (immunity acquisition without infection).
3. **Stringency index** quantifies non-pharmaceutical interventions (NPIs)
   that affect the contact rate (beta modulation).
4. **Hospital/ICU occupancy** validates the model's "I" compartment against
   real healthcare system load.
5. **Excess mortality** provides ground-truth for total disease impact
   beyond officially reported deaths.
6. **Testing positive rate** calibrates the reporting/ascertainment ratio
   between true and observed incidence.

Table: owid_covid_timeseries
-----------------------------
  iso3                   TEXT       (ISO 3166-1 alpha-3)
  date                   DATE
  total_cases            DOUBLE PRECISION
  new_cases              DOUBLE PRECISION
  new_cases_smoothed     DOUBLE PRECISION
  total_deaths           DOUBLE PRECISION
  new_deaths             DOUBLE PRECISION
  new_deaths_smoothed    DOUBLE PRECISION
  reproduction_rate      DOUBLE PRECISION
  total_tests            DOUBLE PRECISION
  positive_rate          DOUBLE PRECISION
  total_vaccinations     DOUBLE PRECISION
  people_vaccinated      DOUBLE PRECISION
  people_fully_vaccinated DOUBLE PRECISION
  total_boosters         DOUBLE PRECISION
  total_vaccinations_per_hundred DOUBLE PRECISION
  stringency_index       DOUBLE PRECISION
  hosp_patients          DOUBLE PRECISION
  icu_patients           DOUBLE PRECISION
  hosp_patients_per_million DOUBLE PRECISION
  icu_patients_per_million  DOUBLE PRECISION
  excess_mortality_cumulative_absolute DOUBLE PRECISION
  excess_mortality_cumulative_per_million DOUBLE PRECISION
  excess_mortality       DOUBLE PRECISION
  source                 TEXT (always 'OWID')
  created_at             TIMESTAMPTZ

Table: owid_covid_country_latest
---------------------------------
Materialized summary with the latest snapshot per country for quick lookups.
"""
from __future__ import annotations

import csv
import io
import os
import time
from datetime import datetime, timedelta

# Per-task ceiling. Without one, a wedged task holds an executor slot
# forever -- and at core.parallelism=2 that is half the install.
# downloads and parses the OWID CSV.
TASK_TIMEOUT = timedelta(minutes=int(os.environ.get("OWID_TASK_TIMEOUT_MIN", "90")))
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
PG_CONN_ID = os.environ.get("PG_CONN_ID", "viss_data_db")

OWID_CSV_URL = os.environ.get(
    "OWID_COVID_CSV_URL",
    "https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/owid-covid-data.csv",
)

# Alternatively, store a local copy to avoid repeated large downloads
DATA_ROOT = Path(os.environ.get("AIRFLOW_DATA_DIR", "/opt/airflow/data"))
DATA_ROOT.mkdir(parents=True, exist_ok=True)
LOCAL_CSV = DATA_ROOT / "owid-covid-data.csv"

REQUEST_TIMEOUT = int(os.environ.get("OWID_REQUEST_TIMEOUT", "300"))
BATCH_SIZE = int(os.environ.get("OWID_BATCH_SIZE", "1000"))

# Country filter (optional, comma-separated ISO3 codes)
_ENV_COUNTRIES = os.environ.get("OWID_COUNTRIES", "").strip()
COUNTRY_FILTER: Optional[set] = None
if _ENV_COUNTRIES:
    COUNTRY_FILTER = {c.strip().upper() for c in _ENV_COUNTRIES.split(",") if c.strip()}

# Columns we extract from the CSV (must match OWID CSV headers exactly)
COLUMNS_OF_INTEREST = [
    "iso_code",
    "date",
    "total_cases",
    "new_cases",
    "new_cases_smoothed",
    "total_deaths",
    "new_deaths",
    "new_deaths_smoothed",
    "reproduction_rate",
    "total_tests",
    "positive_rate",
    "total_vaccinations",
    "people_vaccinated",
    "people_fully_vaccinated",
    "total_boosters",
    "total_vaccinations_per_hundred",
    "stringency_index",
    "hosp_patients",
    "icu_patients",
    "hosp_patients_per_million",
    "icu_patients_per_million",
    "excess_mortality_cumulative_absolute",
    "excess_mortality_cumulative_per_million",
    "excess_mortality",
]

# Numeric columns (everything except iso_code and date)
NUMERIC_COLS = [c for c in COLUMNS_OF_INTEREST if c not in ("iso_code", "date")]


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------
def _ensure_tables():
    hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS owid_covid_timeseries (
                iso3                                   TEXT NOT NULL,
                date                                   DATE NOT NULL,
                total_cases                            DOUBLE PRECISION,
                new_cases                              DOUBLE PRECISION,
                new_cases_smoothed                     DOUBLE PRECISION,
                total_deaths                           DOUBLE PRECISION,
                new_deaths                             DOUBLE PRECISION,
                new_deaths_smoothed                    DOUBLE PRECISION,
                reproduction_rate                      DOUBLE PRECISION,
                total_tests                            DOUBLE PRECISION,
                positive_rate                          DOUBLE PRECISION,
                total_vaccinations                     DOUBLE PRECISION,
                people_vaccinated                      DOUBLE PRECISION,
                people_fully_vaccinated                DOUBLE PRECISION,
                total_boosters                         DOUBLE PRECISION,
                total_vaccinations_per_hundred         DOUBLE PRECISION,
                stringency_index                       DOUBLE PRECISION,
                hosp_patients                          DOUBLE PRECISION,
                icu_patients                           DOUBLE PRECISION,
                hosp_patients_per_million              DOUBLE PRECISION,
                icu_patients_per_million               DOUBLE PRECISION,
                excess_mortality_cumulative_absolute   DOUBLE PRECISION,
                excess_mortality_cumulative_per_million DOUBLE PRECISION,
                excess_mortality                       DOUBLE PRECISION,
                source                                 TEXT NOT NULL DEFAULT 'OWID',
                created_at                             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (iso3, date)
            );
            """
        )

        # Summary table for quick lookups
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS owid_covid_country_latest (
                iso3                       TEXT PRIMARY KEY,
                last_date                  DATE,
                total_cases                DOUBLE PRECISION,
                total_deaths               DOUBLE PRECISION,
                total_vaccinations         DOUBLE PRECISION,
                people_fully_vaccinated    DOUBLE PRECISION,
                last_reproduction_rate     DOUBLE PRECISION,
                last_stringency_index      DOUBLE PRECISION,
                source                     TEXT NOT NULL DEFAULT 'OWID',
                updated_at                 TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """
        )
        conn.commit()


def _safe_float(v: Any) -> Optional[float]:
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (ValueError, TypeError):
        return None


def _safe_date(v: Any) -> Optional[str]:
    """Validate date string is in YYYY-MM-DD format."""
    if not v or not isinstance(v, str):
        return None
    v = v.strip()
    if len(v) != 10:
        return None
    try:
        datetime.strptime(v, "%Y-%m-%d")
        return v
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Download
# ---------------------------------------------------------------------------
def _download_csv():
    """Download the OWID CSV to local storage, skipping if recent copy exists."""
    if LOCAL_CSV.exists():
        age_hours = (time.time() - LOCAL_CSV.stat().st_mtime) / 3600
        if age_hours < 12:
            print(
                f"OWID CSV exists and is {age_hours:.1f}h old (< 12h); "
                "skipping download"
            )
            return str(LOCAL_CSV)

    print(f"Downloading OWID COVID CSV from {OWID_CSV_URL} ...")
    tmp = LOCAL_CSV.with_suffix(".csv.partial")
    with requests.get(OWID_CSV_URL, stream=True, timeout=REQUEST_TIMEOUT) as resp:
        resp.raise_for_status()
        with open(tmp, "wb") as f:
            for chunk in resp.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)
    os.replace(tmp, LOCAL_CSV)
    size_mb = LOCAL_CSV.stat().st_size / (1024 * 1024)
    print(f"Downloaded OWID CSV: {LOCAL_CSV} ({size_mb:.1f} MB)")
    return str(LOCAL_CSV)


# ---------------------------------------------------------------------------
# Ingest
# ---------------------------------------------------------------------------
def _ingest_csv():
    """Parse the CSV and batch-upsert into Postgres."""
    _ensure_tables()
    csv_path = LOCAL_CSV

    if not csv_path.exists():
        raise FileNotFoundError(f"OWID CSV not found at {csv_path}; run download first")

    hook = PostgresHook(postgres_conn_id=PG_CONN_ID)

    # Build the INSERT statement dynamically from NUMERIC_COLS
    col_names = ["iso3", "date"] + NUMERIC_COLS + ["source"]
    placeholders = ", ".join(["%s"] * len(col_names))
    col_list = ", ".join(col_names)

    # ON CONFLICT update all numeric columns
    update_set = ", ".join(
        [f"{c} = EXCLUDED.{c}" for c in NUMERIC_COLS]
    )

    insert_sql = f"""
        INSERT INTO owid_covid_timeseries ({col_list})
        VALUES ({placeholders})
        ON CONFLICT (iso3, date)
        DO UPDATE SET {update_set}
    """

    total_inserted = 0
    countries_seen = set()

    with open(csv_path, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        batch: list = []

        with hook.get_conn() as conn, conn.cursor() as cur:
            for row in reader:
                iso_code = (row.get("iso_code") or "").strip().upper()

                # Skip aggregate rows (OWID_* codes like OWID_WRL, OWID_AFR, etc.)
                if not iso_code or iso_code.startswith("OWID_"):
                    continue

                # Skip if country filter is set and this country is not in it
                if COUNTRY_FILTER and iso_code not in COUNTRY_FILTER:
                    continue

                date_str = _safe_date(row.get("date"))
                if date_str is None:
                    continue

                values = [iso_code, date_str]
                has_any_value = False
                for col in NUMERIC_COLS:
                    val = _safe_float(row.get(col))
                    values.append(val)
                    if val is not None:
                        has_any_value = True

                # Skip rows with zero useful data
                if not has_any_value:
                    continue

                values.append("OWID")  # source
                batch.append(tuple(values))
                countries_seen.add(iso_code)

                if len(batch) >= BATCH_SIZE:
                    cur.executemany(insert_sql, batch)
                    conn.commit()
                    total_inserted += len(batch)
                    batch = []

            # Flush remaining
            if batch:
                cur.executemany(insert_sql, batch)
                conn.commit()
                total_inserted += len(batch)

    print(
        f"OWID: Ingested {total_inserted} rows for "
        f"{len(countries_seen)} countries into owid_covid_timeseries"
    )
    return total_inserted


def _update_country_latest():
    """Refresh the owid_covid_country_latest summary table."""
    _ensure_tables()
    hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    with hook.get_conn() as conn, conn.cursor() as cur:
        # Upsert from the latest row per country
        cur.execute(
            """
            INSERT INTO owid_covid_country_latest
                (iso3, last_date, total_cases, total_deaths,
                 total_vaccinations, people_fully_vaccinated,
                 last_reproduction_rate, last_stringency_index,
                 source, updated_at)
            SELECT DISTINCT ON (iso3)
                iso3,
                date,
                total_cases,
                total_deaths,
                total_vaccinations,
                people_fully_vaccinated,
                reproduction_rate,
                stringency_index,
                'OWID',
                NOW()
            FROM owid_covid_timeseries
            ORDER BY iso3, date DESC
            ON CONFLICT (iso3)
            DO UPDATE SET
                last_date                = EXCLUDED.last_date,
                total_cases              = EXCLUDED.total_cases,
                total_deaths             = EXCLUDED.total_deaths,
                total_vaccinations       = EXCLUDED.total_vaccinations,
                people_fully_vaccinated  = EXCLUDED.people_fully_vaccinated,
                last_reproduction_rate   = EXCLUDED.last_reproduction_rate,
                last_stringency_index    = EXCLUDED.last_stringency_index,
                updated_at               = NOW()
            """
        )
        affected = cur.rowcount
        conn.commit()
    print(f"OWID: Updated {affected} rows in owid_covid_country_latest")


# ---------------------------------------------------------------------------
# DAG definition (single DAG -- the CSV is one global file)
# ---------------------------------------------------------------------------
with DAG(
    dag_id="owid_covid_surveillance",
    description=(
        "Ingest OWID COVID-19 surveillance data (cases, deaths, R_t, "
        "vaccinations, stringency, hospital/ICU, excess mortality) into Postgres"
    ),
    start_date=datetime(2025, 1, 1),
    schedule=None,  # Trigger manually or via external schedule
    catchup=False,
    max_active_runs=1,
    concurrency=1,
    default_args={"owner": "airflow", "retries": 1, "execution_timeout": TASK_TIMEOUT},
    tags=["owid", "covid", "surveillance", "vaccination", "epidemiology"],
) as dag:

    download = PythonOperator(
        task_id="download_owid_csv",
        python_callable=_download_csv,
    )

    ingest = PythonOperator(
        task_id="ingest_owid_csv",
        python_callable=_ingest_csv,
    )

    summarise = PythonOperator(
        task_id="update_country_latest",
        python_callable=_update_country_latest,
    )

    download >> ingest >> summarise
