"""
WHO Global Health Observatory (GHO) - Disease Burden Indicators
===============================================================

Ingests key epidemiological indicators from the WHO GHO OData API into
PostgreSQL for use by the VISS SEIRS compartmental model.

Data source
-----------
- API: https://ghoapi.azureedge.net/api/
- License: WHO Open Data (CC BY-NC-SA 3.0 IGO)
- Auth: None required
- Format: JSON (OData v4)
- Update frequency: Annually (some indicators updated more often)

Indicators ingested
-------------------
The DAG fetches country-level time-series for indicators that directly
parameterise SEIRS transmission, mortality, and recovery dynamics:

  HIV_0000000001 - Estimated number of people (all ages) living with HIV
  HIV_0000000006 - Estimated incidence rate (new HIV infections per 1000 uninfected)
  HIV_0000000026 - Estimated number of deaths due to HIV/AIDS (all ages)
  MDG_0000000020 - Estimated TB incidence per 100 000 population
  WHS3_49       - Total NTD interventions (proxy for health-system reach)
  WHOSIS_000001 - Life expectancy at birth (both sexes)
  WHOSIS_000002 - Healthy life expectancy (HALE) at birth (both sexes)
  WHS7_104      - Measles immunization coverage among 1-year-olds (%)
  NCDMORT3070   - Probability of dying between age 30 and 70 from NCD (%)
  MDG_0000000007 - Under-five mortality rate (per 1000 live births)

Why it matters
--------------
- HIV prevalence/incidence/mortality are the primary calibration targets
  for the VISS HIV-focused SEIRS model.
- TB incidence captures co-infection dynamics critical in sub-Saharan Africa.
- Life expectancy and child/NCD mortality set the background-death component
  of the compartmental "R -> S" (waning) and exit flows.
- Immunization coverage proxies for health-system capacity that modulates
  intervention effectiveness in scenario modelling.

Table: who_gho_indicators
-------------------------
  iso3                TEXT        (ISO 3166-1 alpha-3)
  indicator_code      TEXT        (GHO indicator code)
  indicator_name      TEXT        (human-readable name)
  year                INTEGER
  numeric_value       DOUBLE PRECISION
  low_value           DOUBLE PRECISION  (lower bound if available)
  high_value          DOUBLE PRECISION  (upper bound if available)
  sex                 TEXT        (BTSX, MLE, FMLE, or NULL)
  age_group           TEXT        (if applicable, NULL otherwise)
  source              TEXT        (always 'WHO_GHO')
  created_at          TIMESTAMPTZ
"""
from __future__ import annotations

import os
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import requests

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
PG_CONN_ID = os.environ.get("PG_CONN_ID", "viss_data_db")

# GHO OData API base
GHO_BASE = os.environ.get(
    "WHO_GHO_BASE", "https://ghoapi.azureedge.net/api"
).rstrip("/")

# Rate-limit: GHO API is generous but we stay polite.
REQUEST_DELAY = float(os.environ.get("WHO_GHO_REQUEST_DELAY", "1.0"))
REQUEST_TIMEOUT = float(os.environ.get("WHO_GHO_REQUEST_TIMEOUT", "60"))

# Country selection (override via env; comma-separated ISO3)
_ENV_COUNTRIES = os.environ.get("WHO_GHO_COUNTRIES", "").strip()

ISO3_CODES: List[str] = [
    'ABW','AFG','AGO','ALA','ALB','AND','ARE','ARG','ARM','AUS','AUT','AZE',
    'BDI','BEL','BEN','BFA','BGD','BGR','BHR','BHS','BIH','BLR','BLZ','BOL',
    'BRA','BRB','BRN','BTN','BWA',
    'CAF','CAN','CHE','CHL','CHN','CIV','CMR','COD','COG','COL','COM','CPV',
    'CRI','CUB','CYP','CZE',
    'DEU','DJI','DMA','DNK','DOM','DZA',
    'ECU','EGY','ERI','ESP','EST','ETH',
    'FIN','FJI','FRA',
    'GAB','GBR','GEO','GHA','GIN','GMB','GNB','GNQ','GRC','GRD','GTM','GUY',
    'HND','HRV','HTI','HUN',
    'IDN','IND','IRL','IRN','IRQ','ISL','ISR','ITA',
    'JAM','JOR','JPN',
    'KAZ','KEN','KGZ','KHM','KIR','KOR','KWT',
    'LAO','LBN','LBR','LBY','LCA','LKA','LSO','LTU','LUX','LVA',
    'MAR','MDA','MDG','MDV','MEX','MKD','MLI','MLT','MMR','MNE','MNG','MOZ',
    'MRT','MUS','MWI','MYS',
    'NAM','NER','NGA','NIC','NLD','NOR','NPL','NZL',
    'OMN',
    'PAK','PAN','PER','PHL','PNG','POL','PRT','PRY','PSE',
    'QAT',
    'ROU','RUS','RWA',
    'SAU','SDN','SEN','SGP','SLB','SLE','SLV','SOM','SRB','SSD','STP','SUR',
    'SVK','SVN','SWE','SWZ','SYC','SYR',
    'TCD','TGO','THA','TJK','TKM','TLS','TON','TTO','TUN','TUR','TZA',
    'UGA','UKR','URY','USA','UZB',
    'VCT','VEN','VNM','VUT',
    'WSM',
    'YEM',
    'ZAF','ZMB','ZWE',
]

if _ENV_COUNTRIES:
    _WANT = {c.strip().upper() for c in _ENV_COUNTRIES.split(",") if c.strip()}
    ISO3_CODES = [c for c in ISO3_CODES if c in _WANT]

# Year range
YEAR_START = int(os.environ.get("WHO_GHO_YEAR_START", "2000"))
YEAR_END = int(os.environ.get("WHO_GHO_YEAR_END", "2025"))

# Indicators to ingest -- (code, human-readable name)
INDICATORS: List[Tuple[str, str]] = [
    ("HIV_0000000001", "People living with HIV"),
    ("HIV_0000000006", "HIV incidence rate per 1000 uninfected"),
    ("HIV_0000000026", "HIV/AIDS deaths"),
    ("MDG_0000000020", "TB incidence per 100k"),
    ("WHS3_49",        "NTD interventions"),
    ("WHOSIS_000001",  "Life expectancy at birth"),
    ("WHOSIS_000002",  "Healthy life expectancy (HALE)"),
    ("WHS7_104",       "Measles immunization coverage (%)"),
    ("NCDMORT3070",    "Prob. dying 30-70 from NCD (%)"),
    ("MDG_0000000007", "Under-5 mortality rate per 1000"),
]


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------
def _ensure_table():
    hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS who_gho_indicators (
                iso3            TEXT NOT NULL,
                indicator_code  TEXT NOT NULL,
                indicator_name  TEXT NOT NULL,
                year            INTEGER NOT NULL,
                numeric_value   DOUBLE PRECISION,
                low_value       DOUBLE PRECISION,
                high_value      DOUBLE PRECISION,
                sex             TEXT,
                age_group       TEXT,
                source          TEXT NOT NULL DEFAULT 'WHO_GHO',
                created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (iso3, indicator_code, year, sex, age_group)
            );
            """
        )
        conn.commit()


def _safe_float(v: Any) -> Optional[float]:
    """Convert a value to float, returning None when not possible."""
    if v is None:
        return None
    try:
        return float(v)
    except (ValueError, TypeError):
        return None


# ---------------------------------------------------------------------------
# GHO API helpers
# ---------------------------------------------------------------------------
def _gho_get_json(url: str) -> List[Dict]:
    """Fetch a GHO OData endpoint, handling pagination via @odata.nextLink."""
    all_records: List[Dict] = []
    current_url: Optional[str] = url

    while current_url:
        resp = requests.get(current_url, timeout=REQUEST_TIMEOUT)
        if resp.status_code == 404:
            print(f"GHO 404 for URL (indicator may not exist): {current_url}")
            return []
        resp.raise_for_status()
        payload = resp.json()

        records = payload.get("value", [])
        all_records.extend(records)

        current_url = payload.get("@odata.nextLink")
        if current_url:
            time.sleep(REQUEST_DELAY)

    return all_records


def _fetch_indicator_for_country(
    indicator_code: str,
    iso3: str,
) -> List[Dict]:
    """Fetch all observations for one indicator + one country."""
    # GHO OData filter: SpatialDim eq '<ISO3>' and TimeDim ge YEAR_START and TimeDim le YEAR_END
    url = (
        f"{GHO_BASE}/{indicator_code}"
        f"?$filter=SpatialDim eq '{iso3.upper()}'"
        f" and TimeDim ge {YEAR_START}"
        f" and TimeDim le {YEAR_END}"
    )
    return _gho_get_json(url)


# ---------------------------------------------------------------------------
# Main ingest callable
# ---------------------------------------------------------------------------
def ingest_who_gho_country(iso3: str):
    """Ingest all configured WHO GHO indicators for a single country."""
    iso3 = iso3.upper().strip()
    _ensure_table()

    hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    total_rows = 0

    with hook.get_conn() as conn, conn.cursor() as cur:
        for indicator_code, indicator_name in INDICATORS:
            rows = _fetch_indicator_for_country(indicator_code, iso3)
            time.sleep(REQUEST_DELAY)

            if not rows:
                print(
                    f"WHO GHO: No data for {indicator_code} ({indicator_name}) "
                    f"iso3={iso3} years={YEAR_START}-{YEAR_END}"
                )
                continue

            inserted = 0
            for r in rows:
                year_raw = r.get("TimeDim") or r.get("TimeDimensionValue")
                val_raw = r.get("NumericValue") or r.get("Value")

                if year_raw is None or val_raw is None:
                    continue

                try:
                    year = int(year_raw)
                except (ValueError, TypeError):
                    continue

                numeric_value = _safe_float(val_raw)
                if numeric_value is None:
                    continue

                low = _safe_float(r.get("Low"))
                high = _safe_float(r.get("High"))
                sex = r.get("Dim1") or r.get("Dim1Type") or None
                age_group = r.get("Dim2") or r.get("Dim2Type") or None

                # Normalize sex/age to stable strings for the PK
                sex_str = str(sex).strip().upper() if sex else "BTSX"
                age_str = str(age_group).strip() if age_group else "TOTAL"

                cur.execute(
                    """
                    INSERT INTO who_gho_indicators
                        (iso3, indicator_code, indicator_name, year,
                         numeric_value, low_value, high_value,
                         sex, age_group, source)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (iso3, indicator_code, year, sex, age_group)
                    DO UPDATE SET
                        numeric_value  = EXCLUDED.numeric_value,
                        low_value      = EXCLUDED.low_value,
                        high_value     = EXCLUDED.high_value,
                        indicator_name = EXCLUDED.indicator_name
                    """,
                    (
                        iso3,
                        indicator_code,
                        indicator_name,
                        year,
                        numeric_value,
                        low,
                        high,
                        sex_str,
                        age_str,
                        "WHO_GHO",
                    ),
                )
                inserted += 1

            conn.commit()
            total_rows += inserted
            print(
                f"WHO GHO: Stored {inserted} rows for {indicator_code} "
                f"({indicator_name}) iso3={iso3}"
            )

    print(f"WHO GHO: Completed iso3={iso3}, total rows upserted={total_rows}")


# ---------------------------------------------------------------------------
# DAG factory: one DAG per country
# ---------------------------------------------------------------------------
for _CC3 in ISO3_CODES:
    _cc3u = _CC3.upper()
    _cc3l = _CC3.lower()

    with DAG(
        dag_id=f"who_gho_disease_burden_{_cc3l}",
        description=(
            f"Ingest WHO GHO disease-burden indicators for {_cc3u} "
            f"({YEAR_START}-{YEAR_END}) into Postgres"
        ),
        start_date=datetime(2025, 1, 1),
        schedule=None,
        catchup=False,
        max_active_runs=1,
        default_args={"owner": "airflow", "retries": 2},
        tags=["who", "gho", "disease_burden", "hiv", "epidemiology", _cc3u],
    ) as dag:
        PythonOperator(
            task_id=f"ingest_who_gho_{_cc3l}",
            python_callable=ingest_who_gho_country,
            op_kwargs={"iso3": _cc3u},
        )
