"""
World Bank Health Indicators
=============================

Ingests key health-system and socioeconomic indicators from the World Bank
Open Data API into PostgreSQL for use by the VISS SEIRS compartmental model.

Data source
-----------
- API: https://api.worldbank.org/v2/
- License: Creative Commons Attribution 4.0 (CC-BY 4.0)
- Auth: None required
- Format: JSON (set format=json in query params)
- Update frequency: Annually (most indicators lag 1-3 years)
- Pagination: page/per_page params; metadata in response[0]

Indicators ingested
-------------------
The DAG fetches country-level annual time-series for indicators that capture
the health-system context in which disease transmission occurs:

  SH.XPD.CHEX.GD.ZS  - Current health expenditure (% of GDP)
  SH.XPD.CHEX.PC.CD   - Current health expenditure per capita (current US$)
  SH.MED.BEDS.ZS      - Hospital beds (per 1,000 people)
  SH.MED.PHYS.ZS      - Physicians (per 1,000 people)
  SH.MED.NUMW.P3      - Nurses and midwives (per 1,000 people)
  SP.DYN.LE00.IN       - Life expectancy at birth, total (years)
  SP.DYN.CDRT.IN       - Crude death rate (per 1,000 people)
  SP.DYN.CBRT.IN       - Crude birth rate (per 1,000 people)
  SP.POP.TOTL          - Total population
  SP.URB.TOTL.IN.ZS    - Urban population (% of total)
  SH.IMM.MEAS          - Immunization, measles (% of children ages 12-23 months)
  SH.TBS.INCD          - Incidence of tuberculosis (per 100,000 people)
  SH.HIV.INCD.TL.P3    - Incidence of HIV (per 1,000 uninfected, ages 15-49)
  SH.DYN.AIDS.ZS       - Prevalence of HIV, total (% of population ages 15-49)
  SH.STA.WASH.P5       - People using at least basic sanitation services (%)
  SH.H2O.BASW.ZS       - People using at least basic drinking water services (%)

Why it matters
--------------
- Health expenditure and workforce density determine intervention capacity
  (treatment rates, contact tracing) in the SEIRS model's "I -> R" transition.
- Hospital beds constrain the maximum treatable infected population (ICU
  overflow modelling).
- Crude birth/death rates feed directly into the SEIRS model's demographic
  open-population dynamics (N growth, S replenishment).
- Urbanisation rate affects contact matrices and transmission (beta) scaling.
- WASH indicators modulate environmental transmission pathways.
- HIV/TB prevalence cross-references WHO GHO data for validation.

Table: worldbank_health_indicators
-----------------------------------
  iso3            TEXT        (ISO 3166-1 alpha-3)
  indicator_code  TEXT        (World Bank indicator ID)
  indicator_name  TEXT        (human-readable name)
  year            INTEGER
  value           DOUBLE PRECISION
  source          TEXT        (always 'WORLDBANK')
  created_at      TIMESTAMPTZ
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

WB_BASE = os.environ.get(
    "WORLDBANK_API_BASE", "https://api.worldbank.org/v2"
).rstrip("/")

REQUEST_DELAY = float(os.environ.get("WORLDBANK_REQUEST_DELAY", "0.5"))
REQUEST_TIMEOUT = float(os.environ.get("WORLDBANK_REQUEST_TIMEOUT", "60"))
PER_PAGE = int(os.environ.get("WORLDBANK_PER_PAGE", "500"))

# Country selection
_ENV_COUNTRIES = os.environ.get("WORLDBANK_COUNTRIES", "").strip()

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

YEAR_START = int(os.environ.get("WORLDBANK_YEAR_START", "2000"))
YEAR_END = int(os.environ.get("WORLDBANK_YEAR_END", "2025"))

# (indicator_code, human-readable name)
INDICATORS: List[Tuple[str, str]] = [
    ("SH.XPD.CHEX.GD.ZS",  "Health expenditure (% GDP)"),
    ("SH.XPD.CHEX.PC.CD",   "Health expenditure per capita (US$)"),
    ("SH.MED.BEDS.ZS",      "Hospital beds per 1000"),
    ("SH.MED.PHYS.ZS",      "Physicians per 1000"),
    ("SH.MED.NUMW.P3",      "Nurses & midwives per 1000"),
    ("SP.DYN.LE00.IN",      "Life expectancy at birth"),
    ("SP.DYN.CDRT.IN",      "Crude death rate per 1000"),
    ("SP.DYN.CBRT.IN",      "Crude birth rate per 1000"),
    ("SP.POP.TOTL",         "Total population"),
    ("SP.URB.TOTL.IN.ZS",   "Urban population (%)"),
    ("SH.IMM.MEAS",         "Measles immunization coverage (%)"),
    ("SH.TBS.INCD",         "TB incidence per 100k"),
    ("SH.HIV.INCD.TL.P3",   "HIV incidence per 1000 uninfected (15-49)"),
    ("SH.DYN.AIDS.ZS",      "HIV prevalence (% pop 15-49)"),
    ("SH.STA.WASH.P5",      "Basic sanitation services (%)"),
    ("SH.H2O.BASW.ZS",      "Basic drinking water services (%)"),
]


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------
def _ensure_table():
    hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS worldbank_health_indicators (
                iso3            TEXT NOT NULL,
                indicator_code  TEXT NOT NULL,
                indicator_name  TEXT NOT NULL,
                year            INTEGER NOT NULL,
                value           DOUBLE PRECISION,
                source          TEXT NOT NULL DEFAULT 'WORLDBANK',
                created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (iso3, indicator_code, year)
            );
            """
        )
        conn.commit()


def _safe_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        return float(v)
    except (ValueError, TypeError):
        return None


# ---------------------------------------------------------------------------
# World Bank API helpers
# ---------------------------------------------------------------------------
def _wb_fetch_indicator(iso3: str, indicator_code: str) -> List[Dict]:
    """Fetch all pages of a single indicator for a single country."""
    all_records: List[Dict] = []
    page = 1

    while True:
        url = (
            f"{WB_BASE}/country/{iso3}/indicator/{indicator_code}"
            f"?format=json&date={YEAR_START}:{YEAR_END}"
            f"&per_page={PER_PAGE}&page={page}"
        )
        resp = requests.get(url, timeout=REQUEST_TIMEOUT)

        if resp.status_code == 404:
            print(f"WB 404: indicator={indicator_code} iso3={iso3}")
            return []
        resp.raise_for_status()

        payload = resp.json()

        # World Bank returns [metadata_dict, records_list]
        # If the indicator/country combo has no data, payload may be
        # [{"message": [{"id":"120","key":"...","value":"..."}]}]
        if not isinstance(payload, list) or len(payload) < 2:
            # Could be an error message or empty result
            if isinstance(payload, list) and len(payload) == 1:
                msg = payload[0]
                if isinstance(msg, dict) and "message" in msg:
                    print(
                        f"WB API message for {indicator_code} iso3={iso3}: "
                        f"{msg['message']}"
                    )
            return all_records

        metadata = payload[0]
        records = payload[1]

        if records is None:
            return all_records

        all_records.extend(records)

        # Check pagination
        total_pages = int(metadata.get("pages", 1))
        if page >= total_pages:
            break
        page += 1
        time.sleep(REQUEST_DELAY)

    return all_records


# ---------------------------------------------------------------------------
# Main ingest callable
# ---------------------------------------------------------------------------
def ingest_worldbank_country(iso3: str):
    """Ingest all configured World Bank health indicators for one country."""
    iso3 = iso3.upper().strip()
    _ensure_table()

    hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    total_rows = 0

    with hook.get_conn() as conn, conn.cursor() as cur:
        for indicator_code, indicator_name in INDICATORS:
            records = _wb_fetch_indicator(iso3, indicator_code)
            time.sleep(REQUEST_DELAY)

            if not records:
                print(
                    f"WorldBank: No data for {indicator_code} ({indicator_name}) "
                    f"iso3={iso3}"
                )
                continue

            inserted = 0
            for r in records:
                if not isinstance(r, dict):
                    continue

                year_raw = r.get("date")
                val_raw = r.get("value")

                if year_raw is None or val_raw is None:
                    continue

                try:
                    year = int(year_raw)
                except (ValueError, TypeError):
                    continue

                value = _safe_float(val_raw)
                if value is None:
                    continue

                cur.execute(
                    """
                    INSERT INTO worldbank_health_indicators
                        (iso3, indicator_code, indicator_name, year, value, source)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (iso3, indicator_code, year)
                    DO UPDATE SET
                        value          = EXCLUDED.value,
                        indicator_name = EXCLUDED.indicator_name
                    """,
                    (iso3, indicator_code, indicator_name, year, value, "WORLDBANK"),
                )
                inserted += 1

            conn.commit()
            total_rows += inserted
            print(
                f"WorldBank: Stored {inserted} rows for {indicator_code} "
                f"({indicator_name}) iso3={iso3}"
            )

    print(f"WorldBank: Completed iso3={iso3}, total rows upserted={total_rows}")


# ---------------------------------------------------------------------------
# DAG factory: one DAG per country
# ---------------------------------------------------------------------------
for _CC3 in ISO3_CODES:
    _cc3u = _CC3.upper()
    _cc3l = _CC3.lower()

    with DAG(
        dag_id=f"worldbank_health_{_cc3l}",
        description=(
            f"Ingest World Bank health/socioeconomic indicators for {_cc3u} "
            f"({YEAR_START}-{YEAR_END}) into Postgres"
        ),
        start_date=datetime(2025, 1, 1),
        schedule=None,
        catchup=False,
        max_active_runs=1,
        default_args={"owner": "airflow", "retries": 2},
        tags=["worldbank", "health", "infrastructure", "epidemiology", _cc3u],
    ) as dag:
        PythonOperator(
            task_id=f"ingest_wb_health_{_cc3l}",
            python_callable=ingest_worldbank_country,
            op_kwargs={"iso3": _cc3u},
        )
