"""
OpenSky Network Flight Volumes
===============================

Ingests daily flight arrival/departure data from the OpenSky Network REST API
and estimates passenger volumes per country pair for disease-spread modelling.

Data sources
------------
- OpenSky Network REST API: https://opensky-network.org/apidoc/rest.html
  License: Open (CC-BY-SA 4.0 for derived data)
  Auth: OAuth2 client credentials (basic auth retired 2026-03-18)
  Endpoints used:
    /flights/arrival?airport={icao}&begin={ts}&end={ts}
  Arrivals alone are enough: each record carries estDepartureAirport, so one
  call per destination gives both ends of the country pair. Calling
  /flights/departure as well would double the credit spend for nothing.

- OpenSky aircraft database (static CSV, no credits):
  https://opensky-network.org/datasets/metadata/aircraftDatabase.csv
  Maps icao24 transponder address → ICAO type designator.

- OurAirports (airport metadata): https://ourairports.com/data/
  License: Public domain
  Files: airports.csv

Credit budget
-------------
The registered tier grants 4000 credits/day and every /flights call costs 30 —
independently of how wide a time window it asks for, and including calls that
come back 404. That is ~133 airport-days per day, against 4874 known airports.
The budget, not the API, is what decides how much of the world gets sampled.

Passenger estimation
--------------------
OpenSky reports icao24 and callsign per flight, never the aircraft type. Seats
are resolved icao24 → typecode (aircraft_registry) → seats
(aircraft_seat_capacity), then:
  estimated_pax = typical_seats × global_avg_load_factor (0.82 per IATA)
Airframes the registry does not know fall back to DEFAULT_SEATS, and each run
prints what share of its arrivals that was.

Tables
------
  airports                  — Airport metadata (from OurAirports)
  aircraft_registry         — icao24 transponder address → ICAO type code
  aircraft_seat_capacity    — ICAO type → typical seat count
  flight_passenger_volumes  — Daily country-pair passenger estimates

Why it matters
--------------
Air travel is the primary vector for international epidemic propagation. The
country-pair passenger volume matrix M[i][j] feeds directly into the SEIRS
model's importation term:
  β_import(i) = Σ_j  M[j][i] × prevalence(j) / population(j)
"""
from __future__ import annotations

import csv
import io
import os
import tempfile
import time
from datetime import datetime, timedelta, timezone

# Per-task ceiling. Without one, a wedged task holds an executor slot
# forever -- and at core.parallelism=2 that is half the install.
# many airports, with rate-limit backoff.
TASK_TIMEOUT = timedelta(minutes=int(os.environ.get("OPENSKY_TASK_TIMEOUT_MIN", "90")))
from typing import Any, Dict, List, Optional

import requests

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
PG_CONN_ID = os.environ.get("PG_CONN_ID", "viss_data_db")

OPENSKY_BASE = os.environ.get(
    "OPENSKY_API_BASE", "https://opensky-network.org/api"
).rstrip("/")

# OAuth2 client credentials. Basic auth was retired 2026-03-18 -- without these
# every /flights call is anonymous and answers 403.
OPENSKY_CLIENT_ID = os.environ.get("OPENSKY_CLIENT_ID", "")
OPENSKY_CLIENT_SECRET = os.environ.get("OPENSKY_CLIENT_SECRET", "")
OPENSKY_TOKEN_URL = os.environ.get(
    "OPENSKY_TOKEN_URL",
    "https://auth.opensky-network.org/auth/realms/opensky-network/"
    "protocol/openid-connect/token",
)

# Every /flights request costs 30 credits no matter how wide the window, and the
# registered tier grants 4000/day -- so a run can touch ~133 airports, full stop.
# Shorter windows do not buy more airports; they cost the same 30 each.
CREDITS_PER_FLIGHTS_CALL = 30
DAILY_AIRPORT_BUDGET = int(os.environ.get("OPENSKY_DAILY_AIRPORT_BUDGET", "120"))

# OpenSky publishes historical /flights roughly two days behind. Asking for
# yesterday answers 404 for every airport on earth, and 404 is how the API says
# "no flights" -- so a lookback of 1 does not fail, it quietly writes a world
# with no air travel in it.
LOOKBACK_DAYS = max(2, int(os.environ.get("OPENSKY_LOOKBACK_DAYS", "2")))

REQUEST_DELAY = float(os.environ.get("OPENSKY_REQUEST_DELAY", "1.0"))
REQUEST_TIMEOUT = float(os.environ.get("OPENSKY_REQUEST_TIMEOUT", "60"))
# Rate-limit handling. OpenSky's free tier 429s readily at 243-country scale.
RATE_LIMIT_RETRIES = int(os.environ.get("OPENSKY_RATE_LIMIT_RETRIES", "4"))
RATE_LIMIT_BACKOFF = float(os.environ.get("OPENSKY_RATE_LIMIT_BACKOFF", "30"))
RATE_LIMIT_MAX_SLEEP = float(os.environ.get("OPENSKY_RATE_LIMIT_MAX_SLEEP", "300"))
# An ingest that could not reach this share of a country's airports is not a
# result -- it is a gap, and it fails rather than writing a partial day.
MAX_UNREACHABLE_FRACTION = float(os.environ.get("OPENSKY_MAX_UNREACHABLE_FRACTION", "0.2"))

# IATA 2023 global average load factor
LOAD_FACTOR = float(os.environ.get("OPENSKY_LOAD_FACTOR", "0.82"))

OURAIRPORTS_URL = os.environ.get(
    "OURAIRPORTS_CSV_URL",
    "https://davidmegginson.github.io/ourairports-data/airports.csv",
)

# OpenSky's aircraft database: icao24 -> typecode. Static file, no API credits.
AIRCRAFT_DB_URL = os.environ.get(
    "OPENSKY_AIRCRAFT_DB_URL",
    "https://opensky-network.org/datasets/metadata/aircraftDatabase.csv",
)


# ---------------------------------------------------------------------------
# Common aircraft seat capacities (ICAO type designator → typical seats)
# Covers ~90% of global commercial traffic
# ---------------------------------------------------------------------------
AIRCRAFT_SEATS: Dict[str, int] = {
    # Narrow-body
    "A319": 140, "A320": 180, "A321": 220, "A20N": 180, "A21N": 220,
    "B737": 150, "B738": 189, "B739": 220, "B38M": 178, "B39M": 200,
    "B752": 200, "B753": 243, "E170": 76, "E190": 100, "E195": 120,
    "E75L": 76, "E75S": 76, "E290": 120, "CRJ2": 50, "CRJ7": 70,
    "CRJ9": 90, "CRJX": 100, "AT72": 70, "AT76": 70, "DH8D": 78,
    "C56X": 9, "C68A": 12, "C700": 12, "C750": 16,
    # Wide-body
    "A332": 277, "A333": 300, "A338": 280, "A339": 300,
    "A342": 260, "A343": 295, "A345": 313, "A346": 380,
    "A359": 325, "A35K": 366,
    "A388": 555,
    "B763": 260, "B764": 304,
    "B772": 314, "B77L": 370, "B77W": 396,
    "B788": 248, "B789": 296, "B78X": 318,
    "B744": 416, "B748": 467,
    # Regional / turboprop
    "DH8A": 37, "DH8B": 37, "DH8C": 50, "SF34": 34,
    "JS32": 19, "JS41": 29, "BE20": 8, "B350": 11,
}

# Default for unknown aircraft types (regional jet average)
DEFAULT_SEATS = int(os.environ.get("OPENSKY_DEFAULT_SEATS", "120"))


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------
def _ensure_tables():
    """Create all required tables if they don't exist."""
    hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS airports (
                icao_code   TEXT PRIMARY KEY,
                iata_code   TEXT,
                name        TEXT,
                iso3        TEXT NOT NULL,
                latitude    DOUBLE PRECISION,
                longitude   DOUBLE PRECISION,
                type        TEXT
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS aircraft_seat_capacity (
                icao_type_code TEXT PRIMARY KEY,
                name           TEXT,
                typical_seats  INTEGER NOT NULL,
                source         TEXT NOT NULL DEFAULT 'MANUAL'
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS aircraft_registry (
                icao24   TEXT PRIMARY KEY,
                typecode TEXT
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS flight_passenger_volumes (
                origin_iso3         TEXT NOT NULL,
                destination_iso3    TEXT NOT NULL,
                date                DATE NOT NULL,
                flight_count        INTEGER,
                estimated_passengers INTEGER,
                source              TEXT NOT NULL DEFAULT 'OPENSKY',
                created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (origin_iso3, destination_iso3, date)
            );
        """)
        conn.commit()


def _safe_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        return float(v)
    except (ValueError, TypeError):
        return None


# ---------------------------------------------------------------------------
# OurAirports import
# ---------------------------------------------------------------------------
# ISO-2 to ISO-3 mapping for the countries we care about
# OurAirports uses ISO-2 country codes; we need ISO-3 for VISS consistency
_ISO2_TO_ISO3: Dict[str, str] = {
    "AF": "AFG", "AL": "ALB", "DZ": "DZA", "AO": "AGO", "AR": "ARG",
    "AM": "ARM", "AU": "AUS", "AT": "AUT", "AZ": "AZE", "BS": "BHS",
    "BH": "BHR", "BD": "BGD", "BB": "BRB", "BY": "BLR", "BE": "BEL",
    "BZ": "BLZ", "BJ": "BEN", "BT": "BTN", "BO": "BOL", "BA": "BIH",
    "BW": "BWA", "BR": "BRA", "BN": "BRN", "BG": "BGR", "BF": "BFA",
    "BI": "BDI", "KH": "KHM", "CM": "CMR", "CA": "CAN", "CV": "CPV",
    "CF": "CAF", "TD": "TCD", "CL": "CHL", "CN": "CHN", "CO": "COL",
    "KM": "COM", "CG": "COG", "CD": "COD", "CR": "CRI", "CI": "CIV",
    "HR": "HRV", "CU": "CUB", "CY": "CYP", "CZ": "CZE", "DK": "DNK",
    "DJ": "DJI", "DM": "DMA", "DO": "DOM", "EC": "ECU", "EG": "EGY",
    "SV": "SLV", "GQ": "GNQ", "ER": "ERI", "EE": "EST", "SZ": "SWZ",
    "ET": "ETH", "FJ": "FJI", "FI": "FIN", "FR": "FRA", "GA": "GAB",
    "GM": "GMB", "GE": "GEO", "DE": "DEU", "GH": "GHA", "GR": "GRC",
    "GD": "GRD", "GT": "GTM", "GN": "GIN", "GW": "GNB", "GY": "GUY",
    "HT": "HTI", "HN": "HND", "HU": "HUN", "IS": "ISL", "IN": "IND",
    "ID": "IDN", "IR": "IRN", "IQ": "IRQ", "IE": "IRL", "IL": "ISR",
    "IT": "ITA", "JM": "JAM", "JP": "JPN", "JO": "JOR", "KZ": "KAZ",
    "KE": "KEN", "KI": "KIR", "KR": "KOR", "KW": "KWT", "KG": "KGZ",
    "LA": "LAO", "LV": "LVA", "LB": "LBN", "LS": "LSO", "LR": "LBR",
    "LY": "LBY", "LT": "LTU", "LU": "LUX", "MG": "MDG", "MW": "MWI",
    "MY": "MYS", "MV": "MDV", "ML": "MLI", "MT": "MLT", "MR": "MRT",
    "MU": "MUS", "MX": "MEX", "MD": "MDA", "MN": "MNG", "ME": "MNE",
    "MA": "MAR", "MZ": "MOZ", "MM": "MMR", "NA": "NAM", "NP": "NPL",
    "NL": "NLD", "NZ": "NZL", "NI": "NIC", "NE": "NER", "NG": "NGA",
    "MK": "MKD", "NO": "NOR", "OM": "OMN", "PK": "PAK", "PA": "PAN",
    "PG": "PNG", "PY": "PRY", "PE": "PER", "PH": "PHL", "PL": "POL",
    "PT": "PRT", "QA": "QAT", "RO": "ROU", "RU": "RUS", "RW": "RWA",
    "SA": "SAU", "SN": "SEN", "RS": "SRB", "SL": "SLE", "SG": "SGP",
    "SK": "SVK", "SI": "SVN", "SB": "SLB", "SO": "SOM", "ZA": "ZAF",
    "SS": "SSD", "ES": "ESP", "LK": "LKA", "SD": "SDN", "SR": "SUR",
    "SE": "SWE", "CH": "CHE", "SY": "SYR", "TW": "TWN", "TJ": "TJK",
    "TZ": "TZA", "TH": "THA", "TL": "TLS", "TG": "TGO", "TO": "TON",
    "TT": "TTO", "TN": "TUN", "TR": "TUR", "TM": "TKM", "UG": "UGA",
    "UA": "UKR", "AE": "ARE", "GB": "GBR", "US": "USA", "UY": "URY",
    "UZ": "UZB", "VU": "VUT", "VE": "VEN", "VN": "VNM", "YE": "YEM",
    "ZM": "ZMB", "ZW": "ZWE",
    # Territories
    "AW": "ABW", "HK": "HKG", "MO": "MAC", "PR": "PRI", "PS": "PSE",
    "RE": "REU", "GP": "GLP", "MQ": "MTQ", "GF": "GUF", "NC": "NCL",
    "PF": "PYF",
}


def refresh_airports():
    """Download OurAirports CSV and upsert into airports table."""
    _ensure_tables()

    print(f"Downloading airports from {OURAIRPORTS_URL}")
    resp = requests.get(OURAIRPORTS_URL, timeout=300)
    resp.raise_for_status()

    reader = csv.DictReader(io.StringIO(resp.text))

    hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    inserted = 0

    with hook.get_conn() as conn, conn.cursor() as cur:
        for row in reader:
            # Only large & medium airports (skip heliports, seaplane bases, small strips)
            apt_type = (row.get("type") or "").strip()
            if apt_type not in ("large_airport", "medium_airport"):
                continue

            icao = (row.get("ident") or "").strip().upper()
            if not icao or len(icao) != 4:
                continue

            iso2 = (row.get("iso_country") or "").strip().upper()
            iso3 = _ISO2_TO_ISO3.get(iso2)
            if not iso3:
                continue

            iata = (row.get("iata_code") or "").strip().upper() or None
            name = (row.get("name") or "").strip()
            lat = _safe_float(row.get("latitude_deg"))
            lon = _safe_float(row.get("longitude_deg"))

            cur.execute("""
                INSERT INTO airports (icao_code, iata_code, name, iso3, latitude, longitude, type)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (icao_code)
                DO UPDATE SET
                    iata_code = EXCLUDED.iata_code,
                    name      = EXCLUDED.name,
                    iso3      = EXCLUDED.iso3,
                    latitude  = EXCLUDED.latitude,
                    longitude = EXCLUDED.longitude,
                    type      = EXCLUDED.type
            """, (icao, iata, name, iso3, lat, lon, apt_type))
            inserted += 1

        conn.commit()

    print(f"Airports: upserted {inserted} large/medium airports")


def seed_aircraft_capacity():
    """Seed the aircraft_seat_capacity table from the built-in dictionary."""
    _ensure_tables()

    hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    with hook.get_conn() as conn, conn.cursor() as cur:
        for icao_type, seats in AIRCRAFT_SEATS.items():
            cur.execute("""
                INSERT INTO aircraft_seat_capacity (icao_type_code, name, typical_seats, source)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (icao_type_code)
                DO UPDATE SET typical_seats = EXCLUDED.typical_seats
            """, (icao_type, icao_type, seats, "MANUAL"))
        conn.commit()

    print(f"Aircraft capacity: seeded {len(AIRCRAFT_SEATS)} type entries")


def load_aircraft_registry():
    """Load OpenSky's aircraft database: transponder address -> ICAO type code.

    /flights records carry icao24 (the transponder hex address) and callsign,
    never the aircraft type. Without this table there is nothing to look a seat
    count up by, and every flight silently collapses to DEFAULT_SEATS -- which
    makes estimated_passengers a constant multiple of flight_count wearing a
    different name. The registry is a static CSV, so resolving type costs no
    API credits.
    """
    _ensure_tables()

    print(f"Aircraft registry: downloading {AIRCRAFT_DB_URL}")
    with tempfile.TemporaryDirectory() as workdir:
        # Straight to disk before parsing. The file is ~90 MB and some model
        # fields carry embedded newlines, so it has to be read by a real CSV
        # parser over a seekable file, not line-wise off the socket.
        raw_path = os.path.join(workdir, "aircraftDatabase.csv")
        with requests.get(AIRCRAFT_DB_URL, timeout=REQUEST_TIMEOUT, stream=True) as resp:
            resp.raise_for_status()
            with open(raw_path, "wb") as fh:
                for chunk in resp.iter_content(chunk_size=1 << 20):
                    fh.write(chunk)

        tsv_path = os.path.join(workdir, "registry.tsv")
        rows = 0
        with open(raw_path, encoding="utf-8", errors="replace", newline="") as src, \
                open(tsv_path, "w", encoding="utf-8", newline="") as tmp:
            for rec in csv.DictReader(src):
                icao24 = (rec.get("icao24") or "").strip().lower()
                typecode = (rec.get("typecode") or "").strip().upper()
                if not icao24 or not typecode:
                    continue
                tmp.write(f"{icao24}\t{typecode}\n")
                rows += 1

        hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
        with hook.get_conn() as conn, conn.cursor() as cur, \
                open(tsv_path, encoding="utf-8") as tsv:
            # Rebuild in one transaction: a half-loaded registry would silently
            # downgrade the aircraft it lost to DEFAULT_SEATS.
            cur.execute("CREATE TEMP TABLE _ar_stage (icao24 TEXT, typecode TEXT) ON COMMIT DROP")
            cur.copy_expert("COPY _ar_stage (icao24, typecode) FROM STDIN", tsv)
            cur.execute("""
                INSERT INTO aircraft_registry (icao24, typecode)
                SELECT DISTINCT ON (icao24) icao24, typecode FROM _ar_stage
                ON CONFLICT (icao24) DO UPDATE SET typecode = EXCLUDED.typecode
            """)
            conn.commit()

    print(f"Aircraft registry: loaded {rows} icao24 -> typecode mappings")


# ---------------------------------------------------------------------------
# OpenSky API helpers
# ---------------------------------------------------------------------------
class OpenSkyUnavailable(RuntimeError):
    """OpenSky could not answer. Distinct from OpenSky answering 'no flights'."""


# Access tokens live 30 minutes. Cache one per worker process and refresh a
# minute early rather than re-authenticating on every airport.
_TOKEN_CACHE: Dict[str, Any] = {"token": None, "expires_at": 0.0}


def _opensky_token() -> str:
    """Return a valid OAuth2 access token, fetching or refreshing as needed."""
    if not (OPENSKY_CLIENT_ID and OPENSKY_CLIENT_SECRET):
        raise OpenSkyUnavailable(
            "OPENSKY_CLIENT_ID / OPENSKY_CLIENT_SECRET are unset. OpenSky retired "
            "basic auth on 2026-03-18; anonymous access cannot read /flights."
        )

    if _TOKEN_CACHE["token"] and time.time() < _TOKEN_CACHE["expires_at"]:
        return _TOKEN_CACHE["token"]

    resp = requests.post(
        OPENSKY_TOKEN_URL,
        data={
            "grant_type": "client_credentials",
            "client_id": OPENSKY_CLIENT_ID,
            "client_secret": OPENSKY_CLIENT_SECRET,
        },
        timeout=REQUEST_TIMEOUT,
    )
    if resp.status_code != 200:
        raise OpenSkyUnavailable(
            f"OpenSky token request failed: HTTP {resp.status_code}"
        )

    payload = resp.json()
    token = payload.get("access_token")
    if not token:
        raise OpenSkyUnavailable("OpenSky token response carried no access_token")

    _TOKEN_CACHE["token"] = token
    _TOKEN_CACHE["expires_at"] = time.time() + float(payload.get("expires_in", 1800)) - 60
    return token


def _fetch_flights(endpoint: str, icao_airport: str, begin: int, end: int) -> List[Dict]:
    """Fetch arrivals or departures for one airport in a time window.

    Returns [] ONLY when OpenSky positively reports no flights (HTTP 404 is how
    it says that). Every other failure raises, because a rate-limited fetch that
    returns [] is indistinguishable from a quiet airport -- and that zero flows
    straight into the importation-risk model as if it were an observation.
    """
    url = f"{OPENSKY_BASE}/flights/{endpoint}"
    params = {"airport": icao_airport, "begin": begin, "end": end}

    last_error = "unknown"
    for attempt in range(1, RATE_LIMIT_RETRIES + 1):
        # Fetched per attempt, not once: a task may run for 90 minutes and the
        # token dies after 30, so a retry must be able to carry a fresh one.
        headers = {"Authorization": f"Bearer {_opensky_token()}"}
        try:
            resp = requests.get(url, params=params, headers=headers, timeout=REQUEST_TIMEOUT)
        except requests.exceptions.RequestException as e:
            last_error = f"request error: {e}"
        else:
            if resp.status_code == 404:
                # OpenSky's way of saying "nothing in this window".
                return []
            if resp.status_code == 200:
                return resp.json() or []
            if resp.status_code == 401:
                # Token rejected -- drop it so the next attempt re-authenticates.
                _TOKEN_CACHE["token"] = None
                _TOKEN_CACHE["expires_at"] = 0.0
                last_error = "HTTP 401 (token rejected), re-authenticating"
                if attempt < RATE_LIMIT_RETRIES:
                    continue
            if resp.status_code == 429:
                # Honour Retry-After when the server sends it; otherwise back off.
                retry_after = resp.headers.get("Retry-After")
                try:
                    delay = float(retry_after) if retry_after else RATE_LIMIT_BACKOFF * attempt
                except ValueError:
                    delay = RATE_LIMIT_BACKOFF * attempt
                delay = min(delay, RATE_LIMIT_MAX_SLEEP)
                last_error = f"HTTP 429 (rate limited), waited {delay:.0f}s"
                if attempt < RATE_LIMIT_RETRIES:
                    print(
                        f"OpenSky rate limited for {icao_airport} "
                        f"(attempt {attempt}/{RATE_LIMIT_RETRIES}); sleeping {delay:.0f}s"
                    )
                    time.sleep(delay)
                    continue
            else:
                last_error = f"HTTP {resp.status_code}"

        if attempt < RATE_LIMIT_RETRIES:
            time.sleep(RATE_LIMIT_BACKOFF * attempt)

    raise OpenSkyUnavailable(
        f"OpenSky {endpoint} failed for {icao_airport} after "
        f"{RATE_LIMIT_RETRIES} attempts: {last_error}"
    )


# ---------------------------------------------------------------------------
# Main ingest callable
# ---------------------------------------------------------------------------
def ingest_flight_volumes_for_country(iso3: str):
    """
    Fetch yesterday's arrivals at all airports in a country, estimate
    passenger volumes, and aggregate to country-pair level.
    """
    iso3 = iso3.upper().strip()
    _ensure_tables()

    hook = PostgresHook(postgres_conn_id=PG_CONN_ID)

    # Get airports for this country
    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT icao_code, type FROM airports WHERE iso3 = %s", (iso3,))
        rows = cur.fetchall()
    airports = [r[0] for r in rows]
    has_major_airport = any(r[1] == "large_airport" for r in rows)

    if not airports:
        print(f"OpenSky: No airports found for {iso3}, skipping")
        return

    # Time window: one full UTC day, LOOKBACK_DAYS back (see LOOKBACK_DAYS).
    midnight = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    target = midnight - timedelta(days=LOOKBACK_DAYS)
    begin_ts = int(target.timestamp())
    end_ts = int((target + timedelta(days=1)).timestamp())
    date_str = target.strftime("%Y-%m-%d")

    print(f"OpenSky: Fetching arrivals for {iso3} ({len(airports)} airports) date={date_str}")

    # Load aircraft capacity lookup
    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT icao_type_code, typical_seats FROM aircraft_seat_capacity")
        capacity_map = {row[0]: row[1] for row in cur.fetchall()}

    # Load airport → country mapping
    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT icao_code, iso3 FROM airports")
        airport_country = {row[0]: row[1] for row in cur.fetchall()}

    # (origin_iso3, icao24) per international arrival. Aggregation waits until
    # every airport is in, so aircraft types resolve in one query rather than
    # one per flight.
    observed: List[tuple] = []

    unreachable: List[str] = []

    for icao in airports:
        try:
            arrivals = _fetch_flights("arrival", icao, begin_ts, end_ts)
        except OpenSkyUnavailable as e:
            # One unreachable airport is tolerable; a country's worth is not.
            # Tallied here and judged against MAX_UNREACHABLE_FRACTION below.
            print(f"OpenSky: {e}")
            unreachable.append(icao)
            time.sleep(REQUEST_DELAY)
            continue
        time.sleep(REQUEST_DELAY)

        for flight in arrivals:
            dep_airport = (flight.get("estDepartureAirport") or "").strip().upper()
            if not dep_airport:
                continue

            origin_iso3 = airport_country.get(dep_airport)
            if not origin_iso3 or origin_iso3 == iso3:
                # Skip domestic flights and unknown origins
                continue

            observed.append((origin_iso3, (flight.get("icao24") or "").strip().lower()))

    # Resolve aircraft type -> seats for every airframe seen today, in one
    # round trip. Anything the registry does not know falls back to
    # DEFAULT_SEATS; the share that falls back is printed so a run that is
    # mostly guesswork says so out loud.
    seats_by_icao24: Dict[str, int] = {}
    icao24s = sorted({i for _, i in observed if i})
    if icao24s:
        with hook.get_conn() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT icao24, typecode FROM aircraft_registry WHERE icao24 = ANY(%s)",
                (icao24s,),
            )
            for icao24, typecode in cur.fetchall():
                if typecode in capacity_map:
                    seats_by_icao24[icao24] = capacity_map[typecode]

    # Aggregate: origin_country → (flight_count, estimated_passengers)
    pair_data: Dict[str, Dict[str, int]] = {}
    resolved = 0
    for origin_iso3, icao24 in observed:
        seats = seats_by_icao24.get(icao24)
        if seats is None:
            seats = DEFAULT_SEATS
        else:
            resolved += 1
        bucket = pair_data.setdefault(origin_iso3, {"flights": 0, "pax": 0})
        bucket["flights"] += 1
        bucket["pax"] += int(seats * LOAD_FACTOR)

    if observed:
        print(
            f"OpenSky: {iso3} {resolved}/{len(observed)} arrivals "
            f"({resolved / len(observed):.0%}) had a known aircraft type; "
            f"the rest used DEFAULT_SEATS={DEFAULT_SEATS}"
        )

    # A country with a large international airport does not record zero
    # international arrivals in a day. When that happens every airport answered
    # 404, which OpenSky uses both for "quiet" and for "not published yet" --
    # and writing it would put a fabricated zero in front of the model.
    if not observed and has_major_airport and not unreachable:
        raise OpenSkyUnavailable(
            f"{iso3} reported zero international arrivals on {date_str} across "
            f"{len(airports)} airports, including a large_airport. OpenSky most "
            f"likely has not published that day yet; raise OPENSKY_LOOKBACK_DAYS "
            f"(currently {LOOKBACK_DAYS}) rather than recording this as an observation."
        )

    # Coverage gate, before anything is written. A partial day looks exactly
    # like a quiet day once it is in the table, and downstream it becomes an
    # importation-risk number nobody can tell apart from an observation.
    if unreachable:
        share = len(unreachable) / len(airports)
        detail = (
            f"{len(unreachable)}/{len(airports)} airports unreachable for {iso3} "
            f"on {date_str} ({share:.0%}): {', '.join(sorted(unreachable)[:10])}"
            + (" ..." if len(unreachable) > 10 else "")
        )
        if share > MAX_UNREACHABLE_FRACTION:
            raise OpenSkyUnavailable(
                f"Refusing to write a partial day. {detail}. "
                f"Threshold is {MAX_UNREACHABLE_FRACTION:.0%} "
                f"(OPENSKY_MAX_UNREACHABLE_FRACTION)."
            )
        print(f"WARNING: writing with incomplete coverage. {detail}")

    # Upsert country-pair volumes
    with hook.get_conn() as conn, conn.cursor() as cur:
        for origin, data in pair_data.items():
            cur.execute("""
                INSERT INTO flight_passenger_volumes
                    (origin_iso3, destination_iso3, date, flight_count, estimated_passengers, source)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (origin_iso3, destination_iso3, date)
                DO UPDATE SET
                    flight_count         = EXCLUDED.flight_count,
                    estimated_passengers = EXCLUDED.estimated_passengers
            """, (origin, iso3, date_str, data["flights"], data["pax"], "OPENSKY"))
        conn.commit()

    total_flights = sum(d["flights"] for d in pair_data.values())
    total_pax = sum(d["pax"] for d in pair_data.values())
    print(
        f"OpenSky: {iso3} date={date_str}: "
        f"{len(pair_data)} origin countries, "
        f"{total_flights} international flights, "
        f"~{total_pax} estimated passengers"
    )


# ---------------------------------------------------------------------------
# Country list (same as other VISS DAGs)
# ---------------------------------------------------------------------------
_ENV_COUNTRIES = os.environ.get("OPENSKY_COUNTRIES", "").strip()

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


# ---------------------------------------------------------------------------
# DAG 1: Reference data refresh (airports + aircraft capacity)
# Run once, then monthly
# ---------------------------------------------------------------------------
with DAG(
    dag_id="opensky_reference_data",
    description="Refresh airport list (OurAirports) and aircraft seat capacity reference",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "airflow", "retries": 1, "execution_timeout": TASK_TIMEOUT},
    tags=["opensky", "reference", "airports", "aircraft", "epidemiology"],
) as ref_dag:
    t_airports = PythonOperator(
        task_id="refresh_airports",
        python_callable=refresh_airports,
    )
    t_aircraft = PythonOperator(
        task_id="seed_aircraft_capacity",
        python_callable=seed_aircraft_capacity,
    )
    t_registry = PythonOperator(
        task_id="load_aircraft_registry",
        python_callable=load_aircraft_registry,
    )
    t_airports >> t_aircraft >> t_registry


# ---------------------------------------------------------------------------
# DAG factory: one DAG per country for daily flight volumes
# ---------------------------------------------------------------------------
for _CC3 in ISO3_CODES:
    _cc3u = _CC3.upper()
    _cc3l = _CC3.lower()

    with DAG(
        dag_id=f"opensky_flights_{_cc3l}",
        description=(
            f"Ingest daily international flight arrivals for {_cc3u} "
            f"from OpenSky Network and estimate passenger volumes"
        ),
        start_date=datetime(2025, 1, 1),
        schedule=None,
        catchup=False,
        max_active_runs=1,
        default_args={"owner": "airflow", "retries": 2, "execution_timeout": TASK_TIMEOUT},
        tags=["opensky", "flights", "passengers", "mobility", "epidemiology", _cc3u],
    ) as dag:
        PythonOperator(
            task_id=f"ingest_flights_{_cc3l}",
            python_callable=ingest_flight_volumes_for_country,
            op_kwargs={"iso3": _cc3u},
        )
