#!/usr/bin/env bash
# Mirror the ingested tables from the local dev database into production.
#
# Production runs no Airflow. The DAG services sit behind the compose `dag` profile and
# the prod deploy only ever brings up `--profile prod`, so nothing on that box ever
# writes these tables: dev ingests, prod serves, and this script is the bridge between
# them. That also makes the direction safe to assume — dev is the authority, and every
# table listed below is dropped and rewritten from dev inside one transaction.
#
# Not copied, deliberately:
#   flight_states   six hours of live sky, 556 MB, rewritten continuously by the Kafka
#                   consumer. Prod does not run that consumer and would only hold a
#                   frozen snapshot of where planes were on the day of the last sync.
#   seirs_runs, seirs_series_points
#                   simulation output. Prod produces its own; overwriting it would
#                   delete runs users can still hold a link to.
#   raster_objects  the WorldPop COGs are large objects, not rows. See VOLUME-MIGRATION.md.
#
# SSH: the production deploy key is pinned server-side to a forced command and can only
# run ~/deploy.sh, so it cannot be reused here. Use your own access to the box, or add a
# second key for this.
#
# Usage:
#   scripts/sync-data-to-prod.sh                 # report both sides, change nothing
#   scripts/sync-data-to-prod.sh --yes           # do it
#
# Env: PROD_SSH, PROD_DIR, COMPOSE_DIR, PG_USER, PG_DATABASE
set -euo pipefail

PROD_SSH=${PROD_SSH:-twigg@157.90.172.175}
PROD_DIR=${PROD_DIR:-viss-docker-compose}
# The compose file lives two levels up when this repo is checked out inside
# viss-docker-compose, which is how the stack actually runs.
COMPOSE_DIR=${COMPOSE_DIR:-$(cd "$(dirname "$0")/../.." && pwd)}
PG_USER=${PG_USER:-${POSTGRES_USER:-airflow}}
PG_DATABASE=${PG_DATABASE:-viss}
# Overridable so CI can point ssh at a deploy key without writing to the runner's ~/.ssh.
# Deliberately unquoted where it is used: it carries options, not just a command name.
SSH=${SSH:-ssh}

COMPOSE_FILE="$COMPOSE_DIR/docker-compose.yml"
if [ ! -f "$COMPOSE_FILE" ]; then
  echo "No compose file at $COMPOSE_FILE. Set COMPOSE_DIR to the viss-docker-compose checkout." >&2
  exit 1
fi

TABLES=(
  airports
  aircraft_seat_capacity
  aircraft_registry
  flight_passenger_volumes
  flight_arrivals_by_airport
  flight_routes
  age_pyramid_5yr
  asfr_5yr
  who_gho_indicators
  worldbank_health_indicators
)

APPLY=no
[ "${1:-}" = "--yes" ] && APPLY=yes

# Both read their SQL on stdin. Passing it as an argument would mean quoting it through
# ssh, docker compose and psql in turn, and the first table name with a quote in it wins.
local_psql() { docker compose -f "$COMPOSE_FILE" exec -T postgres psql -qtAX -U "$PG_USER" -d "$PG_DATABASE"; }
prod_psql()  { $SSH "$PROD_SSH" "cd $PROD_DIR && docker compose exec -T postgres psql -qtAX -U $PG_USER -d $PG_DATABASE"; }

# A count per table on both sides, so the operator sees what is about to be replaced
# with what. A table that does not exist on the far side reports "absent" rather than
# failing the query: a first sync into a database that has never run the DAGs is a
# normal case. The count goes through query_to_xml because CASE guards it -- naming a
# missing table directly fails at parse time, before any WHERE can protect it.
counts_sql() {
  local list
  list=$(printf "'%s'," "${TABLES[@]}")
  cat <<SQL
SELECT t.name,
       CASE WHEN to_regclass(t.name) IS NULL THEN 'absent'
            ELSE (xpath('/row/n/text()',
                        query_to_xml(format('SELECT COUNT(*) AS n FROM %I', t.name),
                                     false, true, '')))[1]::text
       END AS rows
FROM unnest(ARRAY[${list%,}]) AS t(name)
ORDER BY 1;
SQL
}

echo "== local (dev) =="
counts_sql | local_psql | sed 's/|/\t/'
echo
echo "== production =="
counts_sql | prod_psql | sed 's/|/\t/'
echo

if [ "$APPLY" != yes ]; then
  echo "Nothing written. Re-run with --yes to replace the production copies."
  exit 0
fi

TABLE_ARGS=()
for t in "${TABLES[@]}"; do TABLE_ARGS+=(-t "$t"); done

echo "== copying =="
# --clean --if-exists so a table missing on prod is created rather than fatal, and one
# whose shape has since changed is replaced rather than half-loaded. DDL is transactional
# in Postgres, so the BEGIN/COMMIT around it means prod is never briefly empty: readers
# either see the old copy or the new one.
{
  echo "BEGIN;"
  docker compose -f "$COMPOSE_FILE" exec -T postgres \
    pg_dump -U "$PG_USER" -d "$PG_DATABASE" \
      --clean --if-exists --no-owner --no-privileges "${TABLE_ARGS[@]}"
  echo "COMMIT;"
} | gzip -c | $SSH "$PROD_SSH" "cd $PROD_DIR && gunzip -c | docker compose exec -T postgres psql -v ON_ERROR_STOP=1 -q -U $PG_USER -d $PG_DATABASE"

echo
echo "== production after =="
counts_sql | prod_psql | sed 's/|/\t/'
