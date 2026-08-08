#!/bin/bash
# Registers all gold Delta tables as Superset datasets in one go.
# Usage: bash scripts/superset_add_datasets.sh

set -e

SUPERSET_URL="http://localhost:8088"
USER="admin"
PASS="admin"

echo "Logging in..."
TOKEN=$(curl -s -X POST "${SUPERSET_URL}/api/v1/security/login" \
  -H "Content-Type: application/json" \
  -d "{\"username\":\"${USER}\",\"password\":\"${PASS}\",\"provider\":\"db\"}" \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['access_token'])")

echo "Finding Trino database ID..."
DB_ID=$(curl -s "${SUPERSET_URL}/api/v1/database/" \
  -H "Authorization: Bearer ${TOKEN}" \
  | python3 -c "
import sys, json
dbs = json.load(sys.stdin)['result']
match = next((d for d in dbs if 'trino' in d['backend'].lower() or 'trino' in d['sqlalchemy_uri'].lower()), None)
print(match['id'] if match else '')
")

if [ -z "$DB_ID" ]; then
  echo "ERROR: Trino database not found in Superset. Add it first via the UI (Settings → Database Connections)."
  exit 1
fi
echo "Trino DB id = ${DB_ID}"

add_dataset() {
  local TABLE=$1
  echo "Adding dataset: gold.${TABLE}..."
  RESULT=$(curl -s -o /dev/null -w "%{http_code}" -X POST "${SUPERSET_URL}/api/v1/dataset/" \
    -H "Authorization: Bearer ${TOKEN}" \
    -H "Content-Type: application/json" \
    -d "{\"database\":${DB_ID},\"schema\":\"gold\",\"table_name\":\"${TABLE}\"}")

  if [ "$RESULT" == "201" ]; then
    echo "  ✓ ${TABLE} added"
  elif [ "$RESULT" == "422" ]; then
    echo "  ~ ${TABLE} already exists, skipping"
  else
    echo "  ✗ ${TABLE} failed (HTTP ${RESULT})"
  fi
}

add_dataset "dim_asset"
add_dataset "dim_date"
add_dataset "dim_time"
add_dataset "fact_daily_ohlc"

echo "Done."
