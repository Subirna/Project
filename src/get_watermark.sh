#!/bin/bash
# =============================================================
#  FILE: get_watermark.sh
#  PROJECT: TFL Data Pipeline - Subirna
# =============================================================
#
#  PURPOSE:
#    Query Hive to find the latest created_at timestamp already
#    loaded for a SPECIFIC TABLE.
#
#    Called once per table so every table gets its own watermark.
#    Only records newer than the watermark are imported next time.
#
#  USAGE:
#    bash get_watermark.sh <table_name> <check_column>
#
#  CALLED FOR ALL 6 TABLES:
#    bash get_watermark.sh dim_date                  created_at
#    bash get_watermark.sh dim_lines                 created_at
#    bash get_watermark.sh dim_networks              created_at
#    bash get_watermark.sh dim_stations              created_at
#    bash get_watermark.sh fact_passenger_entry_exit created_at
#    bash get_watermark.sh fact_station_lines        created_at
#
#  OUTPUT FORMAT (same as Uttam's get_watermark.sh):
#    last_value=2021-03-01 10:00:00
#
#  FALLBACK:
#    If a table is empty (very first run), returns 1970-01-01
#    so that Sqoop imports ALL rows from PostgreSQL.
# =============================================================

# Read table name and column from script arguments
TABLE_NAME="$1"
CHECK_COLUMN="$2"

# Validate — both arguments must be provided
if [ -z "$TABLE_NAME" ] || [ -z "$CHECK_COLUMN" ]; then
    echo "ERROR: Usage: bash get_watermark.sh <table_name> <check_column>"
    echo "last_value=1970-01-01 00:00:00"
    exit 1
fi

# Cluster configuration
HIVESERVER2_HOST="${HIVESERVER2_HOST:-localhost}"
HIVE_PORT="10000"
HIVE_DB="subirna_tfl"

# Fallback: used when the Hive table is empty (first incremental run)
# 1970-01-01 tells Sqoop to import everything from PostgreSQL
FALLBACK_TIMESTAMP="1970-01-01 00:00:00"

echo "Getting watermark for: ${HIVE_DB}.${TABLE_NAME} (column: ${CHECK_COLUMN})"

# Query Hive for the MAX timestamp already loaded for this table.
# COALESCE returns FALLBACK if MAX is NULL (empty table).
# --silent=true   : hide beeline connection headers
# --outputformat=csv2 : plain output without borders
# 2>/dev/null     : hide INFO/WARN logs from beeline
WATERMARK=$(beeline \
    -u "jdbc:hive2://${HIVESERVER2_HOST}:${HIVE_PORT}/${HIVE_DB}" \
    -n hive \
    --silent=true \
    --outputformat=csv2 \
    -e "SELECT COALESCE(MAX(\`${CHECK_COLUMN}\`), '${FALLBACK_TIMESTAMP}') FROM ${HIVE_DB}.${TABLE_NAME};" \
    2>/dev/null | tail -1 | tr -d '"')

# If beeline returned nothing at all, use the fallback
if [ -z "$WATERMARK" ]; then
    echo "Warning: Hive returned no result. Using fallback timestamp."
    WATERMARK="$FALLBACK_TIMESTAMP"
fi

echo "Watermark for ${TABLE_NAME}: ${WATERMARK}"

# Output in key=value format — parsed by incremental_sqoop.py
# Same format as Uttam's: last_value=2023-08-07 17:40:00.0
echo "last_value=${WATERMARK}"
