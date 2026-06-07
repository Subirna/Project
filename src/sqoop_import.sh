#!/bin/bash
# =============================================================
#  FILE: sqoop_import.sh
#  PROJECT: TFL Data Pipeline - Subirna
#  LOAD TYPE: FULL LOAD (years 2017, 2018, 2019 only)
# =============================================================
#
#  PURPOSE:
#    Import historical TFL data (2017-2019) from PostgreSQL to HDFS.
#    This is the FULL LOAD — run ONCE at the beginning.
#
#  DATA SPLIT STRATEGY:
#    Full Load  (this script) → years 2017, 2018, 2019
#    Incremental (next script) → years 2020, 2021  (new data)
#
#  WHICH TABLES GET A WHERE CLAUSE:
#    fact_passenger_entry_exit → WHERE year IN (2017,2018,2019)
#    dim_date                  → WHERE year IN (2017,2018,2019)
#
#  WHICH TABLES IMPORT ALL ROWS (no year split needed):
#    dim_stations    → static reference data, no year concept
#    dim_lines       → static reference data, no year concept
#    dim_networks    → static reference data, no year concept
#    fact_station_lines → station-line mappings, no year concept
#
#  USAGE:
#    bash sqoop_import.sh
#
#  NEXT STEP:
#    spark-submit tfl_spark_analysis.py
# =============================================================

# ── dim_date: import only 2017-2019 date records ──────────────
# These are the date dimension rows for the historical period.
# 2020-2021 date rows will be imported by incremental_sqoop.py.
sqoop import \
  -D mapreduce.framework.name=local \
  -D mapreduce.jobtracker.staging.root.dir=/tmp/consultant/staging \
  --connect 'jdbc:postgresql://13.42.152.118:5432/testdb' \
  --username admin --password admin123 \
  --table dim_date \
  --where "year IN (2017, 2018, 2019)" \
  --target-dir /tmp/subirna/TFL_project/dim_date \
  --num-mappers 1 \
  --fields-terminated-by ','

# ── dim_lines: import ALL rows (no year concept) ───────────────
# Tube line names are static — Central, Jubilee, Bakerloo etc.
# All lines existed in 2017-2019, so import everything.
sqoop import \
  -D mapreduce.framework.name=local \
  -D mapreduce.jobtracker.staging.root.dir=/tmp/consultant/staging \
  --connect 'jdbc:postgresql://13.42.152.118:5432/testdb' \
  --username admin --password admin123 \
  --table dim_lines \
  --target-dir /tmp/subirna/TFL_project/dim_lines \
  --num-mappers 1 \
  --fields-terminated-by ','

# ── dim_networks: import ALL rows (no year concept) ────────────
# Network types (Underground, Overground, DLR etc.) are static.
sqoop import \
  -D mapreduce.framework.name=local \
  -D mapreduce.jobtracker.staging.root.dir=/tmp/consultant/staging \
  --connect 'jdbc:postgresql://13.42.152.118:5432/testdb' \
  --username admin --password admin123 \
  --table dim_networks \
  --target-dir /tmp/subirna/TFL_project/dim_networks \
  --num-mappers 1 \
  --fields-terminated-by ','

# ── dim_stations: import ALL rows (no year concept) ────────────
# Station names and details are static reference data.
sqoop import \
  -D mapreduce.framework.name=local \
  -D mapreduce.jobtracker.staging.root.dir=/tmp/consultant/staging \
  --connect 'jdbc:postgresql://13.42.152.118:5432/testdb' \
  --username admin --password admin123 \
  --table dim_stations \
  --target-dir /tmp/subirna/TFL_project/dim_stations \
  --num-mappers 1 \
  --fields-terminated-by ','

# ── fact_passenger_entry_exit: import only 2017-2019 ──────────
# This is the main fact table with passenger counts.
# We use a subquery to get date_ids that belong to 2017-2019:
#   date_id IN (SELECT date_id FROM dim_date WHERE year IN (2017,2018,2019))
# This way we only import historical passenger records.
# 2020-2021 records will be imported by incremental_sqoop.py.
sqoop import \
  -D mapreduce.framework.name=local \
  -D mapreduce.jobtracker.staging.root.dir=/tmp/consultant/staging \
  --connect 'jdbc:postgresql://13.42.152.118:5432/testdb' \
  --username admin --password admin123 \
  --table fact_passenger_entry_exit \
  --where "date_id IN (SELECT date_id FROM dim_date WHERE year IN (2017, 2018, 2019))" \
  --target-dir /tmp/subirna/TFL_project/fact_passenger_entry_exit \
  --num-mappers 1 \
  --fields-terminated-by ','

# ── fact_station_lines: import ALL rows (no year concept) ──────
# Station-to-line mappings are static — they don't change by year.
sqoop import \
  -D mapreduce.framework.name=local \
  -D mapreduce.jobtracker.staging.root.dir=/tmp/consultant/staging \
  --connect 'jdbc:postgresql://13.42.152.118:5432/testdb' \
  --username admin --password admin123 \
  --table fact_station_lines \
  --target-dir /tmp/subirna/TFL_project/fact_station_lines \
  --num-mappers 1 \
  --fields-terminated-by ','

echo "============================================"
echo "FULL LOAD COMPLETE (years 2017-2018-2019)"
echo "============================================"
echo "Tables with year filter  : dim_date, fact_passenger_entry_exit"
echo "Tables imported fully    : dim_lines, dim_networks, dim_stations, fact_station_lines"
echo "Verifying HDFS output..."
hdfs dfs -ls /tmp/subirna/TFL_project/
echo "Next step: spark-submit tfl_spark_analysis.py"
