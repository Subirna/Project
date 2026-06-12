#!/usr/bin/env python3
"""
=============================================================
 FILE: incremental_sqoop.py
 PROJECT: TFL Data Pipeline - Subirna
 LOAD TYPE: INCREMENTAL LOAD (years 2020, 2021)
=============================================================

PURPOSE:
  Import NEW TFL data (2020-2021) from PostgreSQL to HDFS.
  Runs AFTER sqoop_import.sh (full load for 2017-2019) has run.

DATA SPLIT LOGIC (matches sqoop_import.sh):
  Full load  (sqoop_import.sh) → 2017, 2018, 2019
  Incremental (this script)    → 2020, 2021

TWO DIFFERENT STRATEGIES (based on table type):

  STRATEGY 1 — Year-based WHERE clause (for tables with year data):
    fact_passenger_entry_exit → WHERE year IN (2020, 2021)
    dim_date                  → WHERE year IN (2020, 2021)

    Why: These tables have data split by year.
    Full load got 2017-2019. We now get 2020-2021.
    No watermark needed — year is the split boundary.

  STRATEGY 2 — Watermark from Hive via get_watermark.sh (for static tables):
    dim_stations      → WHERE created_at > MAX(created_at) in Hive
    dim_lines         → WHERE created_at > MAX(created_at) in Hive
    dim_networks      → WHERE created_at > MAX(created_at) in Hive
    fact_station_lines → WHERE created_at > MAX(created_at) in Hive

    Why: These tables have no year concept.
         Full load already got ALL rows.
         Watermark catches any NEW rows added after full load.
         (e.g. a new station opens, a new line is added)

RUN ORDER:
  1. bash sqoop_import.sh               (full load 2017-2019 — ONCE)
  2. spark-submit tfl_spark_analysis.py (full load Spark — ONCE)
  --- 2020-2021 data exists in PostgreSQL ---
  3. python3 incremental_sqoop.py       (this file)
  4. spark-submit incremental_spark.py

USAGE:
  python3 incremental_sqoop.py
=============================================================
"""

import subprocess
from datetime import datetime

# =============================================================
#  CONFIGURATION
# =============================================================

JDBC_URL = "jdbc:postgresql://13.42.152.118:5432/testdb"
DB_USER  = "admin"
DB_PASS  = "admin123"

# Full load data — written by sqoop_import.sh (2017-2019)
HDFS_FULL = "/tmp/subirna/TFL_project"

# Incremental data lands here (2020-2021)
HDFS_INC  = "/tmp/subirna/TFL_project/incremental"

# Script that queries Hive for watermark (for static tables only)
WATERMARK_SCRIPT = "./get_watermark.sh"

# Incremental years — what this script imports
INCREMENTAL_YEARS = "2020, 2021"


# =============================================================
#  SQOOP BASE ARGUMENTS
# =============================================================

SQOOP_BASE = [
    "sqoop", "import",
    "-D", "mapreduce.framework.name=local",
    "-D", "mapreduce.jobtracker.staging.root.dir=/tmp/consultant/staging",
    "--connect",              JDBC_URL,
    "--username",             DB_USER,
    "--password",             DB_PASS,
    "--num-mappers",          "1",
    "--fields-terminated-by", ","
]


# =============================================================
#  SQOOP RUNNER
# =============================================================

def run_sqoop(extra_args, table_name):
    """Build and run sqoop import. Returns True on success."""
    cmd = SQOOP_BASE + extra_args

    print(f"\n{'─'*60}")
    print(f"  Table  : {table_name}")
    print(f"  Command: {' '.join(cmd)}")
    print(f"{'─'*60}")

    # Stream sqoop output directly to the console (no PIPE capture) so Jenkins
    # shows live progress and the job does not appear to hang.
    result = subprocess.run(cmd)

    if result.returncode == 0:
        target_path = f"{HDFS_INC}/{table_name}"
        # Sqoop exits 0 even when 0 rows match (e.g. --delete-target-dir removes the
        # existing dir but no new dir is created for an empty result set).  Create an
        # empty marker directory so incremental_spark.py can tell the difference between
        # "sqoop never ran" and "sqoop ran but found no new rows".
        hdfs_check = subprocess.run(
            ["hdfs", "dfs", "-test", "-e", target_path],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        if hdfs_check.returncode != 0:
            print(f"  NOTE: {table_name} — sqoop succeeded but 0 rows matched (no new data)")
            subprocess.run(
                ["hdfs", "dfs", "-mkdir", "-p", target_path],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE
            )
        print(f"  SUCCESS: {table_name} → {target_path}")
        return True
    else:
        print(f"  FAILED : {table_name} (see sqoop output above for details)")
        return False


# =============================================================
#  STRATEGY 1: YEAR-BASED IMPORT
#  For tables that have year data: fact_passenger_entry_exit, dim_date
#
#  Uses --where to filter only 2020-2021 rows.
#  Simple and clear — year is the split boundary.
#  No watermark needed.
# =============================================================

def import_by_year(table_name, where_clause):
    """
    Import rows for 2020-2021 using a WHERE clause.

    For fact_passenger_entry_exit:
      WHERE date_id IN (SELECT date_id FROM dim_date WHERE year IN (2020,2021))

    For dim_date:
      WHERE year IN (2020, 2021)

    These rows do not exist in the full load (which has 2017-2019 only),
    so there is no overlap — clean separation by year.
    """
    print(f"\n  Year-based import: {table_name}")
    print(f"  Filter: {where_clause}")

    return run_sqoop([
        "--table",          table_name,
        "--where",          where_clause,    # year filter
        "--target-dir",     f"{HDFS_INC}/{table_name}",
        "--delete-target-dir"               # clean previous incremental run
    ], table_name)


# =============================================================
#  STRATEGY 2: WATERMARK-BASED IMPORT
#  For static tables: dim_stations, dim_lines, dim_networks, fact_station_lines
#
#  These tables were fully imported in the full load.
#  Now we only import rows added AFTER the full load ran.
#  get_watermark.sh asks Hive: "what is MAX(created_at) for this table?"
#  Sqoop then imports only rows newer than that timestamp.
# =============================================================

def get_watermark(table_name, check_column):
    """
    Call get_watermark.sh to find the latest created_at already in Hive.
    Returns timestamp string like '2024-01-15 10:30:00'.
    Falls back to '1970-01-01 00:00:00' if Hive is unreachable.
    """
    result = subprocess.run(
        ["bash", WATERMARK_SCRIPT, table_name, check_column],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )

    watermark = "1970-01-01 00:00:00"
    for line in result.stdout.decode('utf-8', errors='replace').splitlines():
        if line.startswith("last_value="):
            watermark = line.split("=", 1)[1].strip()
            break

    return watermark


def import_by_watermark(table_name, check_column):
    """
    Import only NEW rows added after the full load using Hive watermark.

    Used for static reference tables (dim_stations, dim_lines, etc.)
    that have no year column.

    --incremental append : only fetch rows where check_column > watermark
    --check-column       : created_at (timestamp of when row was inserted)
    --last-value         : MAX(created_at) from Hive = our watermark
    """
    print(f"\n  Watermark-based import: {table_name}")
    watermark = get_watermark(table_name, check_column)
    print(f"  Only importing rows where {check_column} > '{watermark}'")

    return run_sqoop([
        "--table",          table_name,
        "--target-dir",     f"{HDFS_INC}/{table_name}",
        "--incremental",    "append",
        "--check-column",   check_column,
        "--last-value",     watermark
    ], table_name)


# =============================================================
#  MAIN PIPELINE
# =============================================================

def main():
    print("=" * 60)
    print("TFL INCREMENTAL SQOOP IMPORT (years 2020-2021)")
    print(f"Run started: {datetime.now()}")
    print("=" * 60)
    print(f"Full load data (2017-2019) : {HDFS_FULL}  ← unchanged")
    print(f"Incremental data (2020-2021): {HDFS_INC}  ← written here")

    results = {}

    # ── STRATEGY 1: Year-based (for tables with year data) ─────
    print("\n" + "=" * 60)
    print("STRATEGY 1 — Year-based import (2020, 2021)")
    print("Tables: dim_date, fact_passenger_entry_exit")
    print("=" * 60)

    # dim_date: import 2020 and 2021 date records
    results["dim_date"] = import_by_year(
        table_name  = "dim_date",
        where_clause = f"year IN ({INCREMENTAL_YEARS})"
    )

    # fact_passenger_entry_exit: import 2020-2021 passenger records
    # Use subquery because the year column is in dim_date, not in this table
    results["fact_passenger_entry_exit"] = import_by_year(
        table_name   = "fact_passenger_entry_exit",
        where_clause = f"date_id IN (SELECT date_id FROM dim_date WHERE year IN ({INCREMENTAL_YEARS}))"
    )

    # ── STRATEGY 2: Watermark-based (for static tables) ────────
    print("\n" + "=" * 60)
    print("STRATEGY 2 — Watermark-based import (new rows since full load)")
    print("Tables: dim_lines, dim_networks, dim_stations, fact_station_lines")
    print("Uses get_watermark.sh to query MAX(created_at) from Hive")
    print("=" * 60)

    # These tables were fully imported in sqoop_import.sh.
    # The watermark catches any new rows added AFTER that full load.
    # (e.g. a new Elizabeth line station opened, a new line was added)
    for table_name in ["dim_lines", "dim_networks", "dim_stations", "fact_station_lines"]:
        results[table_name] = import_by_watermark(
            table_name    = table_name,
            check_column  = "created_at"
        )

    # ── Verify HDFS ─────────────────────────────────────────────
    print("\n[VERIFY] HDFS incremental directory:")
    subprocess.run(["hdfs", "dfs", "-ls", HDFS_INC])

    # ── Summary ─────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("INCREMENTAL SQOOP COMPLETE")
    print("=" * 60)
    print(f"{'Table':<35} {'Strategy':<22} {'Status'}")
    print("─" * 60)
    strategies = {
        "dim_date":                   "year IN (2020,2021)",
        "fact_passenger_entry_exit":  "year IN (2020,2021)",
        "dim_lines":                  "watermark (created_at)",
        "dim_networks":               "watermark (created_at)",
        "dim_stations":               "watermark (created_at)",
        "fact_station_lines":         "watermark (created_at)",
    }
    for table, status in results.items():
        flag = "OK" if status else "FAILED"
        print(f"  {table:<33} {strategies[table]:<22} {flag}")

    print(f"\nNext step: spark-submit incremental_spark.py")
    print("=" * 60)


if __name__ == "__main__":
    main()
