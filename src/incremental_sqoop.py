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

TWO STRATEGIES:

  STRATEGY 1 — Year-based (fact_passenger_entry_exit ONLY):
    fact_passenger_entry_exit has no year column directly.
    It has date_id which links to dim_date which has year.
    So we use a subquery:
      WHERE date_id IN (SELECT date_id FROM dim_date WHERE year IN (2020, 2021))
    This gets only the new passenger records for 2020-2021.

  STRATEGY 2 — Full re-import (all other 5 tables):
    dim_date, dim_lines, dim_networks, dim_stations, fact_station_lines
    These are re-imported fully every incremental run.
    Why: if a new station/line/network is added, a full re-import
    guarantees it is captured. Tables are small so cost is low.

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
#  Only for: fact_passenger_entry_exit
#
#  This table has no year column. It has date_id which links to
#  dim_date. We filter using a subquery:
#    WHERE date_id IN (SELECT date_id FROM dim_date WHERE year IN (2020,2021))
# =============================================================

def import_by_year(table_name, where_clause):
    print(f"\n  Year-based import: {table_name}")
    print(f"  Filter: {where_clause}")

    return run_sqoop([
        "--table",          table_name,
        "--where",          where_clause,    # year filter
        "--target-dir",     f"{HDFS_INC}/{table_name}",
        "--delete-target-dir"               # clean previous incremental run
    ], table_name)


# =============================================================
#  STRATEGY 2: FULL RE-IMPORT
#  For static tables: dim_stations, dim_lines, dim_networks, fact_station_lines
#
#  These reference tables are small and can change at any time
#  (new station opens, new line added, new network created).
#  A full re-import is cheap and guarantees we always have the
#  latest data — no risk of missing newly added rows.
# =============================================================

def import_full(table_name):
    """
    Re-import all rows for a static reference table.
    Deletes the previous incremental copy and writes a fresh one.
    """
    print(f"\n  Full re-import: {table_name}")

    return run_sqoop([
        "--table",          table_name,
        "--target-dir",     f"{HDFS_INC}/{table_name}",
        "--delete-target-dir"
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

    # ── STRATEGY 1: Year-based (fact_passenger_entry_exit only) ───
    print("\n" + "=" * 60)
    print("STRATEGY 1 — Year-based import (fact_passenger_entry_exit only)")
    print("Filter: date_id IN (SELECT date_id FROM dim_date WHERE year IN (2020,2021))")
    print("=" * 60)

    results["fact_passenger_entry_exit"] = import_by_year(
        table_name   = "fact_passenger_entry_exit",
        where_clause = f"date_id IN (SELECT date_id FROM dim_date WHERE year IN ({INCREMENTAL_YEARS}))"
    )

    # ── STRATEGY 2: Full re-import (all other tables) ──────────
    print("\n" + "=" * 60)
    print("STRATEGY 2 — Full re-import (all rows)")
    print("Tables: dim_date, dim_lines, dim_networks, dim_stations, fact_station_lines")
    print("Reason: small tables, any new entries will always be captured")
    print("=" * 60)

    for table_name in ["dim_date", "dim_lines", "dim_networks", "dim_stations", "fact_station_lines"]:
        results[table_name] = import_full(table_name)

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
        "fact_passenger_entry_exit":  "year-based via dim_date",
        "dim_date":                   "full re-import",
        "dim_lines":                  "full re-import",
        "dim_networks":               "full re-import",
        "dim_stations":               "full re-import",
        "fact_station_lines":         "full re-import",
    }
    for table, status in results.items():
        flag = "OK" if status else "FAILED"
        print(f"  {table:<33} {strategies[table]:<22} {flag}")

    print(f"\nNext step: spark-submit incremental_spark.py")
    print("=" * 60)


if __name__ == "__main__":
    main()
