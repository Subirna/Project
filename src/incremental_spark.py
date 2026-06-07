#!/usr/bin/env python3
"""
=============================================================
 FILE: incremental_spark.py
 PROJECT: TFL Data Pipeline - Subirna
=============================================================

PURPOSE:
  This script handles the SPARK PROCESSING for incremental load.
  It runs AFTER incremental_sqoop.py has imported new data.

  It reads BOTH:
    1. The original FULL LOAD data  (from sqoop_import.sh)
    2. The new INCREMENTAL data     (from incremental_sqoop.py)

  Then COMBINES them and recalculates all 7 gold tables.

DIFFERENCE FROM FULL LOAD (tfl_spark_analysis.py):
  Full load Spark    → reads only the HDFS_FULL data
  Incremental Spark  → reads HDFS_FULL + HDFS_INC, unions them together

WHY UNION?
  The full load contains historical rows (e.g. entry_exit_id 1 to 5000).
  The incremental contains new rows (e.g. entry_exit_id 5001 to 5050).
  Union combines both: 1 to 5050 = complete and up-to-date dataset.

HDFS STRUCTURE:
  /tmp/subirna/TFL_project/                    ← FULL LOAD (sqoop_import.sh output)
      dim_date/
      dim_lines/
      dim_networks/
      dim_stations/
      fact_passenger_entry_exit/
      fact_station_lines/

  /tmp/subirna/TFL_project/incremental/        ← INCREMENTAL (incremental_sqoop.py output)
      dim_date/          ← fresh full re-import of dims (latest version)
      dim_lines/
      dim_networks/
      dim_stations/
      fact_passenger_entry_exit/   ← ONLY new rows (not all rows)
      fact_station_lines/          ← ONLY new rows (not all rows)

  /tmp/subirna/TFL_project/gold/               ← OUTPUT (OVERWRITTEN with updated data)
      gold_busiest_stations/
      gold_passengers_by_year/
      gold_passengers_by_line/
      gold_passengers_by_network/
      gold_interchange_stations/
      gold_quarterly_trend/
      gold_night_tube_analysis/

USAGE:
  spark-submit incremental_spark.py
  OR: python3 incremental_spark.py

RUN ORDER:
  Step 1: sqoop_import.sh          (full load — run ONCE at the beginning)
  Step 2: tfl_spark_analysis.py    (full load Spark — run ONCE)
  Step 3: incremental_sqoop.py     (run each time new data arrives)
  Step 4: incremental_spark.py     (this file — run after Step 3)
=============================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, count, avg, desc
from pyspark.sql.types import IntegerType

# =============================================================
#  SPARK SESSION
# =============================================================

spark = SparkSession.builder \
    .appName("TFL_Incremental_Spark_Subirna") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")


# =============================================================
#  HDFS PATH CONFIGURATION
# =============================================================

# Original full load data — created by sqoop_import.sh
# We READ from here but NEVER write or delete it
HDFS_FULL = "/tmp/subirna/TFL_project"

# Incremental data — created by incremental_sqoop.py
# Dimension tables here are fresh full re-imports
# Fact tables here contain ONLY the new rows since last run
HDFS_INC  = "/tmp/subirna/TFL_project/incremental"

# Gold tables — our final analysis output
# These get OVERWRITTEN each run with updated, complete results
GOLD_BASE = "/tmp/subirna/TFL_project/gold"

HIVE_DB = "subirna_tfl"

spark.sql(f"USE {HIVE_DB}")

print("=" * 60)
print("TFL INCREMENTAL SPARK PIPELINE - Subirna")
print("=" * 60)
print(f"Full load data   : {HDFS_FULL}")
print(f"Incremental data : {HDFS_INC}")
print(f"Gold output      : {GOLD_BASE}")


# =============================================================
#  HELPER: Read CSV from HDFS
# =============================================================

def read_csv(path, columns):
    """
    Read a headerless CSV file from HDFS and assign column names.

    Parameters:
      path    - HDFS directory path (sqoop creates one CSV per mapper)
      columns - list of column names in the correct order

    inferSchema=True means Spark automatically detects INT, STRING etc.
    """
    return (
        spark.read
        .option("header", "false")     # sqoop CSV has no header row
        .option("inferSchema", "true") # auto-detect column types
        .csv(path)
        .toDF(*columns)                # assign column names
    )


# =============================================================
#  STEP 1: LOAD DIMENSION TABLES
#
#  We use the INCREMENTAL versions of dimension tables.
#  Why? Because incremental_sqoop.py re-imported them fully,
#  so the incremental/ folder has the LATEST version of each dim.
#
#  (The full load dims in HDFS_FULL could be older)
# =============================================================

print("\n[STEP 1] Loading dimension tables from incremental/...")

dim_date = read_csv(
    path    = f"{HDFS_INC}/dim_date",
    columns = ["date_id","year","quarter","month","is_annual",
               "period_label","period_start","period_end","created_at"]
)

dim_lines = read_csv(
    path    = f"{HDFS_INC}/dim_lines",
    columns = ["line_id","line_name","line_color","is_night_service",
               "created_at","updated_at"]
)

dim_networks = read_csv(
    path    = f"{HDFS_INC}/dim_networks",
    columns = ["network_id","network_name","network_type",
               "created_at","updated_at"]
)

dim_stations = read_csv(
    path    = f"{HDFS_INC}/dim_stations",
    columns = ["station_id","nlc_code","station_name","network_id",
               "has_london_underground","has_elizabeth_line","has_overground",
               "has_dlr","has_night_tube","is_active","created_at","updated_at"]
)

print("Dimension tables loaded (latest version from incremental/).")


# =============================================================
#  STEP 2: LOAD FACT TABLES (FULL LOAD + INCREMENTAL COMBINED)
#
#  This is the KEY DIFFERENCE from the full load script.
#
#  We read the fact data from TWO locations:
#    HDFS_FULL → all historical rows (from sqoop_import.sh)
#    HDFS_INC  → only the new rows (from incremental_sqoop.py)
#
#  Then we UNION them together to get the complete dataset.
#
#  UNION means: stack the two DataFrames on top of each other.
#  The combined DataFrame has ALL rows from both sources.
# =============================================================

print("\n[STEP 2] Loading fact tables (combining full load + incremental)...")

PAX_COLS = [
    "entry_exit_id", "station_id", "date_id",
    "total_entry_exit", "estimated_entries", "estimated_exits",
    "record_type", "data_source", "created_at"
]

# ── fact_passenger_entry_exit ──────────────────────────────────────────────────
# Read historical rows (full load)
fact_pax_full = read_csv(f"{HDFS_FULL}/fact_passenger_entry_exit", PAX_COLS)

# Read new rows (incremental — only rows added since last sqoop run)
fact_pax_inc  = read_csv(f"{HDFS_INC}/fact_passenger_entry_exit", PAX_COLS)

# UNION: combine both into one complete DataFrame
# fact_pax now contains ALL rows: historical + new
fact_pax = fact_pax_full.union(fact_pax_inc)

print(f"  fact_passenger_entry_exit:")
print(f"    Full load rows      = {fact_pax_full.count()}")
print(f"    Incremental rows    = {fact_pax_inc.count()}   ← only new rows")
print(f"    Combined total rows = {fact_pax.count()}")

# ── fact_station_lines ─────────────────────────────────────────────────────────
SL_COLS = [
    "station_line_id", "station_id", "line_id", "is_interchange",
    "effective_from", "effective_to", "created_at"
]

fact_lines_full = read_csv(f"{HDFS_FULL}/fact_station_lines", SL_COLS)
fact_lines_inc  = read_csv(f"{HDFS_INC}/fact_station_lines", SL_COLS)
fact_lines      = fact_lines_full.union(fact_lines_inc)

print(f"\n  fact_station_lines:")
print(f"    Full load rows      = {fact_lines_full.count()}")
print(f"    Incremental rows    = {fact_lines_inc.count()}")
print(f"    Combined total rows = {fact_lines.count()}")


# =============================================================
#  HELPER: Save Gold Table
#  mode("overwrite") replaces old gold table with updated data
# =============================================================

def save_gold_table(df, table_name):
    """
    Write a DataFrame to HDFS as Parquet format.
    mode("overwrite") means the old gold table is replaced with
    the newly calculated result (which includes incremental data).
    """
    path = f"{GOLD_BASE}/{table_name}"
    df.write.mode("overwrite").parquet(path)
    print(f"  Updated → {path}")


# =============================================================
#  ANALYSES (same 7 as the full load script)
#  But now fact_pax and fact_lines contain the COMBINED data,
#  so all analyses automatically include the new rows.
# =============================================================

print("\n[STEP 3] Running analyses on combined dataset...")

# ── ANALYSIS 1: Top 10 Busiest Stations ───────────────────────────────────────
print("\n--- ANALYSIS 1: Top 10 Busiest Stations ---")
busiest_stations = (
    fact_pax
    .join(dim_stations, "station_id")
    .groupBy("station_name")
    .agg(_sum("total_entry_exit").alias("total_passengers"))
    .orderBy(desc("total_passengers"))
    .limit(10)
)
busiest_stations.show(truncate=False)
save_gold_table(busiest_stations, "gold_busiest_stations")

# ── ANALYSIS 2: Passengers by Year ────────────────────────────────────────────
# NEW YEARS from incremental data will now appear here automatically
print("\n--- ANALYSIS 2: Total Passengers by Year ---")
passengers_by_year = (
    fact_pax
    .join(dim_date, "date_id")
    .groupBy("year")
    .agg(_sum("total_entry_exit").alias("total_passengers"))
    .orderBy("year")
)
passengers_by_year.show(truncate=False)
save_gold_table(passengers_by_year, "gold_passengers_by_year")

# ── ANALYSIS 3: Passengers by Tube Line ───────────────────────────────────────
print("\n--- ANALYSIS 3: Passengers by Tube Line ---")
passengers_by_line = (
    fact_pax
    .join(fact_lines, "station_id")
    .join(dim_lines, "line_id")
    .groupBy("line_name")
    .agg(_sum("total_entry_exit").alias("total_passengers"))
    .orderBy(desc("total_passengers"))
)
passengers_by_line.show(truncate=False)
save_gold_table(passengers_by_line, "gold_passengers_by_line")

# ── ANALYSIS 4: Passengers by Network ─────────────────────────────────────────
print("\n--- ANALYSIS 4: Passengers by Network Type ---")
passengers_by_network = (
    fact_pax
    .join(dim_stations, "station_id")
    .join(dim_networks, "network_id")
    .groupBy("network_name", "network_type")
    .agg(_sum("total_entry_exit").alias("total_passengers"))
    .orderBy(desc("total_passengers"))
)
passengers_by_network.show(truncate=False)
save_gold_table(passengers_by_network, "gold_passengers_by_network")

# ── ANALYSIS 5: Interchange Stations ──────────────────────────────────────────
print("\n--- ANALYSIS 5: Top Interchange Stations ---")
interchange_stations = (
    fact_lines
    .join(dim_stations, "station_id")
    .groupBy("station_name")
    .agg(count("line_id").alias("num_lines"))
    .orderBy(desc("num_lines"))
    .limit(15)
)
interchange_stations.show(truncate=False)
save_gold_table(interchange_stations, "gold_interchange_stations")

# ── ANALYSIS 6: Quarterly Trend ───────────────────────────────────────────────
print("\n--- ANALYSIS 6: Passengers by Year and Quarter ---")
quarterly_trend = (
    fact_pax
    .join(dim_date, "date_id")
    .groupBy(
        col("year").cast(IntegerType()),
        col("quarter").cast(IntegerType())
    )
    .agg(_sum("total_entry_exit").alias("total_passengers"))
    .orderBy("year", "quarter")
)
quarterly_trend.show(truncate=False)
save_gold_table(quarterly_trend, "gold_quarterly_trend")

# ── ANALYSIS 7: Night Tube Analysis ───────────────────────────────────────────
print("\n--- ANALYSIS 7: Night Tube vs Regular Stations ---")
night_tube_analysis = (
    fact_pax
    .join(dim_stations, "station_id")
    .groupBy("has_night_tube")
    .agg(
        count("station_id").alias("num_records"),
        _sum("total_entry_exit").alias("total_passengers"),
        avg("total_entry_exit").alias("avg_passengers_per_record")
    )
)
night_tube_analysis.show(truncate=False)
save_gold_table(night_tube_analysis, "gold_night_tube_analysis")


# =============================================================
#  SUMMARY
# =============================================================

print("\n" + "=" * 60)
print("INCREMENTAL SPARK PIPELINE COMPLETE")
print("=" * 60)
print("Data sources used:")
print(f"  Full load data   : {HDFS_FULL}")
print(f"  Incremental data : {HDFS_INC}")
print("\nGold tables updated at:")
for t in ["gold_busiest_stations", "gold_passengers_by_year",
          "gold_passengers_by_line", "gold_passengers_by_network",
          "gold_interchange_stations", "gold_quarterly_trend",
          "gold_night_tube_analysis"]:
    print(f"  {GOLD_BASE}/{t}")
print("=" * 60)

spark.stop()
