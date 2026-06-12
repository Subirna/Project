#!/usr/bin/env python3
"""
=============================================================
 FILE: incremental_spark.py
 PROJECT: TFL Data Pipeline - Subirna
 LOAD TYPE: TRULY INCREMENTAL SPARK
=============================================================

PURPOSE:
  Process ONLY the new incremental data (2020-2021).
  Merge intelligently with existing gold tables.
  Does NOT re-read or re-process the full load data (2017-2019).

WHY THIS IS BETTER THAN READING ALL DATA:
  Old approach (wrong):
    Read 2017-2019 raw data  +  Read 2020-2021 raw data
    Union all  →  Re-calculate everything from scratch
    Problem: Gets slower every time. Not truly incremental.

  New approach (correct — this script):
    Read ONLY 2020-2021 raw data  (fast, small)
    Calculate gold results for 2020-2021 only
    Merge with existing gold tables
    Problem solved: always fast, only processes new data.

TWO MERGE STRATEGIES based on gold table type:

  STRATEGY 1 — APPEND (for year-based gold tables):
    gold_passengers_by_year  → 2020,2021 rows added, no overlap with 2017-2019
    gold_quarterly_trend     → 2020,2021 quarters added, no overlap

    Steps: calculate new year results → append to existing gold table
    Why append works: years 2020-2021 don't exist in old gold (which has 2017-2019)
    No duplicates possible.

  STRATEGY 2 — MERGE (for cross-year aggregate gold tables):
    gold_busiest_stations    → totals across ALL years, need to add new to old
    gold_passengers_by_line  → totals across ALL years
    gold_passengers_by_network → totals across ALL years
    gold_interchange_stations → uses station-line mapping, no year dependency
    gold_night_tube_analysis  → totals across ALL years

    Steps: read existing gold + calculate new → union → re-aggregate → overwrite
    Example:
      Old gold_busiest_stations: King's Cross = 500M (2017-2019)
      New result:                King's Cross = 200M (2020-2021)
      Union + re-SUM:            King's Cross = 700M ← correct total

HDFS PATHS:
  Full load raw data  : /tmp/subirna/TFL_project/              (NOT read here)
  Incremental raw data: /tmp/subirna/TFL_project/incremental/  (read here)
  Gold tables         : /tmp/subirna/TFL_project/gold/         (read + updated)
=============================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, count, avg, desc
from pyspark.sql.types import IntegerType

spark = SparkSession.builder \
    .appName("TFL_Incremental_Spark_Subirna") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# =============================================================
#  HDFS PATHS
# =============================================================

HDFS_INC  = "/tmp/subirna/TFL_project/incremental"  # new data only (2020-2021)
HDFS_FULL = "/tmp/subirna/TFL_project"              # full load data (2017-2019)
GOLD_BASE = "/tmp/subirna/TFL_project/gold"          # existing gold tables
HIVE_DB   = "subirna_tfl"

spark.sql(f"USE {HIVE_DB}")

print("=" * 60)
print("TFL TRULY INCREMENTAL SPARK PIPELINE - Subirna")
print("Processes ONLY new data (2020-2021)")
print("Merges with existing gold tables")
print("=" * 60)


# =============================================================
#  HELPERS
# =============================================================

def _get_fs():
    sc = spark.sparkContext
    return sc._jvm.org.apache.hadoop.fs.FileSystem.get(sc._jsc.hadoopConfiguration())

def _get_path(path):
    return spark.sparkContext._jvm.org.apache.hadoop.fs.Path(path)

def hdfs_exists(path):
    """Return True if the HDFS path exists."""
    try:
        return _get_fs().exists(_get_path(path))
    except Exception:
        return False

def hdfs_has_data(path):
    """Return True only if the path exists AND contains actual data files (part-*)."""
    try:
        fs = _get_fs()
        p  = _get_path(path)
        if not fs.exists(p):
            return False
        statuses = fs.listStatus(p)
        return any("part-" in str(s.getPath().getName()) for s in statuses)
    except Exception:
        return False


def read_csv(path, columns):
    return (
        spark.read
        .option("header", "false")
        .option("inferSchema", "true")
        .csv(path)
        .toDF(*columns)
    )


def read_csv_with_fallback(table_name, columns):
    """
    Read a dimension/static table from incremental path if it exists
    (meaning sqoop found genuinely new rows), otherwise fall back to
    the full load path.  These reference tables (dim_lines, dim_stations,
    etc.) need ALL rows for joins — not just new ones.
    """
    inc_path  = f"{HDFS_INC}/{table_name}"
    full_path = f"{HDFS_FULL}/{table_name}"
    if hdfs_exists(inc_path):
        print(f"  {table_name}: reading from incremental path (new rows found)")
        return read_csv(inc_path, columns)
    elif hdfs_exists(full_path):
        print(f"  {table_name}: no new rows — using full load reference data")
        return read_csv(full_path, columns)
    else:
        print(f"  ERROR: {table_name} not found in incremental or full load path")
        spark.stop()
        exit(1)


# =============================================================
#  STEP 1: LOAD ONLY INCREMENTAL RAW DATA (2020-2021)
#
#  We do NOT read the full load data here.
#  The full load (2017-2019) results already exist in gold tables.
#  We only need the new 2020-2021 data.
# =============================================================

print("\n[STEP 1] Loading ONLY incremental raw data (2020-2021)...")
print(f"Reading from: {HDFS_INC}")
print("NOT reading full load data — gold tables already have 2017-2019 results")

# Guard: distinguish "sqoop never ran" (directory absent) from
# "sqoop ran but found 0 new rows" (empty marker directory created by incremental_sqoop.py).
_fact_inc_path = f"{HDFS_INC}/fact_passenger_entry_exit"
if not hdfs_exists(_fact_inc_path):
    print(f"\nERROR: Incremental HDFS data not found at {_fact_inc_path}")
    print("Please run incremental_sqoop.py first, then re-run this script.")
    spark.stop()
    exit(1)

if not hdfs_has_data(_fact_inc_path):
    print(f"\nINFO: No new passenger records found for this incremental run.")
    print("Sqoop ran successfully but 0 rows matched the year filter.")
    print("Gold tables are already up to date — nothing to process.")
    spark.stop()
    exit(0)

# New fact data — ONLY 2020-2021 rows (imported by incremental_sqoop.py)
fact_pax_new = read_csv(f"{HDFS_INC}/fact_passenger_entry_exit", [
    "entry_exit_id","station_id","date_id","total_entry_exit",
    "estimated_entries","estimated_exits","record_type","data_source","created_at"
])

# dim_date incremental path always exists (year-based import in sqoop)
dim_date = read_csv(f"{HDFS_INC}/dim_date", [
    "date_id","year","quarter","month","is_annual",
    "period_label","period_start","period_end","created_at"
])

# Reference/static tables: fall back to full load path if no new rows were imported
# (watermark-based import only creates incremental dir when genuinely new rows exist)
dim_lines = read_csv_with_fallback("dim_lines", [
    "line_id","line_name","line_color","is_night_service","created_at","updated_at"
])

dim_networks = read_csv_with_fallback("dim_networks", [
    "network_id","network_name","network_type","created_at","updated_at"
])

dim_stations = read_csv_with_fallback("dim_stations", [
    "station_id","nlc_code","station_name","network_id",
    "has_london_underground","has_elizabeth_line","has_overground",
    "has_dlr","has_night_tube","is_active","created_at","updated_at"
])

fact_lines_new = read_csv_with_fallback("fact_station_lines", [
    "station_line_id","station_id","line_id","is_interchange",
    "effective_from","effective_to","created_at"
])

print(f"  New passenger records (2020-2021): {fact_pax_new.count()}")
print(f"  New station-line records          : {fact_lines_new.count()}")


# =============================================================
#  STEP 2: HELPER FUNCTIONS FOR MERGING GOLD TABLES
# =============================================================

def read_gold(table_name):
    """Read existing gold table from HDFS parquet."""
    path = f"{GOLD_BASE}/{table_name}"
    return spark.read.parquet(path)

def save_gold(df, table_name):
    """Write updated gold table to HDFS."""
    path = f"{GOLD_BASE}/{table_name}"
    df.write.mode("overwrite").parquet(path)
    print(f"  Updated → {path}")


# =============================================================
#  STEP 3: STRATEGY 1 — APPEND (year-based gold tables)
#
#  For gold tables where years do NOT overlap between
#  full load (2017-2019) and incremental (2020-2021).
#
#  Just calculate new year results and APPEND to gold table.
#  No risk of duplicates because years are different.
# =============================================================

print("\n[STEP 3] STRATEGY 1 — Append new year results to gold tables")
print("(years 2020-2021 don't exist in old gold → safe to append)")

# ── gold_passengers_by_year ────────────────────────────────────────────────────
# Old gold has: 2017, 2018, 2019 rows
# New result:   2020, 2021 rows
# Append → gold table now has all 5 years, no duplicates
print("\n--- gold_passengers_by_year ---")

new_passengers_by_year = (
    fact_pax_new
    .join(dim_date, "date_id")
    .groupBy("year")
    .agg(_sum("total_entry_exit").alias("total_passengers"))
    .orderBy("year")
)
new_passengers_by_year.show(truncate=False)

# Read existing gold + append new years
existing = read_gold("gold_passengers_by_year")
updated  = existing.union(new_passengers_by_year).orderBy("year")
save_gold(updated, "gold_passengers_by_year")

# ── gold_quarterly_trend ───────────────────────────────────────────────────────
# Old gold has: Q1-Q4 of 2017, 2018, 2019
# New result:   Q1-Q4 of 2020, 2021
# No year overlap → safe to append
print("\n--- gold_quarterly_trend ---")

new_quarterly_trend = (
    fact_pax_new
    .join(dim_date, "date_id")
    .groupBy(
        col("year").cast(IntegerType()),
        col("quarter").cast(IntegerType())
    )
    .agg(_sum("total_entry_exit").alias("total_passengers"))
    .orderBy("year", "quarter")
)
new_quarterly_trend.show(truncate=False)

existing = read_gold("gold_quarterly_trend")
updated  = existing.union(new_quarterly_trend).orderBy("year", "quarter")
save_gold(updated, "gold_quarterly_trend")


# =============================================================
#  STEP 4: STRATEGY 2 — MERGE (cross-year aggregate gold tables)
#
#  For gold tables that SUM across ALL years together.
#  We cannot simply append because totals need to include
#  both old (2017-2019) and new (2020-2021) data.
#
#  Approach:
#    1. Read EXISTING gold table (has 2017-2019 aggregated totals)
#    2. Calculate NEW results from incremental data (2020-2021 only)
#    3. Union old gold + new results
#    4. Re-aggregate (SUM the two partial totals together)
#    5. Overwrite gold table
#
#  Example for gold_busiest_stations:
#    Old gold: King's Cross = 500M (2017-2019 total)
#    New calc: King's Cross = 200M (2020-2021 total)
#    Union → re-SUM → King's Cross = 700M  ← correct all-years total
# =============================================================

print("\n[STEP 4] STRATEGY 2 — Merge new results into cross-year gold tables")
print("(read old gold + calculate new → union → re-aggregate)")

# ── gold_busiest_stations ──────────────────────────────────────────────────────
print("\n--- gold_busiest_stations ---")

# Calculate new station totals from 2020-2021 data only
new_station_totals = (
    fact_pax_new
    .join(dim_stations, "station_id")
    .groupBy("station_name")
    .agg(_sum("total_entry_exit").alias("total_passengers"))
)

# Read old gold (has 2017-2019 totals) + union with new (2020-2021 totals)
# Then re-SUM by station to get the combined all-years total
existing = read_gold("gold_busiest_stations")
merged   = existing.union(new_station_totals)
updated  = (
    merged
    .groupBy("station_name")
    .agg(_sum("total_passengers").alias("total_passengers"))
    .orderBy(desc("total_passengers"))
    .limit(10)
)
updated.show(truncate=False)
save_gold(updated, "gold_busiest_stations")

# ── gold_passengers_by_line ────────────────────────────────────────────────────
print("\n--- gold_passengers_by_line ---")

new_by_line = (
    fact_pax_new
    .join(fact_lines_new, "station_id")
    .join(dim_lines, "line_id")
    .groupBy("line_name")
    .agg(_sum("total_entry_exit").alias("total_passengers"))
)

existing = read_gold("gold_passengers_by_line")
merged   = existing.union(new_by_line)
updated  = (
    merged
    .groupBy("line_name")
    .agg(_sum("total_passengers").alias("total_passengers"))
    .orderBy(desc("total_passengers"))
)
updated.show(truncate=False)
save_gold(updated, "gold_passengers_by_line")

# ── gold_passengers_by_network ─────────────────────────────────────────────────
print("\n--- gold_passengers_by_network ---")

new_by_network = (
    fact_pax_new
    .join(dim_stations, "station_id")
    .join(dim_networks, "network_id")
    .groupBy("network_name", "network_type")
    .agg(_sum("total_entry_exit").alias("total_passengers"))
)

existing = read_gold("gold_passengers_by_network")
merged   = existing.union(new_by_network)
updated  = (
    merged
    .groupBy("network_name", "network_type")
    .agg(_sum("total_passengers").alias("total_passengers"))
    .orderBy(desc("total_passengers"))
)
updated.show(truncate=False)
save_gold(updated, "gold_passengers_by_network")

# ── gold_interchange_stations ──────────────────────────────────────────────────
# Station-line mappings don't change by year — just refresh from new fact_lines
print("\n--- gold_interchange_stations ---")

updated_interchange = (
    fact_lines_new
    .join(dim_stations, "station_id")
    .groupBy("station_name")
    .agg(count("line_id").alias("num_lines"))
    .orderBy(desc("num_lines"))
    .limit(15)
)
updated_interchange.show(truncate=False)
save_gold(updated_interchange, "gold_interchange_stations")

# ── gold_night_tube_analysis ───────────────────────────────────────────────────
print("\n--- gold_night_tube_analysis ---")

new_night_tube = (
    fact_pax_new
    .join(dim_stations, "station_id")
    .groupBy("has_night_tube")
    .agg(
        count("station_id").alias("num_records"),
        _sum("total_entry_exit").alias("total_passengers"),
        avg("total_entry_exit").alias("avg_passengers_per_record")
    )
)

existing = read_gold("gold_night_tube_analysis")
merged   = existing.union(new_night_tube)
updated  = (
    merged
    .groupBy("has_night_tube")
    .agg(
        _sum("num_records").alias("num_records"),
        _sum("total_passengers").alias("total_passengers"),
        avg("avg_passengers_per_record").alias("avg_passengers_per_record")
    )
)
updated.show(truncate=False)
save_gold(updated, "gold_night_tube_analysis")


# =============================================================
#  SUMMARY
# =============================================================

print("\n" + "=" * 60)
print("TRULY INCREMENTAL SPARK PIPELINE COMPLETE")
print("=" * 60)
print("Only processed: 2020-2021 data (incremental)")
print("Did NOT re-read: 2017-2019 data (full load)")
print("")
print("Strategy 1 — Append (year-based, no overlap):")
print("  gold_passengers_by_year  ← appended 2020, 2021 rows")
print("  gold_quarterly_trend     ← appended 2020, 2021 quarters")
print("")
print("Strategy 2 — Merge (cross-year aggregates):")
print("  gold_busiest_stations    ← re-summed with new station totals")
print("  gold_passengers_by_line  ← re-summed with new line totals")
print("  gold_passengers_by_network ← re-summed with new network totals")
print("  gold_interchange_stations  ← refreshed from new mapping data")
print("  gold_night_tube_analysis   ← re-summed with new night tube data")
print("=" * 60)

spark.stop()
