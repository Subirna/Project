from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, avg, max, min, round, desc, asc, year, when

# ============================================================
# TFL Spark Analysis - Subirna
# Reads from HDFS, runs transformations, saves gold layer
# ============================================================

spark = SparkSession.builder \
    .appName("TFL_Analysis_Subirna") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

HDFS_BASE   = "/tmp/subirna/TFL_project"
OUTPUT_BASE = "/tmp/subirna/TFL_project/gold"

print("=" * 60)
print("TFL Data Analysis Pipeline - Subirna")
print("=" * 60)

# ============================================================
# LOAD ALL TABLES
# ============================================================

print("\nLoading tables from HDFS...")

dim_date = spark.read.option("header", "false").option("inferSchema", "true") \
    .csv(f"{HDFS_BASE}/dim_date") \
    .toDF("date_id","year","quarter","month","is_annual","period_label","period_start","period_end","created_at")

dim_lines = spark.read.option("header", "false").option("inferSchema", "true") \
    .csv(f"{HDFS_BASE}/dim_lines") \
    .toDF("line_id","line_name","line_color","is_night_service","created_at","updated_at")

dim_networks = spark.read.option("header", "false").option("inferSchema", "true") \
    .csv(f"{HDFS_BASE}/dim_networks") \
    .toDF("network_id","network_name","network_type","created_at","updated_at")

dim_stations = spark.read.option("header", "false").option("inferSchema", "true") \
    .csv(f"{HDFS_BASE}/dim_stations") \
    .toDF("station_id","nlc_code","station_name","network_id",
          "has_london_underground","has_elizabeth_line","has_overground",
          "has_dlr","has_night_tube","is_active","created_at","updated_at")

fact_pax = spark.read.option("header", "false").option("inferSchema", "true") \
    .csv(f"{HDFS_BASE}/fact_passenger_entry_exit") \
    .toDF("entry_exit_id","station_id","date_id","total_entry_exit",
          "estimated_entries","estimated_exits","record_type","data_source","created_at")

fact_lines = spark.read.option("header", "false").option("inferSchema", "true") \
    .csv(f"{HDFS_BASE}/fact_station_lines") \
    .toDF("station_line_id","station_id","line_id","is_interchange",
          "effective_from","effective_to","created_at")

print("All 6 tables loaded successfully")

# ============================================================
# ANALYSIS 1: Busiest Stations by Total Passengers
# ============================================================

print("\n" + "=" * 60)
print("ANALYSIS 1: Top 10 Busiest Stations")
print("=" * 60)

busiest_stations = fact_pax \
    .join(dim_stations, "station_id") \
    .groupBy("station_name") \
    .agg(sum("total_entry_exit").alias("total_passengers")) \
    .orderBy(desc("total_passengers")) \
    .limit(10)

busiest_stations.show(truncate=False)

busiest_stations.write.mode("overwrite") \
    .parquet(f"{OUTPUT_BASE}/gold_busiest_stations")
print("Saved: gold_busiest_stations")

# ============================================================
# ANALYSIS 2: Passengers by Year
# ============================================================

print("\n" + "=" * 60)
print("ANALYSIS 2: Total Passengers by Year")
print("=" * 60)

passengers_by_year = fact_pax \
    .join(dim_date, "date_id") \
    .groupBy("year") \
    .agg(sum("total_entry_exit").alias("total_passengers")) \
    .orderBy("year")

passengers_by_year.show(truncate=False)

passengers_by_year.write.mode("overwrite") \
    .parquet(f"{OUTPUT_BASE}/gold_passengers_by_year")
print("Saved: gold_passengers_by_year")

# ============================================================
# ANALYSIS 3: Passengers by Tube Line
# ============================================================

print("\n" + "=" * 60)
print("ANALYSIS 3: Passengers by Tube Line")
print("=" * 60)

passengers_by_line = fact_pax \
    .join(fact_lines, "station_id") \
    .join(dim_lines, "line_id") \
    .groupBy("line_name") \
    .agg(sum("total_entry_exit").alias("total_passengers")) \
    .orderBy(desc("total_passengers"))

passengers_by_line.show(truncate=False)

passengers_by_line.write.mode("overwrite") \
    .parquet(f"{OUTPUT_BASE}/gold_passengers_by_line")
print("Saved: gold_passengers_by_line")

# ============================================================
# ANALYSIS 4: Passengers by Network Type
# ============================================================

print("\n" + "=" * 60)
print("ANALYSIS 4: Passengers by Network Type")
print("=" * 60)

passengers_by_network = fact_pax \
    .join(dim_stations, "station_id") \
    .join(dim_networks, "network_id") \
    .groupBy("network_name", "network_type") \
    .agg(sum("total_entry_exit").alias("total_passengers")) \
    .orderBy(desc("total_passengers"))

passengers_by_network.show(truncate=False)

passengers_by_network.write.mode("overwrite") \
    .parquet(f"{OUTPUT_BASE}/gold_passengers_by_network")
print("Saved: gold_passengers_by_network")

# ============================================================
# ANALYSIS 5: Interchange Stations
# ============================================================

print("\n" + "=" * 60)
print("ANALYSIS 5: Top Interchange Stations (Connected to Most Lines)")
print("=" * 60)

interchange_stations = fact_lines \
    .join(dim_stations, "station_id") \
    .groupBy("station_name") \
    .agg(count("line_id").alias("num_lines")) \
    .orderBy(desc("num_lines")) \
    .limit(15)

interchange_stations.show(truncate=False)

interchange_stations.write.mode("overwrite") \
    .parquet(f"{OUTPUT_BASE}/gold_interchange_stations")
print("Saved: gold_interchange_stations")

# ============================================================
# ANALYSIS 6: Quarterly Trend
# ============================================================

print("\n" + "=" * 60)
print("ANALYSIS 6: Passengers by Year and Quarter")
print("=" * 60)

quarterly_trend = fact_pax \
    .join(dim_date, "date_id") \
    .groupBy("year", "quarter") \
    .agg(sum("total_entry_exit").alias("total_passengers")) \
    .orderBy("year", "quarter")

quarterly_trend.show(truncate=False)

quarterly_trend.write.mode("overwrite") \
    .parquet(f"{OUTPUT_BASE}/gold_quarterly_trend")
print("Saved: gold_quarterly_trend")

# ============================================================
# ANALYSIS 7: Night Tube Stations Analysis
# ============================================================

print("\n" + "=" * 60)
print("ANALYSIS 7: Night Tube vs Regular Stations Ridership")
print("=" * 60)

night_tube_analysis = fact_pax \
    .join(dim_stations, "station_id") \
    .groupBy("has_night_tube") \
    .agg(
        count("station_id").alias("num_records"),
        sum("total_entry_exit").alias("total_passengers"),
        avg("total_entry_exit").alias("avg_passengers_per_record")
    )

night_tube_analysis.show(truncate=False)

night_tube_analysis.write.mode("overwrite") \
    .parquet(f"{OUTPUT_BASE}/gold_night_tube_analysis")
print("Saved: gold_night_tube_analysis")

# ============================================================
# SUMMARY
# ============================================================

print("\n" + "=" * 60)
print("PIPELINE COMPLETE - GOLD LAYER SAVED")
print("=" * 60)
print(f"Output location: {OUTPUT_BASE}")
print("Gold tables created:")
print("  - gold_busiest_stations")
print("  - gold_passengers_by_year")
print("  - gold_passengers_by_line")
print("  - gold_passengers_by_network")
print("  - gold_interchange_stations")
print("  - gold_quarterly_trend")
print("  - gold_night_tube_analysis")
print("=" * 60)

spark.stop()
