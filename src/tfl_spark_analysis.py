from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, count, avg, desc

spark = SparkSession.builder \
    .appName("TFL_Analysis_Subirna") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

HDFS_BASE   = "/tmp/subirna/TFL_project"
OUTPUT_BASE = "/tmp/subirna/TFL_project/gold"
HIVE_DB     = "subirna_tfl"

spark.sql(f"CREATE DATABASE IF NOT EXISTS {HIVE_DB}")
spark.sql(f"USE {HIVE_DB}")

print("=" * 60)
print("TFL Data Analysis Pipeline - Subirna")
print("=" * 60)

# ============================================================
# CREATE SOURCE EXTERNAL TABLES IN HIVE
# ============================================================

print("\nCreating source Hive external tables...")

spark.sql(f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS {HIVE_DB}.dim_date (
        date_id INT, year INT, quarter INT, month INT, is_annual BOOLEAN,
        period_label STRING, period_start STRING, period_end STRING, created_at STRING
    ) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    STORED AS TEXTFILE LOCATION '{HDFS_BASE}/dim_date'
""")

spark.sql(f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS {HIVE_DB}.dim_lines (
        line_id INT, line_name STRING, line_color STRING, is_night_service BOOLEAN,
        created_at STRING, updated_at STRING
    ) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    STORED AS TEXTFILE LOCATION '{HDFS_BASE}/dim_lines'
""")

spark.sql(f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS {HIVE_DB}.dim_networks (
        network_id INT, network_name STRING, network_type STRING,
        created_at STRING, updated_at STRING
    ) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    STORED AS TEXTFILE LOCATION '{HDFS_BASE}/dim_networks'
""")

spark.sql(f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS {HIVE_DB}.dim_stations (
        station_id INT, nlc_code STRING, station_name STRING, network_id INT,
        has_london_underground BOOLEAN, has_elizabeth_line BOOLEAN,
        has_overground BOOLEAN, has_dlr BOOLEAN, has_night_tube BOOLEAN,
        is_active BOOLEAN, created_at STRING, updated_at STRING
    ) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    STORED AS TEXTFILE LOCATION '{HDFS_BASE}/dim_stations'
""")

spark.sql(f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS {HIVE_DB}.fact_passenger_entry_exit (
        entry_exit_id BIGINT, station_id INT, date_id INT,
        total_entry_exit BIGINT, estimated_entries BIGINT, estimated_exits BIGINT,
        record_type STRING, data_source STRING, created_at STRING
    ) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    STORED AS TEXTFILE LOCATION '{HDFS_BASE}/fact_passenger_entry_exit'
""")

spark.sql(f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS {HIVE_DB}.fact_station_lines (
        station_line_id INT, station_id INT, line_id INT, is_interchange BOOLEAN,
        effective_from STRING, effective_to STRING, created_at STRING
    ) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    STORED AS TEXTFILE LOCATION '{HDFS_BASE}/fact_station_lines'
""")

print("Source tables created successfully")

# ============================================================
# LOAD ALL TABLES FROM HDFS
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

# Helper: write analysis result to HDFS as parquet.
# Hive external table registration is handled by hive_ddl.hql (runs after this stage).
def save_gold_table(df, table_name):
    path = f"{OUTPUT_BASE}/{table_name}"
    df.write.mode("overwrite").parquet(path)
    print(f"Saved to HDFS: {path}")

# ============================================================
# ANALYSIS 1: Top 10 Busiest Stations
# ============================================================

print("\n" + "=" * 60)
print("ANALYSIS 1: Top 10 Busiest Stations")
print("=" * 60)

busiest_stations = fact_pax \
    .join(dim_stations, "station_id") \
    .groupBy("station_name") \
    .agg(_sum("total_entry_exit").alias("total_passengers")) \
    .orderBy(desc("total_passengers")) \
    .limit(10)

busiest_stations.show(truncate=False)
save_gold_table(busiest_stations, "gold_busiest_stations")

# ============================================================
# ANALYSIS 2: Passengers by Year
# ============================================================

print("\n" + "=" * 60)
print("ANALYSIS 2: Total Passengers by Year")
print("=" * 60)

passengers_by_year = fact_pax \
    .join(dim_date, "date_id") \
    .groupBy("year") \
    .agg(_sum("total_entry_exit").alias("total_passengers")) \
    .orderBy("year")

passengers_by_year.show(truncate=False)
save_gold_table(passengers_by_year, "gold_passengers_by_year")

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
    .agg(_sum("total_entry_exit").alias("total_passengers")) \
    .orderBy(desc("total_passengers"))

passengers_by_line.show(truncate=False)
save_gold_table(passengers_by_line, "gold_passengers_by_line")

# ============================================================
# ANALYSIS 4: Passengers by Network
# ============================================================

print("\n" + "=" * 60)
print("ANALYSIS 4: Passengers by Network Type")
print("=" * 60)

passengers_by_network = fact_pax \
    .join(dim_stations, "station_id") \
    .join(dim_networks, "network_id") \
    .groupBy("network_name", "network_type") \
    .agg(_sum("total_entry_exit").alias("total_passengers")) \
    .orderBy(desc("total_passengers"))

passengers_by_network.show(truncate=False)
save_gold_table(passengers_by_network, "gold_passengers_by_network")

# ============================================================
# ANALYSIS 5: Interchange Stations
# ============================================================

print("\n" + "=" * 60)
print("ANALYSIS 5: Top Interchange Stations")
print("=" * 60)

interchange_stations = fact_lines \
    .join(dim_stations, "station_id") \
    .groupBy("station_name") \
    .agg(count("line_id").alias("num_lines")) \
    .orderBy(desc("num_lines")) \
    .limit(15)

interchange_stations.show(truncate=False)
save_gold_table(interchange_stations, "gold_interchange_stations")

# ============================================================
# ANALYSIS 6: Quarterly Trend
# ============================================================

print("\n" + "=" * 60)
print("ANALYSIS 6: Passengers by Year and Quarter")
print("=" * 60)

quarterly_trend = fact_pax \
    .join(dim_date, "date_id") \
    .groupBy("year", "quarter") \
    .agg(_sum("total_entry_exit").alias("total_passengers")) \
    .orderBy("year", "quarter")

quarterly_trend.show(truncate=False)
save_gold_table(quarterly_trend, "gold_quarterly_trend")

# ============================================================
# ANALYSIS 7: Night Tube Analysis
# ============================================================

print("\n" + "=" * 60)
print("ANALYSIS 7: Night Tube vs Regular Stations")
print("=" * 60)

night_tube_analysis = fact_pax \
    .join(dim_stations, "station_id") \
    .groupBy("has_night_tube") \
    .agg(
        count("station_id").alias("num_records"),
        _sum("total_entry_exit").alias("total_passengers"),
        avg("total_entry_exit").alias("avg_passengers_per_record")
    )

night_tube_analysis.show(truncate=False)
save_gold_table(night_tube_analysis, "gold_night_tube_analysis")

# ============================================================
# SUMMARY
# ============================================================

print("\n" + "=" * 60)
print("PIPELINE COMPLETE")
print("=" * 60)
print(f"Database: {HIVE_DB}")
print("Source tables: dim_date, dim_lines, dim_networks, dim_stations,")
print("               fact_passenger_entry_exit, fact_station_lines")
print("Gold tables:   gold_busiest_stations, gold_passengers_by_year,")
print("               gold_passengers_by_line, gold_passengers_by_network,")
print("               gold_interchange_stations, gold_quarterly_trend,")
print("               gold_night_tube_analysis")
print("=" * 60)

spark.stop()
