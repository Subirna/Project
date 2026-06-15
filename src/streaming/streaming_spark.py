#!/usr/bin/env python3
"""
=============================================================
 FILE: streaming_spark.py
 PROJECT: TFL Data Pipeline - Subirna
 LOAD TYPE: STREAMING (Real-time Spark Structured Streaming)
=============================================================

PURPOSE:
  Reads live tube arrival predictions from Kafka,
  transforms/aggregates the data using Spark,
  and writes results to HBase aggregated table.

  Runs continuously — processes a new micro-batch every 30 seconds.

PIPELINE:
  Kafka: subirna_tfl_arrivals  (raw JSON messages)
       │
       │  Spark reads → parses JSON → aggregates
       │
       ▼
  HBase: subirna_tfl_arrivals_agg  (transformed results)

TWO HBase TABLES IN THIS STREAMING PROJECT:
  subirna_tfl_arrivals      ← raw data  (write_kafka_to_hbase.py)
  subirna_tfl_arrivals_agg  ← transformed data  (THIS script)

TRANSFORMATION:
  Groups arrivals by station + date
  Calculates:
    - total_arrivals    : cumulative count of trains seen today
    - avg_time_seconds  : average seconds until train arrives
    - last_updated      : timestamp of most recent update

HBase Row Key (aggregated table):
  {StationName_no_spaces}_{date}
  Example: GreenParkUndergroundStation_2026-06-14

  One row per station per day. Updated every 30 seconds.
  Total arrivals accumulate throughout the day.

USAGE:
  spark-submit --master local[2] \\
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1 \\
      streaming_spark.py
  (started by Jenkins with nohup — runs until manually stopped)

LOGS:
  /tmp/subirna_spark_streaming.log
=============================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, count, avg, max as _max
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from datetime import datetime

# =============================================================
#  CONFIGURATION
# =============================================================

KAFKA_BROKER    = "ip-172-31-6-42.eu-west-2.compute.internal:9092"
KAFKA_TOPIC     = "subirna_tfl_arrivals"

HBASE_HOST      = "ip-172-31-6-42.eu-west-2.compute.internal"
HBASE_PORT      = 9090
HBASE_AGG_TABLE = "subirna_tfl_arrivals_agg"

CHECKPOINT_DIR  = "/tmp/subirna/TFL_project/checkpoints/streaming"
TRIGGER_SECONDS = "30 seconds"      # process a new batch every 30 seconds


# =============================================================
#  SPARK SESSION
# =============================================================

spark = SparkSession.builder \
    .appName("TFL_Streaming_Spark_Subirna") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("TFL SPARK STRUCTURED STREAMING — Subirna")
print(f"Reading from Kafka : {KAFKA_TOPIC}")
print(f"Writing to HBase   : {HBASE_AGG_TABLE}")
print(f"Trigger interval   : {TRIGGER_SECONDS}")
print("=" * 60)


# =============================================================
#  STEP 1: DEFINE TFL API JSON SCHEMA
#
#  Kafka messages come as raw bytes.
#  We define the schema so Spark knows how to parse
#  the TFL JSON into proper typed columns.
# =============================================================

tfl_schema = StructType([
    StructField("id",              StringType()),
    StructField("vehicleId",       StringType()),
    StructField("stationName",     StringType()),
    StructField("lineId",          StringType()),
    StructField("lineName",        StringType()),
    StructField("platformName",    StringType()),
    StructField("direction",       StringType()),
    StructField("timestamp",       StringType()),   # "2026-06-14T10:00:00Z"
    StructField("timeToStation",   IntegerType()),  # seconds until arrival
    StructField("currentLocation", StringType()),
    StructField("expectedArrival", StringType()),
    StructField("modeName",        StringType()),
])


# =============================================================
#  STEP 2: READ STREAM FROM KAFKA
#
#  readStream is the streaming version of spark.read
#  Kafka sends messages as bytes — we get key + value columns
# =============================================================

raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "latest") \
    .load()


# =============================================================
#  STEP 3: PARSE JSON FROM KAFKA VALUE BYTES
#
#  raw_stream has column: value (bytes)
#  We cast to string first, then parse JSON using our schema
#  Then expand data.* into individual columns
# =============================================================

parsed = raw_stream \
    .select(
        from_json(col("value").cast("string"), tfl_schema).alias("data")
    ) \
    .select("data.*") \
    .filter(col("stationName").isNotNull()) \
    .filter(col("vehicleId").isNotNull())

# Extract just the date part from timestamp
# "2026-06-14T10:00:00Z" → "2026-06-14"
arrivals = parsed.withColumn("date", col("timestamp").substr(1, 10))


# =============================================================
#  STEP 4: foreachBatch — TRANSFORM + WRITE TO HBASE
#
#  Spark calls this function every 30 seconds with a batch of
#  new messages as a regular DataFrame.
#
#  Inside foreachBatch we:
#    1. Aggregate: count arrivals, avg time per station
#    2. Read existing HBase row (cumulative total for today)
#    3. Add new arrivals to existing total
#    4. Write updated row back to HBase
# =============================================================

def process_batch(batch_df, batch_id):
    """Process one micro-batch: aggregate and write to HBase."""

    if batch_df.rdd.isEmpty():
        print(f"  Batch {batch_id}: no new messages")
        return

    # ── Aggregate this batch ──────────────────────────────────
    # Group by station + date → count arrivals, avg time to station
    batch_agg = batch_df \
        .groupBy("stationName", "lineName", "date") \
        .agg(
            count("vehicleId").alias("new_arrivals"),
            avg("timeToStation").alias("avg_time"),
            _max("timestamp").alias("last_updated")
        ) \
        .collect()

    if not batch_agg:
        print(f"  Batch {batch_id}: aggregation returned no rows")
        return

    # ── Write to HBase ────────────────────────────────────────
    import happybase

    connection = happybase.Connection(HBASE_HOST, port=HBASE_PORT)
    table      = connection.table(HBASE_AGG_TABLE)
    rows_written = 0

    for row in batch_agg:
        # Build row key: StationName_no_spaces + date
        station_key = row.stationName.replace(" ", "")
        row_key     = f"{station_key}_{row.date}"

        # Read existing cumulative total from HBase (if row exists)
        existing       = table.row(row_key.encode())
        existing_total = int(
            existing.get(b"cf:total_arrivals", b"0").decode() or 0
        )

        # Add this batch's arrivals to the running total
        cumulative_total = existing_total + row.new_arrivals

        # Write updated row back to HBase
        table.put(row_key.encode(), {
            b"cf:station":          row.stationName.encode(),
            b"cf:line":             row.lineName.encode(),
            b"cf:date":             row.date.encode(),
            b"cf:total_arrivals":   str(cumulative_total).encode(),
            b"cf:avg_time_seconds": str(int(row.avg_time or 0)).encode(),
            b"cf:last_updated":     (row.last_updated or "").encode(),
        })
        rows_written += 1

    connection.close()

    print(f"  Batch {batch_id} | {datetime.now().strftime('%H:%M:%S')} "
          f"| Stations updated: {rows_written} "
          f"| Raw messages: {batch_df.count()}")


# =============================================================
#  STEP 5: START THE STREAMING QUERY
#
#  writeStream is the streaming version of df.write
#  foreachBatch hands each micro-batch to process_batch()
#  checkpointLocation saves progress to HDFS so the job
#  can resume from where it stopped if it crashes
# =============================================================

query = arrivals.writeStream \
    .foreachBatch(process_batch) \
    .option("checkpointLocation", CHECKPOINT_DIR) \
    .trigger(processingTime=TRIGGER_SECONDS) \
    .start()

print("Streaming query started — waiting for Kafka messages...")
print(f"Checkpoint : {CHECKPOINT_DIR}")
print(f"HBase table: {HBASE_AGG_TABLE}")
print("Press Ctrl+C to stop\n")

query.awaitTermination()
