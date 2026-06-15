#!/usr/bin/env python3
"""
=============================================================
 FILE: write_kafka_to_hbase.py
 PROJECT: TFL Data Pipeline - Subirna
 LOAD TYPE: STREAMING (Real-time)
=============================================================

PURPOSE:
  Reads tube arrival predictions from Kafka topic and
  writes each record into HBase for fast real-time lookup.

  Runs continuously — processes every message as it arrives.

KAFKA → HBASE MAPPING:

  Kafka topic : subirna_tfl_arrivals
  HBase table : subirna_tfl_arrivals
  Column family: cf

  HBase row key format:
    {StationName_spaces_removed}_{vehicleId}_{timestamp}
  Example:
    GreenParkUndergroundStation_021_2026-06-14T10:00:00Z

  Why this row key?
    HBase stores rows sorted alphabetically by row key.
    This design groups all arrivals for the same station together,
    then by train number, then by time — making station lookups fast.

  HBase columns:
    cf:station  → stationName       (e.g. "Green Park Underground Station")
    cf:line     → lineName          (e.g. "Victoria")
    cf:platform → platformName      (e.g. "Southbound - Platform 4")
    cf:vehicle  → vehicleId         (e.g. "021")
    cf:arrival  → expectedArrival   (e.g. "2026-06-14T10:13:06Z")

USAGE:
  python3 write_kafka_to_hbase.py
  (started by Jenkins with nohup — runs until manually stopped)

LOGS:
  /tmp/subirna_consumer.log
=============================================================
"""

import json
from datetime import datetime
from kafka import KafkaConsumer
import happybase

# =============================================================
#  CONFIGURATION
# =============================================================

KAFKA_BROKER   = "ip-172-31-6-42.eu-west-2.compute.internal:9092"
KAFKA_TOPIC    = "subirna_tfl_arrivals"
KAFKA_GROUP_ID = "subirna_tfl_consumer"

HBASE_HOST     = "ip-172-31-6-42.eu-west-2.compute.internal"
HBASE_PORT     = 9090                  # HBase Thrift Server port
HBASE_TABLE    = "subirna_tfl_arrivals"

LOG_FILE       = "/tmp/subirna_consumer.log"
LOG_EVERY      = 50                    # log a line every N records written


# =============================================================
#  HELPERS
# =============================================================

def log(msg):
    """Print and append to log file with timestamp."""
    line = f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | {msg}"
    print(line, flush=True)
    with open(LOG_FILE, "a") as f:
        f.write(line + "\n")


def build_row_key(arrival):
    """
    Build HBase row key from arrival data.

    Format: {StationName_no_spaces}_{vehicleId}_{timestamp}
    Example: GreenParkUndergroundStation_021_2026-06-14T10:00:00Z

    Uses 'timestamp' (when TFL generated the prediction), NOT
    expectedArrival — so each poll creates a new unique row even
    for the same train approaching the same station.
    """
    station   = arrival.get("stationName", "Unknown").replace(" ", "")
    vehicle   = arrival.get("vehicleId", "Unknown")
    timestamp = arrival.get("timestamp", "Unknown")
    return f"{station}_{vehicle}_{timestamp}"


def write_to_hbase(table, arrival):
    """Write one arrival prediction to HBase. Returns the row key."""
    row_key = build_row_key(arrival)
    table.put(row_key.encode(), {
        b"cf:station":  arrival.get("stationName", "").encode(),
        b"cf:line":     arrival.get("lineName", "").encode(),
        b"cf:platform": arrival.get("platformName", "").encode(),
        b"cf:vehicle":  arrival.get("vehicleId", "").encode(),
        b"cf:arrival":  arrival.get("expectedArrival", "").encode(),
    })
    return row_key


# =============================================================
#  MAIN LOOP
# =============================================================

def main():
    log("=" * 50)
    log("TFL CONSUMER STARTING — Subirna")
    log(f"  Kafka topic : {KAFKA_TOPIC}")
    log(f"  Kafka broker: {KAFKA_BROKER}")
    log(f"  HBase host  : {HBASE_HOST}:{HBASE_PORT}")
    log(f"  HBase table : {HBASE_TABLE}")
    log("=" * 50)

    # Connect to HBase via Thrift
    connection = happybase.Connection(HBASE_HOST, port=HBASE_PORT)
    table = connection.table(HBASE_TABLE)
    log("Connected to HBase")

    # Connect to Kafka
    # auto_offset_reset='latest' means we only read NEW messages
    # (not replay everything from the beginning every restart)
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        group_id=KAFKA_GROUP_ID,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )
    log("Connected to Kafka — waiting for messages...")

    total_written = 0

    for message in consumer:
        try:
            arrival = message.value
            row_key = write_to_hbase(table, arrival)
            total_written += 1

            if total_written % LOG_EVERY == 0:
                log(f"Written {total_written} records | Last row key: {row_key}")

        except Exception as e:
            log(f"Error writing record: {e}")
            log(f"  Failed message: {message.value}")


if __name__ == "__main__":
    main()
