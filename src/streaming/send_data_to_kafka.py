#!/usr/bin/env python3
"""
=============================================================
 FILE: send_data_to_kafka.py
 PROJECT: TFL Data Pipeline - Subirna
 LOAD TYPE: STREAMING (Real-time)
=============================================================

PURPOSE:
  Fetches live tube arrival predictions from the TFL Unified API
  and publishes each prediction as a JSON message to Kafka.

  Runs continuously — polls TFL API every 30 seconds and
  sends all returned predictions to the Kafka topic.

WHAT TFL API RETURNS (one message per train):
  {
    "vehicleId":       "021",
    "stationName":     "Green Park Underground Station",
    "lineName":        "Victoria",
    "platformName":    "Southbound - Platform 4",
    "direction":       "inbound",
    "timestamp":       "2026-06-14T10:00:00Z",
    "timeToStation":   786,        <- seconds until arrival
    "currentLocation": "Between Seven Sisters and Finsbury Park",
    "expectedArrival": "2026-06-14T10:13:06Z"
  }

KAFKA:
  Topic  : subirna_tfl_arrivals
  Broker : ip-172-31-6-42.eu-west-2.compute.internal:9092

USAGE:
  python3 send_data_to_kafka.py
  (started by Jenkins with nohup — runs until manually stopped)

LOGS:
  /tmp/subirna_producer.log
=============================================================
"""

import json
import time
import requests
from datetime import datetime
from kafka import KafkaProducer

# =============================================================
#  CONFIGURATION
# =============================================================

KAFKA_BROKER  = "ip-172-31-6-42.eu-west-2.compute.internal:9092"
KAFKA_TOPIC   = "subirna_tfl_arrivals"

# TFL Unified API — Victoria line live arrivals
# Register free at: https://api.tfl.gov.uk to get an app_key for higher rate limits
# Leave TFL_APP_KEY empty for anonymous access (lower rate limit, fine for 1 line)
TFL_API_URL   = "https://api.tfl.gov.uk/Line/victoria/Arrivals"
TFL_APP_KEY   = ""

POLL_INTERVAL = 30           # seconds between each TFL API call
LOG_FILE      = "/tmp/subirna_producer.log"


# =============================================================
#  HELPERS
# =============================================================

def log(msg):
    """Print and append to log file with timestamp."""
    line = f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | {msg}"
    print(line, flush=True)
    with open(LOG_FILE, "a") as f:
        f.write(line + "\n")


def create_producer():
    """Create and return a Kafka producer."""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        retries=5,
        acks="all",
    )


def fetch_arrivals():
    """
    Call TFL API and return list of arrival predictions.
    Each item in the list is one train arriving at one station.
    """
    params = {}
    if TFL_APP_KEY:
        params["app_key"] = TFL_APP_KEY

    response = requests.get(TFL_API_URL, params=params, timeout=10)
    response.raise_for_status()
    return response.json()


# =============================================================
#  MAIN LOOP
# =============================================================

def main():
    log("=" * 50)
    log("TFL PRODUCER STARTING — Subirna")
    log(f"  Topic  : {KAFKA_TOPIC}")
    log(f"  Broker : {KAFKA_BROKER}")
    log(f"  API    : {TFL_API_URL}")
    log(f"  Polling: every {POLL_INTERVAL} seconds")
    log("=" * 50)

    producer = create_producer()
    log("Connected to Kafka")

    total_sent = 0

    while True:
        try:
            arrivals = fetch_arrivals()

            count = 0
            for arrival in arrivals:
                producer.send(KAFKA_TOPIC, value=arrival)
                count += 1

            producer.flush()
            total_sent += count
            log(f"Sent {count} arrivals | Total so far: {total_sent}")

        except requests.RequestException as e:
            log(f"TFL API error: {e}")
        except Exception as e:
            log(f"Unexpected error: {e}")

        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
