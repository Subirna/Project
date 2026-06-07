#!/usr/bin/env python3
"""
=============================================================
 FILE: simulate_data_split.py
 PROJECT: TFL Data Pipeline - Subirna
=============================================================

PURPOSE:
  Splits the TFL passenger data into two separate CSV files
  that simulate how data arrives in a real pipeline:

    1. full_load_data.csv        → historical data (2017, 2018, 2019)
    2. incremental_load_data.csv → newer data     (2020, 2021)

WHY WE DO THIS (Uttam's approach):
  In a real company, new transaction/passenger records arrive
  every day. The FULL LOAD runs once to load all old history.
  After that, only the NEW records (incremental) are loaded
  each day — saving time and compute.

  We cannot travel back in time to get data in two batches,
  so we SIMULATE it by pre-splitting the data by year:
    - 2017-2019 pretends to be "old data already in the system"
    - 2020-2021 pretends to be "new data arriving later"

  This is exactly what Uttam's simulate_data_split.py does —
  he splits by Timestamp (oldest = full load, newer = incremental).
  We split by Year (same idea, adapted for TFL data).

HOW IT WORKS:
  1. Connect to PostgreSQL (where all TFL data is stored)
  2. Read fact_passenger_entry_exit joined with dim_date (to get year)
  3. Sort chronologically: oldest year first
  4. Split: years 2017-2019 → full_load_data.csv
             years 2020-2021 → incremental_load_data.csv
  5. Save both CSVs to data/split/

SPLIT BREAKDOWN:
  Full Load (2017-2019)    → historical baseline, loaded ONCE
  Incremental (2020-2021)  → new records, loaded each time new data arrives

OUTPUT FILES:
  data/split/full_load_data.csv        ← used by sqoop_import.sh
  data/split/incremental_load_data.csv ← used by incremental_sqoop.py

USAGE:
  Run ONCE before starting the pipeline:
    python3 simulate_data_split.py

  Compare with Uttam's usage:
    python3 simulate_data_split.py   (same command, same purpose)
=============================================================
"""

import pandas as pd          # for reading data and saving CSV
import psycopg2              # for connecting to PostgreSQL
import os                    # for creating output directory
from datetime import datetime

# =============================================================
#  CONFIGURATION
# =============================================================

# PostgreSQL connection (same credentials as sqoop_import.sh)
DB_CONFIG = {
    "host":     "13.42.152.118",
    "port":     5432,
    "dbname":   "testdb",
    "user":     "admin",
    "password": "admin123"
}

# Year-based split boundaries
# Years 2017, 2018, 2019 → treated as historical full load data
# Years 2020, 2021       → treated as new incremental data
FULL_LOAD_YEARS        = [2017, 2018, 2019]
INCREMENTAL_LOAD_YEARS = [2020, 2021]

# Where to save the split CSV files
OUTPUT_DIR = "data/split"


# =============================================================
#  STEP 1: CONNECT TO POSTGRESQL AND READ DATA
# =============================================================

def read_from_postgres():
    """
    Read fact_passenger_entry_exit joined with dim_date.
    We need dim_date to get the YEAR for each passenger record.

    Returns a pandas DataFrame with all passenger records
    plus a 'year' column, sorted oldest first.

    This is like Uttam reading his synthetic_fraud_dataset.csv —
    we read from PostgreSQL instead of a local CSV.
    """
    print("Connecting to PostgreSQL...")
    conn = psycopg2.connect(**DB_CONFIG)

    # JOIN fact table with dim_date to get the year of each record
    # ORDER BY year ASC = oldest records first (chronological order)
    # This matches Uttam's: sort_values(by='Timestamp', ascending=True)
    query = """
        SELECT
            f.entry_exit_id,
            f.station_id,
            f.date_id,
            f.total_entry_exit,
            f.estimated_entries,
            f.estimated_exits,
            f.record_type,
            f.data_source,
            f.created_at,
            d.year
        FROM fact_passenger_entry_exit f
        JOIN dim_date d ON f.date_id = d.date_id
        ORDER BY d.year ASC, f.date_id ASC
    """

    print("Reading fact_passenger_entry_exit from PostgreSQL...")
    df = pd.read_sql(query, conn)
    conn.close()

    print(f"Total records read: {len(df):,}")
    print(f"Years found: {sorted(df['year'].unique().tolist())}")
    return df


# =============================================================
#  STEP 2: SPLIT THE DATA BY YEAR
# =============================================================

def split_data(df):
    """
    Split the DataFrame into two parts based on year.

    Full Load (2017-2019):
      - The historical baseline
      - Loaded ONCE using sqoop_import.sh

    Incremental (2020-2021):
      - Newer records that arrived later
      - Loaded using incremental_sqoop.py each time new data comes

    This mirrors Uttam's approach:
      df_full        = df.iloc[:full_end]           (first 60% by timestamp)
      df_incremental = df.iloc[full_end:inc_end]    (next 20% by timestamp)

    We split by year instead of row percentage because TFL data
    is naturally divided by year.
    """
    df_full        = df[df['year'].isin(FULL_LOAD_YEARS)].drop(columns=['year'])
    df_incremental = df[df['year'].isin(INCREMENTAL_LOAD_YEARS)].drop(columns=['year'])

    return df_full, df_incremental


# =============================================================
#  STEP 3: SAVE TO CSV FILES
# =============================================================

def save_splits(df_full, df_incremental):
    """
    Save both DataFrames as CSV files.

    These CSV files are the 'simulated' inputs:
      full_load_data.csv        → sqoop_import.sh reads/loads this first
      incremental_load_data.csv → incremental_sqoop.py loads this later

    Uttam outputs:
      data/split/full_load.csv
      data/split/incremental_load.csv

    We output:
      data/split/full_load_data.csv
      data/split/incremental_load_data.csv
    """
    os.makedirs(OUTPUT_DIR, exist_ok=True)  # create folder if not exists

    full_path = f"{OUTPUT_DIR}/full_load_data.csv"
    inc_path  = f"{OUTPUT_DIR}/incremental_load_data.csv"

    df_full.to_csv(full_path, index=False)
    df_incremental.to_csv(inc_path, index=False)

    return full_path, inc_path


# =============================================================
#  MAIN
# =============================================================

def main():
    print("=" * 60)
    print("TFL SIMULATE DATA SPLIT - Subirna")
    print(f"Run time: {datetime.now()}")
    print("=" * 60)

    # Step 1: Read from PostgreSQL
    df = read_from_postgres()

    # Step 2: Split by year
    print("\nSplitting data by year...")
    df_full, df_incremental = split_data(df)

    # Step 3: Save CSV files
    full_path, inc_path = save_splits(df_full, df_incremental)

    # Summary — like Uttam's print statements
    print("\n" + "=" * 60)
    print("DATA SPLIT COMPLETE")
    print("=" * 60)
    print(f"Full Load  (years {FULL_LOAD_YEARS})        : {len(df_full):>8,} rows → {full_path}")
    print(f"Incremental(years {INCREMENTAL_LOAD_YEARS})      : {len(df_incremental):>8,} rows → {inc_path}")
    print("\nNext Steps:")
    print("  1. Full load is already in PostgreSQL (sqoop_import.sh already ran)")
    print("  2. Load incremental_load_data.csv rows into PostgreSQL to simulate new arrivals")
    print("  3. Run: python3 incremental_sqoop.py")
    print("  4. Run: spark-submit incremental_spark.py")
    print("=" * 60)


if __name__ == "__main__":
    main()
