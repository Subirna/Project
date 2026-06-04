CREATE DATABASE IF NOT EXISTS subirna_tfl;
USE subirna_tfl;

CREATE EXTERNAL TABLE IF NOT EXISTS dim_date (
  date_id      INT,
  year         INT,
  quarter      INT,
  month        INT,
  is_annual    BOOLEAN,
  period_name  STRING,
  start_date   DATE,
  end_date     DATE,
  load_ts      TIMESTAMP
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/tmp/subirna/TFL_project/dim_date'
TBLPROPERTIES ('serialization.null.format'='null');

CREATE EXTERNAL TABLE IF NOT EXISTS dim_lines (
  line_id     INT,
  line_name   STRING,
  line_colour STRING,
  is_active   BOOLEAN,
  created_ts  TIMESTAMP,
  updated_ts  TIMESTAMP
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/tmp/subirna/TFL_project/dim_lines'
TBLPROPERTIES ('serialization.null.format'='null');

CREATE EXTERNAL TABLE IF NOT EXISTS dim_networks (
  network_id   INT,
  network_name STRING,
  network_type STRING,
  created_ts   TIMESTAMP,
  updated_ts   TIMESTAMP
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/tmp/subirna/TFL_project/dim_networks'
TBLPROPERTIES ('serialization.null.format'='null');

CREATE EXTERNAL TABLE IF NOT EXISTS dim_stations (
  station_id   INT,
  station_code DOUBLE,
  station_name STRING,
  network_id   INT,
  flag_1       BOOLEAN,
  flag_2       BOOLEAN,
  flag_3       BOOLEAN,
  flag_4       BOOLEAN,
  flag_5       BOOLEAN,
  flag_6       BOOLEAN,
  created_ts   TIMESTAMP,
  updated_ts   TIMESTAMP
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/tmp/subirna/TFL_project/dim_stations'
TBLPROPERTIES ('serialization.null.format'='null');

CREATE EXTERNAL TABLE IF NOT EXISTS fact_station_lines (
  station_line_id INT,
  station_id      INT,
  line_id         INT,
  is_interchange  BOOLEAN,
  effective_from  DATE,
  effective_to    DATE,
  created_at      TIMESTAMP
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/tmp/subirna/TFL_project/fact_station_lines'
TBLPROPERTIES ('serialization.null.format'='null');

CREATE EXTERNAL TABLE IF NOT EXISTS fact_passenger_entry_exit (
  entry_exit_id     BIGINT,
  station_id        INT,
  date_id           INT,
  total_entry_exit  BIGINT,
  estimated_entries BIGINT,
  estimated_exits   BIGINT,
  record_type       STRING,
  data_source       STRING,
  created_at        TIMESTAMP
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/tmp/subirna/TFL_project/fact_passenger_entry_exit'
TBLPROPERTIES ('serialization.null.format'='null');

-- ============================================================
-- GOLD LAYER: Drop old managed/ACID tables then re-create as
-- external parquet tables pointing to Spark output on HDFS.
-- ============================================================

DROP TABLE IF EXISTS subirna_tfl.gold_busiest_stations;
DROP TABLE IF EXISTS subirna_tfl.gold_passengers_by_year;
DROP TABLE IF EXISTS subirna_tfl.gold_passengers_by_line;
DROP TABLE IF EXISTS subirna_tfl.gold_passengers_by_network;
DROP TABLE IF EXISTS subirna_tfl.gold_interchange_stations;
DROP TABLE IF EXISTS subirna_tfl.gold_quarterly_trend;
DROP TABLE IF EXISTS subirna_tfl.gold_night_tube_analysis;

CREATE EXTERNAL TABLE subirna_tfl.gold_busiest_stations (
  station_name      STRING,
  total_passengers  BIGINT
)
STORED AS PARQUET
LOCATION '/tmp/subirna/TFL_project/gold/gold_busiest_stations';

CREATE EXTERNAL TABLE subirna_tfl.gold_passengers_by_year (
  year             INT,
  total_passengers BIGINT
)
STORED AS PARQUET
LOCATION '/tmp/subirna/TFL_project/gold/gold_passengers_by_year';

CREATE EXTERNAL TABLE subirna_tfl.gold_passengers_by_line (
  line_name        STRING,
  total_passengers BIGINT
)
STORED AS PARQUET
LOCATION '/tmp/subirna/TFL_project/gold/gold_passengers_by_line';

CREATE EXTERNAL TABLE subirna_tfl.gold_passengers_by_network (
  network_name     STRING,
  network_type     STRING,
  total_passengers BIGINT
)
STORED AS PARQUET
LOCATION '/tmp/subirna/TFL_project/gold/gold_passengers_by_network';

CREATE EXTERNAL TABLE subirna_tfl.gold_interchange_stations (
  station_name STRING,
  num_lines    BIGINT
)
STORED AS PARQUET
LOCATION '/tmp/subirna/TFL_project/gold/gold_interchange_stations';

CREATE EXTERNAL TABLE subirna_tfl.gold_quarterly_trend (
  year             INT,
  quarter          INT,
  total_passengers BIGINT
)
STORED AS PARQUET
LOCATION '/tmp/subirna/TFL_project/gold/gold_quarterly_trend';

CREATE EXTERNAL TABLE subirna_tfl.gold_night_tube_analysis (
  has_night_tube           BOOLEAN,
  num_records              BIGINT,
  total_passengers         BIGINT,
  avg_passengers_per_record DOUBLE
)
STORED AS PARQUET
LOCATION '/tmp/subirna/TFL_project/gold/gold_night_tube_analysis';
