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

CREATE TABLE IF NOT EXISTS subirna_tfl.gold_busiest_stations AS
SELECT d.year, s.station_name, SUM(f.total_entry_exit) AS total
FROM fact_passenger_entry_exit f
JOIN dim_date d     ON f.date_id = d.date_id
JOIN dim_stations s ON f.station_id = s.station_id
GROUP BY d.year, s.station_name;
