#!/bin/bash

sqoop import \
  -D mapreduce.framework.name=local \
  -D mapreduce.jobtracker.staging.root.dir=/tmp/consultant/staging \
  --connect 'jdbc:postgresql://13.42.152.118:5432/testdb' \
  --username admin --password admin123 \
  --table dim_date \
  --target-dir /tmp/subirna/TFL_project/dim_date \
  --num-mappers 1 \
  --fields-terminated-by ','

sqoop import \
  -D mapreduce.framework.name=local \
  -D mapreduce.jobtracker.staging.root.dir=/tmp/consultant/staging \
  --connect 'jdbc:postgresql://13.42.152.118:5432/testdb' \
  --username admin --password admin123 \
  --table dim_lines \
  --target-dir /tmp/subirna/TFL_project/dim_lines \
  --num-mappers 1 \
  --fields-terminated-by ','

sqoop import \
  -D mapreduce.framework.name=local \
  -D mapreduce.jobtracker.staging.root.dir=/tmp/consultant/staging \
  --connect 'jdbc:postgresql://13.42.152.118:5432/testdb' \
  --username admin --password admin123 \
  --table dim_networks \
  --target-dir /tmp/subirna/TFL_project/dim_networks \
  --num-mappers 1 \
  --fields-terminated-by ','

sqoop import \
  -D mapreduce.framework.name=local \
  -D mapreduce.jobtracker.staging.root.dir=/tmp/consultant/staging \
  --connect 'jdbc:postgresql://13.42.152.118:5432/testdb' \
  --username admin --password admin123 \
  --table dim_stations \
  --target-dir /tmp/subirna/TFL_project/dim_stations \
  --num-mappers 1 \
  --fields-terminated-by ','

sqoop import \
  -D mapreduce.framework.name=local \
  -D mapreduce.jobtracker.staging.root.dir=/tmp/consultant/staging \
  --connect 'jdbc:postgresql://13.42.152.118:5432/testdb' \
  --username admin --password admin123 \
  --table fact_passenger_entry_exit \
  --target-dir /tmp/subirna/TFL_project/fact_passenger_entry_exit \
  --num-mappers 1 \
  --fields-terminated-by ','

sqoop import \
  -D mapreduce.framework.name=local \
  -D mapreduce.jobtracker.staging.root.dir=/tmp/consultant/staging \
  --connect 'jdbc:postgresql://13.42.152.118:5432/testdb' \
  --username admin --password admin123 \
  --table fact_station_lines \
  --target-dir /tmp/subirna/TFL_project/fact_station_lines \
  --num-mappers 1 \
  --fields-terminated-by ','

echo "Verifying import..."
hdfs dfs -ls /tmp/subirna/TFL_project/
