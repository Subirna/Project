#!/bin/bash
set -e

sqoop import \
  -D mapreduce.framework.name=local \
  --connect 'jdbc:postgresql://13.42.152.118:5432/testdb' \
  --username admin --password admin123 \
  --table dim_date \
  --target-dir /tmp/subirna/TFL_project/dim_date \
  --num-mappers 1 \
  --fields-terminated-by ',' \
  --delete-target-dir

sqoop import \
  -D mapreduce.framework.name=local \
  --connect 'jdbc:postgresql://13.42.152.118:5432/testdb' \
  --username admin --password admin123 \
  --table dim_lines \
  --target-dir /tmp/subirna/TFL_project/dim_lines \
  --num-mappers 1 \
  --fields-terminated-by ',' \
  --delete-target-dir

sqoop import \
  -D mapreduce.framework.name=local \
  --connect 'jdbc:postgresql://13.42.152.118:5432/testdb' \
  --username admin --password admin123 \
  --table dim_networks \
  --target-dir /tmp/subirna/TFL_project/dim_networks \
  --num-mappers 1 \
  --fields-terminated-by ',' \
  --delete-target-dir

sqoop import \
  -D mapreduce.framework.name=local \
  --connect 'jdbc:postgresql://13.42.152.118:5432/testdb' \
  --username admin --password admin123 \
  --table dim_stations \
  --target-dir /tmp/subirna/TFL_project/dim_stations \
  --num-mappers 1 \
  --fields-terminated-by ',' \
  --delete-target-dir

sqoop import \
  -D mapreduce.framework.name=local \
  --connect 'jdbc:postgresql://13.42.152.118:5432/testdb' \
  --username admin --password admin123 \
  --table fact_passenger_entry_exit \
  --target-dir /tmp/subirna/TFL_project/fact_passenger_entry_exit \
  --num-mappers 1 \
  --fields-terminated-by ',' \
  --delete-target-dir

sqoop import \
  -D mapreduce.framework.name=local \
  --connect 'jdbc:postgresql://13.42.152.118:5432/testdb' \
  --username admin --password admin123 \
  --table fact_station_lines \
  --target-dir /tmp/subirna/TFL_project/fact_station_lines \
  --num-mappers 1 \
  --fields-terminated-by ',' \
  --delete-target-dir

echo "Verifying import..."
hdfs dfs -cat /tmp/subirna/TFL_project/fact_station_lines/part-m-00000 | head -3
