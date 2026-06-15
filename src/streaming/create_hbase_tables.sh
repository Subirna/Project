#!/bin/bash
# Creates the two HBase tables for the TFL streaming pipeline.
# Run as a file to avoid SSH quoting issues with HBase shell syntax.

echo "=== Creating HBase tables ==="

echo "create 'subirna_tfl_arrivals', 'cf'" | hbase shell -n 2>&1 | grep -v "AlreadyExistsException" | tail -3
echo "create 'subirna_tfl_arrivals_agg', 'cf'" | hbase shell -n 2>&1 | grep -v "AlreadyExistsException" | tail -3

echo "=== HBase tables ready ==="
