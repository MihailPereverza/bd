#!/bin/bash

echo "Loading CSV data into ecom_offers..."
docker exec -i clickhouse clickhouse-client --query="INSERT INTO ecom.ecom_offers FORMAT CSVWithNames" < data/10ozon.csv

echo "Loading Parquet data into raw_events..."
docker exec -i clickhouse clickhouse-client --query="INSERT INTO ecom.raw_events FORMAT Parquet" < data/part-00000-fe025c1f-5ca7-4f31-8143-5b648fcc9879-c000.snappy.parquet

echo "Done."
