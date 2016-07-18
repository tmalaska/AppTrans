CREATE EXTERNAL TABLE app_event (
  account_id STRING,
  app_id STRING,
  event_id STRING,
  event_type STRING,
  event_timestamp TIMESTAMP,
  purchase DOUBLE,
  payment_type STRING,
  session_id STRING,
  latitude DOUBLE,
  longitude DOUBLE
)
STORED AS PARQUET
LOCATION 'usr/root/hive/app_event';