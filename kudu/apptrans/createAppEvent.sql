CREATE EXTERNAL TABLE app_event_kudu (
  account_id STRING,
  app_id STRING,
  event_timestamp BIGINT,
  event_id STRING,
  event_type STRING,
  purchase DOUBLE,
  payment_type STRING,
  session_id STRING,
  latitude DOUBLE,
  longitude DOUBLE
)
DISTRIBUTE BY HASH (account_id) INTO 3 BUCKETS
TBLPROPERTIES(
  'storage_handler' = 'com.cloudera.kudu.hive.KuduStorageHandler',
  'kudu.table_name' = 'app_event_kudu',
  'kudu.master_addresses' = 'ted-training-1-2.vpc.cloudera.com:7051',
  'kudu.key_columns' = 'account_id,app_id,event_timestamp,event_id,event_type'
);