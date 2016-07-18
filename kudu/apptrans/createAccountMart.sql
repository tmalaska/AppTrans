CREATE EXTERNAL TABLE account_mart_kudu (
  account_id STRING,
  app_id STRING,
  sign_on_count BIGINT,
  win_count BIGINT,
  lose_count BIGINT,
  purchase_total DOUBLE,
  payment_credit_total DOUBLE,
  payment_credit_count BIGINT,
  payment_debit_total DOUBLE,
  payment_debit_count BIGINT,
  payment_paypal_total DOUBLE,
  payment_paypal_count BIGINT
)
DISTRIBUTE BY HASH (account_id) INTO 3 BUCKETS
TBLPROPERTIES(
  'storage_handler' = 'com.cloudera.kudu.hive.KuduStorageHandler',
  'kudu.table_name' = 'account_mart_kudu',
  'kudu.master_addresses' = 'ted-training-1-2.vpc.cloudera.com:7051',
  'kudu.key_columns' = 'account_id,app_id'
);