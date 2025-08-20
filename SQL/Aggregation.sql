
-- ---------- 0) Database ----------
CREATE DATABASE IF NOT EXISTS aml;

-- Use it (optional in Athena console; harmless elsewhere)
-- USE aml;

-- ---------- 1) SILVER LAYER ----------
-- External over your Spark-curated Parquet with yr/mo/day partitions.
DROP TABLE IF EXISTS aml_silver_transactions;

CREATE EXTERNAL TABLE aml_silver_transactions (
  timestamp           timestamp,
  from_bank           string,
  from_account        string,
  to_bank             string,
  to_account          string,
  amount_received     double,
  receiving_currency  string,
  amount_paid         double,
  payment_currency    string,
  payment_format      string,
  is_laundering       int
)
PARTITIONED BY (yr int, mo int, day int)
STORED AS PARQUET
LOCATION 's3://ibmrawaml/Spark_curated_day/';

-- ---- Partition handling for Silver (Projection; preferred) ----
ALTER TABLE aml_silver_transactions SET TBLPROPERTIES (
  'projection.enabled'='true',
  'projection.yr.type'='integer',  'projection.yr.range'='2022,2035',
  'projection.mo.type'='integer',  'projection.mo.range'='1,12',
  'projection.day.type'='integer', 'projection.day.range'='1,31',
  'storage.location.template'='s3://ibmrawaml/Spark_curated_day/yr=${yr}/mo=${mo}/day=${day}/'
);

-- MSCK REPAIR TABLE aml_silver_transactions;

------------- 2) GOLD LAYER -------------

-- 2.1 Daily KPIs
DROP TABLE IF EXISTS aml_gold_daily;

CREATE TABLE aml_gold_daily
WITH (
  format            = 'PARQUET',
  write_compression = 'SNAPPY',
  external_location = 's3://ibmrawaml/gold/daily/',
  partitioned_by    = ARRAY['yr','mo','day']
) AS
SELECT
  txn_ct,
  total_amt,
  avg_amt,
  big_tx_ct,
  CAST(100.0 * big_tx_ct / NULLIF(txn_ct,0) AS DECIMAL(10,2)) AS pct_big_tx,
  label_ct,
  CAST(100.0 * label_ct / NULLIF(txn_ct,0)  AS DECIMAL(10,2)) AS pct_label,
  yr, mo, day
FROM (
  SELECT
    yr, mo, day,
    COUNT(*)                                   AS txn_ct,
    SUM(amount_received)                       AS total_amt,
    AVG(amount_received)                       AS avg_amt,
    SUM(CASE WHEN amount_received >= 10000 THEN 1 ELSE 0 END) AS big_tx_ct,
    SUM(is_laundering)                         AS label_ct
  FROM aml_silver_transactions
  GROUP BY yr, mo, day
) t;

ALTER TABLE aml_gold_daily SET TBLPROPERTIES (
  'projection.enabled'='true',
  'projection.yr.type'='integer',  'projection.yr.range'='2022,2035',
  'projection.mo.type'='integer',  'projection.mo.range'='1,12',
  'projection.day.type'='integer', 'projection.day.range'='1,31',
  'storage.location.template'='s3://ibmrawaml/gold/daily/yr=${yr}/mo=${mo}/day=${day}/'
);

-- 2.2 Fan-Out Index (per sender)
DROP TABLE IF EXISTS aml_gold_fanout_idx;

CREATE TABLE aml_gold_fanout_idx
WITH (
  format            = 'PARQUET',
  write_compression = 'SNAPPY',
  external_location = 's3://ibmrawaml/gold/fanout/',
  partitioned_by    = ARRAY['yr','mo','day']
) AS
SELECT
  from_account,
  COUNT(DISTINCT to_account) AS fanout_deg,
  SUM(amount_received)       AS sum_amt,
  MAX(amount_received)       AS max_amt,
  yr, mo, day
FROM aml_silver_transactions
GROUP BY yr, mo, day, from_account;

ALTER TABLE aml_gold_fanout_idx SET TBLPROPERTIES (
  'projection.enabled'='true',
  'projection.yr.type'='integer',  'projection.yr.range'='2022,2035',
  'projection.mo.type'='integer',  'projection.mo.range'='1,12',
  'projection.day.type'='integer', 'projection.day.range'='1,31',
  'storage.location.template'='s3://ibmrawaml/gold/fanout/yr=${yr}/mo=${mo}/day=${day}/'
);

-- 2.3 FX Spreads (cross-currency)
DROP TABLE IF EXISTS aml_gold_fx;

CREATE TABLE aml_gold_fx
WITH (
  format            = 'PARQUET',
  write_compression = 'SNAPPY',
  external_location = 's3://ibmrawaml/gold/fx/',
  partitioned_by    = ARRAY['yr','mo','day']
) AS
SELECT
  CONCAT(receiving_currency,'→',payment_currency) AS fx_pair,
  COUNT(*)                                        AS tx_ct,
  SUM(amount_received)                            AS amt_recv,
  SUM(amount_paid)                                AS amt_paid,
  SUM(amount_received - amount_paid)              AS spread_abs,
  CAST(
    100.0 * SUM(amount_received - amount_paid) / NULLIF(SUM(amount_paid),0)
    AS DECIMAL(10,4)
  )                                               AS spread_pct,
  yr, mo, day
FROM aml_silver_transactions
WHERE receiving_currency <> payment_currency
GROUP BY yr, mo, day, CONCAT(receiving_currency,'→',payment_currency);

ALTER TABLE aml_gold_fx SET TBLPROPERTIES (
  'projection.enabled'='true',
  'projection.yr.type'='integer',  'projection.yr.range'='2022,2035',
  'projection.mo.type'='integer',  'projection.mo.range'='1,12',
  'projection.day.type'='integer', 'projection.day.range'='1,31',
  'storage.location.template'='s3://ibmrawaml/gold/fx/yr=${yr}/mo=${mo}/day=${day}/'
);

-- 2.4 Hourly Drilldown
DROP TABLE IF EXISTS aml_gold_hourly;

CREATE TABLE aml_gold_hourly
WITH (
  format            = 'PARQUET',
  write_compression = 'SNAPPY',
  external_location = 's3://ibmrawaml/gold/hourly/',
  partitioned_by    = ARRAY['yr','mo','day']
) AS
SELECT
  hour(timestamp)      AS hr,
  COUNT(*)             AS tx_ct,
  SUM(amount_received) AS amt,
  yr, mo, day
FROM aml_silver_transactions
GROUP BY yr, mo, day, hour(timestamp);

ALTER TABLE aml_gold_hourly SET TBLPROPERTIES (
  'projection.enabled'='true',
  'projection.yr.type'='integer',  'projection.yr.range'='2022,2035',
  'projection.mo.type'='integer',  'projection.mo.range'='1,12',
  'projection.day.type'='integer', 'projection.day.range'='1,31',
  'storage.location.template'='s3://ibmrawaml/gold/hourly/yr=${yr}/mo=${mo}/day=${day}/'
);

-- 2.5 Interbank Corridors
DROP TABLE IF EXISTS aml_gold_corridors;

CREATE TABLE aml_gold_corridors
WITH (
  format            = 'PARQUET',
  write_compression = 'SNAPPY',
  external_location = 's3://ibmrawaml/gold/corridors/',
  partitioned_by    = ARRAY['yr','mo','day']
) AS
SELECT
  CONCAT(from_bank,'→',to_bank) AS corridor,
  COUNT(*)                      AS tx_ct,
  SUM(amount_received)          AS amt,
  yr, mo, day
FROM aml_silver_transactions
GROUP BY yr, mo, day, CONCAT(from_bank,'→',to_bank);

ALTER TABLE aml_gold_corridors SET TBLPROPERTIES (
  'projection.enabled'='true',
  'projection.yr.type'='integer',  'projection.yr.range'='2022,2035',
  'projection.mo.type'='integer',  'projection.mo.range'='1,12',
  'projection.day.type'='integer', 'projection.day.range'='1,31',
  'storage.location.template'='s3://ibmrawaml/gold/corridors/yr=${yr}/mo=${mo}/day=${day}/'
);

-- ---------- 3) REPORTING VIEWS  ----------

-- FX split into sender/receiver currency columns
CREATE OR REPLACE VIEW aml_gold_fx_expanded AS
SELECT
  fx_pair,
  split_part(fx_pair,'→',1) AS receiving_currency,
  split_part(fx_pair,'→',2) AS payment_currency,
  tx_ct, amt_recv, amt_paid, spread_abs, spread_pct,
  yr, mo, day
FROM aml_gold_fx;

-- Corridor split into from_bank / to_bank
CREATE OR REPLACE VIEW aml_gold_corridors_expanded AS
SELECT
  corridor,
  split_part(corridor,'→',1) AS from_bank,
  split_part(corridor,'→',2) AS to_bank,
  tx_ct, amt, yr, mo, day
FROM aml_gold_corridors;

