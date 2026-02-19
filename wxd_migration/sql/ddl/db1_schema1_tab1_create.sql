-- Create table for TAB1 from DB1.SCHEMA1
CREATE TABLE IF NOT EXISTS iceberg_data.archive_data.db1_schema1_tab1 (
  COL11 DECIMAL(11,4),
  COL12 VARCHAR(310),
  COL13 INTEGER,
  COL14 REAL,
  COL15 TIMESTAMP,
  COL16 DOUBLE,
  COL17 DATE
)
WITH (
  format = 'parquet',
  location = 's3://${S3_BUCKET}/archive_data/db1_schema1_tab1/'
);