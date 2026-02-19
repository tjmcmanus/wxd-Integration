-- Create table for TAB2 from DB1.SCHEMA1
CREATE TABLE IF NOT EXISTS iceberg_data.archive_data.db1_schema1_tab2 (
  COL21 VARCHAR(100),
  COL22 VARCHAR(310),
  COL23 INTEGER
)
WITH (
  format = 'parquet',
  location = 's3://${S3_BUCKET}/archive_data/db1_schema1_tab2/'
);