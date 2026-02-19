-- Create table for TAB1 from DB2.SCHEMA2
CREATE TABLE IF NOT EXISTS iceberg_data.archive_data.db2_schema2_tab1 (
  COL31 INTEGER,
  COL32 CHAR(300),
  COL33 SMALLINT
)
WITH (
  format = 'parquet',
  location = 's3://${S3_BUCKET}/archive_data/db2_schema2_tab1/'
);