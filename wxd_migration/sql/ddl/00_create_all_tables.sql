-- Master DDL for Archive Data Migration
-- Generated from master.xml
-- Total tables: 3

-- Create catalog and schema
CREATE SCHEMA IF NOT EXISTS iceberg_data.archive_data;

-- Create all tables

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
