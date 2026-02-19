-- Load data for TAB1
-- Source: C:/IDV/DATA/source*.csv
-- Note: Files must be staged in S3 or accessible location first

-- Example using external table approach:
-- CREATE EXTERNAL TABLE temp_db1_schema1_tab1_staging (
--   ... columns ...
-- )
-- WITH (
--   format = 'csv',
--   external_location = 's3://staging-bucket/path/',
--   field_delimiter = '@#',
--   line_delimiter = '\n'
-- );

-- INSERT INTO iceberg_data.archive_data.db1_schema1_tab1
-- SELECT * FROM temp_db1_schema1_tab1_staging;

-- DROP TABLE temp_db1_schema1_tab1_staging;
