-- Load data for TAB2
-- Source: C:/DATA/XML/data*.txt
-- Note: Files must be staged in S3 or accessible location first

-- Example using external table approach:
-- CREATE EXTERNAL TABLE temp_db1_schema1_tab2_staging (
--   ... columns ...
-- )
-- WITH (
--   format = 'delimited',
--   external_location = 's3://staging-bucket/path/',
--   field_delimiter = '|',
--   line_delimiter = '|\n'
-- );

-- INSERT INTO iceberg_data.archive_data.db1_schema1_tab2
-- SELECT * FROM temp_db1_schema1_tab2_staging;

-- DROP TABLE temp_db1_schema1_tab2_staging;
