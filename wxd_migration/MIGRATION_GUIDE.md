# Archive Data Migration Guide

## Understanding the ARCHIVE Job Type

### What is an ARCHIVE Job?

The **ARCHIVE** job type in InfoSphere Data Privacy (formerly Optim Data Privacy) is designed for:

1. **Data Extraction**: Extract data from production databases
2. **Data Protection**: Apply encryption, masking, and subsetting
3. **Long-term Storage**: Store data in archive format for:
   - Regulatory compliance (GDPR, HIPAA, SOX)
   - Test data management
   - Historical data retention
   - Database decommissioning

### Key Components from master.xml

#### Global Parameters
```xml
<GLOBAL_PARAM>
  <KEEP_DATA>1</KEEP_DATA>              <!-- Retain data after archiving -->
  <COLUMN_SEPARATOR>@#@</COLUMN_SEPARATOR>  <!-- Default delimiter -->
  <ROW_SEPARATOR>\n</ROW_SEPARATOR>     <!-- Line terminator -->
  <CRYPTO_KEY>12DF</CRYPTO_KEY>         <!-- Encryption key -->
</GLOBAL_PARAM>
```

#### Table-Specific Configuration
Each table can override global settings:
- Custom delimiters (column/row separators)
- Null indicators
- File paths for source data
- SCT (Security Configuration Tool) paths for encryption metadata

### Migration Strategy

The migration transforms:
- **From**: File-based archives with custom formats
- **To**: Watsonx.data lakehouse with Parquet/Iceberg

## Step-by-Step Migration Process

### Phase 1: Pre-Migration Assessment

#### 1.1 Inventory Source Data
```bash
# List all source files referenced in master.xml
# DB1.SCHEMA1.TAB1: C:\IDV\DATA\source*.csv
# DB1.SCHEMA1.TAB2: C:\DATA\XML\data*.txt
# DB2.SCHEMA2.TAB1: C:\BACK\file*.bcp
```

#### 1.2 Estimate Data Volume
- Count total rows across all files
- Calculate storage requirements
- Plan S3 bucket capacity

#### 1.3 Review Data Types
- Verify all data types are supported
- Check for special types (WVARCHAR, WCHAR)
- Validate precision/scale for DECIMAL columns

### Phase 2: Environment Setup

#### 2.1 Configure Watsonx.data Access
```bash
# Set environment variables
export WXD_HOST="your-watsonx-host.cloud.ibm.com"
export WXD_PORT="8443"
export WXD_API_KEY="your-api-key-here"
```

#### 2.2 Configure S3 Storage
```bash
export S3_BUCKET="archive-data-bucket"
export AWS_ACCESS_KEY_ID="your-access-key"
export AWS_SECRET_ACCESS_KEY="your-secret-key"
export AWS_REGION="us-east-1"
```

#### 2.3 Install Dependencies
```bash
cd wxd_migration
pip install -r scripts/requirements.txt
```

### Phase 3: Data Preparation

#### 3.1 Stage Source Files
```bash
# Upload source files to S3 staging area
aws s3 cp C:/IDV/DATA/ s3://${S3_BUCKET}/staging/db1_schema1_tab1/ --recursive
aws s3 cp C:/DATA/XML/ s3://${S3_BUCKET}/staging/db1_schema1_tab2/ --recursive
aws s3 cp C:/BACK/ s3://${S3_BUCKET}/staging/db2_schema2_tab1/ --recursive
```

#### 3.2 Convert File Formats (if needed)
```python
# Example: Convert BCP to CSV
import pandas as pd

# Read BCP file with custom delimiter
df = pd.read_csv('file.bcp', sep='|', lineterminator='|\n')

# Write as Parquet
df.to_parquet('file.parquet', compression='snappy')
```

### Phase 4: Schema Creation

#### 4.1 Generate DDL Scripts
```bash
cd wxd_migration
./scripts/run_migration.sh
```

This generates:
- `sql/ddl/00_create_all_tables.sql` - Master DDL
- `sql/ddl/{table}_create.sql` - Individual table DDL
- `sql/ddl/{table}_load.sql` - Data loading templates

#### 4.2 Review Generated DDL
```sql
-- Example: db1_schema1_tab1_create.sql
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
  location = 's3://archive-data-bucket/archive_data/db1_schema1_tab1/'
);
```

#### 4.3 Execute DDL in Watsonx.data
```bash
# Using watsonx.data CLI or SQL editor
presto-cli --server ${WXD_HOST}:${WXD_PORT} \
  --catalog iceberg_data \
  --schema archive_data \
  --file sql/ddl/00_create_all_tables.sql
```

### Phase 5: Data Loading

#### 5.1 Create External Tables for Staging
```sql
-- Create external table pointing to staged CSV files
CREATE EXTERNAL TABLE iceberg_data.staging.db1_schema1_tab1_ext (
  COL11 DECIMAL(11,4),
  COL12 VARCHAR(310),
  COL13 INTEGER,
  COL14 REAL,
  COL15 TIMESTAMP,
  COL16 DOUBLE,
  COL17 DATE
)
WITH (
  format = 'CSV',
  external_location = 's3://archive-data-bucket/staging/db1_schema1_tab1/',
  field_delimiter = '@#',
  skip_header_line_count = 0
);
```

#### 5.2 Load Data into Iceberg Tables
```sql
-- Insert data from external table to Iceberg table
INSERT INTO iceberg_data.archive_data.db1_schema1_tab1
SELECT * FROM iceberg_data.staging.db1_schema1_tab1_ext;
```

#### 5.3 Verify Data Loading
```sql
-- Check row counts
SELECT COUNT(*) FROM iceberg_data.archive_data.db1_schema1_tab1;

-- Sample data
SELECT * FROM iceberg_data.archive_data.db1_schema1_tab1 LIMIT 10;

-- Check for nulls
SELECT 
  COUNT(*) as total_rows,
  COUNT(COL11) as col11_non_null,
  COUNT(COL12) as col12_non_null
FROM iceberg_data.archive_data.db1_schema1_tab1;
```

### Phase 6: Security Configuration

#### 6.1 Implement Encryption
```sql
-- Enable encryption at rest (configured at catalog level)
-- Watsonx.data uses S3 bucket encryption by default

-- For column-level encryption, use masking views
CREATE VIEW iceberg_data.archive_data.db1_schema1_tab1_masked AS
SELECT 
  COL11,
  MASK(COL12) as COL12,  -- Mask sensitive column
  COL13,
  COL14,
  COL15,
  COL16,
  COL17
FROM iceberg_data.archive_data.db1_schema1_tab1;
```

#### 6.2 Configure Access Control
```sql
-- Grant read access to specific users/roles
GRANT SELECT ON iceberg_data.archive_data.db1_schema1_tab1 
TO ROLE data_analyst;

-- Restrict write access
GRANT INSERT, UPDATE, DELETE ON iceberg_data.archive_data.db1_schema1_tab1 
TO ROLE data_engineer;
```

### Phase 7: Validation

#### 7.1 Data Quality Checks
```sql
-- Check for data type consistency
SELECT 
  typeof(COL11) as col11_type,
  typeof(COL12) as col12_type,
  typeof(COL15) as col15_type
FROM iceberg_data.archive_data.db1_schema1_tab1
LIMIT 1;

-- Validate date ranges
SELECT 
  MIN(COL17) as earliest_date,
  MAX(COL17) as latest_date
FROM iceberg_data.archive_data.db1_schema1_tab1;

-- Check for duplicates (if applicable)
SELECT COL11, COUNT(*) as cnt
FROM iceberg_data.archive_data.db1_schema1_tab1
GROUP BY COL11
HAVING COUNT(*) > 1;
```

#### 7.2 Performance Testing
```sql
-- Test query performance
EXPLAIN ANALYZE
SELECT * FROM iceberg_data.archive_data.db1_schema1_tab1
WHERE COL17 >= DATE '2024-01-01';

-- Check table statistics
SHOW STATS FOR iceberg_data.archive_data.db1_schema1_tab1;
```

### Phase 8: Documentation

#### 8.1 Update Asset Catalog
- Document table purposes and business context
- Record data lineage (source → archive → watsonx.data)
- Add data quality rules and validation criteria

#### 8.2 Create Runbooks
- Data refresh procedures
- Backup and recovery processes
- Troubleshooting guides

## Data Type Mapping Details

### Numeric Types
| Source | Target | Notes |
|--------|--------|-------|
| DECIMAL(11,4) | DECIMAL(11,4) | Exact precision preserved |
| INT | INTEGER | 32-bit signed integer |
| SMALLINT | SMALLINT | 16-bit signed integer |
| FLOAT | REAL | 32-bit floating point |
| DOUBLE | DOUBLE | 64-bit floating point |

### String Types
| Source | Target | Notes |
|--------|--------|-------|
| VARCHAR(310) | VARCHAR(310) | Variable length, max 310 chars |
| WVARCHAR(100) | VARCHAR(100) | Wide char converted to UTF-8 |
| CHAR(300) | CHAR(300) | Fixed length |
| WCHAR(300) | CHAR(300) | Wide char converted to UTF-8 |

### Temporal Types
| Source | Target | Notes |
|--------|--------|-------|
| TIMESTAMP | TIMESTAMP | Microsecond precision |
| DATE | DATE | Date only, no time |
| TIME | TIME | Time only, no date |

## Troubleshooting

### Common Issues

#### Issue: Character Encoding Errors
**Symptom**: Garbled text in VARCHAR/CHAR columns
**Solution**: 
```python
# Convert files to UTF-8 before loading
import codecs
with codecs.open('input.txt', 'r', 'windows-1252') as f:
    content = f.read()
with codecs.open('output.txt', 'w', 'utf-8') as f:
    f.write(content)
```

#### Issue: Delimiter Conflicts
**Symptom**: Columns split incorrectly
**Solution**: Verify delimiter in source files matches configuration
```bash
# Check actual delimiter in file
head -1 source.csv | od -c
```

#### Issue: Null Value Handling
**Symptom**: String "NULL" instead of actual NULL
**Solution**: Configure null indicator in external table definition
```sql
WITH (
  null_format = 'NULL'  -- or 'null' depending on source
)
```

#### Issue: Performance Degradation
**Symptom**: Slow queries on large tables
**Solution**: 
```sql
-- Add partitioning
CREATE TABLE iceberg_data.archive_data.db1_schema1_tab1 (
  ...
)
PARTITIONED BY (year(COL17));

-- Optimize file layout
OPTIMIZE iceberg_data.archive_data.db1_schema1_tab1;
```

## Best Practices

### 1. Incremental Loading
```sql
-- Use time-based partitioning for incremental loads
INSERT INTO iceberg_data.archive_data.db1_schema1_tab1
SELECT * FROM staging_table
WHERE load_date = CURRENT_DATE;
```

### 2. Data Retention
```sql
-- Implement retention policy using Iceberg snapshots
-- Keep 90 days of history
CALL iceberg_data.system.expire_snapshots(
  'archive_data.db1_schema1_tab1',
  TIMESTAMP '2024-01-01 00:00:00'
);
```

### 3. Monitoring
- Set up alerts for failed data loads
- Monitor storage growth
- Track query performance metrics

### 4. Backup Strategy
- Enable S3 versioning for data files
- Regular Iceberg metadata backups
- Document recovery procedures

## Next Steps After Migration

1. **Decommission Legacy Archive**: Once validated, retire old archive system
2. **Automate Refresh**: Set up scheduled jobs for ongoing data archiving
3. **Optimize Performance**: Add indexes, partitioning as needed
4. **Enable Analytics**: Connect BI tools to watsonx.data
5. **Train Users**: Provide documentation and training on new system

## Support Resources

- **Watsonx.data Documentation**: https://www.ibm.com/docs/en/watsonx/watsonxdata
- **Iceberg Documentation**: https://iceberg.apache.org/docs/latest/
- **Migration Scripts**: See `wxd_migration/scripts/` directory
- **Sample Queries**: See `wxd_migration/sql/` directory

---

**Document Version**: 1.0  
**Last Updated**: 2026-02-19  
**Migration Status**: Ready for Execution