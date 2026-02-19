# Archive Flow Guide - Watsonx.data Integration

Complete guide for using the repeatable archive flow to migrate data from master.xml sources to watsonx.data.

## Overview

The archive flow provides an automated, repeatable process to:
1. Parse table definitions from `master.xml`
2. Convert source data files to Parquet format
3. Create tables in watsonx.data (Iceberg catalog)
4. Load data into the lakehouse
5. Track and validate the migration

## Architecture

```
┌─────────────┐
│ master.xml  │
└──────┬──────┘
       │
       ▼
┌─────────────────┐      ┌──────────────────┐
│  xml_parser.py  │─────▶│ table_definitions│
└─────────────────┘      │     .json        │
                         └──────────────────┘
                                  │
                                  ▼
                         ┌──────────────────┐
                         │ archive_flow.py  │
                         └────────┬─────────┘
                                  │
                    ┌─────────────┼─────────────┐
                    ▼             ▼             ▼
              ┌──────────┐  ┌──────────┐  ┌──────────┐
              │ Convert  │  │  Create  │  │   Load   │
              │   to     │─▶│  Tables  │─▶│   Data   │
              │ Parquet  │  │  in WXD  │  │  to WXD  │
              └──────────┘  └──────────┘  └──────────┘
                                                │
                                                ▼
                                    ┌────────────────────┐
                                    │ Watsonx.data       │
                                    │ (Iceberg Catalog)  │
                                    └────────────────────┘
```

## Prerequisites

### 1. Environment Setup

Set required environment variables:

```bash
export WXD_HOST="your-watsonx-data-host.cloud.ibm.com"
export WXD_PORT="443"
export WXD_ENGINE_ID="presto-01"  # Your Presto or Spark engine ID
export WXD_API_KEY="your-api-key-here"
export S3_BUCKET="your-s3-bucket-name"
```

### 2. Install Dependencies

```bash
cd wxd_migration/scripts
pip install -r requirements.txt
```

This installs:
- `PyYAML` - Configuration file parsing
- `ibm-watsonx-data-integration` - Watsonx.data Integration Python SDK
- `boto3` - AWS S3 client
- `pandas` - Data manipulation
- `pyarrow` - Parquet file handling

### 3. Prepare Source Files

Create a `source_files.json` mapping asset IDs to source file paths:

```json
{
  "db1_schema1_tab1": "/data/sources/source1.csv",
  "db1_schema1_tab2": "/data/sources/data1.txt",
  "db2_schema2_tab1": "/data/sources/file1.bcp"
}
```

**Asset ID Format**: `{database}_{schema}_{table}` (all lowercase)

## Quick Start

### Option 1: Using the Shell Script (Recommended)

```bash
cd wxd_migration
./scripts/run_archive.sh
```

This automatically:
- Validates environment variables
- Installs dependencies
- Parses master.xml
- Generates DDL scripts
- Runs the archive flow
- Produces a summary report

### Option 2: Manual Step-by-Step

#### Step 1: Parse master.xml

```bash
python3 scripts/xml_parser.py ../master.xml data_assets/table_definitions.json
```

**Output**: `data_assets/table_definitions.json` with parsed table definitions

#### Step 2: Generate DDL Scripts

```bash
python3 scripts/wxd_integration.py config/wxd_config.yaml data_assets/table_definitions.json
```

**Output**: 
- `sql/ddl/00_create_all_tables.sql` - Master DDL
- `sql/ddl/{asset_id}_create.sql` - Individual table DDL
- `data_assets/asset_manifest.json` - Asset inventory

#### Step 3: Run Archive Flow

```bash
python3 scripts/archive_flow.py \
  config/wxd_config.yaml \
  ../master.xml \
  source_files.json
```

**Output**: `archive_summary.json` with migration results

## Archive Flow Details

### Data Conversion Process

The [`archive_flow.py`](scripts/archive_flow.py) script performs these operations for each table:

1. **Read Source File**
   - Respects column/row separators from master.xml
   - Handles null indicators
   - Supports CSV, delimited text, and BCP formats

2. **Convert to Parquet**
   - Maps data types to Parquet schema
   - Applies Snappy compression
   - Validates data integrity

3. **Create Table in Watsonx.data**
   - Generates Iceberg table with proper schema
   - Sets S3 location for data files
   - Configures Parquet format

4. **Load Data**
   - Uploads Parquet to S3 table location
   - Refreshes table metadata
   - Validates row counts

### Configuration

Edit [`config/wxd_config.yaml`](config/wxd_config.yaml) to customize:

```yaml
watsonx_data:
  catalog: "iceberg_data"      # Target catalog
  schema: "archive_data"       # Target schema

storage:
  bucket: "${S3_BUCKET}"       # S3 bucket
  path_prefix: "archive_data"  # Path prefix in bucket
  format: "parquet"            # File format
  compression: "snappy"        # Compression algorithm
```

## Source File Formats

### CSV Files (TAB1)
```
COL11@#COL12@#COL13@#COL14@#COL15@#COL16@#COL17
123.45@#Sample Text@#100@#3.14@#2024-01-01 10:00:00@#2.718@#2024-01-01
```

**Configuration**:
- Column separator: `@#`
- Row separator: `\n`
- Null indicator: `null`

### Delimited Text Files (TAB2)
```
Value1|Value2|123
Value3|Value4|456
```

**Configuration**:
- Column separator: `|`
- Row separator: `|\n`
- Null indicator: `NULL`

### BCP Files (TAB1 in DB2)
```
100|Sample Data|50
200|More Data|75
```

**Configuration**:
- Column separator: `|`
- Row separator: `|\n`
- Null indicator: `NULL`

## Data Type Mappings

The archive flow automatically maps source types to watsonx.data types:

| Source Type | Watsonx.data Type | Notes |
|-------------|-------------------|-------|
| DECIMAL(p,s) | DECIMAL(p,s) | Preserves precision and scale |
| VARCHAR(n) | VARCHAR(n) | Direct mapping |
| WVARCHAR(n) | VARCHAR(n) | Wide char converted |
| INT | INTEGER | Standard integer |
| SMALLINT | SMALLINT | 16-bit integer |
| FLOAT | REAL | Single precision |
| DOUBLE | DOUBLE | Double precision |
| TIMESTAMP | TIMESTAMP | Date and time |
| DATE | DATE | Date only |

## Output Artifacts

### 1. Table Definitions JSON
**Location**: `data_assets/table_definitions.json`

Contains parsed table metadata:
```json
{
  "data_assets": [
    {
      "asset_id": "db1_schema1_tab1",
      "name": "TAB1",
      "database": "DB1",
      "schema": "SCHEMA1",
      "columns": [...],
      "source": {...},
      "target": {...}
    }
  ]
}
```

### 2. DDL Scripts
**Location**: `sql/ddl/`

- `00_create_all_tables.sql` - Complete DDL for all tables
- `{asset_id}_create.sql` - Individual table creation
- `{asset_id}_load.sql` - Data loading templates

### 3. Archive Summary
**Location**: `archive_summary.json`

Migration results:
```json
{
  "total_assets": 3,
  "successful": 3,
  "failed": 0,
  "skipped": 0,
  "results": [
    {
      "asset_id": "db1_schema1_tab1",
      "status": "success",
      "row_count": 1000,
      "duration_seconds": 12.5,
      "target_table": "iceberg_data.archive_data.db1_schema1_tab1"
    }
  ]
}
```

## Validation

### Verify Tables Created

```sql
-- Connect to watsonx.data
SHOW TABLES FROM iceberg_data.archive_data;
```

Expected output:
```
db1_schema1_tab1
db1_schema1_tab2
db2_schema2_tab1
```

### Check Row Counts

```sql
SELECT COUNT(*) FROM iceberg_data.archive_data.db1_schema1_tab1;
SELECT COUNT(*) FROM iceberg_data.archive_data.db1_schema1_tab2;
SELECT COUNT(*) FROM iceberg_data.archive_data.db2_schema2_tab1;
```

### Verify Data Quality

```sql
-- Sample data from first table
SELECT * FROM iceberg_data.archive_data.db1_schema1_tab1 LIMIT 10;

-- Check for nulls
SELECT 
  COUNT(*) as total_rows,
  COUNT(COL11) as col11_non_null,
  COUNT(COL12) as col12_non_null
FROM iceberg_data.archive_data.db1_schema1_tab1;
```

## Troubleshooting

### Issue: Import Errors

**Error**: `ImportError: No module named 'ibm_watsonx_data'`

**Solution**:
```bash
pip install -r wxd_migration/scripts/requirements.txt
```

### Issue: Connection Timeout

**Error**: `Connection timeout to watsonx.data`

**Solution**:
1. Verify `WXD_HOST` and `WXD_PORT` are correct
2. Check network connectivity
3. Validate API key has proper permissions

### Issue: S3 Access Denied

**Error**: `Access Denied when uploading to S3`

**Solution**:
1. Verify S3 bucket exists
2. Check AWS credentials are configured
3. Ensure bucket policy allows writes

### Issue: Type Conversion Error

**Error**: `Cannot convert type X to Y`

**Solution**:
1. Review data type mappings in [`xml_parser.py`](scripts/xml_parser.py)
2. Check source data format matches expected type
3. Adjust precision/scale if needed

### Issue: File Not Found

**Error**: `Source file not found: /path/to/file.csv`

**Solution**:
1. Verify file paths in `source_files.json`
2. Ensure files are accessible from execution environment
3. Check file permissions

## Advanced Usage

### Custom Configuration

Create a custom config file:

```yaml
# custom_config.yaml
watsonx_data:
  host: "custom-host.com"
  catalog: "my_catalog"
  schema: "my_schema"

storage:
  bucket: "my-bucket"
  path_prefix: "custom/path"
```

Run with custom config:
```bash
python3 scripts/archive_flow.py custom_config.yaml ../master.xml source_files.json
```

### Selective Archive

Archive specific tables only:

```json
{
  "db1_schema1_tab1": "/data/source1.csv"
}
```

Other tables will be skipped automatically.

### Dry Run Mode

Generate DDL without loading data:

```bash
# Step 1 & 2 only
python3 scripts/xml_parser.py ../master.xml data_assets/table_definitions.json
python3 scripts/wxd_integration.py config/wxd_config.yaml data_assets/table_definitions.json

# Review generated DDL in sql/ddl/
cat sql/ddl/00_create_all_tables.sql
```

## Performance Optimization

### Large Files

For files > 1GB:
1. Use chunked reading in Pandas
2. Increase memory allocation
3. Consider parallel processing

### Multiple Tables

Process tables in parallel:
```python
from concurrent.futures import ThreadPoolExecutor

with ThreadPoolExecutor(max_workers=3) as executor:
    futures = [executor.submit(orchestrator.archive_asset, asset, file) 
               for asset, file in assets.items()]
```

### Network Optimization

- Use S3 Transfer Acceleration
- Enable multipart uploads for large files
- Compress data before transfer

## Security Best Practices

### 1. Credential Management

**Never hardcode credentials**. Use environment variables or secret managers:

```bash
# Use AWS Secrets Manager
export WXD_API_KEY=$(aws secretsmanager get-secret-value \
  --secret-id wxd-api-key --query SecretString --output text)
```

### 2. Encryption

Enable encryption at rest:
```yaml
encryption:
  enabled: true
  algorithm: "AES-256"
  key_source: "vault"
```

### 3. Access Control

- Use least-privilege IAM roles
- Enable S3 bucket versioning
- Configure watsonx.data RBAC

### 4. Audit Logging

Track all operations:
```python
logger.info(f"User {user} archived {asset_id} at {timestamp}")
```

## Monitoring

### Track Progress

Monitor `archive_summary.json` for:
- Success/failure rates
- Processing times
- Row counts

### Set Up Alerts

```bash
# Example: Alert on failures
if [ $(jq '.failed' archive_summary.json) -gt 0 ]; then
  echo "Archive failures detected!" | mail -s "Alert" admin@example.com
fi
```

## Next Steps

1. **Review Generated Artifacts**: Check DDL scripts and table definitions
2. **Validate Data**: Run sample queries in watsonx.data
3. **Schedule Regular Runs**: Set up cron jobs for periodic archival
4. **Monitor Performance**: Track execution times and optimize
5. **Document Custom Changes**: Update this guide with project-specific details

## Support

For issues or questions:
- Review logs in console output
- Check `archive_summary.json` for error details
- Consult [IBM watsonx.data documentation](https://www.ibm.com/docs/en/watsonx/watsonxdata)

## References

- [IBM Watsonx.data Integration SDK](https://cloud.ibm.com/apidocs/watsonx-data)
- [Apache Iceberg Documentation](https://iceberg.apache.org/)
- [Parquet Format Specification](https://parquet.apache.org/docs/)
- [AWS S3 Best Practices](https://docs.aws.amazon.com/AmazonS3/latest/userguide/best-practices.html)

---

**Last Updated**: 2026-02-19  
**Version**: 1.0.0  
**Status**: Production Ready