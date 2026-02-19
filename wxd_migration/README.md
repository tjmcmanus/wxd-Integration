# Watsonx.data Integration - Archive Data Migration

This project migrates InfoSphere Data Privacy (formerly Optim Data Privacy) ARCHIVE job definitions from `master.xml` to watsonx.data as data assets.

## Overview

### Source System: InfoSphere Data Privacy
- **Job Type**: ARCHIVE
- **Purpose**: Data archiving with encryption and masking capabilities
- **Source Format**: XML configuration defining tables, columns, and file sources

### Target System: Watsonx.data
- **Platform**: IBM watsonx.data (Presto/Iceberg-based lakehouse)
- **Storage**: S3-compatible object storage with Parquet format
- **Catalog**: Iceberg catalog for ACID transactions and schema evolution

## Project Structure

```
wxd_migration/
├── config/
│   └── wxd_config.yaml           # Watsonx.data connection and storage config
├── data_assets/
│   ├── table_definitions.json    # Parsed table definitions from master.xml
│   └── asset_manifest.json       # Generated manifest of all data assets
├── scripts/
│   ├── xml_parser.py             # Parse master.xml and extract table definitions
│   ├── wxd_integration.py        # Generate DDL and integration artifacts
│   └── requirements.txt          # Python dependencies
├── sql/
│   └── ddl/                      # Generated DDL scripts (created during execution)
└── README.md                     # This file
```

## Data Assets

The migration includes **3 tables** from the ARCHIVE job:

### 1. DB1.SCHEMA1.TAB1
- **Columns**: 7 (COL11-COL17)
- **Types**: DECIMAL, VARCHAR, INT, FLOAT, TIMESTAMP, DOUBLE, DATE
- **Source**: CSV files (`C:\IDV\DATA\source*.csv`)
- **Separator**: `@#`
- **Target**: `iceberg_data.archive_data.db1_schema1_tab1`

### 2. DB1.SCHEMA1.TAB2
- **Columns**: 3 (COL21-COL23)
- **Types**: WVARCHAR, VARCHAR, INT
- **Source**: Text files (`C:\DATA\XML\data*.txt`)
- **Separator**: `|`
- **Target**: `iceberg_data.archive_data.db1_schema1_tab2`

### 3. DB2.SCHEMA2.TAB1
- **Columns**: 3 (COL31-COL33)
- **Types**: INT, WCHAR, SMALLINT
- **Source**: BCP files (`C:\BACK\file*.bcp`)
- **Separator**: `|`
- **Target**: `iceberg_data.archive_data.db2_schema2_tab1`

## Understanding the JOB_TYPE: ARCHIVE

The **ARCHIVE** job type in InfoSphere Data Privacy indicates:

1. **Data Extraction**: Extract data from source databases or files
2. **Data Transformation**: Apply masking, encryption, or subsetting rules
3. **Data Storage**: Store in archive format for:
   - Long-term retention
   - Compliance requirements
   - Test data management
   - Data privacy protection

### Key Features from master.xml:
- **Encryption**: Uses crypto key (12DF) for data protection
- **Flexible Delimiters**: Custom column/row separators per table
- **Null Handling**: Configurable null indicators
- **Keep Data Flag**: Controls whether to retain data after archiving
- **SCT Paths**: Security Configuration Tool paths for encryption metadata

## Setup Instructions

### Prerequisites
- Python 3.8+
- Access to watsonx.data instance
- S3-compatible storage bucket
- Source data files accessible

### 1. Install Dependencies
```bash
cd wxd_migration/scripts
pip install -r requirements.txt
```

### 2. Configure Environment Variables
Set the following environment variables:
```bash
export WXD_HOST="your-watsonx-data-host"
export WXD_PORT="8443"
export WXD_API_KEY="your-api-key"
export S3_BUCKET="your-s3-bucket"
```

### 3. Parse master.xml
```bash
python scripts/xml_parser.py ../master.xml data_assets/table_definitions.json
```

This will:
- Parse the XML structure
- Extract table and column definitions
- Map data types to watsonx.data compatible types
- Generate JSON definitions

### 4. Generate Integration Artifacts
```bash
python scripts/wxd_integration.py config/wxd_config.yaml data_assets/table_definitions.json
```

This will generate:
- DDL scripts for creating tables in watsonx.data
- Data loading templates
- Asset manifest for tracking

## Generated Artifacts

### DDL Scripts (`sql/ddl/`)
- `00_create_all_tables.sql` - Master DDL for all tables
- `{asset_id}_create.sql` - Individual table creation scripts
- `{asset_id}_load.sql` - Data loading templates

### Data Asset Manifest
- `data_assets/asset_manifest.json` - Complete inventory of migrated assets

## Data Type Mappings

| Source Type | Watsonx.data Type | Notes |
|-------------|-------------------|-------|
| DECIMAL(p,s) | DECIMAL(p,s) | Direct mapping |
| VARCHAR(n) | VARCHAR(n) | Direct mapping |
| WVARCHAR(n) | VARCHAR(n) | Wide char to standard |
| CHAR(n) | CHAR(n) | Direct mapping |
| WCHAR(n) | CHAR(n) | Wide char to standard |
| INT | INTEGER | Direct mapping |
| SMALLINT | SMALLINT | Direct mapping |
| BIGINT | BIGINT | Direct mapping |
| FLOAT | REAL | Mapped to REAL |
| DOUBLE | DOUBLE | Direct mapping |
| TIMESTAMP | TIMESTAMP | Direct mapping |
| DATE | DATE | Direct mapping |

## Migration Workflow

### Phase 1: Assessment (Completed)
- ✅ Analyze master.xml structure
- ✅ Identify tables and columns
- ✅ Map data types
- ✅ Document source file locations

### Phase 2: Configuration (Completed)
- ✅ Create watsonx.data configuration
- ✅ Define storage settings
- ✅ Set up encryption parameters

### Phase 3: Code Generation (Completed)
- ✅ Parse XML to JSON
- ✅ Generate DDL scripts
- ✅ Create data loading templates
- ✅ Build asset manifest

### Phase 4: Deployment (Manual)
1. **Stage Source Files**: Upload source files to S3 staging area
2. **Execute DDL**: Run generated DDL scripts in watsonx.data
3. **Load Data**: Execute data loading scripts
4. **Validate**: Verify row counts and data quality
5. **Document**: Update asset catalog with metadata

### Phase 5: Validation (Manual)
- Verify table creation
- Validate data loading
- Check encryption settings
- Confirm row counts match source

## Security Considerations

### Encryption
- Original crypto key (12DF) should be migrated to secure vault
- Use watsonx.data encryption at rest
- Enable S3 bucket encryption
- Implement column-level encryption for sensitive data

### Access Control
- Configure watsonx.data RBAC
- Restrict S3 bucket access
- Use API keys with minimal permissions
- Audit access logs regularly

## Troubleshooting

### Common Issues

**Issue**: Type conversion errors
- **Solution**: Review data type mappings in `xml_parser.py`
- Check precision/scale values for DECIMAL types

**Issue**: File path not found
- **Solution**: Update file paths in source configuration
- Ensure files are staged in accessible location

**Issue**: Connection timeout
- **Solution**: Verify watsonx.data host and port
- Check network connectivity and firewall rules

## Next Steps

1. **Review Generated DDL**: Examine `sql/ddl/` scripts before execution
2. **Stage Source Data**: Upload files to S3 staging area
3. **Test with Sample**: Run DDL and load for one table first
4. **Full Migration**: Execute all scripts after validation
5. **Monitor Performance**: Track load times and optimize as needed

## Support

For questions or issues:
- Review watsonx.data documentation
- Check IBM support portal
- Consult with data engineering team

## References

- [IBM watsonx.data Documentation](https://www.ibm.com/docs/en/watsonx/watsonxdata)
- [Apache Iceberg Documentation](https://iceberg.apache.org/)
- [InfoSphere Data Privacy Documentation](https://www.ibm.com/docs/en/iodp)

---

**Migration Date**: 2026-02-19  
**Source**: master.xml (ARCHIVE job)  
**Target**: watsonx.data (Iceberg catalog)  
**Status**: Configuration Complete - Ready for Deployment