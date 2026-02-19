#!/bin/bash
# Watsonx.data Migration Execution Script
# This script orchestrates the complete migration process

set -e  # Exit on error

echo "=========================================="
echo "Watsonx.data Archive Migration"
echo "=========================================="
echo ""

# Configuration
MASTER_XML="../master.xml"
CONFIG_YAML="config/wxd_config.yaml"
TABLE_DEFS="data_assets/table_definitions.json"
DDL_OUTPUT="sql/ddl"

# Check prerequisites
echo "Step 1: Checking prerequisites..."
if [ ! -f "$MASTER_XML" ]; then
    echo "ERROR: master.xml not found at $MASTER_XML"
    exit 1
fi

if [ ! -f "$CONFIG_YAML" ]; then
    echo "ERROR: Configuration file not found at $CONFIG_YAML"
    exit 1
fi

# Check Python
if ! command -v python3 &> /dev/null; then
    echo "ERROR: Python 3 is required but not installed"
    exit 1
fi

echo "✓ Prerequisites check passed"
echo ""

# Install dependencies
echo "Step 2: Installing Python dependencies..."
pip install -q -r scripts/requirements.txt
echo "✓ Dependencies installed"
echo ""

# Parse master.xml
echo "Step 3: Parsing master.xml..."
python3 scripts/xml_parser.py "$MASTER_XML" "$TABLE_DEFS"
echo "✓ XML parsed successfully"
echo ""

# Generate integration artifacts
echo "Step 4: Generating watsonx.data integration artifacts..."
python3 scripts/wxd_integration.py "$CONFIG_YAML" "$TABLE_DEFS"
echo "✓ Integration artifacts generated"
echo ""

# Summary
echo "=========================================="
echo "Migration Preparation Complete!"
echo "=========================================="
echo ""
echo "Generated artifacts:"
echo "  - Table definitions: $TABLE_DEFS"
echo "  - DDL scripts: $DDL_OUTPUT/"
echo "  - Asset manifest: data_assets/asset_manifest.json"
echo ""
echo "Next steps:"
echo "  1. Review generated DDL scripts in $DDL_OUTPUT/"
echo "  2. Configure environment variables (WXD_HOST, WXD_API_KEY, S3_BUCKET)"
echo "  3. Stage source data files in S3"
echo "  4. Execute DDL scripts in watsonx.data"
echo "  5. Run data loading scripts"
echo ""
echo "For detailed instructions, see README.md"

# Made with Bob
