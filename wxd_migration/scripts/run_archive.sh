#!/bin/bash
# End-to-end Archive Flow Runner
# Executes the complete archive workflow from master.xml to watsonx.data

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}Archive Flow Runner${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

# Check required environment variables
echo -e "${YELLOW}Checking environment variables...${NC}"
REQUIRED_VARS=("WXD_HOST" "WXD_PORT" "WXD_ENGINE_ID" "WXD_API_KEY" "S3_BUCKET")
MISSING_VARS=()

for var in "${REQUIRED_VARS[@]}"; do
    if [ -z "${!var}" ]; then
        MISSING_VARS+=("$var")
    fi
done

if [ ${#MISSING_VARS[@]} -ne 0 ]; then
    echo -e "${RED}Error: Missing required environment variables:${NC}"
    for var in "${MISSING_VARS[@]}"; do
        echo -e "${RED}  - $var${NC}"
    done
    echo ""
    echo "Please set these variables before running:"
    echo "  export WXD_HOST='your-watsonx-data-host'"
    echo "  export WXD_PORT='443'"
    echo "  export WXD_ENGINE_ID='presto-01'"
    echo "  export WXD_API_KEY='your-api-key'"
    echo "  export S3_BUCKET='your-s3-bucket'"
    exit 1
fi

echo -e "${GREEN}✓ All required environment variables set${NC}"
echo ""

# Check if Python dependencies are installed
echo -e "${YELLOW}Checking Python dependencies...${NC}"
if ! python3 -c "import yaml, boto3, pandas, pyarrow" 2>/dev/null; then
    echo -e "${YELLOW}Installing Python dependencies...${NC}"
    pip install -r "$SCRIPT_DIR/requirements.txt"
    echo -e "${GREEN}✓ Dependencies installed${NC}"
else
    echo -e "${GREEN}✓ Dependencies already installed${NC}"
fi
echo ""

# Set default paths
MASTER_XML="${MASTER_XML:-$PROJECT_ROOT/../master.xml}"
CONFIG_YAML="${CONFIG_YAML:-$PROJECT_ROOT/config/wxd_config.yaml}"
SOURCE_FILES_JSON="${SOURCE_FILES_JSON:-$PROJECT_ROOT/source_files.json}"

# Check if master.xml exists
if [ ! -f "$MASTER_XML" ]; then
    echo -e "${RED}Error: master.xml not found at: $MASTER_XML${NC}"
    echo "Set MASTER_XML environment variable to specify location"
    exit 1
fi

echo -e "${GREEN}✓ Found master.xml: $MASTER_XML${NC}"
echo ""

# Step 1: Parse master.xml
echo -e "${BLUE}Step 1: Parsing master.xml...${NC}"
TABLE_DEFS="$PROJECT_ROOT/data_assets/table_definitions.json"
python3 "$SCRIPT_DIR/xml_parser.py" "$MASTER_XML" "$TABLE_DEFS"

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ XML parsed successfully${NC}"
else
    echo -e "${RED}✗ XML parsing failed${NC}"
    exit 1
fi
echo ""

# Step 2: Generate DDL scripts
echo -e "${BLUE}Step 2: Generating DDL scripts...${NC}"
python3 "$SCRIPT_DIR/wxd_integration.py" "$CONFIG_YAML" "$TABLE_DEFS"

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ DDL scripts generated${NC}"
else
    echo -e "${RED}✗ DDL generation failed${NC}"
    exit 1
fi
echo ""

# Step 3: Check for source files mapping
if [ ! -f "$SOURCE_FILES_JSON" ]; then
    echo -e "${YELLOW}Warning: Source files mapping not found: $SOURCE_FILES_JSON${NC}"
    echo ""
    echo "To run the archive flow, create a source_files.json with:"
    echo '{'
    echo '  "db1_schema1_tab1": "/path/to/source1.csv",'
    echo '  "db1_schema1_tab2": "/path/to/source2.txt",'
    echo '  "db2_schema2_tab1": "/path/to/source3.bcp"'
    echo '}'
    echo ""
    echo -e "${YELLOW}Skipping archive flow execution...${NC}"
    echo ""
    echo -e "${GREEN}========================================${NC}"
    echo -e "${GREEN}Preparation Complete!${NC}"
    echo -e "${GREEN}========================================${NC}"
    echo ""
    echo "Next steps:"
    echo "1. Create source_files.json with your data file paths"
    echo "2. Run: ./run_archive.sh"
    echo "   or: python3 scripts/archive_flow.py config/wxd_config.yaml ../master.xml source_files.json"
    exit 0
fi

# Step 4: Run archive flow
echo -e "${BLUE}Step 3: Running archive flow...${NC}"
python3 "$SCRIPT_DIR/archive_flow.py" "$CONFIG_YAML" "$MASTER_XML" "$SOURCE_FILES_JSON"

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ Archive flow completed successfully${NC}"
else
    echo -e "${RED}✗ Archive flow failed${NC}"
    exit 1
fi
echo ""

# Summary
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}Archive Flow Complete!${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo "Generated artifacts:"
echo "  - Table definitions: $TABLE_DEFS"
echo "  - DDL scripts: $PROJECT_ROOT/sql/ddl/"
echo "  - Archive summary: $PROJECT_ROOT/archive_summary.json"
echo ""
echo "Check watsonx.data for archived tables in:"
echo "  Catalog: iceberg_data"
echo "  Schema: archive_data"

# Made with Bob
