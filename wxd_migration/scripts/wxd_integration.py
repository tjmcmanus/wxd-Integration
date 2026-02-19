"""
Watsonx.data Integration Script
Loads data assets into watsonx.data from parsed table definitions
"""

import json
import yaml
import os
from pathlib import Path
from typing import Dict, List, Any, Optional
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class WatsonxDataIntegration:
    """Integration handler for watsonx.data"""
    
    def __init__(self, config_path: str):
        """Initialize with configuration file"""
        self.config = self._load_config(config_path)
        self.wxd_config = self.config.get('watsonx_data', {})
        self.storage_config = self.config.get('storage', {})
        
    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Load YAML configuration"""
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        
        # Expand environment variables
        return self._expand_env_vars(config)
    
    def _expand_env_vars(self, obj: Any) -> Any:
        """Recursively expand environment variables in config"""
        if isinstance(obj, dict):
            return {k: self._expand_env_vars(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._expand_env_vars(item) for item in obj]
        elif isinstance(obj, str) and obj.startswith('${') and obj.endswith('}'):
            env_var = obj[2:-1]
            return os.getenv(env_var, obj)
        return obj
    
    def load_table_definitions(self, definitions_path: str) -> List[Dict[str, Any]]:
        """Load table definitions from JSON"""
        with open(definitions_path, 'r') as f:
            data = json.load(f)
        return data.get('data_assets', [])
    
    def generate_create_table_ddl(self, asset: Dict[str, Any]) -> str:
        """Generate CREATE TABLE DDL for watsonx.data"""
        catalog = asset['target']['catalog']
        schema = asset['target']['schema']
        table = asset['target']['table']
        
        columns_ddl = []
        for col in asset['columns']:
            nullable = '' if col['nullable'] else ' NOT NULL'
            columns_ddl.append(f"  {col['name']} {col['wxd_type']}{nullable}")
        
        # Extract storage config values outside f-string
        bucket = self.storage_config.get('bucket', 'bucket')
        path_prefix = self.storage_config.get('path_prefix', 'data')
        table_format = asset['target']['format']
        columns_joined = ',\n'.join(columns_ddl)
        
        ddl = f"""-- Create table for {asset['name']} from {asset['database']}.{asset['schema']}
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.{table} (
{columns_joined}
)
WITH (
  format = '{table_format}',
  location = 's3://{bucket}/{path_prefix}/{table}/'
);"""
        
        return ddl
    
    def generate_insert_from_file_sql(self, asset: Dict[str, Any]) -> str:
        """Generate SQL to load data from source files"""
        catalog = asset['target']['catalog']
        schema = asset['target']['schema']
        table = asset['target']['table']
        
        source = asset['source']
        file_path = source['file_path'].replace('\\', '/')
        
        # Extract values outside f-string to avoid backslash issues
        source_format = source['format']
        col_sep = source['column_separator']
        row_sep = source['row_separator']
        
        # Note: This is a template - actual implementation depends on how files are staged
        sql = f"""-- Load data for {asset['name']}
-- Source: {file_path}
-- Note: Files must be staged in S3 or accessible location first

-- Example using external table approach:
-- CREATE EXTERNAL TABLE temp_{table}_staging (
--   ... columns ...
-- )
-- WITH (
--   format = '{source_format}',
--   external_location = 's3://staging-bucket/path/',
--   field_delimiter = '{col_sep}',
--   line_delimiter = '{row_sep}'
-- );

-- INSERT INTO {catalog}.{schema}.{table}
-- SELECT * FROM temp_{table}_staging;

-- DROP TABLE temp_{table}_staging;
"""
        return sql
    
    def generate_all_ddl(self, definitions_path: str, output_dir: str):
        """Generate all DDL scripts for data assets"""
        assets = self.load_table_definitions(definitions_path)
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        # Generate individual table DDL files
        for asset in assets:
            asset_id = asset['asset_id']
            
            # Create table DDL
            create_ddl = self.generate_create_table_ddl(asset)
            create_file = output_path / f"{asset_id}_create.sql"
            with open(create_file, 'w') as f:
                f.write(create_ddl)
            logger.info(f"Generated CREATE DDL: {create_file}")
            
            # Load data SQL template
            load_sql = self.generate_insert_from_file_sql(asset)
            load_file = output_path / f"{asset_id}_load.sql"
            with open(load_file, 'w') as f:
                f.write(load_sql)
            logger.info(f"Generated LOAD SQL: {load_file}")
        
        # Generate master DDL file
        master_ddl = self._generate_master_ddl(assets)
        master_file = output_path / "00_create_all_tables.sql"
        with open(master_file, 'w') as f:
            f.write(master_ddl)
        logger.info(f"Generated master DDL: {master_file}")
        
        return len(assets)
    
    def _generate_master_ddl(self, assets: List[Dict[str, Any]]) -> str:
        """Generate master DDL file with all tables"""
        catalog = self.wxd_config.get('catalog', 'iceberg_data')
        schema = self.wxd_config.get('schema', 'archive_data')
        
        ddl_parts = [
            "-- Master DDL for Archive Data Migration",
            "-- Generated from master.xml",
            f"-- Total tables: {len(assets)}",
            "",
            f"-- Create catalog and schema",
            f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema};",
            "",
            "-- Create all tables",
            ""
        ]
        
        for asset in assets:
            ddl_parts.append(self.generate_create_table_ddl(asset))
            ddl_parts.append("")
        
        return "\n".join(ddl_parts)
    
    def generate_data_asset_manifest(self, definitions_path: str, output_path: str):
        """Generate a manifest file for all data assets"""
        assets = self.load_table_definitions(definitions_path)
        
        manifest = {
            'project': self.config.get('project', {}),
            'watsonx_data': self.wxd_config,
            'storage': self.storage_config,
            'assets': []
        }
        
        for asset in assets:
            manifest['assets'].append({
                'asset_id': asset['asset_id'],
                'name': asset['name'],
                'database': asset['database'],
                'schema': asset['schema'],
                'target_table': f"{asset['target']['catalog']}.{asset['target']['schema']}.{asset['target']['table']}",
                'column_count': len(asset['columns']),
                'source_file': asset['source']['file_path'],
                'format': asset['target']['format']
            })
        
        with open(output_path, 'w') as f:
            json.dump(manifest, f, indent=2)
        
        logger.info(f"Generated manifest: {output_path}")
        return manifest


def main():
    """Main execution function"""
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python wxd_integration.py <config_yaml> [table_definitions_json]")
        print("\nExample:")
        print("  python wxd_integration.py config/wxd_config.yaml data_assets/table_definitions.json")
        sys.exit(1)
    
    config_path = sys.argv[1]
    definitions_path = sys.argv[2] if len(sys.argv) > 2 else 'data_assets/table_definitions.json'
    
    # Initialize integration
    integration = WatsonxDataIntegration(config_path)
    
    # Generate DDL scripts
    print("\n=== Generating DDL Scripts ===")
    ddl_output_dir = 'sql/ddl'
    table_count = integration.generate_all_ddl(definitions_path, ddl_output_dir)
    print(f"Generated DDL for {table_count} tables in {ddl_output_dir}/")
    
    # Generate manifest
    print("\n=== Generating Data Asset Manifest ===")
    manifest_path = 'data_assets/asset_manifest.json'
    manifest = integration.generate_data_asset_manifest(definitions_path, manifest_path)
    print(f"Generated manifest with {len(manifest['assets'])} assets")
    
    print("\n=== Summary ===")
    print(f"Project: {manifest['project'].get('name', 'N/A')}")
    print(f"Catalog: {manifest['watsonx_data'].get('catalog', 'N/A')}")
    print(f"Schema: {manifest['watsonx_data'].get('schema', 'N/A')}")
    print(f"\nData Assets:")
    for asset in manifest['assets']:
        print(f"  - {asset['target_table']} ({asset['column_count']} columns)")
    
    print("\n✓ Migration artifacts generated successfully!")


if __name__ == '__main__':
    main()

# Made with Bob
