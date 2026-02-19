"""
Archive Flow Orchestrator for Watsonx.data
Implements repeatable data archival workflow from master.xml sources
"""

import json
import logging
import os
import sys
from pathlib import Path
from typing import Dict, List, Any, Optional
from datetime import datetime
import yaml

# Third-party imports
try:
    import boto3
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq
    from ibm_watsonx_data_integration import WatsonxDataIntegrationV1
    from ibm_cloud_sdk_core.authenticators import IAMAuthenticator
except ImportError as e:
    print(f"Error: Missing required package. Run: pip install -r requirements.txt")
    print(f"Details: {e}")
    sys.exit(1)

from xml_parser import MasterXMLParser

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ArchiveFlowOrchestrator:
    """
    Orchestrates the complete archive flow from source files to watsonx.data
    """
    
    def __init__(self, config_path: str, xml_path: str):
        """
        Initialize the archive flow orchestrator
        
        Args:
            config_path: Path to wxd_config.yaml
            xml_path: Path to master.xml
        """
        self.config = self._load_config(config_path)
        self.xml_parser = MasterXMLParser(xml_path)
        self.wxd_client = None
        self.s3_client = None
        
        # Extract configuration
        self.wxd_config = self.config.get('watsonx_data', {})
        self.storage_config = self.config.get('storage', {})
        self.global_params = self.config.get('global_params', {})
        
        # Initialize clients
        self._initialize_clients()
        
    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Load and expand environment variables in config"""
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        return self._expand_env_vars(config)
    
    def _expand_env_vars(self, obj: Any) -> Any:
        """Recursively expand environment variables"""
        if isinstance(obj, dict):
            return {k: self._expand_env_vars(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._expand_env_vars(item) for item in obj]
        elif isinstance(obj, str) and obj.startswith('${') and obj.endswith('}'):
            env_var = obj[2:-1]
            return os.getenv(env_var, obj)
        return obj
    
    def _initialize_clients(self):
        """Initialize watsonx.data and S3 clients"""
        try:
            # Initialize watsonx.data integration client
            logger.info("Initializing watsonx.data integration client...")
            api_key = self.wxd_config.get('auth', {}).get('api_key')
            
            if not api_key:
                raise ValueError("WXD_API_KEY not set in environment or config")
            
            authenticator = IAMAuthenticator(api_key)
            
            # Build service URL
            host = self.wxd_config.get('host')
            port = self.wxd_config.get('port', '443')
            service_url = f"https://{host}:{port}"
            
            self.wxd_client = WatsonxDataIntegrationV1(
                authenticator=authenticator
            )
            self.wxd_client.set_service_url(service_url)
            
            # Initialize S3 client
            logger.info("Initializing S3 client...")
            self.s3_client = boto3.client('s3')
            
            logger.info("Clients initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize clients: {e}")
            raise
    
    def stage_source_file(self, source_path: str, asset_id: str) -> str:
        """
        Stage source file to S3 for processing
        
        Args:
            source_path: Local path to source file
            asset_id: Unique identifier for the asset
            
        Returns:
            S3 URI of staged file
        """
        bucket = self.storage_config.get('bucket')
        prefix = self.storage_config.get('path_prefix', 'archive_data')
        
        # Generate S3 key
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = Path(source_path).name
        s3_key = f"{prefix}/staging/{asset_id}/{timestamp}/{filename}"
        
        try:
            logger.info(f"Staging {source_path} to s3://{bucket}/{s3_key}")
            self.s3_client.upload_file(source_path, bucket, s3_key)
            s3_uri = f"s3://{bucket}/{s3_key}"
            logger.info(f"File staged successfully: {s3_uri}")
            return s3_uri
        except Exception as e:
            logger.error(f"Failed to stage file: {e}")
            raise
    
    def convert_to_parquet(self, source_file: str, asset: Dict[str, Any], 
                          output_path: str) -> str:
        """
        Convert source file to Parquet format
        
        Args:
            source_file: Path to source file
            asset: Asset definition with column metadata
            output_path: Path for output Parquet file
            
        Returns:
            Path to generated Parquet file
        """
        source = asset['source']
        col_sep = source.get('column_separator', ',')
        row_sep = source.get('row_separator', '\\n').replace('\\n', '\n')
        null_indicator = source.get('null_indicator', 'NULL')
        
        try:
            logger.info(f"Converting {source_file} to Parquet...")
            
            # Read source file based on format
            if source['format'] in ['csv', 'delimited']:
                df = pd.read_csv(
                    source_file,
                    sep=col_sep,
                    lineterminator=row_sep,
                    na_values=[null_indicator],
                    names=[col['name'] for col in asset['columns']],
                    dtype=self._get_pandas_dtypes(asset['columns'])
                )
            else:
                # For other formats, read as delimited
                df = pd.read_csv(
                    source_file,
                    sep=col_sep,
                    lineterminator=row_sep,
                    na_values=[null_indicator],
                    names=[col['name'] for col in asset['columns']]
                )
            
            # Convert to Parquet
            output_file = Path(output_path)
            output_file.parent.mkdir(parents=True, exist_ok=True)
            
            df.to_parquet(
                output_file,
                engine='pyarrow',
                compression=self.storage_config.get('compression', 'snappy'),
                index=False
            )
            
            logger.info(f"Parquet file created: {output_file} ({len(df)} rows)")
            return str(output_file)
            
        except Exception as e:
            logger.error(f"Failed to convert to Parquet: {e}")
            raise
    
    def _get_pandas_dtypes(self, columns: List[Dict[str, Any]]) -> Dict[str, str]:
        """Map column types to pandas dtypes"""
        dtype_map = {}
        for col in columns:
            col_type = col['type'].upper()
            if col_type in ['INT', 'INTEGER', 'SMALLINT']:
                dtype_map[col['name']] = 'Int64'
            elif col_type in ['FLOAT', 'DOUBLE', 'REAL']:
                dtype_map[col['name']] = 'float64'
            elif col_type == 'DECIMAL':
                dtype_map[col['name']] = 'float64'
            else:
                dtype_map[col['name']] = 'object'
        return dtype_map
    
    def create_table_if_not_exists(self, asset: Dict[str, Any]) -> bool:
        """
        Create table in watsonx.data if it doesn't exist
        
        Args:
            asset: Asset definition
            
        Returns:
            True if table was created or already exists
        """
        catalog = asset['target']['catalog']
        schema = asset['target']['schema']
        table = asset['target']['table']
        
        try:
            # Check if schema exists, create if not
            logger.info(f"Ensuring schema {catalog}.{schema} exists...")
            create_schema_sql = f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}"
            
            # Execute SQL using the integration SDK
            response = self.wxd_client.execute_sql_query(
                engine_id=self.wxd_config.get('engine_id', 'presto-01'),
                sql=create_schema_sql
            )
            
            # Generate CREATE TABLE DDL
            columns_ddl = []
            for col in asset['columns']:
                nullable = '' if col['nullable'] else ' NOT NULL'
                columns_ddl.append(f"  {col['name']} {col['wxd_type']}{nullable}")
            
            bucket = self.storage_config.get('bucket')
            prefix = self.storage_config.get('path_prefix', 'archive_data')
            location = f"s3://{bucket}/{prefix}/{table}/"
            columns_joined = ',\n'.join(columns_ddl)
            
            create_table_sql = f"""CREATE TABLE IF NOT EXISTS {catalog}.{schema}.{table} (
{columns_joined}
)
WITH (
  format = '{asset['target']['format']}',
  location = '{location}'
)"""
            
            logger.info(f"Creating table {catalog}.{schema}.{table}...")
            response = self.wxd_client.execute_sql_query(
                engine_id=self.wxd_config.get('engine_id', 'presto-01'),
                sql=create_table_sql
            )
            logger.info(f"Table {catalog}.{schema}.{table} ready")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create table: {e}")
            raise
    
    def load_data_to_table(self, parquet_file: str, asset: Dict[str, Any]) -> int:
        """
        Load Parquet data into watsonx.data table
        
        Args:
            parquet_file: Path to Parquet file
            asset: Asset definition
            
        Returns:
            Number of rows loaded
        """
        catalog = asset['target']['catalog']
        schema = asset['target']['schema']
        table = asset['target']['table']
        bucket = self.storage_config.get('bucket')
        prefix = self.storage_config.get('path_prefix', 'archive_data')
        
        try:
            # Upload Parquet to S3 table location
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            s3_key = f"{prefix}/{table}/data_{timestamp}.parquet"
            
            logger.info(f"Uploading Parquet to s3://{bucket}/{s3_key}...")
            self.s3_client.upload_file(parquet_file, bucket, s3_key)
            
            # Refresh table metadata to pick up new files
            refresh_sql = f"CALL system.sync_partition_metadata('{catalog}', '{schema}', '{table}')"
            response = self.wxd_client.execute_sql_query(
                engine_id=self.wxd_config.get('engine_id', 'presto-01'),
                sql=refresh_sql
            )
            
            # Get row count
            count_sql = f"SELECT COUNT(*) as cnt FROM {catalog}.{schema}.{table}"
            response = self.wxd_client.execute_sql_query(
                engine_id=self.wxd_config.get('engine_id', 'presto-01'),
                sql=count_sql
            )
            
            # Extract row count from response
            row_count = 0
            if response and hasattr(response, 'result'):
                result_data = response.result
                if result_data and 'rows' in result_data and len(result_data['rows']) > 0:
                    row_count = result_data['rows'][0][0]
            
            logger.info(f"Data loaded successfully. Total rows: {row_count}")
            return row_count
            
        except Exception as e:
            logger.error(f"Failed to load data: {e}")
            raise
    
    def archive_asset(self, asset: Dict[str, Any], source_file: str) -> Dict[str, Any]:
        """
        Archive a single data asset through the complete flow
        
        Args:
            asset: Asset definition
            source_file: Path to source data file
            
        Returns:
            Archive result summary
        """
        asset_id = asset['asset_id']
        start_time = datetime.now()
        
        logger.info(f"\n{'='*60}")
        logger.info(f"Starting archive flow for: {asset_id}")
        logger.info(f"{'='*60}")
        
        try:
            # Step 1: Convert to Parquet
            temp_dir = Path('temp_parquet')
            temp_dir.mkdir(exist_ok=True)
            parquet_file = temp_dir / f"{asset_id}.parquet"
            
            self.convert_to_parquet(source_file, asset, str(parquet_file))
            
            # Step 2: Create table if not exists
            self.create_table_if_not_exists(asset)
            
            # Step 3: Load data
            row_count = self.load_data_to_table(str(parquet_file), asset)
            
            # Step 4: Cleanup temp file
            parquet_file.unlink()
            
            duration = (datetime.now() - start_time).total_seconds()
            
            result = {
                'asset_id': asset_id,
                'status': 'success',
                'row_count': row_count,
                'duration_seconds': duration,
                'target_table': f"{asset['target']['catalog']}.{asset['target']['schema']}.{asset['target']['table']}",
                'timestamp': datetime.now().isoformat()
            }
            
            logger.info(f"Archive completed successfully for {asset_id}")
            logger.info(f"Duration: {duration:.2f}s, Rows: {row_count}")
            
            return result
            
        except Exception as e:
            logger.error(f"Archive failed for {asset_id}: {e}")
            return {
                'asset_id': asset_id,
                'status': 'failed',
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
    
    def run_archive_flow(self, source_files_map: Dict[str, str]) -> Dict[str, Any]:
        """
        Run complete archive flow for all assets
        
        Args:
            source_files_map: Mapping of asset_id to source file path
            
        Returns:
            Summary of archive results
        """
        logger.info("\n" + "="*60)
        logger.info("ARCHIVE FLOW ORCHESTRATOR")
        logger.info("="*60)
        
        # Parse all tables from XML
        assets = self.xml_parser.parse_all_tables()
        
        results = []
        successful = 0
        failed = 0
        
        for asset in assets:
            asset_id = asset['asset_id']
            
            # Get source file for this asset
            source_file = source_files_map.get(asset_id)
            if not source_file:
                logger.warning(f"No source file provided for {asset_id}, skipping...")
                results.append({
                    'asset_id': asset_id,
                    'status': 'skipped',
                    'reason': 'No source file provided'
                })
                continue
            
            if not Path(source_file).exists():
                logger.warning(f"Source file not found: {source_file}, skipping...")
                results.append({
                    'asset_id': asset_id,
                    'status': 'skipped',
                    'reason': f'File not found: {source_file}'
                })
                continue
            
            # Archive the asset
            result = self.archive_asset(asset, source_file)
            results.append(result)
            
            if result['status'] == 'success':
                successful += 1
            else:
                failed += 1
        
        # Generate summary
        summary = {
            'total_assets': len(assets),
            'successful': successful,
            'failed': failed,
            'skipped': len(assets) - successful - failed,
            'results': results,
            'timestamp': datetime.now().isoformat()
        }
        
        logger.info("\n" + "="*60)
        logger.info("ARCHIVE FLOW SUMMARY")
        logger.info("="*60)
        logger.info(f"Total Assets: {summary['total_assets']}")
        logger.info(f"Successful: {summary['successful']}")
        logger.info(f"Failed: {summary['failed']}")
        logger.info(f"Skipped: {summary['skipped']}")
        
        return summary


def main():
    """Main execution function"""
    if len(sys.argv) < 3:
        print("Usage: python archive_flow.py <config_yaml> <master_xml> [source_files_json]")
        print("\nExample:")
        print("  python archive_flow.py config/wxd_config.yaml ../master.xml source_files.json")
        print("\nSource files JSON format:")
        print('  {')
        print('    "db1_schema1_tab1": "/path/to/source1.csv",')
        print('    "db1_schema1_tab2": "/path/to/source2.txt",')
        print('    "db2_schema2_tab1": "/path/to/source3.bcp"')
        print('  }')
        sys.exit(1)
    
    config_path = sys.argv[1]
    xml_path = sys.argv[2]
    source_files_json = sys.argv[3] if len(sys.argv) > 3 else None
    
    # Load source files mapping
    source_files_map = {}
    if source_files_json and Path(source_files_json).exists():
        with open(source_files_json, 'r') as f:
            source_files_map = json.load(f)
    else:
        logger.warning("No source files mapping provided. Use --help for format.")
    
    # Initialize orchestrator
    orchestrator = ArchiveFlowOrchestrator(config_path, xml_path)
    
    # Run archive flow
    summary = orchestrator.run_archive_flow(source_files_map)
    
    # Save summary
    summary_file = Path('archive_summary.json')
    with open(summary_file, 'w') as f:
        json.dump(summary, f, indent=2)
    
    logger.info(f"\nSummary saved to: {summary_file}")
    
    # Exit with appropriate code
    sys.exit(0 if summary['failed'] == 0 else 1)


if __name__ == '__main__':
    main()

# Made with Bob
