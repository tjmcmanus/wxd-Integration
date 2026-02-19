"""
XML Parser for master.xml to watsonx.data migration
Parses InfoSphere Data Privacy ARCHIVE job XML and extracts table definitions
"""

import xml.etree.ElementTree as ET
import json
from typing import Dict, List, Any
from pathlib import Path


class MasterXMLParser:
    """Parser for master.xml ARCHIVE job definitions"""
    
    def __init__(self, xml_path: str):
        self.xml_path = xml_path
        self.tree = ET.parse(xml_path)
        self.root = self.tree.getroot()
        
    def get_job_type(self) -> str:
        """Extract job type from XML"""
        job_type = self.root.find('.//JOB_TYPE')
        return job_type.text if job_type is not None and job_type.text else "UNKNOWN"
    
    def get_global_params(self) -> Dict[str, Any]:
        """Extract global parameters"""
        global_param = self.root.find('.//GLOBAL_PARAM')
        if global_param is None:
            return {}
        
        params = {}
        for child in global_param:
            params[child.tag.lower()] = child.text
        return params
    
    def map_data_type(self, original_type: str, precision: int, scale: int) -> str:
        """Map original data types to watsonx.data compatible types"""
        type_mapping = {
            'DECIMAL': f'DECIMAL({precision},{scale})' if scale > 0 else f'DECIMAL({precision})',
            'VARCHAR': f'VARCHAR({precision})',
            'WVARCHAR': f'VARCHAR({precision})',  # Wide char to standard VARCHAR
            'CHAR': f'CHAR({precision})',
            'WCHAR': f'CHAR({precision})',  # Wide char to standard CHAR
            'INT': 'INTEGER',
            'INTEGER': 'INTEGER',
            'SMALLINT': 'SMALLINT',
            'BIGINT': 'BIGINT',
            'FLOAT': 'REAL',
            'DOUBLE': 'DOUBLE',
            'REAL': 'REAL',
            'TIMESTAMP': 'TIMESTAMP',
            'DATE': 'DATE',
            'TIME': 'TIME',
            'BOOLEAN': 'BOOLEAN',
            'BLOB': 'VARBINARY',
            'CLOB': 'VARCHAR(65535)'
        }
        
        base_type = original_type.upper()
        return type_mapping.get(base_type, 'VARCHAR(255)')
    
    def parse_table(self, table_elem: ET.Element) -> Dict[str, Any]:
        """Parse a single table definition"""
        table_name = table_elem.get('NAME')
        database = table_elem.get('DATABASE')
        schema = table_elem.get('SCHEMA')
        
        # Extract table-specific parameters
        keep_data = table_elem.find('KEEP_DATA')
        col_sep = table_elem.find('COLUMN_SEPARATOR')
        row_sep = table_elem.find('ROW_SEPARATOR')
        null_ind = table_elem.find('NULL_INDICATOR')
        file_path = table_elem.find('FILE_PATH')
        sct_path = table_elem.find('SCT_PATH')
        
        # Parse columns
        columns = []
        columns_elem = table_elem.find('COLUMNS')
        if columns_elem is not None:
            for col in columns_elem.findall('COLUMN'):
                col_name = col.get('NAME', '')
                
                type_elem = col.find('TYPE')
                col_type = type_elem.text if type_elem is not None and type_elem.text else 'VARCHAR'
                
                precision_elem = col.find('PRECISION')
                precision = int(precision_elem.text) if precision_elem is not None and precision_elem.text else 0
                
                scale_elem = col.find('SCALE')
                scale = int(scale_elem.text) if scale_elem is not None and scale_elem.text else 0
                
                nullable_elem = col.find('NULLABLE')
                nullable = nullable_elem.text == '1' if nullable_elem is not None and nullable_elem.text else True
                
                wxd_type = self.map_data_type(col_type, precision, scale)
                
                column_def = {
                    'name': col_name,
                    'type': col_type,
                    'precision': precision,
                    'scale': scale,
                    'nullable': nullable,
                    'wxd_type': wxd_type
                }
                
                # Add notes for special type mappings
                if col_type and col_type.upper() in ['WVARCHAR', 'WCHAR']:
                    column_def['notes'] = f'Wide character {col_type} mapped to standard type'
                
                columns.append(column_def)
        
        # Determine file format from extension
        file_format = 'csv'
        if file_path is not None and file_path.text:
            path_lower = file_path.text.lower()
            if path_lower.endswith('.txt'):
                file_format = 'delimited'
            elif path_lower.endswith('.bcp'):
                file_format = 'bcp'
        
        # Build table definition
        db_safe = database.lower() if database else 'unknown'
        schema_safe = schema.lower() if schema else 'unknown'
        table_safe = table_name.lower() if table_name else 'unknown'
        asset_id = f"{db_safe}_{schema_safe}_{table_safe}"
        
        table_def = {
            'asset_id': asset_id,
            'name': table_name,
            'database': database,
            'schema': schema,
            'description': f'Archive table {table_name} from {database}.{schema}',
            'source': {
                'type': 'file',
                'file_path': file_path.text if file_path is not None else '',
                'sct_path': sct_path.text if sct_path is not None else '',
                'format': file_format,
                'column_separator': col_sep.text if col_sep is not None else ',',
                'row_separator': row_sep.text if row_sep is not None else '\\n',
                'null_indicator': null_ind.text if null_ind is not None else 'NULL'
            },
            'target': {
                'catalog': 'iceberg_data',
                'schema': 'archive_data',
                'table': asset_id,
                'format': 'parquet'
            },
            'columns': columns,
            'metadata': {
                'keep_data': keep_data.text == '1' if keep_data is not None else True,
                'original_source': 'master.xml',
                'migration_date': '2026-02-19'
            }
        }
        
        return table_def
    
    def parse_all_tables(self) -> List[Dict[str, Any]]:
        """Parse all table definitions from XML"""
        tables = []
        tables_elem = self.root.find('.//TABLES')
        
        if tables_elem is not None:
            for table_elem in tables_elem.findall('TABLE'):
                table_def = self.parse_table(table_elem)
                tables.append(table_def)
        
        return tables
    
    def export_to_json(self, output_path: str):
        """Export parsed tables to JSON format"""
        tables = self.parse_all_tables()
        
        output_data = {
            'data_assets': tables
        }
        
        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, indent=2, ensure_ascii=False)
        
        return output_data
    
    def get_summary(self) -> Dict[str, Any]:
        """Get summary of parsed XML"""
        job_type = self.get_job_type()
        global_params = self.get_global_params()
        tables = self.parse_all_tables()
        
        return {
            'job_type': job_type,
            'global_params': global_params,
            'table_count': len(tables),
            'tables': [
                {
                    'asset_id': t['asset_id'],
                    'name': t['name'],
                    'database': t['database'],
                    'schema': t['schema'],
                    'column_count': len(t['columns'])
                }
                for t in tables
            ]
        }


def main():
    """Main execution function"""
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python xml_parser.py <path_to_master.xml> [output_json_path]")
        sys.exit(1)
    
    xml_path = sys.argv[1]
    output_path = sys.argv[2] if len(sys.argv) > 2 else 'data_assets/table_definitions.json'
    
    parser = MasterXMLParser(xml_path)
    
    # Print summary
    summary = parser.get_summary()
    print(f"\n=== Master XML Parser Summary ===")
    print(f"Job Type: {summary['job_type']}")
    print(f"Total Tables: {summary['table_count']}")
    print(f"\nTables:")
    for table in summary['tables']:
        print(f"  - {table['database']}.{table['schema']}.{table['name']} ({table['column_count']} columns)")
    
    # Export to JSON
    print(f"\nExporting to: {output_path}")
    parser.export_to_json(output_path)
    print("Export complete!")


if __name__ == '__main__':
    main()

# Made with Bob
