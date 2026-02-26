"""Inventory and manifest generation for Lakebridge extraction."""

import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional


class InventoryWriter:
    """Generates extraction inventory/manifest files."""

    def __init__(self, output_dir: str, source_database: str = "UNKNOWN"):
        """
        Initialize the inventory writer.

        Args:
            output_dir: Base output directory for extraction
            source_database: Name of the source database
        """
        self.output_dir = Path(output_dir)
        self.source_database = source_database
        self.extraction_date = datetime.now().isoformat()
        self.schemas: Dict[str, Dict[str, Any]] = {}

    def add_schema(self, schema: str) -> None:
        """
        Initialize tracking for a schema.

        Args:
            schema: Schema name to track
        """
        if schema not in self.schemas:
            self.schemas[schema] = {
                'objects_extracted': {},
                'objects_failed': {},
                'total_files': 0,
                'table_details': [],
                'procedure_details': [],
                'package_details': [],
                'source_metrics': {},
                'errors': []
            }

    def record_extraction(
        self,
        schema: str,
        object_type: str,
        object_name: str,
        success: bool,
        error_message: Optional[str] = None
    ) -> None:
        """
        Record the result of an object extraction.

        Args:
            schema: Schema name
            object_type: Type of object (TABLE, VIEW, etc.)
            object_name: Name of the object
            success: Whether extraction was successful
            error_message: Error message if extraction failed
        """
        self.add_schema(schema)
        schema_data = self.schemas[schema]

        if success:
            if object_type not in schema_data['objects_extracted']:
                schema_data['objects_extracted'][object_type] = 0
            schema_data['objects_extracted'][object_type] += 1
            schema_data['total_files'] += 1
        else:
            if object_type not in schema_data['objects_failed']:
                schema_data['objects_failed'][object_type] = 0
            schema_data['objects_failed'][object_type] += 1
            if error_message:
                schema_data['errors'].append({
                    'object_type': object_type,
                    'object_name': object_name,
                    'error': error_message
                })

    def add_table_details(self, schema: str, details: List[Dict]) -> None:
        """
        Add table detail information for a schema.

        Args:
            schema: Schema name
            details: List of table detail dictionaries
        """
        self.add_schema(schema)
        self.schemas[schema]['table_details'] = details

    def add_procedure_details(self, schema: str, details: List[Dict]) -> None:
        """
        Add procedure/function detail information for a schema.

        Args:
            schema: Schema name
            details: List of procedure detail dictionaries
        """
        self.add_schema(schema)
        self.schemas[schema]['procedure_details'] = details

    def add_package_details(self, schema: str, details: List[Dict]) -> None:
        """
        Add package detail information for a schema.

        Args:
            schema: Schema name
            details: List of package detail dictionaries
        """
        self.add_schema(schema)
        self.schemas[schema]['package_details'] = details

    def add_source_metrics(self, schema: str, metrics: Dict) -> None:
        """
        Add source code metrics for a schema.

        Args:
            schema: Schema name
            metrics: Dictionary of source code metrics
        """
        self.add_schema(schema)
        self.schemas[schema]['source_metrics'] = metrics

    def generate_summary(self) -> Dict[str, Any]:
        """
        Generate summary statistics across all schemas.

        Returns:
            Dictionary with summary statistics
        """
        total_objects = 0
        objects_by_type: Dict[str, int] = {}

        for schema_data in self.schemas.values():
            for obj_type, count in schema_data['objects_extracted'].items():
                total_objects += count
                if obj_type not in objects_by_type:
                    objects_by_type[obj_type] = 0
                objects_by_type[obj_type] += count

        return {
            'total_schemas': len(self.schemas),
            'total_objects': total_objects,
            'objects_by_type': objects_by_type
        }

    def write_inventory(self, filename: str = 'lakebridge_inventory.json') -> str:
        """
        Write the inventory to a JSON file.

        Args:
            filename: Name of the inventory file

        Returns:
            Path to the written inventory file
        """
        inventory = {
            'extraction_date': self.extraction_date,
            'source_database': self.source_database,
            'schemas': self.schemas,
            'summary': self.generate_summary()
        }

        output_path = self.output_dir / filename
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(inventory, f, indent=2, default=str)

        return str(output_path)

    def print_summary(self) -> None:
        """Print a summary of the extraction to stdout."""
        summary = self.generate_summary()

        print("\n" + "=" * 60)
        print("EXTRACTION SUMMARY")
        print("=" * 60)
        print(f"Extraction Date: {self.extraction_date}")
        print(f"Source Database: {self.source_database}")
        print(f"Schemas Processed: {summary['total_schemas']}")
        print(f"Total Objects Extracted: {summary['total_objects']}")
        print()

        if summary['objects_by_type']:
            print("Objects by Type:")
            for obj_type, count in sorted(summary['objects_by_type'].items()):
                print(f"  {obj_type}: {count}")

        # Print any errors
        total_errors = 0
        for schema, data in self.schemas.items():
            if data['errors']:
                total_errors += len(data['errors'])

        if total_errors > 0:
            print(f"\nWarnings/Errors: {total_errors}")
            for schema, data in self.schemas.items():
                for error in data['errors'][:5]:  # Show first 5 errors
                    print(f"  [{schema}] {error['object_type']}.{error['object_name']}: {error['error']}")
                if len(data['errors']) > 5:
                    print(f"  ... and {len(data['errors']) - 5} more")

        print("=" * 60)


def create_inventory_from_directory(output_dir: str) -> Dict[str, Any]:
    """
    Create an inventory by scanning an existing extraction directory.

    Args:
        output_dir: Base output directory containing extracted files

    Returns:
        Dictionary with inventory data
    """
    base_path = Path(output_dir)
    schemas = {}

    # Scan for schema directories
    for schema_dir in base_path.iterdir():
        if schema_dir.is_dir() and not schema_dir.name.startswith('.'):
            schema_name = schema_dir.name.upper()
            objects_extracted = {}

            # Scan for object type directories
            for obj_dir in schema_dir.iterdir():
                if obj_dir.is_dir():
                    sql_files = list(obj_dir.glob('*.sql'))
                    if sql_files:
                        # Convert folder name back to object type
                        obj_type = obj_dir.name.upper().replace('_', ' ')
                        if obj_type == 'MATERIALIZED VIEWS':
                            obj_type = 'MATERIALIZED_VIEW'
                        elif obj_type == 'DB LINKS':
                            obj_type = 'DATABASE LINK'
                        elif obj_type.endswith('S'):
                            obj_type = obj_type[:-1]  # Remove plural 's'

                        objects_extracted[obj_type] = len(sql_files)

            if objects_extracted:
                schemas[schema_name] = {
                    'objects_extracted': objects_extracted,
                    'objects_failed': {},
                    'total_files': sum(objects_extracted.values())
                }

    total_objects = sum(
        s['total_files'] for s in schemas.values()
    )

    objects_by_type: Dict[str, int] = {}
    for schema_data in schemas.values():
        for obj_type, count in schema_data['objects_extracted'].items():
            if obj_type not in objects_by_type:
                objects_by_type[obj_type] = 0
            objects_by_type[obj_type] += count

    return {
        'extraction_date': datetime.now().isoformat(),
        'source_database': 'SCANNED',
        'schemas': schemas,
        'summary': {
            'total_schemas': len(schemas),
            'total_objects': total_objects,
            'objects_by_type': objects_by_type
        }
    }
