#!/usr/bin/env python3
"""
Oracle Lakebridge Extractor

Extract Oracle database objects (DDL and metadata) for Databricks Lakebridge Analyzer.
Supports direct extraction, SQL*Plus script generation, and PL/SQL UTL_FILE export.
"""

import argparse
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
from contextlib import contextmanager

try:
    import oracledb
except ImportError:
    oracledb = None

from .config import OBJECT_TYPES, TRANSFORM_PARAMS
from .utils.ddl_cleaner import clean_ddl, clean_package_ddl, clean_type_ddl
from .utils.oracle_queries import OracleQueries
from .utils.inventory import InventoryWriter


class OracleConnection:
    """Context manager for Oracle database connections."""

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize Oracle connection.

        Args:
            config: Dictionary with connection parameters:
                - host: Database host
                - port: Database port
                - service: Service name or SID
                - user: Username
                - password: Password
                - use_sid: If True, use SID instead of service name
        """
        self.config = config
        self.connection = None

    def __enter__(self):
        """Establish database connection."""
        if oracledb is None:
            raise ImportError(
                "oracledb module not found. Install with: pip install oracledb"
            )

        host = self.config['host']
        port = self.config.get('port', 1521)
        service = self.config['service']
        user = self.config['user']
        password = self.config['password']
        use_sid = self.config.get('use_sid', False)

        if use_sid:
            dsn = oracledb.makedsn(host, port, sid=service)
        else:
            dsn = oracledb.makedsn(host, port, service_name=service)

        self.connection = oracledb.connect(
            user=user,
            password=password,
            dsn=dsn
        )
        return self.connection

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Close database connection."""
        if self.connection:
            self.connection.close()


def setup_transform_params(cursor) -> None:
    """
    Configure DBMS_METADATA transform parameters for clean DDL output.

    Args:
        cursor: Oracle database cursor
    """
    for param, value in TRANSFORM_PARAMS.items():
        if isinstance(value, bool):
            sql_value = 'TRUE' if value else 'FALSE'
        else:
            sql_value = str(value)

        try:
            cursor.execute(f"""
                BEGIN
                    DBMS_METADATA.SET_TRANSFORM_PARAM(
                        DBMS_METADATA.SESSION_TRANSFORM,
                        '{param}',
                        {sql_value}
                    );
                END;
            """)
        except Exception as e:
            # Some parameters may not be available in all Oracle versions
            print(f"Warning: Could not set transform param {param}: {e}")


def get_schema_objects(
    cursor,
    schema: str,
    object_type: str
) -> List[Tuple[str, str]]:
    """
    Get list of objects for a schema and type.

    Args:
        cursor: Oracle database cursor
        schema: Schema name
        object_type: Type of object (TABLE, VIEW, etc.)

    Returns:
        List of (owner, object_name) tuples
    """
    query = OracleQueries.get_query(object_type)
    cursor.execute(query, {'schema': schema.upper()})
    return [(row[0], row[1]) for row in cursor.fetchall()]


def extract_ddl(
    cursor,
    object_type: str,
    object_name: str,
    schema: str
) -> Optional[str]:
    """
    Extract DDL for an object using DBMS_METADATA.GET_DDL.

    Args:
        cursor: Oracle database cursor
        object_type: Type of object
        object_name: Name of the object
        schema: Schema name

    Returns:
        DDL string or None if extraction failed
    """
    # Map our object types to DBMS_METADATA types
    metadata_type = object_type
    if object_type == 'MATERIALIZED_VIEW':
        metadata_type = 'MATERIALIZED_VIEW'
    elif object_type == 'DATABASE LINK':
        metadata_type = 'DB_LINK'

    try:
        cursor.execute("""
            SELECT DBMS_METADATA.GET_DDL(:obj_type, :obj_name, :schema)
            FROM DUAL
        """, {
            'obj_type': metadata_type,
            'obj_name': object_name,
            'schema': schema.upper()
        })
        result = cursor.fetchone()
        if result and result[0]:
            # Handle CLOB
            if hasattr(result[0], 'read'):
                return result[0].read()
            return str(result[0])
    except Exception as e:
        # ORA-31603: object not found, ORA-00942: table or view does not exist
        if 'ORA-31603' in str(e) or 'ORA-00942' in str(e):
            return None
        raise

    return None


def extract_dependent_ddl(
    cursor,
    base_object_type: str,
    base_object_name: str,
    schema: str,
    dependent_type: str
) -> Optional[str]:
    """
    Extract dependent DDL (indexes, constraints) for an object.

    Args:
        cursor: Oracle database cursor
        base_object_type: Type of base object (usually TABLE)
        base_object_name: Name of the base object
        schema: Schema name
        dependent_type: Type of dependent object (INDEX, CONSTRAINT, etc.)

    Returns:
        DDL string or None if no dependent objects
    """
    try:
        cursor.execute("""
            SELECT DBMS_METADATA.GET_DEPENDENT_DDL(:dep_type, :obj_name, :schema)
            FROM DUAL
        """, {
            'dep_type': dependent_type,
            'obj_name': base_object_name,
            'schema': schema.upper()
        })
        result = cursor.fetchone()
        if result and result[0]:
            if hasattr(result[0], 'read'):
                return result[0].read()
            return str(result[0])
    except Exception as e:
        # No dependent objects of this type
        if 'ORA-31608' in str(e):
            return None
        raise

    return None


def extract_package_body(
    cursor,
    package_name: str,
    schema: str
) -> Optional[str]:
    """
    Extract package body DDL separately from package specification.

    Args:
        cursor: Oracle database cursor
        package_name: Name of the package
        schema: Schema name

    Returns:
        Package body DDL or None if no body exists
    """
    # First check if body exists
    cursor.execute(OracleQueries.PACKAGE_BODY_EXISTS, {
        'schema': schema.upper(),
        'package_name': package_name
    })
    result = cursor.fetchone()
    if not result or result[0] == 0:
        return None

    try:
        cursor.execute("""
            SELECT DBMS_METADATA.GET_DDL('PACKAGE_BODY', :pkg_name, :schema)
            FROM DUAL
        """, {
            'pkg_name': package_name,
            'schema': schema.upper()
        })
        result = cursor.fetchone()
        if result and result[0]:
            if hasattr(result[0], 'read'):
                return result[0].read()
            return str(result[0])
    except Exception:
        return None

    return None


def extract_type_body(
    cursor,
    type_name: str,
    schema: str
) -> Optional[str]:
    """
    Extract type body DDL separately from type specification.

    Args:
        cursor: Oracle database cursor
        type_name: Name of the type
        schema: Schema name

    Returns:
        Type body DDL or None if no body exists
    """
    # First check if body exists
    cursor.execute(OracleQueries.TYPE_BODY_EXISTS, {
        'schema': schema.upper(),
        'type_name': type_name
    })
    result = cursor.fetchone()
    if not result or result[0] == 0:
        return None

    try:
        cursor.execute("""
            SELECT DBMS_METADATA.GET_DDL('TYPE_BODY', :type_name, :schema)
            FROM DUAL
        """, {
            'type_name': type_name,
            'schema': schema.upper()
        })
        result = cursor.fetchone()
        if result and result[0]:
            if hasattr(result[0], 'read'):
                return result[0].read()
            return str(result[0])
    except Exception:
        return None

    return None


def get_table_details(cursor, schema: str) -> List[Dict]:
    """
    Get detailed table information for inventory.

    Args:
        cursor: Oracle database cursor
        schema: Schema name

    Returns:
        List of table detail dictionaries
    """
    cursor.execute(OracleQueries.TABLE_DETAILS, {'schema': schema.upper()})
    columns = [desc[0].lower() for desc in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def get_procedure_details(cursor, schema: str) -> List[Dict]:
    """
    Get detailed procedure/function information for inventory.

    Args:
        cursor: Oracle database cursor
        schema: Schema name

    Returns:
        List of procedure detail dictionaries
    """
    cursor.execute(OracleQueries.PROCEDURE_DETAILS, {'schema': schema.upper()})
    columns = [desc[0].lower() for desc in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def get_package_details(cursor, schema: str) -> List[Dict]:
    """
    Get detailed package information for inventory.

    Args:
        cursor: Oracle database cursor
        schema: Schema name

    Returns:
        List of package detail dictionaries
    """
    cursor.execute(OracleQueries.PACKAGE_DETAILS, {'schema': schema.upper()})
    columns = [desc[0].lower() for desc in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def get_source_code_metrics(cursor, schema: str) -> Dict:
    """
    Get source code line count metrics.

    Args:
        cursor: Oracle database cursor
        schema: Schema name

    Returns:
        Dictionary of source metrics by object type and name
    """
    cursor.execute(OracleQueries.SOURCE_CODE_METRICS, {'schema': schema.upper()})
    metrics = {}
    for row in cursor.fetchall():
        obj_type, obj_name, line_count, comment_lines = row
        if obj_type not in metrics:
            metrics[obj_type] = {}
        metrics[obj_type][obj_name] = {
            'line_count': line_count,
            'comment_lines': comment_lines
        }
    return metrics


class LakebridgeExtractor:
    """Main extraction class for Oracle to Lakebridge migration."""

    def __init__(
        self,
        config: Dict[str, Any],
        output_dir: str,
        schemas: List[str],
        verbose: bool = False
    ):
        """
        Initialize the extractor.

        Args:
            config: Database connection configuration
            output_dir: Base output directory for extracted files
            schemas: List of schemas to extract
            verbose: Enable verbose output
        """
        self.config = config
        self.output_dir = Path(output_dir)
        self.schemas = [s.upper() for s in schemas]
        self.verbose = verbose
        self.inventory = InventoryWriter(
            output_dir,
            config.get('service', 'UNKNOWN')
        )

    def _log(self, message: str) -> None:
        """Print message if verbose mode is enabled."""
        if self.verbose:
            print(message)

    def _create_directory_structure(self) -> None:
        """Create output directory structure for all schemas."""
        self.output_dir.mkdir(parents=True, exist_ok=True)

        for schema in self.schemas:
            schema_dir = self.output_dir / schema.lower()
            schema_dir.mkdir(exist_ok=True)

            for obj_config in OBJECT_TYPES.values():
                folder = schema_dir / obj_config['folder']
                folder.mkdir(exist_ok=True)

    def _extract_and_save_object(
        self,
        cursor,
        obj_type: str,
        obj_name: str,
        schema: str,
        obj_config: Dict[str, str]
    ) -> bool:
        """
        Extract and save a single object's DDL.

        Args:
            cursor: Oracle database cursor
            obj_type: Object type
            obj_name: Object name
            schema: Schema name
            obj_config: Configuration for this object type

        Returns:
            True if extraction was successful
        """
        try:
            # Extract main DDL
            ddl = extract_ddl(cursor, obj_type, obj_name, schema)

            if ddl is None:
                self.inventory.record_extraction(
                    schema, obj_type, obj_name, False,
                    "Object not found or no privileges"
                )
                return False

            # Handle packages - need to combine spec and body
            if obj_type == 'PACKAGE':
                body_ddl = extract_package_body(cursor, obj_name, schema)
                ddl = clean_package_ddl(ddl, body_ddl)
            # Handle types - may have a body
            elif obj_type == 'TYPE':
                body_ddl = extract_type_body(cursor, obj_name, schema)
                ddl = clean_type_ddl(ddl, body_ddl)
            else:
                ddl = clean_ddl(ddl)

            # Write to file
            filename = obj_name.lower() + obj_config['ext']
            filepath = self.output_dir / schema.lower() / obj_config['folder'] / filename

            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(ddl)

            self.inventory.record_extraction(schema, obj_type, obj_name, True)
            self._log(f"  Extracted: {obj_type} {schema}.{obj_name}")
            return True

        except Exception as e:
            self.inventory.record_extraction(
                schema, obj_type, obj_name, False, str(e)
            )
            self._log(f"  Failed: {obj_type} {schema}.{obj_name}: {e}")
            return False

    def _extract_schema(self, cursor, schema: str) -> None:
        """
        Extract all objects from a single schema.

        Args:
            cursor: Oracle database cursor
            schema: Schema name
        """
        print(f"\nProcessing schema: {schema}")
        self.inventory.add_schema(schema)

        for obj_type, obj_config in OBJECT_TYPES.items():
            self._log(f"  Extracting {obj_type}s...")

            try:
                objects = get_schema_objects(cursor, schema, obj_type)
                self._log(f"    Found {len(objects)} {obj_type}(s)")

                for owner, obj_name in objects:
                    self._extract_and_save_object(
                        cursor, obj_type, obj_name, schema, obj_config
                    )

            except KeyError:
                # Object type not supported in queries
                self._log(f"    Skipping {obj_type} (not supported)")
            except Exception as e:
                print(f"    Error extracting {obj_type}s: {e}")

        # Collect metadata for inventory
        try:
            table_details = get_table_details(cursor, schema)
            self.inventory.add_table_details(schema, table_details)
        except Exception as e:
            self._log(f"  Could not get table details: {e}")

        try:
            proc_details = get_procedure_details(cursor, schema)
            self.inventory.add_procedure_details(schema, proc_details)
        except Exception as e:
            self._log(f"  Could not get procedure details: {e}")

        try:
            pkg_details = get_package_details(cursor, schema)
            self.inventory.add_package_details(schema, pkg_details)
        except Exception as e:
            self._log(f"  Could not get package details: {e}")

        try:
            metrics = get_source_code_metrics(cursor, schema)
            self.inventory.add_source_metrics(schema, metrics)
        except Exception as e:
            self._log(f"  Could not get source metrics: {e}")

    def extract_all(self) -> None:
        """Extract all objects from all configured schemas."""
        print(f"Starting extraction to: {self.output_dir}")
        print(f"Schemas: {', '.join(self.schemas)}")

        self._create_directory_structure()

        with OracleConnection(self.config) as conn:
            cursor = conn.cursor()

            # Configure DBMS_METADATA
            setup_transform_params(cursor)

            # Extract each schema
            for schema in self.schemas:
                self._extract_schema(cursor, schema)

            cursor.close()

        # Write inventory and print summary
        inventory_path = self.inventory.write_inventory()
        print(f"\nInventory written to: {inventory_path}")
        self.inventory.print_summary()


def generate_sqlplus_script(schemas: List[str], output_dir: str) -> str:
    """
    Generate a SQL*Plus script for environments without Python connectivity.

    Args:
        schemas: List of schemas to extract
        output_dir: Base output directory

    Returns:
        SQL*Plus script content
    """
    script_lines = [
        "-- Oracle Lakebridge Extractor - SQL*Plus Script",
        f"-- Generated: {datetime.now().isoformat()}",
        "-- Usage: sqlplus user/password@database @extract_ddl.sql",
        "",
        "SET ECHO OFF",
        "SET FEEDBACK OFF",
        "SET HEADING OFF",
        "SET LINESIZE 32767",
        "SET LONG 2000000",
        "SET LONGCHUNKSIZE 2000000",
        "SET PAGESIZE 0",
        "SET TRIMSPOOL ON",
        "SET TERMOUT OFF",
        "",
        "-- Configure DBMS_METADATA transforms",
        "BEGIN",
        "    DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'SEGMENT_ATTRIBUTES', FALSE);",
        "    DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'STORAGE', FALSE);",
        "    DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'TABLESPACE', FALSE);",
        "    DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'PRETTY', TRUE);",
        "    DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'SQLTERMINATOR', TRUE);",
        "END;",
        "/",
        "",
    ]

    for schema in schemas:
        schema_upper = schema.upper()
        schema_lower = schema.lower()

        for obj_type, obj_config in OBJECT_TYPES.items():
            folder = obj_config['folder']
            metadata_type = obj_type
            if obj_type == 'MATERIALIZED_VIEW':
                metadata_type = 'MATERIALIZED_VIEW'
            elif obj_type == 'DATABASE LINK':
                metadata_type = 'DB_LINK'

            script_lines.extend([
                f"-- Extract {obj_type}s for {schema_upper}",
                f"PROMPT Extracting {obj_type}s from {schema_upper}...",
            ])

            # Query to get objects
            if obj_type == 'TABLE':
                query = f"""
SELECT table_name FROM all_tables
WHERE owner = '{schema_upper}'
AND nested = 'NO' AND secondary = 'N'
AND table_name NOT LIKE 'BIN$%'"""
            elif obj_type == 'VIEW':
                query = f"SELECT view_name FROM all_views WHERE owner = '{schema_upper}'"
            elif obj_type == 'PROCEDURE':
                query = f"""
SELECT object_name FROM all_procedures
WHERE owner = '{schema_upper}'
AND object_type = 'PROCEDURE'
AND procedure_name IS NULL"""
            elif obj_type == 'FUNCTION':
                query = f"""
SELECT object_name FROM all_procedures
WHERE owner = '{schema_upper}'
AND object_type = 'FUNCTION'
AND procedure_name IS NULL"""
            elif obj_type == 'PACKAGE':
                query = f"""
SELECT object_name FROM all_objects
WHERE owner = '{schema_upper}' AND object_type = 'PACKAGE'"""
            elif obj_type == 'INDEX':
                query = f"""
SELECT index_name FROM all_indexes
WHERE owner = '{schema_upper}'
AND index_type NOT IN ('LOB')
AND index_name NOT LIKE 'SYS_%'
AND generated = 'N'"""
            elif obj_type == 'SEQUENCE':
                query = f"SELECT sequence_name FROM all_sequences WHERE sequence_owner = '{schema_upper}'"
            elif obj_type == 'TRIGGER':
                query = f"SELECT trigger_name FROM all_triggers WHERE owner = '{schema_upper}'"
            elif obj_type == 'TYPE':
                query = f"SELECT type_name FROM all_types WHERE owner = '{schema_upper}'"
            elif obj_type == 'MATERIALIZED_VIEW':
                query = f"SELECT mview_name FROM all_mviews WHERE owner = '{schema_upper}'"
            elif obj_type == 'SYNONYM':
                query = f"SELECT synonym_name FROM all_synonyms WHERE owner = '{schema_upper}'"
            elif obj_type == 'DATABASE LINK':
                query = f"SELECT db_link FROM all_db_links WHERE owner = '{schema_upper}'"
            else:
                continue

            # Generate spool commands using PL/SQL
            script_lines.extend([
                "DECLARE",
                "    v_ddl CLOB;",
                "BEGIN",
                f"    FOR obj IN ({query}) LOOP",
                "        BEGIN",
                f"            v_ddl := DBMS_METADATA.GET_DDL('{metadata_type}', obj.{list(obj_config.values())[0] if obj_type != 'TABLE' else 'table_name'}, '{schema_upper}');",
                f"            -- Note: Manual spool to {output_dir}/{schema_lower}/{folder}/ required",
                "            DBMS_OUTPUT.PUT_LINE(v_ddl);",
                "        EXCEPTION",
                "            WHEN OTHERS THEN NULL;",
                "        END;",
                "    END LOOP;",
                "END;",
                "/",
                "",
            ])

    script_lines.extend([
        "SET TERMOUT ON",
        "SET FEEDBACK ON",
        "PROMPT Extraction complete.",
        "EXIT",
    ])

    return '\n'.join(script_lines)


def generate_individual_export_script(schemas: List[str], output_dir: str) -> str:
    """
    Generate a PL/SQL script that exports each object to individual files using UTL_FILE.

    Args:
        schemas: List of schemas to extract
        output_dir: Oracle directory path for UTL_FILE

    Returns:
        PL/SQL script content
    """
    script_lines = [
        "-- Oracle Lakebridge Extractor - PL/SQL UTL_FILE Export Script",
        f"-- Generated: {datetime.now().isoformat()}",
        "-- Prerequisites:",
        f"--   1. Create Oracle directory: CREATE DIRECTORY LAKEBRIDGE_DIR AS '{output_dir}';",
        "--   2. Grant access: GRANT READ, WRITE ON DIRECTORY LAKEBRIDGE_DIR TO <user>;",
        "--",
        "-- Usage: Execute this script as a user with UTL_FILE and DBMS_METADATA privileges",
        "",
        "SET SERVEROUTPUT ON SIZE UNLIMITED",
        "",
        "DECLARE",
        "    v_file UTL_FILE.FILE_TYPE;",
        "    v_ddl CLOB;",
        "    v_buffer VARCHAR2(32767);",
        "    v_amount INTEGER := 32767;",
        "    v_offset INTEGER := 1;",
        "    v_clob_len INTEGER;",
        "    v_filename VARCHAR2(255);",
        "    v_dir_name VARCHAR2(30) := 'LAKEBRIDGE_DIR';",
        "",
        "    PROCEDURE write_clob_to_file(p_dir VARCHAR2, p_filename VARCHAR2, p_clob CLOB) IS",
        "        l_file UTL_FILE.FILE_TYPE;",
        "        l_buffer VARCHAR2(32767);",
        "        l_amount INTEGER := 32767;",
        "        l_offset INTEGER := 1;",
        "        l_clob_len INTEGER;",
        "    BEGIN",
        "        l_clob_len := DBMS_LOB.GETLENGTH(p_clob);",
        "        IF l_clob_len > 0 THEN",
        "            l_file := UTL_FILE.FOPEN(p_dir, p_filename, 'w', 32767);",
        "            WHILE l_offset <= l_clob_len LOOP",
        "                DBMS_LOB.READ(p_clob, l_amount, l_offset, l_buffer);",
        "                UTL_FILE.PUT(l_file, l_buffer);",
        "                l_offset := l_offset + l_amount;",
        "            END LOOP;",
        "            UTL_FILE.FCLOSE(l_file);",
        "        END IF;",
        "    EXCEPTION",
        "        WHEN OTHERS THEN",
        "            IF UTL_FILE.IS_OPEN(l_file) THEN",
        "                UTL_FILE.FCLOSE(l_file);",
        "            END IF;",
        "            DBMS_OUTPUT.PUT_LINE('Error writing ' || p_filename || ': ' || SQLERRM);",
        "    END;",
        "",
        "BEGIN",
        "    -- Configure DBMS_METADATA transforms",
        "    DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'SEGMENT_ATTRIBUTES', FALSE);",
        "    DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'STORAGE', FALSE);",
        "    DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'TABLESPACE', FALSE);",
        "    DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'PRETTY', TRUE);",
        "    DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'SQLTERMINATOR', TRUE);",
        "",
    ]

    for schema in schemas:
        schema_upper = schema.upper()
        schema_lower = schema.lower()

        # Tables
        script_lines.extend([
            f"    -- Extract TABLES for {schema_upper}",
            f"    DBMS_OUTPUT.PUT_LINE('Extracting tables from {schema_upper}...');",
            f"    FOR obj IN (SELECT table_name FROM all_tables WHERE owner = '{schema_upper}' AND nested = 'NO' AND secondary = 'N' AND table_name NOT LIKE 'BIN$%') LOOP",
            "        BEGIN",
            f"            v_ddl := DBMS_METADATA.GET_DDL('TABLE', obj.table_name, '{schema_upper}');",
            f"            v_filename := '{schema_lower}_tables_' || LOWER(obj.table_name) || '.sql';",
            "            write_clob_to_file(v_dir_name, v_filename, v_ddl);",
            "            DBMS_OUTPUT.PUT_LINE('  Extracted: ' || obj.table_name);",
            "        EXCEPTION WHEN OTHERS THEN",
            "            DBMS_OUTPUT.PUT_LINE('  Failed: ' || obj.table_name || ' - ' || SQLERRM);",
            "        END;",
            "    END LOOP;",
            "",
        ])

        # Views
        script_lines.extend([
            f"    -- Extract VIEWS for {schema_upper}",
            f"    DBMS_OUTPUT.PUT_LINE('Extracting views from {schema_upper}...');",
            f"    FOR obj IN (SELECT view_name FROM all_views WHERE owner = '{schema_upper}') LOOP",
            "        BEGIN",
            f"            v_ddl := DBMS_METADATA.GET_DDL('VIEW', obj.view_name, '{schema_upper}');",
            f"            v_filename := '{schema_lower}_views_' || LOWER(obj.view_name) || '.sql';",
            "            write_clob_to_file(v_dir_name, v_filename, v_ddl);",
            "            DBMS_OUTPUT.PUT_LINE('  Extracted: ' || obj.view_name);",
            "        EXCEPTION WHEN OTHERS THEN",
            "            DBMS_OUTPUT.PUT_LINE('  Failed: ' || obj.view_name || ' - ' || SQLERRM);",
            "        END;",
            "    END LOOP;",
            "",
        ])

        # Procedures
        script_lines.extend([
            f"    -- Extract PROCEDURES for {schema_upper}",
            f"    DBMS_OUTPUT.PUT_LINE('Extracting procedures from {schema_upper}...');",
            f"    FOR obj IN (SELECT object_name FROM all_procedures WHERE owner = '{schema_upper}' AND object_type = 'PROCEDURE' AND procedure_name IS NULL) LOOP",
            "        BEGIN",
            f"            v_ddl := DBMS_METADATA.GET_DDL('PROCEDURE', obj.object_name, '{schema_upper}');",
            f"            v_filename := '{schema_lower}_procedures_' || LOWER(obj.object_name) || '.sql';",
            "            write_clob_to_file(v_dir_name, v_filename, v_ddl);",
            "            DBMS_OUTPUT.PUT_LINE('  Extracted: ' || obj.object_name);",
            "        EXCEPTION WHEN OTHERS THEN",
            "            DBMS_OUTPUT.PUT_LINE('  Failed: ' || obj.object_name || ' - ' || SQLERRM);",
            "        END;",
            "    END LOOP;",
            "",
        ])

        # Functions
        script_lines.extend([
            f"    -- Extract FUNCTIONS for {schema_upper}",
            f"    DBMS_OUTPUT.PUT_LINE('Extracting functions from {schema_upper}...');",
            f"    FOR obj IN (SELECT object_name FROM all_procedures WHERE owner = '{schema_upper}' AND object_type = 'FUNCTION' AND procedure_name IS NULL) LOOP",
            "        BEGIN",
            f"            v_ddl := DBMS_METADATA.GET_DDL('FUNCTION', obj.object_name, '{schema_upper}');",
            f"            v_filename := '{schema_lower}_functions_' || LOWER(obj.object_name) || '.sql';",
            "            write_clob_to_file(v_dir_name, v_filename, v_ddl);",
            "            DBMS_OUTPUT.PUT_LINE('  Extracted: ' || obj.object_name);",
            "        EXCEPTION WHEN OTHERS THEN",
            "            DBMS_OUTPUT.PUT_LINE('  Failed: ' || obj.object_name || ' - ' || SQLERRM);",
            "        END;",
            "    END LOOP;",
            "",
        ])

        # Packages (spec + body)
        script_lines.extend([
            f"    -- Extract PACKAGES for {schema_upper}",
            f"    DBMS_OUTPUT.PUT_LINE('Extracting packages from {schema_upper}...');",
            f"    FOR obj IN (SELECT object_name FROM all_objects WHERE owner = '{schema_upper}' AND object_type = 'PACKAGE') LOOP",
            "        BEGIN",
            f"            v_ddl := DBMS_METADATA.GET_DDL('PACKAGE', obj.object_name, '{schema_upper}');",
            "            BEGIN",
            f"                v_ddl := v_ddl || CHR(10) || CHR(10) || DBMS_METADATA.GET_DDL('PACKAGE_BODY', obj.object_name, '{schema_upper}');",
            "            EXCEPTION WHEN OTHERS THEN NULL; END;",
            f"            v_filename := '{schema_lower}_packages_' || LOWER(obj.object_name) || '.sql';",
            "            write_clob_to_file(v_dir_name, v_filename, v_ddl);",
            "            DBMS_OUTPUT.PUT_LINE('  Extracted: ' || obj.object_name);",
            "        EXCEPTION WHEN OTHERS THEN",
            "            DBMS_OUTPUT.PUT_LINE('  Failed: ' || obj.object_name || ' - ' || SQLERRM);",
            "        END;",
            "    END LOOP;",
            "",
        ])

        # Triggers
        script_lines.extend([
            f"    -- Extract TRIGGERS for {schema_upper}",
            f"    DBMS_OUTPUT.PUT_LINE('Extracting triggers from {schema_upper}...');",
            f"    FOR obj IN (SELECT trigger_name FROM all_triggers WHERE owner = '{schema_upper}') LOOP",
            "        BEGIN",
            f"            v_ddl := DBMS_METADATA.GET_DDL('TRIGGER', obj.trigger_name, '{schema_upper}');",
            f"            v_filename := '{schema_lower}_triggers_' || LOWER(obj.trigger_name) || '.sql';",
            "            write_clob_to_file(v_dir_name, v_filename, v_ddl);",
            "            DBMS_OUTPUT.PUT_LINE('  Extracted: ' || obj.trigger_name);",
            "        EXCEPTION WHEN OTHERS THEN",
            "            DBMS_OUTPUT.PUT_LINE('  Failed: ' || obj.trigger_name || ' - ' || SQLERRM);",
            "        END;",
            "    END LOOP;",
            "",
        ])

        # Sequences
        script_lines.extend([
            f"    -- Extract SEQUENCES for {schema_upper}",
            f"    DBMS_OUTPUT.PUT_LINE('Extracting sequences from {schema_upper}...');",
            f"    FOR obj IN (SELECT sequence_name FROM all_sequences WHERE sequence_owner = '{schema_upper}') LOOP",
            "        BEGIN",
            f"            v_ddl := DBMS_METADATA.GET_DDL('SEQUENCE', obj.sequence_name, '{schema_upper}');",
            f"            v_filename := '{schema_lower}_sequences_' || LOWER(obj.sequence_name) || '.sql';",
            "            write_clob_to_file(v_dir_name, v_filename, v_ddl);",
            "            DBMS_OUTPUT.PUT_LINE('  Extracted: ' || obj.sequence_name);",
            "        EXCEPTION WHEN OTHERS THEN",
            "            DBMS_OUTPUT.PUT_LINE('  Failed: ' || obj.sequence_name || ' - ' || SQLERRM);",
            "        END;",
            "    END LOOP;",
            "",
        ])

        # Types
        script_lines.extend([
            f"    -- Extract TYPES for {schema_upper}",
            f"    DBMS_OUTPUT.PUT_LINE('Extracting types from {schema_upper}...');",
            f"    FOR obj IN (SELECT type_name FROM all_types WHERE owner = '{schema_upper}') LOOP",
            "        BEGIN",
            f"            v_ddl := DBMS_METADATA.GET_DDL('TYPE', obj.type_name, '{schema_upper}');",
            "            BEGIN",
            f"                v_ddl := v_ddl || CHR(10) || CHR(10) || DBMS_METADATA.GET_DDL('TYPE_BODY', obj.type_name, '{schema_upper}');",
            "            EXCEPTION WHEN OTHERS THEN NULL; END;",
            f"            v_filename := '{schema_lower}_types_' || LOWER(obj.type_name) || '.sql';",
            "            write_clob_to_file(v_dir_name, v_filename, v_ddl);",
            "            DBMS_OUTPUT.PUT_LINE('  Extracted: ' || obj.type_name);",
            "        EXCEPTION WHEN OTHERS THEN",
            "            DBMS_OUTPUT.PUT_LINE('  Failed: ' || obj.type_name || ' - ' || SQLERRM);",
            "        END;",
            "    END LOOP;",
            "",
        ])

    script_lines.extend([
        "    DBMS_OUTPUT.PUT_LINE('Extraction complete.');",
        "END;",
        "/",
    ])

    return '\n'.join(script_lines)


def main():
    """Main entry point for CLI."""
    parser = argparse.ArgumentParser(
        description='Extract Oracle DDL for Databricks Lakebridge Analyzer',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Direct extraction
  python -m src.oracle_lakebridge_extractor \\
      --host oracle.example.com --port 1521 --service ORCL \\
      --user migration_user --password secret \\
      --schemas HR,FINANCE --output /data/lakebridge_staging

  # Generate SQL*Plus script
  python -m src.oracle_lakebridge_extractor \\
      --generate-sqlplus --schemas HR,FINANCE \\
      --output /data/lakebridge_staging

  # Generate PL/SQL UTL_FILE export script
  python -m src.oracle_lakebridge_extractor \\
      --generate-plsql --schemas HR,FINANCE \\
      --output /oracle/export
"""
    )

    # Connection arguments
    conn_group = parser.add_argument_group('Connection Options')
    conn_group.add_argument('--host', help='Oracle host')
    conn_group.add_argument('--port', type=int, default=1521, help='Oracle port (default: 1521)')
    conn_group.add_argument('--service', help='Oracle service name')
    conn_group.add_argument('--sid', help='Oracle SID (use instead of --service)')
    conn_group.add_argument('--user', help='Oracle username')
    conn_group.add_argument('--password', help='Oracle password')

    # Extraction options
    extract_group = parser.add_argument_group('Extraction Options')
    extract_group.add_argument(
        '--schemas', required=True,
        help='Comma-separated list of schemas to extract'
    )
    extract_group.add_argument(
        '--output', required=True,
        help='Output directory for extracted files'
    )
    extract_group.add_argument(
        '--verbose', '-v', action='store_true',
        help='Enable verbose output'
    )

    # Script generation modes
    script_group = parser.add_argument_group('Script Generation')
    script_group.add_argument(
        '--generate-sqlplus', action='store_true',
        help='Generate SQL*Plus extraction script instead of direct extraction'
    )
    script_group.add_argument(
        '--generate-plsql', action='store_true',
        help='Generate PL/SQL UTL_FILE export script'
    )

    args = parser.parse_args()

    schemas = [s.strip() for s in args.schemas.split(',')]

    # Generate SQL*Plus script mode
    if args.generate_sqlplus:
        script = generate_sqlplus_script(schemas, args.output)
        output_file = Path(args.output) / 'extract_ddl.sql'
        output_file.parent.mkdir(parents=True, exist_ok=True)
        with open(output_file, 'w') as f:
            f.write(script)
        print(f"SQL*Plus script generated: {output_file}")
        return

    # Generate PL/SQL script mode
    if args.generate_plsql:
        script = generate_individual_export_script(schemas, args.output)
        output_file = Path(args.output) / 'extract_ddl_plsql.sql'
        output_file.parent.mkdir(parents=True, exist_ok=True)
        with open(output_file, 'w') as f:
            f.write(script)
        print(f"PL/SQL script generated: {output_file}")
        return

    # Direct extraction mode - validate connection parameters
    if not all([args.host, args.user, args.password]):
        parser.error("Direct extraction requires --host, --user, and --password")

    if not args.service and not args.sid:
        parser.error("Direct extraction requires --service or --sid")

    config = {
        'host': args.host,
        'port': args.port,
        'service': args.service or args.sid,
        'user': args.user,
        'password': args.password,
        'use_sid': bool(args.sid),
    }

    extractor = LakebridgeExtractor(
        config=config,
        output_dir=args.output,
        schemas=schemas,
        verbose=args.verbose
    )
    extractor.extract_all()


if __name__ == '__main__':
    main()
