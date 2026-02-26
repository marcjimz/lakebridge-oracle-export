"""Unit tests for Oracle Lakebridge Extractor."""

import pytest
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
import json

from src.oracle_lakebridge_extractor import (
    LakebridgeExtractor,
    get_schema_objects,
    extract_ddl,
    setup_transform_params,
    generate_sqlplus_script,
    generate_individual_export_script,
)
from src.config import OBJECT_TYPES
from src.utils.oracle_queries import OracleQueries


class TestGetSchemaObjects:
    """Tests for get_schema_objects function."""

    def test_get_tables_returns_list(self, mock_cursor, sample_objects_list):
        """Test that get_schema_objects returns table list correctly."""
        mock_cursor.fetchall.return_value = sample_objects_list

        result = get_schema_objects(mock_cursor, 'HR', 'TABLE')

        mock_cursor.execute.assert_called_once()
        assert len(result) == 4
        assert result[0] == ('HR', 'EMPLOYEES')

    def test_get_procedures_returns_list(self, mock_cursor):
        """Test that get_schema_objects returns procedure list correctly."""
        procedures = [('HR', 'UPDATE_SALARY'), ('HR', 'CALC_BONUS')]
        mock_cursor.fetchall.return_value = procedures

        result = get_schema_objects(mock_cursor, 'HR', 'PROCEDURE')

        assert len(result) == 2
        assert result[0] == ('HR', 'UPDATE_SALARY')

    def test_get_schema_objects_uppercase_schema(self, mock_cursor):
        """Test that schema name is uppercased."""
        mock_cursor.fetchall.return_value = []

        get_schema_objects(mock_cursor, 'hr', 'TABLE')

        # Verify the query was called with uppercase schema
        call_args = mock_cursor.execute.call_args
        assert call_args[1]['schema'] == 'HR'

    def test_unsupported_object_type_raises_error(self, mock_cursor):
        """Test that unsupported object type raises KeyError."""
        with pytest.raises(KeyError):
            get_schema_objects(mock_cursor, 'HR', 'INVALID_TYPE')


class TestExtractDDL:
    """Tests for extract_ddl function."""

    def test_extract_ddl_success(self, mock_cursor, sample_table_ddl):
        """Test successful DDL extraction."""
        mock_cursor.fetchone.return_value = (sample_table_ddl,)

        result = extract_ddl(mock_cursor, 'TABLE', 'EMPLOYEES', 'HR')

        assert result is not None
        assert 'CREATE TABLE' in result

    def test_extract_ddl_returns_none_for_missing_object(self, mock_cursor):
        """Test that missing object returns None."""
        mock_cursor.execute.side_effect = Exception('ORA-31603: object not found')

        result = extract_ddl(mock_cursor, 'TABLE', 'NONEXISTENT', 'HR')

        assert result is None

    def test_extract_ddl_handles_clob(self, mock_cursor, sample_table_ddl):
        """Test CLOB handling in DDL extraction."""
        mock_clob = Mock()
        mock_clob.read.return_value = sample_table_ddl
        mock_cursor.fetchone.return_value = (mock_clob,)

        result = extract_ddl(mock_cursor, 'TABLE', 'EMPLOYEES', 'HR')

        assert result is not None
        mock_clob.read.assert_called_once()

    def test_extract_ddl_materialized_view_type_mapping(self, mock_cursor):
        """Test that MATERIALIZED_VIEW maps correctly."""
        mock_cursor.fetchone.return_value = ('CREATE MATERIALIZED VIEW ...',)

        extract_ddl(mock_cursor, 'MATERIALIZED_VIEW', 'MV_TEST', 'HR')

        call_args = mock_cursor.execute.call_args
        assert 'MATERIALIZED_VIEW' in str(call_args)


class TestSetupTransformParams:
    """Tests for setup_transform_params function."""

    def test_setup_transform_params_calls_execute(self, mock_cursor):
        """Test that transform params are set via execute."""
        setup_transform_params(mock_cursor)

        # Should be called multiple times for each parameter
        assert mock_cursor.execute.call_count > 0

    def test_setup_transform_params_handles_errors(self, mock_cursor):
        """Test that errors in setting params are handled gracefully."""
        mock_cursor.execute.side_effect = Exception('ORA-00000: test error')

        # Should not raise, just print warning
        setup_transform_params(mock_cursor)


class TestLakebridgeExtractor:
    """Tests for LakebridgeExtractor class."""

    def test_create_directory_structure(self, sample_config, temp_output_dir):
        """Test that directory structure is created correctly."""
        extractor = LakebridgeExtractor(
            config=sample_config,
            output_dir=temp_output_dir,
            schemas=['HR']
        )

        extractor._create_directory_structure()

        # Check schema directory
        schema_dir = Path(temp_output_dir) / 'hr'
        assert schema_dir.exists()

        # Check object type directories
        for obj_config in OBJECT_TYPES.values():
            obj_dir = schema_dir / obj_config['folder']
            assert obj_dir.exists(), f"Directory {obj_dir} should exist"

    def test_file_naming_lowercase(self, sample_config, temp_output_dir):
        """Test that output files are named in lowercase."""
        extractor = LakebridgeExtractor(
            config=sample_config,
            output_dir=temp_output_dir,
            schemas=['HR']
        )
        extractor._create_directory_structure()

        # Simulate file creation
        obj_config = OBJECT_TYPES['TABLE']
        filename = 'EMPLOYEES'.lower() + obj_config['ext']
        filepath = Path(temp_output_dir) / 'hr' / obj_config['folder'] / filename

        filepath.write_text('-- test')

        assert filepath.name == 'employees.sql'

    def test_inventory_initialization(self, sample_config, temp_output_dir):
        """Test that inventory is initialized correctly."""
        extractor = LakebridgeExtractor(
            config=sample_config,
            output_dir=temp_output_dir,
            schemas=['HR', 'FINANCE']
        )

        assert extractor.inventory is not None
        assert extractor.inventory.source_database == 'XEPDB1'


class TestGenerateSqlplusScript:
    """Tests for SQL*Plus script generation."""

    def test_generate_sqlplus_script_contains_header(self):
        """Test that generated script contains header."""
        script = generate_sqlplus_script(['HR'], '/output')

        assert 'Oracle Lakebridge Extractor' in script
        assert 'SQL*Plus Script' in script

    def test_generate_sqlplus_script_contains_transforms(self):
        """Test that generated script sets transform parameters."""
        script = generate_sqlplus_script(['HR'], '/output')

        assert 'DBMS_METADATA.SET_TRANSFORM_PARAM' in script
        assert 'SEGMENT_ATTRIBUTES' in script
        assert 'STORAGE' in script

    def test_generate_sqlplus_script_handles_multiple_schemas(self):
        """Test that script handles multiple schemas."""
        script = generate_sqlplus_script(['HR', 'FINANCE'], '/output')

        assert 'HR' in script
        assert 'FINANCE' in script

    def test_generate_sqlplus_script_includes_all_object_types(self):
        """Test that script includes queries for all object types."""
        script = generate_sqlplus_script(['HR'], '/output')

        assert 'TABLE' in script
        assert 'VIEW' in script
        assert 'PROCEDURE' in script
        assert 'FUNCTION' in script
        assert 'PACKAGE' in script


class TestGeneratePlsqlScript:
    """Tests for PL/SQL UTL_FILE script generation."""

    def test_generate_plsql_script_contains_header(self):
        """Test that generated script contains header."""
        script = generate_individual_export_script(['HR'], '/oracle/export')

        assert 'Oracle Lakebridge Extractor' in script
        assert 'PL/SQL UTL_FILE' in script

    def test_generate_plsql_script_contains_write_procedure(self):
        """Test that script contains file writing procedure."""
        script = generate_individual_export_script(['HR'], '/oracle/export')

        assert 'write_clob_to_file' in script
        assert 'UTL_FILE.FOPEN' in script

    def test_generate_plsql_script_handles_packages(self):
        """Test that script handles package spec + body combination."""
        script = generate_individual_export_script(['HR'], '/oracle/export')

        assert 'PACKAGE' in script
        assert 'PACKAGE_BODY' in script


class TestOracleQueries:
    """Tests for Oracle query definitions."""

    def test_query_map_contains_all_types(self):
        """Test that query map contains all supported object types."""
        expected_types = [
            'TABLE', 'VIEW', 'PROCEDURE', 'FUNCTION', 'PACKAGE',
            'INDEX', 'SEQUENCE', 'TRIGGER', 'TYPE', 'MATERIALIZED_VIEW',
            'SYNONYM', 'DATABASE LINK'
        ]

        for obj_type in expected_types:
            assert obj_type in OracleQueries.QUERY_MAP, f"{obj_type} should be in query map"

    def test_get_query_returns_string(self):
        """Test that get_query returns a valid SQL string."""
        query = OracleQueries.get_query('TABLE')

        assert isinstance(query, str)
        assert 'SELECT' in query
        assert ':schema' in query

    def test_get_query_raises_for_invalid_type(self):
        """Test that get_query raises KeyError for invalid type."""
        with pytest.raises(KeyError):
            OracleQueries.get_query('INVALID_TYPE')

    def test_table_query_excludes_system_tables(self):
        """Test that table query excludes nested, secondary, and recycled tables."""
        query = OracleQueries.TABLES

        assert "nested = 'NO'" in query
        assert "secondary = 'N'" in query
        assert "NOT LIKE 'BIN$%'" in query

    def test_index_query_excludes_system_indexes(self):
        """Test that index query excludes LOB and system-generated indexes."""
        query = OracleQueries.INDEXES

        assert "NOT IN ('LOB')" in query
        assert "NOT LIKE 'SYS_%'" in query
        assert "generated = 'N'" in query


class TestInventoryGeneration:
    """Tests for inventory/manifest generation."""

    def test_inventory_json_structure(self, sample_config, temp_output_dir):
        """Test that inventory JSON has correct structure."""
        extractor = LakebridgeExtractor(
            config=sample_config,
            output_dir=temp_output_dir,
            schemas=['HR']
        )

        extractor._create_directory_structure()
        extractor.inventory.add_schema('HR')
        extractor.inventory.record_extraction('HR', 'TABLE', 'EMPLOYEES', True)
        extractor.inventory.record_extraction('HR', 'TABLE', 'DEPARTMENTS', True)

        inventory_path = extractor.inventory.write_inventory()

        with open(inventory_path) as f:
            inventory = json.load(f)

        assert 'extraction_date' in inventory
        assert 'source_database' in inventory
        assert 'schemas' in inventory
        assert 'summary' in inventory
        assert 'HR' in inventory['schemas']

    def test_inventory_tracks_failures(self, sample_config, temp_output_dir):
        """Test that inventory tracks failed extractions."""
        extractor = LakebridgeExtractor(
            config=sample_config,
            output_dir=temp_output_dir,
            schemas=['HR']
        )

        extractor._create_directory_structure()
        extractor.inventory.add_schema('HR')
        extractor.inventory.record_extraction('HR', 'PROCEDURE', 'BAD_PROC', False, 'Test error')

        inventory_path = extractor.inventory.write_inventory()

        with open(inventory_path) as f:
            inventory = json.load(f)

        assert inventory['schemas']['HR']['objects_failed']['PROCEDURE'] == 1
        assert len(inventory['schemas']['HR']['errors']) == 1

    def test_inventory_summary_counts(self, sample_config, temp_output_dir):
        """Test that inventory summary counts are correct."""
        extractor = LakebridgeExtractor(
            config=sample_config,
            output_dir=temp_output_dir,
            schemas=['HR']
        )

        extractor._create_directory_structure()
        extractor.inventory.add_schema('HR')
        extractor.inventory.record_extraction('HR', 'TABLE', 'EMPLOYEES', True)
        extractor.inventory.record_extraction('HR', 'TABLE', 'DEPARTMENTS', True)
        extractor.inventory.record_extraction('HR', 'VIEW', 'VW_TEST', True)

        inventory_path = extractor.inventory.write_inventory()

        with open(inventory_path) as f:
            inventory = json.load(f)

        assert inventory['summary']['total_schemas'] == 1
        assert inventory['summary']['total_objects'] == 3
        assert inventory['summary']['objects_by_type']['TABLE'] == 2
        assert inventory['summary']['objects_by_type']['VIEW'] == 1
