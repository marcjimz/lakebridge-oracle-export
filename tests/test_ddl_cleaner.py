"""Unit tests for DDL cleaner utilities."""

import pytest

from src.utils.ddl_cleaner import (
    clean_ddl,
    remove_schema_prefix,
    normalize_whitespace,
    clean_package_ddl,
    clean_type_ddl,
)


class TestCleanDdl:
    """Tests for clean_ddl function."""

    def test_remove_pctfree(self):
        """Test removal of PCTFREE clause."""
        ddl = "CREATE TABLE test (id NUMBER) PCTFREE 10"
        result = clean_ddl(ddl)
        assert 'PCTFREE' not in result

    def test_remove_pctused(self):
        """Test removal of PCTUSED clause."""
        ddl = "CREATE TABLE test (id NUMBER) PCTUSED 40"
        result = clean_ddl(ddl)
        assert 'PCTUSED' not in result

    def test_remove_initrans(self):
        """Test removal of INITRANS clause."""
        ddl = "CREATE TABLE test (id NUMBER) INITRANS 2"
        result = clean_ddl(ddl)
        assert 'INITRANS' not in result

    def test_remove_maxtrans(self):
        """Test removal of MAXTRANS clause."""
        ddl = "CREATE TABLE test (id NUMBER) MAXTRANS 255"
        result = clean_ddl(ddl)
        assert 'MAXTRANS' not in result

    def test_remove_compress(self):
        """Test removal of COMPRESS clause."""
        ddl = "CREATE TABLE test (id NUMBER) COMPRESS"
        result = clean_ddl(ddl)
        assert 'COMPRESS' not in result.upper()

    def test_remove_nocompress(self):
        """Test removal of NOCOMPRESS clause."""
        ddl = "CREATE TABLE test (id NUMBER) NOCOMPRESS"
        result = clean_ddl(ddl)
        assert 'NOCOMPRESS' not in result.upper()

    def test_remove_logging(self):
        """Test removal of LOGGING clause."""
        ddl = "CREATE TABLE test (id NUMBER) LOGGING"
        result = clean_ddl(ddl)
        # Should not contain standalone LOGGING (but NOLOGGING check is different)
        assert 'LOGGING' not in result.upper() or 'NOLOGGING' in ddl.upper()

    def test_remove_nologging(self):
        """Test removal of NOLOGGING clause."""
        ddl = "CREATE TABLE test (id NUMBER) NOLOGGING"
        result = clean_ddl(ddl)
        assert 'NOLOGGING' not in result.upper()

    def test_remove_cache(self):
        """Test removal of CACHE clause."""
        ddl = "CREATE TABLE test (id NUMBER) CACHE"
        result = clean_ddl(ddl)
        assert 'CACHE' not in result.upper() or 'NOCACHE' in result.upper()

    def test_remove_nocache(self):
        """Test removal of NOCACHE clause."""
        ddl = "CREATE TABLE test (id NUMBER) NOCACHE"
        result = clean_ddl(ddl)
        assert 'NOCACHE' not in result.upper()

    def test_remove_parallel(self):
        """Test removal of PARALLEL clause."""
        ddl = "CREATE TABLE test (id NUMBER) PARALLEL 4"
        result = clean_ddl(ddl)
        assert 'PARALLEL' not in result.upper()

    def test_remove_noparallel(self):
        """Test removal of NOPARALLEL clause."""
        ddl = "CREATE TABLE test (id NUMBER) NOPARALLEL"
        result = clean_ddl(ddl)
        assert 'NOPARALLEL' not in result.upper()

    def test_remove_tablespace(self):
        """Test removal of TABLESPACE clause."""
        ddl = 'CREATE TABLE test (id NUMBER) TABLESPACE "USERS"'
        result = clean_ddl(ddl)
        assert 'TABLESPACE' not in result.upper()

    def test_remove_storage_clause(self):
        """Test removal of STORAGE clause with parameters."""
        ddl = """CREATE TABLE test (id NUMBER)
            STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1)"""
        result = clean_ddl(ddl)
        assert 'STORAGE' not in result.upper()
        assert 'INITIAL' not in result.upper()

    def test_preserve_column_definitions(self):
        """Test that column definitions are preserved."""
        ddl = """CREATE TABLE test (
            id NUMBER(10) NOT NULL,
            name VARCHAR2(100),
            created_date DATE DEFAULT SYSDATE
        ) TABLESPACE USERS"""
        result = clean_ddl(ddl)

        assert 'id NUMBER(10)' in result
        assert 'name VARCHAR2(100)' in result
        assert 'created_date DATE DEFAULT SYSDATE' in result

    def test_preserve_constraints(self):
        """Test that constraints are preserved."""
        ddl = """CREATE TABLE test (
            id NUMBER PRIMARY KEY,
            name VARCHAR2(100) NOT NULL,
            email VARCHAR2(100) UNIQUE,
            status CHAR(1) CHECK (status IN ('A', 'I'))
        )"""
        result = clean_ddl(ddl)

        assert 'PRIMARY KEY' in result
        assert 'NOT NULL' in result
        assert 'UNIQUE' in result
        assert 'CHECK' in result

    def test_preserve_foreign_keys(self):
        """Test that foreign key constraints are preserved."""
        ddl = """CREATE TABLE test (
            id NUMBER PRIMARY KEY,
            dept_id NUMBER REFERENCES departments(department_id)
        )"""
        result = clean_ddl(ddl)

        assert 'REFERENCES' in result

    def test_adds_semicolon_if_missing(self):
        """Test that semicolon is added if missing."""
        ddl = "CREATE TABLE test (id NUMBER)"
        result = clean_ddl(ddl)
        assert result.endswith(';')

    def test_does_not_duplicate_semicolon(self):
        """Test that semicolon is not duplicated."""
        ddl = "CREATE TABLE test (id NUMBER);"
        result = clean_ddl(ddl)
        assert not result.endswith(';;')

    def test_handles_empty_input(self):
        """Test handling of empty input."""
        assert clean_ddl('') == ''
        assert clean_ddl(None) == ''

    def test_cleanup_whitespace(self):
        """Test that excessive whitespace is cleaned up."""
        ddl = "CREATE TABLE test  (id   NUMBER)\n\n\n\n;"
        result = clean_ddl(ddl)
        assert '\n\n\n\n' not in result

    def test_complex_table_ddl(self, sample_table_ddl):
        """Test cleaning of complex table DDL with many Oracle clauses."""
        result = clean_ddl(sample_table_ddl)

        # Should remove storage-related clauses
        assert 'PCTFREE' not in result
        assert 'PCTUSED' not in result
        assert 'STORAGE(' not in result
        assert 'TABLESPACE' not in result
        assert 'FLASH_CACHE' not in result

        # Should preserve table structure
        assert 'CREATE TABLE' in result
        assert 'EMPLOYEE_ID' in result
        assert 'PRIMARY KEY' in result
        assert 'NOT NULL' in result


class TestRemoveSchemaPrefix:
    """Tests for remove_schema_prefix function."""

    def test_remove_quoted_schema_prefix(self):
        """Test removal of quoted schema prefix."""
        ddl = 'CREATE TABLE "HR"."EMPLOYEES" (id NUMBER)'
        result = remove_schema_prefix(ddl, 'HR')
        assert '"HR".' not in result
        assert 'EMPLOYEES' in result

    def test_remove_unquoted_schema_prefix(self):
        """Test removal of unquoted schema prefix."""
        ddl = 'CREATE TABLE HR.EMPLOYEES (id NUMBER)'
        result = remove_schema_prefix(ddl, 'HR')
        assert 'HR.' not in result
        assert 'EMPLOYEES' in result

    def test_case_insensitive_removal(self):
        """Test case-insensitive schema prefix removal."""
        ddl = 'CREATE TABLE hr.employees (id NUMBER)'
        result = remove_schema_prefix(ddl, 'HR')
        assert 'hr.' not in result

    def test_handles_empty_input(self):
        """Test handling of empty input."""
        assert remove_schema_prefix('', 'HR') == ''
        assert remove_schema_prefix(None, 'HR') is None


class TestNormalizeWhitespace:
    """Tests for normalize_whitespace function."""

    def test_normalize_line_endings(self):
        """Test normalization of line endings."""
        ddl = "line1\r\nline2\rline3\nline4"
        result = normalize_whitespace(ddl)
        assert '\r' not in result
        assert result.count('\n') == 3

    def test_remove_trailing_whitespace(self):
        """Test removal of trailing whitespace."""
        ddl = "line1   \nline2\t\nline3"
        result = normalize_whitespace(ddl)
        for line in result.split('\n'):
            assert line == line.rstrip()

    def test_reduce_excessive_blank_lines(self):
        """Test reduction of excessive blank lines."""
        ddl = "line1\n\n\n\n\nline2"
        result = normalize_whitespace(ddl)
        assert '\n\n\n' not in result

    def test_handles_empty_input(self):
        """Test handling of empty input."""
        assert normalize_whitespace('') == ''


class TestCleanPackageDdl:
    """Tests for clean_package_ddl function."""

    def test_combines_spec_and_body(
        self,
        sample_package_spec_ddl,
        sample_package_body_ddl
    ):
        """Test that spec and body are combined correctly."""
        result = clean_package_ddl(sample_package_spec_ddl, sample_package_body_ddl)

        assert '-- Package Specification' in result
        assert '-- Package Body' in result
        assert 'CREATE OR REPLACE PACKAGE' in result
        assert 'CREATE OR REPLACE PACKAGE BODY' in result

    def test_handles_spec_only(self, sample_package_spec_ddl):
        """Test handling when only spec is provided."""
        result = clean_package_ddl(sample_package_spec_ddl, None)

        assert '-- Package Specification' in result
        assert 'CREATE OR REPLACE PACKAGE' in result
        assert 'PACKAGE BODY' not in result

    def test_handles_body_only(self, sample_package_body_ddl):
        """Test handling when only body is provided."""
        result = clean_package_ddl(None, sample_package_body_ddl)

        assert '-- Package Body' in result
        assert 'PACKAGE BODY' in result


class TestCleanTypeDdl:
    """Tests for clean_type_ddl function."""

    def test_combines_type_and_body(self):
        """Test that type spec and body are combined correctly."""
        type_spec = """CREATE TYPE address_type AS OBJECT (
            line1 VARCHAR2(100),
            MEMBER FUNCTION full_address RETURN VARCHAR2
        )"""
        type_body = """CREATE TYPE BODY address_type AS
            MEMBER FUNCTION full_address RETURN VARCHAR2 IS
            BEGIN RETURN line1; END;
        END"""

        result = clean_type_ddl(type_spec, type_body)

        assert 'CREATE TYPE' in result
        assert '-- Type Body' in result

    def test_handles_type_only(self):
        """Test handling when only type spec is provided."""
        type_spec = "CREATE TYPE my_type AS OBJECT (id NUMBER)"
        result = clean_type_ddl(type_spec, None)

        assert 'CREATE TYPE' in result
        assert 'Type Body' not in result


class TestRealWorldDdlExamples:
    """Tests using real-world DDL examples."""

    def test_healthcare_table(self):
        """Test cleaning healthcare schema table DDL."""
        ddl = """
        CREATE TABLE "HEALTHCARE"."PATIENTS"
        (   "PATIENT_ID" NUMBER(10,0),
            "MRN" VARCHAR2(20 BYTE) NOT NULL ENABLE,
            "FIRST_NAME" VARCHAR2(50 BYTE) NOT NULL ENABLE,
            "DATE_OF_BIRTH" DATE NOT NULL ENABLE,
            CONSTRAINT "PATIENTS_PK" PRIMARY KEY ("PATIENT_ID")
            USING INDEX PCTFREE 10 INITRANS 2 MAXTRANS 255
            TABLESPACE "USERS"  ENABLE
        ) SEGMENT CREATION IMMEDIATE
        PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255
        NOCOMPRESS LOGGING
        STORAGE(INITIAL 65536 NEXT 1048576)
        TABLESPACE "USERS"
        """

        result = clean_ddl(ddl)

        # Verify cleanup
        assert 'PCTFREE' not in result
        assert 'TABLESPACE' not in result
        assert 'STORAGE(' not in result
        assert 'SEGMENT CREATION' not in result

        # Verify preservation
        assert 'PATIENT_ID' in result
        assert 'MRN' in result
        assert 'NOT NULL' in result
        assert 'PRIMARY KEY' in result

    def test_view_with_complex_query(self):
        """Test cleaning view DDL with complex query."""
        ddl = """
        CREATE OR REPLACE VIEW "HEALTHCARE"."VW_PATIENT_SUMMARY" AS
        SELECT
            p.patient_id,
            p.first_name || ' ' || p.last_name AS full_name,
            FLOOR(MONTHS_BETWEEN(SYSDATE, p.date_of_birth) / 12) AS age,
            pr.first_name || ' ' || pr.last_name AS pcp_name
        FROM patients p
        LEFT JOIN providers pr ON p.pcp_provider_id = pr.provider_id
        """

        result = clean_ddl(ddl)

        # Should preserve view definition intact
        assert 'CREATE OR REPLACE VIEW' in result
        assert 'FLOOR(MONTHS_BETWEEN' in result
        assert 'LEFT JOIN' in result

    def test_index_cleanup(self):
        """Test cleaning index DDL."""
        ddl = """
        CREATE INDEX "HR"."IDX_EMP_NAME" ON "HR"."EMPLOYEES" ("LAST_NAME", "FIRST_NAME")
        PCTFREE 10 INITRANS 2 MAXTRANS 255
        STORAGE(INITIAL 65536 NEXT 1048576)
        TABLESPACE "USERS"
        """

        result = clean_ddl(ddl)

        # Should preserve index structure
        assert 'CREATE INDEX' in result
        assert '"LAST_NAME"' in result
        assert '"FIRST_NAME"' in result

        # Should remove storage
        assert 'TABLESPACE' not in result
        assert 'STORAGE' not in result
